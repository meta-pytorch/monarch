/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ibverbs-specific device selection: pairs a [`MemoryLocation`] with the
//! RDMA NIC(s) that have the best PCIe path to it.

use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::Context;
use anyhow::Result;
use dashmap::DashMap;

use super::device::IbvDevice;
use super::device::IbvDeviceImpl;
use super::primitives::IbvDeviceInfo;
use crate::device_selection::MemoryLocation;
use crate::device_selection::PCIAddress;
use crate::device_selection::PciPath;
use crate::device_selection::cpu_path;
use crate::device_selection::get_cuda_pci_address;
use crate::device_selection::pci_path;

/// What an [`IbvConfig`](super::primitives::IbvConfig) targets: a memory
/// location (whose best NIC is auto-selected) or an explicit NIC by name.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IbvDeviceTarget {
    /// Auto-select the best NIC for a CPU/GPU memory location.
    MemoryLocation(MemoryLocation),
    /// Use the NIC with this exact device name (e.g. `"mlx5_0"`).
    Nic(String),
}

impl IbvDeviceTarget {
    /// Target the best NIC for CPU memory on NUMA node `numa`.
    pub fn cpu(numa: u32) -> Self {
        Self::MemoryLocation(MemoryLocation::Cpu(Some(numa)))
    }

    /// Target the best NIC for GPU memory on CUDA ordinal `ordinal`.
    pub fn gpu(ordinal: u32) -> Self {
        Self::MemoryLocation(MemoryLocation::Gpu(Some(ordinal)))
    }

    /// Target the NIC with the given device name.
    pub fn nic(name: impl Into<String>) -> Self {
        Self::Nic(name.into())
    }
}

impl FromStr for IbvDeviceTarget {
    type Err = anyhow::Error;

    fn from_str(spec: &str) -> Result<Self> {
        let (kind, value) = spec.split_once(':').with_context(|| {
            format!(
                "ibverbs target {spec:?} must be `cpu:<numa>`, `gpu:<ordinal>`, or `nic:<name>`"
            )
        })?;

        let parse_index = |what: &str| -> Result<u32> {
            value
                .parse()
                .with_context(|| format!("{what} {value:?} must be an integer in 0..={}", u32::MAX))
        };

        match kind {
            "cpu" => Ok(Self::cpu(parse_index("cpu target NUMA node")?)),
            "gpu" => Ok(Self::gpu(parse_index("gpu target ordinal")?)),
            "nic" => {
                anyhow::ensure!(
                    !value.is_empty(),
                    "nic target must name a device, for example `nic:mlx5_0`"
                );
                Ok(Self::nic(value))
            }
            other => anyhow::bail!(
                "unknown ibverbs target kind {other:?}; expected `cpu`, `gpu`, or `nic`"
            ),
        }
    }
}

/// Return the configured ibverbs target, or `None` when it is unset.
///
/// Returns an error when the non-empty value is malformed.
pub(crate) fn configured_ibverbs_target() -> Result<Option<IbvDeviceTarget>> {
    let spec = hyperactor_config::global::get_cloned(crate::config::RDMA_IBVERBS_TARGET);
    let spec = spec.trim();
    if spec.is_empty() {
        return Ok(None);
    }
    let target = spec.parse().map_err(|error| {
        anyhow::anyhow!(
            "invalid RDMA_IBVERBS_TARGET (`rdma_ibverbs_target` in Python) value {spec:?}: {error}"
        )
    })?;
    Ok(Some(target))
}

/// The PCI address of an RDMA NIC, resolved from its sysfs device link
/// (`/sys/class/infiniband/<name>/device`).
pub fn get_pci_address(device: &IbvDeviceInfo) -> Result<PCIAddress> {
    let link = format!("/sys/class/infiniband/{}/device", device.name());
    let resolved =
        std::fs::canonicalize(&link).with_context(|| format!("resolving sysfs link {link}"))?;
    resolved
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(PCIAddress::parse)
        .with_context(|| format!("no PCI address in resolved path {resolved:?}"))
}

/// The NIC(s) of backend `I` with the best path to `location`, ranked by
/// [`PciPath::is_better_than`] (most local, then highest port-capped
/// bandwidth) and returning all that tie for best.
///
/// NICs with no ACTIVE port are excluded, so this is empty on a host whose
/// RDMA links are all down.
///
/// A NIC's path bandwidth is the lesser of its PCIe-chain bottleneck and
/// its RDMA port speed. Results are cached per `(backend, location)` since
/// the PCI/NUMA topology is fixed for the process lifetime.
pub fn select_optimal_ibv_devices<I: IbvDeviceImpl>(
    location: MemoryLocation,
) -> Vec<IbvDeviceInfo> {
    static CACHE: LazyLock<DashMap<(&'static str, MemoryLocation), Vec<IbvDeviceInfo>>> =
        LazyLock::new(DashMap::new);
    let key = (I::typename(), location);
    if let Some(cached) = CACHE.get(&key) {
        return cached.value().clone();
    }
    let result = compute_optimal_ibv_devices::<I>(location);
    CACHE.insert(key, result.clone());
    result
}

/// Uncached core of [`select_optimal_ibv_devices`].
fn compute_optimal_ibv_devices<I: IbvDeviceImpl>(location: MemoryLocation) -> Vec<IbvDeviceInfo> {
    let mut best: Option<PciPath> = None;
    let mut devices: Vec<IbvDeviceInfo> = Vec::new();
    for nic in IbvDevice::<I>::list() {
        let Some(path) = nic_path(&nic, location) else {
            continue;
        };
        match best {
            Some(current) if current.is_better_than(&path) => continue,
            Some(current) if path.is_better_than(&current) => devices.clear(),
            _ => {}
        }
        best = Some(path);
        devices.push(nic);
    }
    devices
}

/// The best [`PciPath`] from `location` to `nic`, capped by the NIC's RDMA
/// port speed. `None` when the NIC cannot carry traffic or the path can't be
/// computed: the NIC has no ACTIVE port, or its PCI address or a required GPU
/// address can't be resolved.
fn nic_path(nic: &IbvDeviceInfo, location: MemoryLocation) -> Option<PciPath> {
    // A NIC with no ACTIVE port cannot carry traffic, so drop it from the
    // candidate set. Leaving it in with a zero bandwidth is not equivalent:
    // `is_better_than` compares `PathType` first, so a down NIC that happens to
    // be PCIe-closer would outrank a working but more distant one.
    let port_speed_mbytes_per_sec = NonZeroU32::new(nic.port_speed_mbytes_per_sec())?;
    let nic_addr = get_pci_address(nic).ok()?;
    let base = match location {
        MemoryLocation::Cpu(numa) => cpu_path(&nic_addr, numa),
        MemoryLocation::Gpu(Some(ordinal)) => pci_path(&get_cuda_pci_address(ordinal)?, &nic_addr),
        MemoryLocation::Gpu(None) => best_gpu_path(&nic_addr)?,
    };
    Some(cap_by_port_speed(base, port_speed_mbytes_per_sec))
}

/// The best path from any visible CUDA device to `nic_addr`, or `None` if
/// no GPU's PCI address resolves.
fn best_gpu_path(nic_addr: &PCIAddress) -> Option<PciPath> {
    (0..cuda_device_count())
        .filter_map(|ordinal| get_cuda_pci_address(ordinal as u32))
        .map(|gpu_addr| pci_path(&gpu_addr, nic_addr))
        .reduce(|a, b| if b.is_better_than(&a) { b } else { a })
}

/// Caps `path`'s bottleneck at the NIC's RDMA port speed, so a wide PCIe path
/// to a slow NIC is not ranked as though the NIC could saturate it.
fn cap_by_port_speed(path: PciPath, port_speed_mbytes_per_sec: NonZeroU32) -> PciPath {
    PciPath {
        bottleneck_mbytes_per_sec: path
            .bottleneck_mbytes_per_sec
            .min(port_speed_mbytes_per_sec.get()),
        ..path
    }
}

/// Number of NVIDIA GPUs visible to the kernel driver, counted from the
/// per-GPU directories under `/proc/driver/nvidia/gpus`. This reads kernel
/// driver state, so unlike `cuDeviceGetCount` it needs no CUDA
/// initialization; it returns 0 when the NVIDIA driver is absent (no GPU, or
/// a non-NVIDIA platform).
///
/// TODO(slurye): Generalize this (and `get_cuda_pci_address`) to support AMD.
pub(crate) fn cuda_device_count() -> i32 {
    std::fs::read_dir("/proc/driver/nvidia/gpus")
        .map(|entries| entries.flatten().count() as i32)
        .unwrap_or(0)
}

/// Resolves an [`IbvDeviceTarget`] to a single NIC of backend `I`: the
/// named device for [`IbvDeviceTarget::Nic`], or the best NIC for a memory
/// location. Both arms are scoped to backend `I`, so a name belonging to a
/// different backend resolves to `None`.
pub fn resolve_target<I: IbvDeviceImpl>(target: &IbvDeviceTarget) -> Option<IbvDeviceInfo> {
    match target {
        IbvDeviceTarget::Nic(name) => IbvDevice::<I>::list()
            .into_iter()
            .find(|device| device.name() == name),
        IbvDeviceTarget::MemoryLocation(location) => select_optimal_ibv_devices::<I>(*location)
            .into_iter()
            .next(),
    }
}

/// Process-wide CUDA ordinal → optimal NIC map, computed once per backend and
/// cached. CPU-only workloads pay no initialization cost.
pub fn get_cuda_device_to_ibv_device<I: IbvDeviceImpl>() -> &'static Vec<Option<IbvDeviceInfo>> {
    // A function-local `static` in a generic fn is one cell shared across every
    // `I`, so the cache must be keyed per backend; `I::typename()` selects it.
    // Each backend's map is `Box::leak`ed once so the cache stores (and returns) a
    // `&'static`.
    static CACHE: LazyLock<DashMap<&'static str, &'static Vec<Option<IbvDeviceInfo>>>> =
        LazyLock::new(DashMap::new);
    *CACHE.entry(I::typename()).or_insert_with(|| {
        let result: Vec<Option<IbvDeviceInfo>> = (0..cuda_device_count())
            .map(|ordinal| {
                select_optimal_ibv_devices::<I>(MemoryLocation::Gpu(Some(ordinal as u32)))
                    .into_iter()
                    .next()
            })
            .collect();
        Box::leak(Box::new(result))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_each_target_kind() {
        assert_eq!(
            "cpu:0"
                .parse::<IbvDeviceTarget>()
                .expect("cpu target should parse"),
            IbvDeviceTarget::cpu(0),
        );
        assert_eq!(
            "gpu:1"
                .parse::<IbvDeviceTarget>()
                .expect("gpu target should parse"),
            IbvDeviceTarget::gpu(1),
        );
        assert_eq!(
            "nic:mlx5_0"
                .parse::<IbvDeviceTarget>()
                .expect("NIC target should parse"),
            IbvDeviceTarget::nic("mlx5_0"),
        );
    }

    #[test]
    fn rejects_malformed_targets() {
        for invalid in ["", "cpu", "cpu:", "cpu:x", "gpu:-1", "nic:", "other:0"] {
            assert!(
                invalid.parse::<IbvDeviceTarget>().is_err(),
                "expected {invalid:?} to be rejected",
            );
        }
    }

    #[test]
    fn empty_configured_target_is_unset() {
        let lock = hyperactor_config::global::lock();
        let _guard = lock.override_key(crate::config::RDMA_IBVERBS_TARGET, String::new());
        assert_eq!(
            configured_ibverbs_target().expect("empty target should be valid"),
            None,
        );
    }

    #[test]
    fn parses_configured_target() {
        let lock = hyperactor_config::global::lock();
        let _guard = lock.override_key(
            crate::config::RDMA_IBVERBS_TARGET,
            "  nic:mlx5_1  ".to_string(),
        );

        assert_eq!(
            configured_ibverbs_target().expect("configured target should be valid"),
            Some(IbvDeviceTarget::nic("mlx5_1")),
        );
    }

    #[test]
    fn nic_with_no_active_port_is_not_a_candidate() {
        // `for_test_named` builds a device with no ports, so its port speed is 0.
        // Such a NIC cannot carry traffic and must be rejected outright: capping
        // it to zero bandwidth instead would leave it eligible, and
        // `is_better_than` compares `PathType` before bandwidth.
        let down = IbvDeviceInfo::for_test_named("mlx5_down");
        assert_eq!(down.port_speed_mbytes_per_sec(), 0);
        for location in [
            MemoryLocation::Cpu(None),
            MemoryLocation::Cpu(Some(0)),
            MemoryLocation::Gpu(None),
            MemoryLocation::Gpu(Some(0)),
        ] {
            assert!(
                nic_path(&down, location).is_none(),
                "a NIC with no ACTIVE port must not be a selection candidate",
            );
        }
    }

    #[test]
    fn malformed_configured_target_names_the_setting() {
        let lock = hyperactor_config::global::lock();
        let _guard = lock.override_key(crate::config::RDMA_IBVERBS_TARGET, "mlx5_1".to_string());

        let error = configured_ibverbs_target().expect_err("malformed target should fail");
        assert!(error.to_string().contains("RDMA_IBVERBS_TARGET"));
    }
}
