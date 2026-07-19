/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ibverbs-specific device selection: pairs a [`MemoryLocation`] with the
//! RDMA NIC(s) that have the best PCIe path to it.

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

    /// Parse a `kind:value` spec: `cpu:<numa>`, `gpu:<ordinal>`, or
    /// `nic:<name>` (e.g. `nic:mlx5_0`). This is the string form the
    /// `rdma_ibverbs_target` runtime config accepts.
    fn from_str(s: &str) -> Result<Self> {
        let (kind, value) = s.split_once(':').with_context(|| {
            format!("ibverbs target {s:?} must be `cpu:<numa>`, `gpu:<ordinal>`, or `nic:<name>`")
        })?;
        match kind {
            "cpu" => Ok(Self::cpu(value.parse().with_context(|| {
                format!("cpu target numa node {value:?} is not a u32")
            })?)),
            "gpu" => Ok(Self::gpu(value.parse().with_context(|| {
                format!("gpu target ordinal {value:?} is not a u32")
            })?)),
            "nic" => {
                anyhow::ensure!(
                    !value.is_empty(),
                    "nic target must name a device, e.g. `nic:mlx5_0`"
                );
                Ok(Self::nic(value))
            }
            other => {
                anyhow::bail!(
                    "unknown ibverbs target kind {other:?}; expected `cpu`, `gpu`, or `nic`"
                )
            }
        }
    }
}

/// The ibverbs device target from the `RDMA_IBVERBS_TARGET` runtime config,
/// parsed from its `cpu:<numa>` / `gpu:<ordinal>` / `nic:<name>` form, or
/// `None` when the attr is empty (leaving device selection to the consumer).
pub fn runtime_ibverbs_target() -> Result<Option<IbvDeviceTarget>> {
    let spec = hyperactor_config::global::get_cloned(crate::config::RDMA_IBVERBS_TARGET);
    if spec.is_empty() {
        return Ok(None);
    }
    Ok(Some(spec.parse()?))
}

/// The effective ibverbs target: a caller-provided `explicit` target wins;
/// otherwise fall back to the runtime `RDMA_IBVERBS_TARGET` config. This is the
/// precedence [`super::manager_actor::IbvManagerActor::new`] applies, so a
/// caller that sets `IbvConfig::target` is never overridden by config.
pub fn effective_ibverbs_target(
    explicit: Option<IbvDeviceTarget>,
) -> Result<Option<IbvDeviceTarget>> {
    match explicit {
        Some(target) => Ok(Some(target)),
        None => runtime_ibverbs_target(),
    }
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
/// port speed. `None` when the path can't be computed — e.g. the NIC's PCI
/// address or a required GPU address can't be resolved.
fn nic_path(nic: &IbvDeviceInfo, location: MemoryLocation) -> Option<PciPath> {
    let nic_addr = get_pci_address(nic).ok()?;
    let base = match location {
        MemoryLocation::Cpu(numa) => cpu_path(&nic_addr, numa),
        MemoryLocation::Gpu(Some(ordinal)) => pci_path(&get_cuda_pci_address(ordinal)?, &nic_addr),
        MemoryLocation::Gpu(None) => best_gpu_path(&nic_addr)?,
    };
    Some(cap_by_port_speed(base, nic))
}

/// The best path from any visible CUDA device to `nic_addr`, or `None` if
/// no GPU's PCI address resolves.
fn best_gpu_path(nic_addr: &PCIAddress) -> Option<PciPath> {
    (0..cuda_device_count())
        .filter_map(|ordinal| get_cuda_pci_address(ordinal as u32))
        .map(|gpu_addr| pci_path(&gpu_addr, nic_addr))
        .reduce(|a, b| if b.is_better_than(&a) { b } else { a })
}

/// Caps `path`'s bottleneck at the NIC's RDMA port speed. A NIC with no
/// ACTIVE port reports a port speed of 0 and is dragged to the worst
/// case, like an unreadable PCIe link.
fn cap_by_port_speed(path: PciPath, nic: &IbvDeviceInfo) -> PciPath {
    PciPath {
        bottleneck_mbytes_per_sec: path
            .bottleneck_mbytes_per_sec
            .min(nic.port_speed_mbytes_per_sec()),
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
            "cpu:0".parse::<IbvDeviceTarget>().unwrap(),
            IbvDeviceTarget::cpu(0),
        );
        assert_eq!(
            "gpu:1".parse::<IbvDeviceTarget>().unwrap(),
            IbvDeviceTarget::gpu(1),
        );
        assert_eq!(
            "nic:mlx5_0".parse::<IbvDeviceTarget>().unwrap(),
            IbvDeviceTarget::nic("mlx5_0"),
        );
    }

    #[test]
    fn rejects_malformed_targets() {
        // No separator, unknown kind, missing or non-numeric value, and an
        // empty NIC name must all fail rather than silently misparse.
        for bad in ["", "cpu", "cpu:", "cpu:x", "gpu:-1", "nic:", "bogus:0"] {
            assert!(
                bad.parse::<IbvDeviceTarget>().is_err(),
                "expected {bad:?} to fail to parse",
            );
        }
    }

    #[test]
    fn runtime_target_reflects_config() {
        let lock = hyperactor_config::global::lock();
        // Empty by default: selection is left to the consumer.
        assert_eq!(runtime_ibverbs_target().unwrap(), None);
        {
            let _guard =
                lock.override_key(crate::config::RDMA_IBVERBS_TARGET, "nic:mlx5_0".to_string());
            assert_eq!(
                runtime_ibverbs_target().unwrap(),
                Some(IbvDeviceTarget::nic("mlx5_0")),
            );
        }
        // Once the override is dropped the target is unset again.
        assert_eq!(runtime_ibverbs_target().unwrap(), None);
    }

    #[test]
    fn explicit_target_beats_runtime_config() {
        let lock = hyperactor_config::global::lock();
        let _guard =
            lock.override_key(crate::config::RDMA_IBVERBS_TARGET, "nic:mlx5_1".to_string());
        // A caller-provided target wins over the runtime config.
        assert_eq!(
            effective_ibverbs_target(Some(IbvDeviceTarget::nic("mlx5_0"))).unwrap(),
            Some(IbvDeviceTarget::nic("mlx5_0")),
        );
        // With no explicit target, the runtime config fills in.
        assert_eq!(
            effective_ibverbs_target(None).unwrap(),
            Some(IbvDeviceTarget::nic("mlx5_1")),
        );
    }
}
