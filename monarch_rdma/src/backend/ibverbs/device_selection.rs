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
use crate::device_selection::cuda_device_count;
use crate::device_selection::cuda_pci_address;
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
/// A NIC that cannot be ranked -- no ACTIVE port, unresolvable PCI address -- is
/// logged and skipped, so a host with no usable NIC yields an empty list rather
/// than an error. Errors are reserved for a memory location that cannot be
/// resolved at all, which no NIC could have compensated for.
///
/// A NIC's path bandwidth is the lesser of its PCIe-chain bottleneck and its
/// RDMA port speed. Results are cached per `(backend, location)`, empty ones
/// included, since the PCI/NUMA topology is fixed for the process lifetime. A
/// host whose links are all down at the first call therefore keeps an empty
/// answer for the rest of the process.
pub fn select_optimal_ibv_devices<I: IbvDeviceImpl>(
    location: MemoryLocation,
) -> Result<Vec<IbvDeviceInfo>> {
    static CACHE: LazyLock<DashMap<(&'static str, MemoryLocation), Vec<IbvDeviceInfo>>> =
        LazyLock::new(DashMap::new);
    let key = (I::typename(), location);
    if let Some(cached) = CACHE.get(&key) {
        return Ok(cached.value().clone());
    }
    let result = compute_optimal_ibv_devices::<I>(location)?;
    CACHE.insert(key, result.clone());
    Ok(result)
}

/// Uncached core of [`select_optimal_ibv_devices`].
fn compute_optimal_ibv_devices<I: IbvDeviceImpl>(
    location: MemoryLocation,
) -> Result<Vec<IbvDeviceInfo>> {
    // Resolve the GPU side once, before ranking NICs. An unresolvable GPU
    // location is a fact about the request, not a reason to skip each NIC in
    // turn.
    let gpus = match location {
        MemoryLocation::Cpu(_) => Vec::new(),
        MemoryLocation::Gpu(ordinal) => gpu_pci_addresses(ordinal).with_context(|| {
            format!(
                "cannot rank RDMA NICs for {location:?}; if the CUDA driver is not initialized \
                 in this process, initialize it first -- allocate a tensor on the device, call \
                 torch.cuda.init(), or use your method of choice."
            )
        })?,
    };

    let mut best: Option<PciPath> = None;
    let mut devices: Vec<IbvDeviceInfo> = Vec::new();
    for nic in IbvDevice::<I>::list() {
        // A NIC that cannot be ranked is not a failure of the request; drop it
        // and say why, rather than failing the whole ranking or skipping in
        // silence.
        let path = match nic_path(&nic, location, &gpus) {
            Ok(path) => path,
            Err(error) => {
                tracing::warn!("excluding RDMA device {}: {error:#}", nic.name());
                continue;
            }
        };
        match best {
            Some(current) if current.is_better_than(&path) => continue,
            Some(current) if path.is_better_than(&current) => devices.clear(),
            _ => {}
        }
        best = Some(path);
        devices.push(nic);
    }
    Ok(devices)
}

/// The PCI addresses a [`MemoryLocation::Gpu`] names: just the one for a specific
/// ordinal, or every visible device in ordinal order when unspecified.
fn gpu_pci_addresses(ordinal: Option<u32>) -> Result<Vec<PCIAddress>> {
    match ordinal {
        Some(ordinal) => Ok(vec![cuda_pci_address(ordinal)?]),
        None => (0..cuda_device_count()? as u32)
            .map(cuda_pci_address)
            .collect(),
    }
}

/// The best [`PciPath`] from `location` to `nic`, capped by the NIC's RDMA port
/// speed. Errors when `nic` cannot be ranked, naming the reason so the caller
/// can report which device it dropped and why.
///
/// `gpus` holds the already-resolved GPU addresses for a
/// [`MemoryLocation::Gpu`] location, and is empty for a CPU location.
fn nic_path(nic: &IbvDeviceInfo, location: MemoryLocation, gpus: &[PCIAddress]) -> Result<PciPath> {
    // A NIC with no ACTIVE port cannot carry traffic, so drop it from the
    // candidate set. Leaving it in with a zero bandwidth is not equivalent:
    // `is_better_than` compares `PathType` first, so a down NIC that happens to
    // be PCIe-closer would outrank a working but more distant one.
    let port_speed_mbytes_per_sec =
        NonZeroU32::new(nic.port_speed_mbytes_per_sec()).context("no ACTIVE port")?;
    let nic_addr = get_pci_address(nic)?;
    let base = match location {
        MemoryLocation::Cpu(numa) => cpu_path(&nic_addr, numa),
        MemoryLocation::Gpu(_) => gpus
            .iter()
            .map(|gpu_addr| pci_path(gpu_addr, &nic_addr))
            .reduce(|a, b| if b.is_better_than(&a) { b } else { a })
            .context("no visible CUDA device to measure a path from")?,
    };
    Ok(cap_by_port_speed(base, port_speed_mbytes_per_sec))
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

/// Resolves an [`IbvDeviceTarget`] to a single NIC of backend `I`: the
/// named device for [`IbvDeviceTarget::Nic`], or the best NIC for a memory
/// location. Both arms are scoped to backend `I`, so a name belonging to a
/// different backend resolves to `Ok(None)`.
///
/// `Ok(None)` means "no such device"; an error means the target could not be
/// evaluated at all, which for a GPU location includes CUDA not being
/// initialized in this process.
pub fn resolve_target<I: IbvDeviceImpl>(target: &IbvDeviceTarget) -> Result<Option<IbvDeviceInfo>> {
    match target {
        IbvDeviceTarget::Nic(name) => Ok(IbvDevice::<I>::list()
            .into_iter()
            .find(|device| device.name() == name)),
        IbvDeviceTarget::MemoryLocation(location) => {
            Ok(select_optimal_ibv_devices::<I>(*location)?
                .into_iter()
                .next())
        }
    }
}

/// The NICs of backend `I` for each CUDA runtime ordinal, indexed by ordinal.
///
/// Each entry holds every NIC tied for the best path to that ordinal, so it is
/// empty when no NIC could be ranked against it. Errors only when the ordinals
/// themselves cannot be enumerated, which in practice means the CUDA driver is
/// not initialized in this process.
pub fn get_cuda_device_to_ibv_devices<I: IbvDeviceImpl>() -> Result<Vec<Vec<IbvDeviceInfo>>> {
    (0..cuda_device_count()?)
        .map(|ordinal| select_optimal_ibv_devices::<I>(MemoryLocation::Gpu(Some(ordinal as u32))))
        .collect()
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
            let error = format!(
                "{:#}",
                nic_path(&down, location, &[])
                    .expect_err("a NIC with no ACTIVE port must not be a selection candidate")
            );
            assert!(
                error.contains("no ACTIVE port"),
                "the error should name the reason the NIC was excluded, got: {error}"
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
