/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ibverbs-specific device selection: pairs a [`MemoryLocation`] with the
//! RDMA NIC(s) that have the best PCIe path to it.

use std::sync::LazyLock;
use std::sync::OnceLock;

use anyhow::Context;
use anyhow::Result;
use dashmap::DashMap;

use super::device::IbvDevice;
use super::device::IbvDeviceImpl;
use super::mlx_device::MlxDevice;
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

/// Number of CUDA devices visible to this process (0 if CUDA is
/// unavailable or can't be initialized).
pub(crate) fn cuda_device_count() -> usize {
    // SAFETY: FFI to the CUDA driver. `cuInit` must precede any other
    // driver call, and each call writes only through its out-pointer; a
    // non-success status is treated as "no devices".
    unsafe {
        if rdmaxcel_sys::rdmaxcel_cuInit(0) != rdmaxcel_sys::CUDA_SUCCESS {
            return 0;
        }
        let mut count: i32 = 0;
        if rdmaxcel_sys::rdmaxcel_cuDeviceGetCount(&mut count) != rdmaxcel_sys::CUDA_SUCCESS {
            return 0;
        }
        count.max(0) as usize
    }
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

/// Process-wide CUDA ordinal → optimal Mellanox NIC map, computed once on
/// first use. CPU-only workloads pay no initialization cost.
pub fn get_cuda_device_to_ibv_device() -> &'static Vec<Option<IbvDeviceInfo>> {
    static CUDA_DEVICE_TO_IBV: OnceLock<Vec<Option<IbvDeviceInfo>>> = OnceLock::new();
    CUDA_DEVICE_TO_IBV.get_or_init(|| {
        (0..cuda_device_count())
            .map(|ordinal| {
                select_optimal_ibv_devices::<MlxDevice>(MemoryLocation::Gpu(Some(ordinal as u32)))
                    .into_iter()
                    .next()
            })
            .collect()
    })
}
