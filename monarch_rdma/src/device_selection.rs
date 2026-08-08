/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! PCI topology parsing and device discovery utilities for RDMA device selection.
//!
//! ibverbs-specific selection logic lives in [`crate::backend::ibverbs::device_selection`].

use std::fmt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::LazyLock;

use anyhow::Error;
use anyhow::Result;
use dashmap::DashSet;
use rdmaxcel_sys::CUresult;

fn cuda_error_string(rc: CUresult) -> String {
    // The lookup below goes through the same driver wrapper as every other call,
    // so it cannot name the one code that means there is no driver to ask: it
    // would fail the same way and leave `s` null.
    if rc == rdmaxcel_sys::CUDA_ERROR_NOT_INITIALIZED {
        return "CUDA_ERROR_NOT_INITIALIZED".to_owned();
    }
    let mut s: *const std::os::raw::c_char = std::ptr::null();
    // SAFETY: `&mut s` is a valid, properly aligned, writable pointer
    // to a `const char*`, valid for the duration of the call.
    unsafe { rdmaxcel_sys::rdmaxcel_cuGetErrorString(rc, &mut s) };
    if s.is_null() {
        format!("unknown error code ({rc})")
    } else {
        // SAFETY: `s` is non-null (checked above) and points to a
        // null-terminated string with static lifetime, as guaranteed
        // by `cuGetErrorString`.
        unsafe { std::ffi::CStr::from_ptr(s) }
            .to_string_lossy()
            .into_owned()
    }
}

/// Number of CUDA devices visible to this process.
///
/// Never loads libcuda and never calls `cuInit`. The `rdmaxcel_cu*` wrappers
/// adopt an already-resident driver via `dlopen(RTLD_NOLOAD)` and otherwise
/// report `CUDA_ERROR_NOT_INITIALIZED`, so a process that has not touched CUDA
/// pays only a failed symbol lookup and never gains a CUDA context -- and gets
/// an error here.
pub fn cuda_device_count() -> Result<i32> {
    let mut count: i32 = 0;
    // SAFETY: FFI writes one `i32` through the out-pointer and has no other
    // effect; on a non-success status `count` is left unread.
    let rc = unsafe { rdmaxcel_sys::rdmaxcel_cuDeviceGetCount(&mut count) };
    anyhow::ensure!(
        rc == rdmaxcel_sys::CUDA_SUCCESS,
        "cuDeviceGetCount failed: {}",
        cuda_error_string(rc),
    );
    Ok(count)
}

/// One `cuDeviceGetAttribute` query on `device`.
fn cuda_device_attribute(attr: rdmaxcel_sys::CUdevice_attribute, device: i32) -> Result<i32> {
    let mut value: i32 = 0;
    // SAFETY: FFI writes one `i32` through the out-pointer and has no other effect.
    let rc = unsafe { rdmaxcel_sys::rdmaxcel_cuDeviceGetAttribute(&mut value, attr, device) };
    anyhow::ensure!(
        rc == rdmaxcel_sys::CUDA_SUCCESS,
        "cuDeviceGetAttribute({attr}) failed on CUDA device {device}: {}",
        cuda_error_string(rc),
    );
    Ok(value)
}

/// PCI address of the CUDA device with runtime ordinal `ordinal`, from the CUDA
/// driver.
///
/// `ordinal` is a *runtime* ordinal: the numbering CUDA exposes after applying
/// `CUDA_VISIBLE_DEVICES`, as reported by `CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL`
/// or `torch.cuda.current_device()`. Asking the driver rather than
/// reconstructing that numbering from `/proc/driver/nvidia` keeps it correct for
/// every `CUDA_VISIBLE_DEVICES` form (indices, `GPU-<uuid>`, `MIG-<uuid>`), for
/// any `CUDA_DEVICE_ORDER`, and inside a container exposing a GPU subset.
///
/// Errors when the CUDA driver is not initialized in this process, or when
/// `ordinal` is not visible.
///
/// Backend-agnostic: under ROCm rdmaxcel's wrappers resolve to `hipDeviceGet` /
/// `hipGetDeviceCount` / `hipDeviceGetAttribute`, and `rocm_compat` aliases the
/// `CU_DEVICE_ATTRIBUTE_PCI_*` constants to their HIP equivalents.
/// TODO(slurye): validate that this actually works on ROCm.
pub fn cuda_pci_address(ordinal: u32) -> Result<PCIAddress> {
    let count = cuda_device_count()?;
    anyhow::ensure!(
        i64::from(ordinal) < i64::from(count),
        "CUDA device {ordinal} is not visible to this process ({count} visible)"
    );

    let mut device: rdmaxcel_sys::CUdevice = 0;
    // SAFETY: FFI writes one `CUdevice` through the out-pointer; `ordinal` is in
    // range per the check above.
    let rc = unsafe { rdmaxcel_sys::rdmaxcel_cuDeviceGet(&mut device, ordinal as i32) };
    anyhow::ensure!(
        rc == rdmaxcel_sys::CUDA_SUCCESS,
        "cuDeviceGet failed for CUDA device {ordinal}: {}",
        cuda_error_string(rc),
    );

    let domain = cuda_device_attribute(rdmaxcel_sys::CU_DEVICE_ATTRIBUTE_PCI_DOMAIN_ID, device)?;
    let bus = cuda_device_attribute(rdmaxcel_sys::CU_DEVICE_ATTRIBUTE_PCI_BUS_ID, device)?;
    let slot = cuda_device_attribute(rdmaxcel_sys::CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID, device)?;
    Ok(PCIAddress {
        domain: u16::try_from(domain)?,
        bus: u8::try_from(bus)?,
        device: u8::try_from(slot)?,
        // Assume GPUs are always PCI function 0
        function: 0,
    })
}

/// A PCI address, e.g. `0000:07:00.0`, as found under
/// `/sys/bus/pci/devices`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PCIAddress {
    pub domain: u16,
    pub bus: u8,
    pub device: u8,
    pub function: u8,
}

impl PCIAddress {
    /// Parse a `dddd:bb:dd.f` lowercase-hex PCI address.
    pub fn parse(s: &str) -> Option<Self> {
        let (domain, rest) = s.split_once(':')?;
        let (bus, rest) = rest.split_once(':')?;
        let (device, function) = rest.split_once('.')?;
        Some(Self {
            domain: u16::from_str_radix(domain, 16).ok()?,
            bus: u8::from_str_radix(bus, 16).ok()?,
            device: u8::from_str_radix(device, 16).ok()?,
            function: u8::from_str_radix(function, 16).ok()?,
        })
    }

    /// This device's sysfs directory, `/sys/bus/pci/devices/<bdf>`.
    pub fn sysfs_path(&self) -> PathBuf {
        Path::new("/sys/bus/pci/devices").join(self.to_string())
    }
}

impl fmt::Display for PCIAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:04x}:{:02x}:{:02x}.{:x}",
            self.domain, self.bus, self.device, self.function
        )
    }
}

/// A source of memory for a transfer. A `None` index means "any device of
/// this kind": the location is then ranked against the best of all CPU
/// nodes or all GPUs.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize
)]
pub enum MemoryLocation {
    /// CPU memory on the given NUMA node, or any CPU node if `None`.
    Cpu(Option<u32>),
    /// GPU memory on the given CUDA device ordinal, or any GPU if `None`.
    Gpu(Option<u32>),
}

/// Locality of a path between two PCI endpoints, ordered best to worst.
/// A path's type is its worst (least local) segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PathType {
    /// Within a single PCIe switch.
    Pix,
    /// Across multiple PCIe switches under one host bridge.
    Pxb,
    /// Up through the CPU host bridge, within one NUMA node.
    Phb,
    /// Across the inter-socket / cross-NUMA interconnect.
    Sys,
    /// No path between the endpoints.
    Dis,
}

/// A classified path between two PCI endpoints: its [`PathType`] and the
/// bottleneck (minimum) PCIe link bandwidth along it, in MB/s.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PciPath {
    pub path_type: PathType,
    pub bottleneck_mbytes_per_sec: u32,
}

impl PciPath {
    /// Whether this path is preferable to `other`: more local (lower
    /// [`PathType`]) wins, and among equally-local paths the higher
    /// bottleneck bandwidth wins.
    pub fn is_better_than(&self, other: &PciPath) -> bool {
        (self.path_type, other.bottleneck_mbytes_per_sec)
            < (other.path_type, self.bottleneck_mbytes_per_sec)
    }
}

/// The [`PathType`] and bottleneck bandwidth of the PCIe path between two
/// PCI endpoints.
///
/// Walks each endpoint's sysfs ancestor chain toward the root complex,
/// finds their lowest common ancestor, and takes the minimum link
/// bandwidth along the way. When the endpoints share no PCIe ancestor the
/// path runs through the CPU: [`PathType::Phb`] when both sit on the same
/// NUMA node, [`PathType::Sys`] otherwise — including when either
/// endpoint's affinity is unknown.
pub fn pci_path(a: &PCIAddress, b: &PCIAddress) -> PciPath {
    classify(
        &ancestor_chain(a),
        numa_node(a),
        &ancestor_chain(b),
        numa_node(b),
    )
}

/// Path from CPU memory to the device at `addr`. With a specific NUMA
/// node, the device is [`PathType::Phb`] when it sits on that node and
/// [`PathType::Sys`] otherwise; with `None` (any CPU) it is judged against
/// its own node and so is always [`PathType::Phb`]. Bandwidth is the
/// device's PCIe chain bottleneck.
pub fn cpu_path(addr: &PCIAddress, numa: Option<u32>) -> PciPath {
    let bottleneck_mbytes_per_sec = min_link_mbytes_per_sec(&ancestor_chain(addr));
    let path_type = match numa {
        Some(node) if numa_node(addr) != Some(node) => PathType::Sys,
        _ => PathType::Phb,
    };
    PciPath {
        path_type,
        bottleneck_mbytes_per_sec,
    }
}

/// One node in a device's PCIe ancestor chain: its resolved sysfs path
/// and the bandwidth (MB/s) of the PCIe link immediately upstream of it.
struct PciHop {
    sysfs: PathBuf,
    link_mbytes_per_sec: u32,
}

/// Classify the path between two ancestor chains (device → root complex).
/// Pure over its inputs, so it is unit-tested without touching sysfs.
fn classify(a: &[PciHop], numa_a: Option<u32>, b: &[PciHop], numa_b: Option<u32>) -> PciPath {
    if let Some((ia, ib)) = common_ancestor(a, b) {
        // The path traverses the links upstream of each hop below the common
        // ancestor (`a[..ia]` then `b[..ib]`). The ancestor's own upstream
        // link runs further up the tree and is not part of the path, so it is
        // excluded.
        let bottleneck_mbytes_per_sec = a[..ia]
            .iter()
            .chain(&b[..ib])
            .map(|h| h.link_mbytes_per_sec)
            .min()
            .unwrap_or(0);
        // A PCIe switch spans two sysfs hops (its downstream and upstream
        // ports), so meeting within two hops of the common ancestor means
        // both devices sit under one switch; farther means multiple.
        let single_switch = ia <= 2 && ib <= 2;
        let path_type = if single_switch {
            PathType::Pix
        } else {
            PathType::Pxb
        };
        return PciPath {
            path_type,
            bottleneck_mbytes_per_sec,
        };
    }
    // No shared PCIe ancestor: the path runs through the CPU. An unknown
    // NUMA node cannot be proven same-node, so it takes the worst reachable
    // class instead of being reported as unreachable.
    let bottleneck_mbytes_per_sec = min_link_mbytes_per_sec(a).min(min_link_mbytes_per_sec(b));
    let path_type = match (numa_a, numa_b) {
        (Some(x), Some(y)) if x == y => PathType::Phb,
        _ => PathType::Sys,
    };
    PciPath {
        path_type,
        bottleneck_mbytes_per_sec,
    }
}

/// Indices of the deepest sysfs path common to both chains, scanning each
/// from its device end.
fn common_ancestor(a: &[PciHop], b: &[PciHop]) -> Option<(usize, usize)> {
    a.iter().enumerate().find_map(|(ia, na)| {
        b.iter()
            .position(|nb| nb.sysfs == na.sysfs)
            .map(|ib| (ia, ib))
    })
}

/// Minimum upstream link bandwidth (MB/s) across `hops`. Per-hop bandwidths
/// are never 0 — unreadable link attributes fall back to a default — so this
/// is 0 only for an empty `hops`, meaning a device whose sysfs ancestor chain
/// could not be resolved at all.
fn min_link_mbytes_per_sec(hops: &[PciHop]) -> u32 {
    hops.iter()
        .map(|h| h.link_mbytes_per_sec)
        .min()
        .unwrap_or(0)
}

/// Walk `addr`'s PCIe ancestor chain from the device up toward the root
/// complex, resolving sysfs symlinks. Empty if the device's sysfs entry
/// can't be resolved.
fn ancestor_chain(addr: &PCIAddress) -> Vec<PciHop> {
    let mut chain = Vec::new();
    let mut current = match fs::canonicalize(addr.sysfs_path()) {
        Ok(p) => p,
        Err(_) => return chain,
    };
    loop {
        let link_mbytes_per_sec = link_bandwidth_mbytes_per_sec(&current);
        let parent = current.parent().map(Path::to_path_buf);
        chain.push(PciHop {
            sysfs: current,
            link_mbytes_per_sec,
        });
        match parent {
            Some(parent) if is_pci_bdf(&parent) => current = parent,
            _ => break,
        }
    }
    chain
}

/// Whether `path`'s final component parses as a PCI address
/// (`dddd:bb:dd.f`), e.g. `0000:00:01.0`. The root complex (`pci0000:00`)
/// does not, which stops the ancestor walk there.
fn is_pci_bdf(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| PCIAddress::parse(n).is_some())
}

/// NUMA node of the device at `addr`, from `<sysfs>/numa_node`. A `-1`
/// ("unknown") maps to `None`.
fn numa_node(addr: &PCIAddress) -> Option<u32> {
    let raw = fs::read_to_string(addr.sysfs_path().join("numa_node")).ok()?;
    u32::try_from(raw.trim().parse::<i32>().ok()?).ok()
}

/// Per-lane PCIe rate (Mbit/s) assumed when `max_link_speed` is missing or
/// unrecognized: Gen3, mirroring NCCL's `kvDictPciGen` fallback
/// (graph/topo.cc).
const DEFAULT_SPEED_MBITS_PER_LANE: u32 = 6000;

/// Lane count assumed when `max_link_width` is missing or unparseable,
/// mirroring NCCL's `if (width == 0) width = 16` (graph/topo.cc).
const DEFAULT_LINK_WIDTH: u32 = 16;

/// Bandwidth (MB/s) of the PCIe link immediately upstream of the device at
/// `sysfs`, from its own `max_link_speed` / `max_link_width`.
///
/// Missing or unparseable attributes fall back to Gen3 x16 rather than to 0.
/// Link bandwidths are combined with `min` along a path, so a 0 here would
/// erase every real measurement on that path and collapse unrelated
/// candidates into a spurious tie.
fn link_bandwidth_mbytes_per_sec(sysfs: &Path) -> u32 {
    let speed = read_speed_mbits_per_lane(sysfs);
    let width = read_link_width(sysfs);
    // `speed` is effective megabits/s per lane (PCIe line-encoding overhead
    // is already folded into the table), so `speed * width` is the link's
    // total megabits/s; dividing by 8 converts bits to bytes → MB/s.
    speed.saturating_mul(width) / 8
}

/// Report, at most once per `(attr, dir)`, that a PCIe link attribute could not
/// be used, and why. Selection still works — the bandwidth falls back to Gen3
/// x16 — but every ranking that traverses this link then rests on an assumed
/// value, and nothing else makes that visible.
fn warn_unusable_link_attr(attr: &str, dir: &Path, error: &str) {
    static WARNED: LazyLock<DashSet<(String, String)>> = LazyLock::new(DashSet::new);
    // `insert` is true only for the first caller to claim this key, and it takes
    // the shard lock, so exactly one of them warns.
    if WARNED.insert((attr.to_string(), dir.to_string_lossy().into_owned())) {
        tracing::warn!(
            "unusable PCIe {attr} at {}: {error}; assuming Gen3 x16, so RDMA device selection may rank NICs on an assumed bandwidth",
            dir.display()
        );
    }
}

/// PCIe lane count from `<dir>/max_link_width`, or [`DEFAULT_LINK_WIDTH`] if
/// the attribute is missing or does not parse as an integer.
fn read_link_width(dir: &Path) -> u32 {
    let parsed = fs::read_to_string(dir.join("max_link_width"))
        .map_err(Error::from)
        .and_then(|raw| raw.trim().parse::<u32>().map_err(Error::from));
    match parsed {
        Ok(width) => {
            if width == 0 {
                DEFAULT_LINK_WIDTH
            } else {
                width
            }
        }
        Err(error) => {
            warn_unusable_link_attr("max_link_width", dir, &format!("{error:#}"));
            DEFAULT_LINK_WIDTH
        }
    }
}

/// Per-lane PCIe rate (Mbit/s) from `<dir>/max_link_speed`, or
/// [`DEFAULT_SPEED_MBITS_PER_LANE`] if the attribute is missing or names no
/// generation this knows.
fn read_speed_mbits_per_lane(dir: &Path) -> u32 {
    let parsed = fs::read_to_string(dir.join("max_link_speed"))
        .map_err(Error::from)
        .and_then(|raw| pcie_speed_mbits_per_lane(&raw));
    match parsed {
        Ok(speed) => speed,
        Err(error) => {
            warn_unusable_link_attr("max_link_speed", dir, &format!("{error:#}"));
            DEFAULT_SPEED_MBITS_PER_LANE
        }
    }
}

/// Per-lane PCIe bandwidth (Mbit/s) for a `max_link_speed` string such as
/// `"16 GT/s PCIe"`, with line-encoding overhead folded in. `rate * lanes
/// / 8` gives the link's MB/s. The values mirror NCCL's `kvDictPciGen`
/// (graph/topo.cc).
///
/// Errors when the rate names no generation in the table, which is what a
/// generation newer than this list looks like. The caller substitutes a default
/// and warns; returning one from here would make that silent.
fn pcie_speed_mbits_per_lane(speed: &str) -> Result<u32> {
    // Match the leading "<rate> GT/s" token; the kernel may append a
    // trailing "PCIe" and prints either "8" or "8.0" style rates.
    let rate = speed.split_whitespace().next().unwrap_or("");
    match rate {
        "2.5" => Ok(1500),          // Gen1
        "5" | "5.0" => Ok(3000),    // Gen2
        "8" | "8.0" => Ok(6000),    // Gen3
        "16" | "16.0" => Ok(12000), // Gen4
        "32" | "32.0" => Ok(24000), // Gen5
        "64" | "64.0" => Ok(48000), // Gen6
        other => anyhow::bail!("unrecognized PCIe rate {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pci_address_parse_and_display() {
        let addr = PCIAddress::parse("0000:07:00.0").unwrap();
        assert_eq!(
            addr,
            PCIAddress {
                domain: 0,
                bus: 7,
                device: 0,
                function: 0,
            }
        );
        assert_eq!(addr.to_string(), "0000:07:00.0");
        // Hex components round-trip through Display.
        assert_eq!(
            PCIAddress::parse("00ff:1a:1f.7").unwrap().to_string(),
            "00ff:1a:1f.7"
        );
        assert_eq!(PCIAddress::parse("not-an-address"), None);
        assert_eq!(PCIAddress::parse("0000:07:00"), None);
    }

    #[test]
    fn test_cuda_pci_address_rejects_an_absent_ordinal() {
        // Deterministic whether or not another test in this binary has already
        // initialized CUDA: ordinal 4096 is never visible, so this errors either
        // way. What matters is that the message names the cause — an
        // uninitialized driver is the one a caller can act on.
        let error = format!(
            "{:#}",
            cuda_pci_address(4096).expect_err("ordinal 4096 must not resolve")
        );
        assert!(
            error.contains("CUDA_ERROR_NOT_INITIALIZED") || error.contains("not visible"),
            "the error should name its cause, got: {error}"
        );
    }

    #[test]
    fn test_path_type_orders_best_to_worst() {
        assert!(PathType::Pix < PathType::Pxb);
        assert!(PathType::Pxb < PathType::Phb);
        assert!(PathType::Phb < PathType::Sys);
        assert!(PathType::Sys < PathType::Dis);
    }

    #[test]
    fn test_pci_path_is_better_than() {
        let pix_slow = PciPath {
            path_type: PathType::Pix,
            bottleneck_mbytes_per_sec: 1000,
        };
        let pix_fast = PciPath {
            path_type: PathType::Pix,
            bottleneck_mbytes_per_sec: 2000,
        };
        let phb_fast = PciPath {
            path_type: PathType::Phb,
            bottleneck_mbytes_per_sec: 9000,
        };
        // A more local path wins regardless of bandwidth.
        assert!(pix_slow.is_better_than(&phb_fast));
        assert!(!phb_fast.is_better_than(&pix_slow));
        // Equal locality: higher bandwidth wins.
        assert!(pix_fast.is_better_than(&pix_slow));
        assert!(!pix_slow.is_better_than(&pix_fast));
        // A path is not strictly better than itself.
        assert!(!pix_fast.is_better_than(&pix_fast));
    }

    #[test]
    fn test_pcie_speed_mbits_per_lane() {
        let rate = |s: &str| pcie_speed_mbits_per_lane(s).expect("known generation");
        assert_eq!(rate("2.5 GT/s PCIe"), 1500);
        assert_eq!(rate("5 GT/s"), 3000);
        assert_eq!(rate("8.0 GT/s"), 6000);
        assert_eq!(rate("16 GT/s PCIe"), 12000);
        assert_eq!(rate("32 GT/s"), 24000);
        assert_eq!(rate("64 GT/s"), 48000);
        // An unrecognized rate errors so the caller warns, rather than
        // substituting a default here where nothing would report it. "128 GT/s"
        // stands in for a generation newer than the table.
        for unknown in ["garbage", "", "128 GT/s"] {
            assert!(
                pcie_speed_mbits_per_lane(unknown).is_err(),
                "{unknown:?} names no known PCIe generation"
            );
        }
    }

    #[test]
    fn test_missing_link_attrs_fall_back_to_gen3_x16() {
        // A path with no max_link_speed / max_link_width must not report a
        // zero-bandwidth link: link bandwidths are combined with `min`, so a 0
        // would erase every real measurement on the path.
        let missing = Path::new("/nonexistent/pci/device");
        assert_eq!(read_link_width(missing), DEFAULT_LINK_WIDTH);
        assert_eq!(
            read_speed_mbits_per_lane(missing),
            DEFAULT_SPEED_MBITS_PER_LANE
        );
        assert_eq!(
            link_bandwidth_mbytes_per_sec(missing),
            12000,
            "Gen3 (6000 Mbit/s per lane) x16 is 12000 MB/s"
        );
    }

    fn hop(sysfs: &str, link_mbytes_per_sec: u32) -> PciHop {
        PciHop {
            sysfs: PathBuf::from(sysfs),
            link_mbytes_per_sec,
        }
    }

    #[test]
    fn test_classify_pix_single_switch() {
        // Both devices meet at a shared switch within two hops: PIX, with
        // the bottleneck being the slowest link to that switch.
        let a = vec![
            hop("/d/a", 12000),
            hop("/d/sw_a", 12000),
            hop("/d/sw", 8000),
        ];
        let b = vec![hop("/d/b", 12000), hop("/d/sw_b", 6000), hop("/d/sw", 8000)];
        let path = classify(&a, Some(0), &b, Some(0));
        assert_eq!(path.path_type, PathType::Pix);
        assert_eq!(path.bottleneck_mbytes_per_sec, 6000);
    }

    #[test]
    fn test_classify_excludes_common_ancestor_upstream() {
        // The common ancestor's own upstream link runs further up the tree and
        // is not part of the path between `a` and `b`, so a slow link there
        // must not lower the bottleneck.
        let a = vec![hop("/d/a", 12000), hop("/d/sw", 2000)];
        let b = vec![hop("/d/b", 12000), hop("/d/sw", 2000)];
        let path = classify(&a, Some(0), &b, Some(0));
        assert_eq!(path.path_type, PathType::Pix);
        assert_eq!(
            path.bottleneck_mbytes_per_sec, 12000,
            "the common ancestor's 2000 MB/s upstream link is above the path and must be excluded"
        );
    }

    #[test]
    fn test_classify_pxb_multiple_switches() {
        // The chains meet only at the root, several hops up one side: PXB.
        let a = vec![
            hop("/d/a", 12000),
            hop("/d/sw_a1", 12000),
            hop("/d/sw_a2", 12000),
            hop("/d/sw_a3", 12000),
            hop("/d/root", 16000),
        ];
        let b = vec![
            hop("/d/b", 10000),
            hop("/d/sw_b", 10000),
            hop("/d/root", 16000),
        ];
        let path = classify(&a, Some(0), &b, Some(0));
        assert_eq!(path.path_type, PathType::Pxb);
        assert_eq!(path.bottleneck_mbytes_per_sec, 10000);
    }

    #[test]
    fn test_classify_through_cpu() {
        // No shared PCIe ancestor — the path goes through the CPU.
        let a = vec![hop("/d/a", 4000), hop("/d/root_a", 16000)];
        let b = vec![hop("/d/b", 8000), hop("/d/root_b", 16000)];
        // Same NUMA node → PHB; bottleneck is the slowest link overall.
        let phb = classify(&a, Some(0), &b, Some(0));
        assert_eq!(phb.path_type, PathType::Phb);
        assert_eq!(phb.bottleneck_mbytes_per_sec, 4000);
        // Different NUMA nodes → SYS.
        assert_eq!(classify(&a, Some(0), &b, Some(1)).path_type, PathType::Sys);
    }

    #[test]
    fn test_classify_unknown_numa_is_reachable() {
        // An unknown NUMA node can't be proven same-node, so the path is SYS
        // (the worst reachable class) rather than DIS. Reporting DIS would
        // drop the device from selection entirely.
        let a = vec![hop("/d/a", 4000), hop("/d/root_a", 16000)];
        let b = vec![hop("/d/b", 8000), hop("/d/root_b", 16000)];
        assert_eq!(classify(&a, None, &b, Some(0)).path_type, PathType::Sys);
        assert_eq!(classify(&a, Some(0), &b, None).path_type, PathType::Sys);
        assert_eq!(classify(&a, None, &b, None).path_type, PathType::Sys);
    }
}
