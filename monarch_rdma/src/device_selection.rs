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

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use regex::Regex;

// ==== PCI TOPOLOGY DISTANCE CONSTANTS ====
//
// These constants define penalty values for cross-NUMA communication in PCI topology:
//
// - CROSS_NUMA_BASE_PENALTY (20.0): Base penalty for cross-NUMA communication.
//   This value is higher than typical intra-NUMA distances (usually 0-8 hops)
//   to ensure same-NUMA devices are always preferred over cross-NUMA devices.
//
// - ADDRESS_PARSE_FAILURE_PENALTY (Inf): Penalty when PCI address parsing fails.
//   Used as fallback when we can't determine bus relationships between devices.
//
// - CROSS_DOMAIN_PENALTY (1000.0): Very high penalty for different PCI domains.
//   Different domains typically indicate completely separate I/O subsystems.
//
// - BUS_DISTANCE_SCALE (0.1): Scaling factor for bus distance in cross-NUMA penalty.
//   Small factor to provide tie-breaking between devices at different bus numbers.

const CROSS_NUMA_BASE_PENALTY: f64 = 20.0;
const ADDRESS_PARSE_FAILURE_PENALTY: f64 = f64::INFINITY;
const CROSS_DOMAIN_PENALTY: f64 = 1000.0;
const BUS_DISTANCE_SCALE: f64 = 0.1;

#[derive(Debug, Clone)]
pub struct PCIDevice {
    pub address: String,
    pub parent: Option<Box<PCIDevice>>,
}

impl PCIDevice {
    pub fn new(address: String) -> Self {
        Self {
            address,
            parent: None,
        }
    }

    pub fn get_path_to_root(&self) -> Vec<String> {
        let mut path = vec![self.address.clone()];
        let mut current = self;

        while let Some(ref parent) = current.parent {
            path.push(parent.address.clone());
            current = parent;
        }

        path
    }
    pub fn get_numa_node(&self) -> Option<i32> {
        let numa_file = format!("/sys/bus/pci/devices/{}/numa_node", self.address);
        std::fs::read_to_string(numa_file).ok()?.trim().parse().ok()
    }

    pub fn distance_to(&self, other: &PCIDevice) -> f64 {
        if self.address == other.address {
            return 0.0;
        }

        // Get paths to root for both devices
        let path1 = self.get_path_to_root();
        let path2 = other.get_path_to_root();

        // Find lowest common ancestor (first common element from the end)
        let mut common_ancestor = None;
        let min_len = path1.len().min(path2.len());

        // Check from the root down to find the deepest common ancestor
        for i in 1..=min_len {
            if path1[path1.len() - i] == path2[path2.len() - i] {
                common_ancestor = Some(&path1[path1.len() - i]);
            } else {
                break;
            }
        }

        if let Some(ancestor) = common_ancestor {
            let hops1 = path1.iter().position(|addr| addr == ancestor).unwrap_or(0);
            let hops2 = path2.iter().position(|addr| addr == ancestor).unwrap_or(0);
            (hops1 + hops2) as f64
        } else {
            self.calculate_cross_numa_distance(other)
        }
    }

    /// Calculate distance between devices on different NUMA domains/root complexes
    /// This handles cases where devices don't share a common PCI ancestor
    fn calculate_cross_numa_distance(&self, other: &PCIDevice) -> f64 {
        let self_parts = self.parse_pci_address();
        let other_parts = other.parse_pci_address();

        match (self_parts, other_parts) {
            (Some((self_domain, self_bus, _, _)), Some((other_domain, other_bus, _, _))) => {
                if self_domain != other_domain {
                    return CROSS_DOMAIN_PENALTY;
                }

                let bus_distance = (self_bus as i32 - other_bus as i32).abs() as f64;
                CROSS_NUMA_BASE_PENALTY + bus_distance * BUS_DISTANCE_SCALE
            }
            _ => ADDRESS_PARSE_FAILURE_PENALTY,
        }
    }

    /// Parse PCI address into components (domain, bus, device, function)
    fn parse_pci_address(&self) -> Option<(u16, u8, u8, u8)> {
        let parts: Vec<&str> = self.address.split(':').collect();
        if parts.len() != 3 {
            return None;
        }

        let domain = u16::from_str_radix(parts[0], 16).ok()?;
        let bus = u8::from_str_radix(parts[1], 16).ok()?;

        let dev_func: Vec<&str> = parts[2].split('.').collect();
        if dev_func.len() != 2 {
            return None;
        }

        let device = u8::from_str_radix(dev_func[0], 16).ok()?;
        let function = u8::from_str_radix(dev_func[1], 16).ok()?;

        Some((domain, bus, device, function))
    }

    /// Find the index of the closest device from a list of candidates
    pub fn find_closest(&self, candidate_devices: &[PCIDevice]) -> Option<usize> {
        if candidate_devices.is_empty() {
            return None;
        }

        let mut closest_idx = 0;
        let mut min_distance = self.distance_to(&candidate_devices[0]);

        for (idx, device) in candidate_devices.iter().enumerate().skip(1) {
            let distance = self.distance_to(device);
            if distance < min_distance {
                min_distance = distance;
                closest_idx = idx;
            }
        }

        Some(closest_idx)
    }
}

/// Resolve all symlinks in a path (equivalent to Python's os.path.realpath)
fn realpath(path: &Path) -> Result<std::path::PathBuf, std::io::Error> {
    let mut current = path.to_path_buf();
    let mut seen = std::collections::HashSet::new();

    loop {
        if seen.contains(&current) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Circular symlink detected",
            ));
        }
        seen.insert(current.clone());

        match fs::read_link(&current) {
            Ok(target) => {
                current = if target.is_absolute() {
                    target
                } else {
                    current.parent().unwrap_or(Path::new("/")).join(target)
                };
            }
            Err(_) => break, // Not a symlink or error reading
        }
    }

    Ok(current)
}

pub fn parse_pci_topology() -> Result<HashMap<String, PCIDevice>, std::io::Error> {
    let mut devices = HashMap::new();
    let mut parent_addresses = HashMap::new();
    let pci_devices_dir = "/sys/bus/pci/devices";

    if !Path::new(pci_devices_dir).exists() {
        return Ok(devices);
    }

    let pci_addr_regex = Regex::new(r"([0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-9])$").unwrap();

    // First pass: create all devices without parent references
    for entry in fs::read_dir(pci_devices_dir)? {
        let entry = entry?;
        let pci_addr = entry.file_name().to_string_lossy().to_string();
        let device_path = entry.path();

        // Find parent device by following the device symlink and extracting PCI address from the path
        let parent_addr = match realpath(&device_path) {
            Ok(real_path) => {
                if let Some(parent_path) = real_path.parent() {
                    let parent_path_str = parent_path.to_string_lossy();
                    pci_addr_regex
                        .captures(&parent_path_str)
                        .map(|captures| captures.get(1).unwrap().as_str().to_string())
                } else {
                    None
                }
            }
            Err(_) => None,
        };

        devices.insert(pci_addr.clone(), PCIDevice::new(pci_addr.clone()));
        if let Some(ref parent) = parent_addr
            && !devices.contains_key(parent)
        {
            devices.insert(parent.clone(), PCIDevice::new(parent.clone()));
        }
        parent_addresses.insert(pci_addr, parent_addr);
    }

    // Second pass: set up parent references recursively
    fn build_parent_chain(
        devices: &mut HashMap<String, PCIDevice>,
        parent_addresses: &HashMap<String, Option<String>>,
        pci_addr: &str,
        visited: &mut std::collections::HashSet<String>,
    ) {
        if visited.contains(pci_addr) {
            return;
        }
        visited.insert(pci_addr.to_string());

        if let Some(Some(parent_addr)) = parent_addresses.get(pci_addr) {
            build_parent_chain(devices, parent_addresses, parent_addr, visited);

            if let Some(parent_device) = devices.get(parent_addr).cloned()
                && let Some(device) = devices.get_mut(pci_addr)
            {
                device.parent = Some(Box::new(parent_device));
            }
        }
    }

    let mut visited = std::collections::HashSet::new();
    for pci_addr in devices.keys().cloned().collect::<Vec<_>>() {
        visited.clear();
        build_parent_chain(&mut devices, &parent_addresses, &pci_addr, &mut visited);
    }

    Ok(devices)
}

pub fn parse_device_string(device_str: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = device_str.split(':').collect();
    if parts.len() == 2 {
        Some((parts[0].to_string(), parts[1].to_string()))
    } else {
        None
    }
}

/// PCI address of the CUDA device with ordinal `idx`, read from
/// `/proc/driver/nvidia/gpus/*/information` (the NVIDIA driver keys each
/// GPU's bus id there by its device minor, which equals the CUDA ordinal).
pub fn get_cuda_pci_address(idx: u32) -> Option<PCIAddress> {
    let gpu_proc_dir = "/proc/driver/nvidia/gpus";
    if !Path::new(gpu_proc_dir).exists() {
        return None;
    }

    let minor_regex =
        Regex::new(r"Device Minor:\s*(\d+)").expect("should compile: regex literal is valid");
    for entry in fs::read_dir(gpu_proc_dir).ok()? {
        let entry = entry.ok()?;
        let info_file = entry.path().join("information");

        if let Ok(content) = fs::read_to_string(&info_file)
            && let Some(captures) = minor_regex.captures(&content)
            && let Ok(device_minor) = captures
                .get(1)
                .expect("should be present: capture group 1 matched")
                .as_str()
                .parse::<u32>()
            && device_minor == idx
        {
            return PCIAddress::parse(&entry.file_name().to_string_lossy().to_lowercase());
        }
    }
    None
}

pub fn get_numa_pci_address(numa_node: &str) -> Option<String> {
    let node: i32 = numa_node.parse().ok()?;
    let pci_devices = parse_pci_topology().ok()?;

    let mut candidates = Vec::new();
    for (pci_addr, device) in &pci_devices {
        if let Some(device_numa) = device.get_numa_node()
            && device_numa == node
        {
            candidates.push(pci_addr.clone());
        }
    }

    if candidates.is_empty() {
        return None;
    }

    let mut best_candidate = candidates[0].clone();
    let mut shortest_path = usize::MAX;

    for pci_addr in &candidates {
        if let Some(device) = pci_devices.get(pci_addr) {
            let path_length = device.get_path_to_root().len();
            if path_length < shortest_path
                || (path_length == shortest_path && pci_addr < &best_candidate)
            {
                shortest_path = path_length;
                best_candidate = pci_addr.clone();
            }
        }
    }

    Some(best_candidate)
}

pub fn get_all_rdma_devices() -> Vec<(String, String)> {
    let mut rdma_devices = Vec::new();
    let ib_class_dir = "/sys/class/infiniband";

    if !Path::new(ib_class_dir).exists() {
        return rdma_devices;
    }

    let pci_regex = Regex::new(r"([0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-9])").unwrap();

    if let Ok(entries) = fs::read_dir(ib_class_dir) {
        let mut sorted_entries: Vec<_> = entries.collect::<Result<Vec<_>, _>>().unwrap_or_default();
        sorted_entries.sort_by_key(|entry| entry.file_name());

        for entry in sorted_entries {
            let ib_dev = entry.file_name().to_string_lossy().to_string();
            let device_path = entry.path().join("device");

            if let Ok(real_path) = fs::read_link(&device_path) {
                let real_path_str = real_path.to_string_lossy();
                let pci_matches: Vec<&str> = pci_regex
                    .find_iter(&real_path_str)
                    .map(|m| m.as_str())
                    .collect();

                if let Some(&last_pci_addr) = pci_matches.last() {
                    rdma_devices.push((ib_dev, last_pci_addr.to_string()));
                }
            }
        }
    }

    rdma_devices
}

pub fn get_nic_pci_address(nic_name: &str) -> Option<String> {
    let rdma_devices = get_all_rdma_devices();
    for (name, pci_addr) in rdma_devices {
        if name == nic_name {
            return Some(pci_addr);
        }
    }
    None
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
/// path runs through the CPU: same NUMA node → [`PathType::Phb`],
/// different nodes → [`PathType::Sys`], unknown → [`PathType::Dis`].
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
    // No shared PCIe ancestor: the path runs through the CPU.
    let bottleneck_mbytes_per_sec = min_link_mbytes_per_sec(a).min(min_link_mbytes_per_sec(b));
    let path_type = match (numa_a, numa_b) {
        (Some(x), Some(y)) if x == y => PathType::Phb,
        (Some(_), Some(_)) => PathType::Sys,
        _ => PathType::Dis,
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

/// Minimum upstream link bandwidth (MB/s) across `hops`. A hop whose
/// bandwidth couldn't be read is 0 and drags the whole range to 0, so a
/// path with an unmeasurable link is treated as the worst case. 0 when
/// `hops` is empty.
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

/// Bandwidth (MB/s) of the PCIe link immediately upstream of the device at
/// `sysfs`, from its own `max_link_speed` / `max_link_width`. An unreadable
/// value is 0, so the link is treated as the worst case.
fn link_bandwidth_mbytes_per_sec(sysfs: &Path) -> u32 {
    let speed = read_speed_mbits_per_lane(sysfs);
    let width = read_link_width(sysfs);
    // `speed` is effective megabits/s per lane (PCIe line-encoding overhead
    // is already folded into the table), so `speed * width` is the link's
    // total megabits/s; dividing by 8 converts bits to bytes → MB/s.
    speed.saturating_mul(width) / 8
}

/// PCIe lane count from `<dir>/max_link_width`, or 0 if unreadable.
fn read_link_width(dir: &Path) -> u32 {
    fs::read_to_string(dir.join("max_link_width"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

/// Per-lane PCIe rate (Mbit/s) from `<dir>/max_link_speed`, or 0 if unreadable.
fn read_speed_mbits_per_lane(dir: &Path) -> u32 {
    fs::read_to_string(dir.join("max_link_speed"))
        .ok()
        .map(|s| pcie_speed_mbits_per_lane(&s))
        .unwrap_or(0)
}

/// Per-lane PCIe bandwidth (Mbit/s) for a `max_link_speed` string such as
/// `"16 GT/s PCIe"`, with line-encoding overhead folded in. `rate * lanes
/// / 8` gives the link's MB/s. The values and the Gen3 fallback mirror
/// NCCL's `kvDictPciGen` (graph/topo.cc).
fn pcie_speed_mbits_per_lane(speed: &str) -> u32 {
    // Match the leading "<rate> GT/s" token; the kernel may append a
    // trailing "PCIe" and prints either "8" or "8.0" style rates.
    match speed.split_whitespace().next().unwrap_or("") {
        "2.5" => 1500,          // Gen1
        "5" | "5.0" => 3000,    // Gen2
        "8" | "8.0" => 6000,    // Gen3
        "16" | "16.0" => 12000, // Gen4
        "32" | "32.0" => 24000, // Gen5
        "64" | "64.0" => 48000, // Gen6
        _ => 6000,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_device_string() {
        assert_eq!(
            parse_device_string("cuda:0"),
            Some(("cuda".to_string(), "0".to_string()))
        );
        assert_eq!(
            parse_device_string("cpu:1"),
            Some(("cpu".to_string(), "1".to_string()))
        );
        assert_eq!(parse_device_string("invalid"), None);
        assert_eq!(
            parse_device_string("cuda:"),
            Some(("cuda".to_string(), "".to_string()))
        );
    }

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
        assert_eq!(pcie_speed_mbits_per_lane("2.5 GT/s PCIe"), 1500);
        assert_eq!(pcie_speed_mbits_per_lane("5 GT/s"), 3000);
        assert_eq!(pcie_speed_mbits_per_lane("8.0 GT/s"), 6000);
        assert_eq!(pcie_speed_mbits_per_lane("16 GT/s PCIe"), 12000);
        assert_eq!(pcie_speed_mbits_per_lane("32 GT/s"), 24000);
        assert_eq!(pcie_speed_mbits_per_lane("64 GT/s"), 48000);
        // Unrecognized / empty defaults to Gen3.
        assert_eq!(pcie_speed_mbits_per_lane("garbage"), 6000);
        assert_eq!(pcie_speed_mbits_per_lane(""), 6000);
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
        // Unknown NUMA node → DIS.
        assert_eq!(classify(&a, None, &b, Some(0)).path_type, PathType::Dis);
    }
}
