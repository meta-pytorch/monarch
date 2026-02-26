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
use std::fs;
use std::path::Path;

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
        if let Some(ref parent) = parent_addr {
            if !devices.contains_key(parent) {
                devices.insert(parent.clone(), PCIDevice::new(parent.clone()));
            }
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

            if let Some(parent_device) = devices.get(parent_addr).cloned() {
                if let Some(device) = devices.get_mut(pci_addr) {
                    device.parent = Some(Box::new(parent_device));
                }
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

pub fn get_cuda_pci_address(device_idx: &str) -> Option<String> {
    let idx: i32 = device_idx.parse().ok()?;
    let gpu_proc_dir = "/proc/driver/nvidia/gpus";

    if !Path::new(gpu_proc_dir).exists() {
        return None;
    }

    for entry in fs::read_dir(gpu_proc_dir).ok()? {
        let entry = entry.ok()?;
        let pci_addr = entry.file_name().to_string_lossy().to_lowercase();
        let info_file = entry.path().join("information");

        if let Ok(content) = fs::read_to_string(&info_file) {
            let minor_regex = Regex::new(r"Device Minor:\s*(\d+)").unwrap();
            if let Some(captures) = minor_regex.captures(&content) {
                if let Ok(device_minor) = captures.get(1).unwrap().as_str().parse::<i32>() {
                    if device_minor == idx {
                        return Some(pci_addr);
                    }
                }
            }
        }
    }
    None
}

pub fn get_numa_pci_address(numa_node: &str) -> Option<String> {
    let node: i32 = numa_node.parse().ok()?;
    let pci_devices = parse_pci_topology().ok()?;

    let mut candidates = Vec::new();
    for (pci_addr, device) in &pci_devices {
        if let Some(device_numa) = device.get_numa_node() {
            if device_numa == node {
                candidates.push(pci_addr.clone());
            }
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
}
