/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module provides functionality to automatically pair compute devices with
//! the best available RDMA NICs based on PCI topology distance.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use regex::Regex;

use crate::ibverbs_primitives::RdmaDevice;

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

/// Step 1: Parse device string into prefix and postfix
/// Step 2: Get PCI address from compute device
/// Step 3: Get PCI address for all RDMA NIC devices
/// Step 4: Calculate PCI distances and return closest RDMA NIC device
pub fn select_optimal_rdma_device(device_hint: Option<&str>) -> Option<RdmaDevice> {
    let device_hint = device_hint?;

    let (prefix, postfix) = parse_device_string(device_hint)?;

    match prefix.as_str() {
        "nic" => {
            let all_rdma_devices = crate::ibverbs_primitives::get_all_devices();
            all_rdma_devices
                .into_iter()
                .find(|dev| dev.name() == &postfix)
        }
        "cuda" | "cpu" => {
            let source_pci_addr = match prefix.as_str() {
                "cuda" => get_cuda_pci_address(&postfix)?,
                "cpu" => get_numa_pci_address(&postfix)?,
                _ => unreachable!(),
            };
            let rdma_devices = get_all_rdma_devices();
            if rdma_devices.is_empty() {
                return RdmaDevice::first_available();
            }
            let pci_devices = parse_pci_topology().ok()?;
            let source_device = pci_devices.get(&source_pci_addr)?;

            let rdma_names: Vec<String> =
                rdma_devices.iter().map(|(name, _)| name.clone()).collect();
            let rdma_pci_devices: Vec<PCIDevice> = rdma_devices
                .iter()
                .filter_map(|(_, addr)| pci_devices.get(addr).cloned())
                .collect();

            if let Some(closest_idx) = source_device.find_closest(&rdma_pci_devices) {
                if let Some(optimal_name) = rdma_names.get(closest_idx) {
                    let all_rdma_devices = crate::ibverbs_primitives::get_all_devices();
                    for device in all_rdma_devices {
                        if *device.name() == *optimal_name {
                            return Some(device);
                        }
                    }
                }
            }

            // Fallback
            RdmaDevice::first_available()
        }
        _ => {
            // Direct device name lookup for backward compatibility
            let rdma_devices = crate::ibverbs_primitives::get_all_devices();
            rdma_devices
                .into_iter()
                .find(|dev| dev.name() == device_hint)
        }
    }
}

/// Creates a mapping from CUDA PCI addresses to optimal RDMA devices
///
/// This function discovers all available CUDA devices and determines the best
/// RDMA device for each one using the device selection algorithm.
///
/// # Returns
///
/// * `HashMap<String, RdmaDevice>` - Map from CUDA PCI address to optimal RDMA device
pub fn create_cuda_to_rdma_mapping() -> HashMap<String, RdmaDevice> {
    let mut mapping = HashMap::new();

    // Try to discover CUDA devices (GPU 0-8 should be sufficient for most systems)
    for gpu_idx in 0..8 {
        let gpu_idx_str = gpu_idx.to_string();
        if let Some(cuda_pci_addr) = get_cuda_pci_address(&gpu_idx_str) {
            let cuda_hint = format!("cuda:{}", gpu_idx);
            if let Some(rdma_device) = select_optimal_rdma_device(Some(&cuda_hint)) {
                mapping.insert(cuda_pci_addr, rdma_device);
            }
        }
    }

    mapping
}

/// Resolves RDMA device using auto-detection logic when needed
///
/// This function applies auto-detection for default devices, but otherwise  
/// returns the device as-is. The main device selection logic happens in
/// `select_optimal_rdma_device` and `IbverbsConfig::with_device_hint`.
///
/// # Arguments
///
/// * `device` - The RdmaDevice to potentially resolve
///
/// # Returns
///
/// * `Option<RdmaDevice>` - The resolved device, or None if resolution fails
pub fn resolve_rdma_device(device: &RdmaDevice) -> Option<RdmaDevice> {
    let device_name = device.name();

    if device_name.starts_with("mlx") {
        return Some(device.clone());
    }

    let all_devices = crate::ibverbs_primitives::get_all_devices();
    let is_likely_default = if let Some(first_device) = all_devices.first() {
        device_name == first_device.name()
    } else {
        false
    };

    if is_likely_default {
        select_optimal_rdma_device(Some("cpu:0"))
    } else {
        Some(device.clone())
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

    /// Detect if we're running on GT20 hardware by checking for expected RDMA device configuration
    fn is_gt20_hardware() -> bool {
        let rdma_devices = get_all_rdma_devices();
        let device_names: std::collections::HashSet<String> =
            rdma_devices.iter().map(|(name, _)| name.clone()).collect();

        // GT20 hardware should have these specific RDMA devices
        let expected_gt20_devices = [
            "mlx5_0", "mlx5_3", "mlx5_4", "mlx5_5", "mlx5_6", "mlx5_9", "mlx5_10", "mlx5_11",
        ];

        // Check if we have at least 8 GPUs (GT20 characteristic)
        let gpu_count = (0..8)
            .filter(|&i| get_cuda_pci_address(&i.to_string()).is_some())
            .count();

        // Must have expected RDMA devices AND 8 GPUs
        let has_expected_rdma = expected_gt20_devices
            .iter()
            .all(|&device| device_names.contains(device));

        has_expected_rdma && gpu_count == 8
    }

    /// Test each function step by step using the new simplified API - GT20 hardware only
    #[test]
    fn test_gt20_hardware() {
        // Early exit if not on GT20 hardware
        if !is_gt20_hardware() {
            println!("⚠️  Skipping test_gt20_hardware: Not running on GT20 hardware");
            return;
        }

        println!("✓ Detected GT20 hardware - running full validation test");
        // Step 1: Test PCI topology parsing
        println!("\n1. PCI TOPOLOGY PARSING");
        let pci_devices = match parse_pci_topology() {
            Ok(devices) => {
                println!("✓ Found {} PCI devices", devices.len());
                devices
            }
            Err(e) => {
                println!("✗ Error: {}", e);
                return;
            }
        };

        // Step 2: Test unified RDMA device discovery
        println!("\n2. RDMA DEVICE DISCOVERY");
        let rdma_devices = get_all_rdma_devices();
        println!("✓ Found {} RDMA devices", rdma_devices.len());
        for (name, pci_addr) in &rdma_devices {
            println!("  RDMA {}: {}", name, pci_addr);
        }

        // Step 3: Test device string parsing
        println!("\n3. DEVICE STRING PARSING");
        let test_strings = ["cuda:0", "cuda:1", "cpu:0", "cpu:1"];
        for device_str in &test_strings {
            if let Some((prefix, postfix)) = parse_device_string(device_str) {
                println!(
                    "  '{}' -> prefix: '{}', postfix: '{}'",
                    device_str, prefix, postfix
                );
            } else {
                println!("  '{}' -> PARSE FAILED", device_str);
            }
        }

        // Step 4: Test CUDA PCI address resolution
        println!("\n4. CUDA PCI ADDRESS RESOLUTION");
        for gpu_idx in 0..8 {
            let gpu_idx_str = gpu_idx.to_string();
            match get_cuda_pci_address(&gpu_idx_str) {
                Some(pci_addr) => {
                    println!("  GPU {} -> PCI: {}", gpu_idx, pci_addr);
                }
                None => {
                    println!("  GPU {} -> PCI: NOT FOUND", gpu_idx);
                }
            }
        }

        // Step 5: Test CPU/NUMA PCI address resolution
        println!("\n5. CPU/NUMA PCI ADDRESS RESOLUTION");
        for numa_node in 0..4 {
            let numa_str = numa_node.to_string();
            match get_numa_pci_address(&numa_str) {
                Some(pci_addr) => {
                    println!("  NUMA {} -> PCI: {}", numa_node, pci_addr);
                }
                None => {
                    println!("  NUMA {} -> PCI: NOT FOUND", numa_node);
                }
            }
        }

        // Step 6: Test distance calculation for GPU 0
        println!("\n6. DISTANCE CALCULATION TEST (GPU 0)");
        if let Some(gpu0_pci_addr) = get_cuda_pci_address("0") {
            if let Some(gpu0_device) = pci_devices.get(&gpu0_pci_addr) {
                println!("GPU 0 PCI: {}", gpu0_pci_addr);
                println!("GPU 0 path to root: {:?}", gpu0_device.get_path_to_root());

                let mut all_distances = Vec::new();
                for (nic_name, nic_pci_addr) in &rdma_devices {
                    if let Some(nic_device) = pci_devices.get(nic_pci_addr) {
                        let distance = gpu0_device.distance_to(nic_device);
                        all_distances.push((distance, nic_name.clone(), nic_pci_addr.clone()));
                        println!("  {} ({}): distance = {}", nic_name, nic_pci_addr, distance);
                        println!("    NIC path to root: {:?}", nic_device.get_path_to_root());
                    }
                }

                // Find the minimum distance
                all_distances.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                if let Some((min_dist, min_nic, min_addr)) = all_distances.first() {
                    println!(
                        "  → CLOSEST: {} ({}) with distance {}",
                        min_nic, min_addr, min_dist
                    );
                }
            }
        }

        // Step 7: Test unified device selection interface
        println!("\n7. UNIFIED DEVICE SELECTION TEST");
        let test_cases = [
            ("cuda:0", "CUDA device 0"),
            ("cuda:1", "CUDA device 1"),
            ("cpu:0", "CPU/NUMA node 0"),
            ("cpu:1", "CPU/NUMA node 1"),
        ];

        for (device_hint, description) in &test_cases {
            let selected_device = select_optimal_rdma_device(Some(device_hint));
            match selected_device {
                Some(device) => {
                    println!("  {} ({}) -> {}", device_hint, description, device.name());
                }
                None => {
                    println!("  {} ({}) -> NOT FOUND", device_hint, description);
                }
            }
        }

        // Step 8: Test all 8 GPU mappings against expected GT20 hardware results
        println!("\n8. GPU-TO-RDMA MAPPING VALIDATION (ALL 8 GPUs)");

        // Expected results from original Python implementation on GT20 hardware
        let python_expected = [
            (0, "mlx5_0"),
            (1, "mlx5_3"),
            (2, "mlx5_4"),
            (3, "mlx5_5"),
            (4, "mlx5_6"),
            (5, "mlx5_9"),
            (6, "mlx5_10"),
            (7, "mlx5_11"),
        ];

        let mut rust_results = std::collections::HashMap::new();
        let mut all_match = true;

        // Test all 8 GPU mappings using new unified API
        for gpu_idx in 0..8 {
            let cuda_hint = format!("cuda:{}", gpu_idx);
            let selected_device = select_optimal_rdma_device(Some(&cuda_hint));

            match selected_device {
                Some(device) => {
                    let device_name = device.name().to_string();
                    rust_results.insert(gpu_idx, device_name.clone());
                    println!("  GPU {} -> {}", gpu_idx, device_name);
                }
                None => {
                    println!("  GPU {} -> NOT FOUND", gpu_idx);
                    rust_results.insert(gpu_idx, "NOT_FOUND".to_string());
                }
            }
        }

        // Compare against expected results
        println!("\n=== VALIDATION AGAINST EXPECTED RESULTS ===");
        for (gpu_idx, expected_nic) in python_expected {
            if let Some(actual_nic) = rust_results.get(&gpu_idx) {
                let matches = actual_nic == expected_nic;
                println!(
                    "  GPU {} -> {} {} (expected {})",
                    gpu_idx,
                    actual_nic,
                    if matches { "✓" } else { "✗" },
                    expected_nic
                );
                all_match = all_match && matches;
            } else {
                println!(
                    "  GPU {} -> NOT FOUND ✗ (expected {})",
                    gpu_idx, expected_nic
                );
                all_match = false;
            }
        }

        if all_match {
            println!("\n🎉 SUCCESS: All GPU-NIC pairings match expected GT20 hardware results!");
            println!("✓ New unified API produces identical results to proven algorithm");
        } else {
            println!("\n⚠️  WARNING: Some GPU-NIC pairings differ from expected results");
            println!("   This could indicate:");
            println!("   - Hardware configuration differences");
            println!("   - Algorithm implementation differences");
            println!("   - Environment setup differences");
        }

        // Step 9: Detailed CPU device selection analysis
        println!("\n9. DETAILED CPU DEVICE SELECTION ANALYSIS");

        // Check what representative PCI addresses we found for each NUMA node
        if let Some(numa0_addr) = get_numa_pci_address("0") {
            println!("  NUMA 0 representative PCI: {}", numa0_addr);
        } else {
            println!("  NUMA 0 representative PCI: NOT FOUND");
        }

        if let Some(numa1_addr) = get_numa_pci_address("1") {
            println!("  NUMA 1 representative PCI: {}", numa1_addr);
        } else {
            println!("  NUMA 1 representative PCI: NOT FOUND");
        }

        // Now test the actual selections
        let cpu0_device = select_optimal_rdma_device(Some("cpu:0"));
        let cpu1_device = select_optimal_rdma_device(Some("cpu:1"));

        match (
            cpu0_device.as_ref().map(|d| d.name()),
            cpu1_device.as_ref().map(|d| d.name()),
        ) {
            (Some(cpu0_name), Some(cpu1_name)) => {
                println!("\n  FINAL SELECTIONS:");
                println!("    CPU:0 -> {}", cpu0_name);
                println!("    CPU:1 -> {}", cpu1_name);
                if cpu0_name != cpu1_name {
                    println!("    ✓ Different NUMA nodes select different RDMA devices");
                } else {
                    println!("    ⚠️  Same RDMA device selected for both NUMA nodes");
                    println!("       This could indicate:");
                    println!(
                        "       - {} is genuinely closest to both NUMA nodes",
                        cpu0_name
                    );
                    println!("       - NUMA topology detection issue");
                    println!("       - Cross-NUMA penalty algorithm working correctly");
                }
            }
            _ => {
                println!("    ○ CPU device selection not available");
            }
        }

        println!("\n✓ GT20 hardware test completed");

        // Assert for the test framework
        assert!(
            all_match,
            "All GPU-NIC pairings should match expected GT20 hardware results"
        );
    }
}
