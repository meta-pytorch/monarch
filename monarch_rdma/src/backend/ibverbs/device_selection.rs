/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ibverbs-specific device selection logic that pairs compute devices
//! with the best available RDMA NICs based on PCI topology distance.

use std::collections::HashMap;

use super::primitives::IbvDevice;
use super::primitives::get_all_devices;
use crate::device_selection::PCIDevice;
use crate::device_selection::get_all_rdma_devices;
use crate::device_selection::get_cuda_pci_address;
use crate::device_selection::get_numa_pci_address;
use crate::device_selection::parse_device_string;
use crate::device_selection::parse_pci_topology;

/// Step 1: Parse device string into prefix and postfix
/// Step 2: Get PCI address from compute device
/// Step 3: Get PCI address for all RDMA NIC devices
/// Step 4: Calculate PCI distances and return closest RDMA NIC device
pub fn select_optimal_ibv_device(device_hint: Option<&str>) -> Option<IbvDevice> {
    let device_hint = device_hint?;

    let (prefix, postfix) = parse_device_string(device_hint)?;

    match prefix.as_str() {
        "nic" => {
            let all_rdma_devices = get_all_devices();
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
                return IbvDevice::first_available();
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
                    let all_rdma_devices = get_all_devices();
                    for device in all_rdma_devices {
                        if *device.name() == *optimal_name {
                            return Some(device);
                        }
                    }
                }
            }

            // Fallback
            IbvDevice::first_available()
        }
        _ => {
            // Direct device name lookup for backward compatibility
            let rdma_devices = get_all_devices();
            rdma_devices
                .into_iter()
                .find(|dev| dev.name() == device_hint)
        }
    }
}

/// Creates a mapping from CUDA PCI addresses to optimal RDMA devices.
///
/// Discovers all available CUDA devices and determines the best
/// RDMA device for each one using the device selection algorithm.
pub fn create_cuda_to_ibv_mapping() -> HashMap<String, IbvDevice> {
    let mut mapping = HashMap::new();

    // Try to discover CUDA devices (GPU 0-8 should be sufficient for most systems)
    for gpu_idx in 0..8 {
        let gpu_idx_str = gpu_idx.to_string();
        if let Some(cuda_pci_addr) = get_cuda_pci_address(&gpu_idx_str) {
            let cuda_hint = format!("cuda:{}", gpu_idx);
            if let Some(rdma_device) = select_optimal_ibv_device(Some(&cuda_hint)) {
                mapping.insert(cuda_pci_addr, rdma_device);
            }
        }
    }

    mapping
}

/// Resolves RDMA device using auto-detection logic when needed.
///
/// Applies auto-detection for default devices, but otherwise
/// returns the device as-is.
pub fn resolve_ibv_device(device: &IbvDevice) -> Option<IbvDevice> {
    let device_name = device.name();

    if device_name.starts_with("mlx") {
        return Some(device.clone());
    }

    let all_devices = get_all_devices();
    let is_likely_default = if let Some(first_device) = all_devices.first() {
        device_name == first_device.name()
    } else {
        false
    };

    if is_likely_default {
        select_optimal_ibv_device(Some("cpu:0"))
    } else {
        Some(device.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            let selected_device = select_optimal_ibv_device(Some(device_hint));
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
            let selected_device = select_optimal_ibv_device(Some(&cuda_hint));

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
        let cpu0_device = select_optimal_ibv_device(Some("cpu:0"));
        let cpu1_device = select_optimal_ibv_device(Some("cpu:1"));

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

        // we can't gaurantee that the test will always match given test infra but is good for diagnostic purposes / tracking.
    }
}
