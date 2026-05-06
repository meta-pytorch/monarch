/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Integration test for per-PD segment registration with two GPU buffers
//! on different RDMA NICs. Requires 8 GPUs and a multi-NIC RoCEv2 fabric;
//! skips at runtime when these are unavailable.

use hyperactor_mesh::ActorMesh;
use hyperactor_mesh::context;
use hyperactor_mesh::host_mesh::HostMesh;
use monarch_rdma::IbvConfig;
use monarch_rdma::RdmaManagerActor;
use monarch_rdma::backend::cuda_test_utils::ReceiverActor;
use monarch_rdma::backend::cuda_test_utils::ReceiverMessageClient;
use monarch_rdma::backend::cuda_test_utils::SenderActor;
use monarch_rdma::backend::cuda_test_utils::SenderMessageClient;
use monarch_rdma::backend::ibverbs::device_selection::select_optimal_ibv_device;
use ndslice::ViewExt;

/// Finds two CUDA devices that map to different RDMA NICs via PCI topology.
/// Returns `Some((device_a, device_b))` or `None` if all devices share one NIC.
fn find_devices_on_different_nics() -> Option<(i32, i32)> {
    let mut gpu_to_nic: Vec<(i32, String)> = Vec::new();
    for gpu_idx in 0..8 {
        let hint = format!("cuda:{gpu_idx}");
        if let Some(device) = select_optimal_ibv_device(Some(&hint)) {
            gpu_to_nic.push((gpu_idx, device.name().to_string()));
        }
    }

    for i in 0..gpu_to_nic.len() {
        for j in (i + 1)..gpu_to_nic.len() {
            if gpu_to_nic[i].1 != gpu_to_nic[j].1 {
                return Some((gpu_to_nic[i].0, gpu_to_nic[j].0));
            }
        }
    }
    None
}

/// Validates that per-PD segment registration works correctly when two GPU
/// buffers on different RDMA NICs are registered through the same
/// RdmaManagerActor. The manager internally creates a separate PD for each NIC,
/// so segments on different devices get different PDs.
///
/// Dynamically discovers two CUDA devices that map to different NICs, allocates
/// memory on each (so the scanner sees both before registration), then registers
/// both and performs RDMA reads from separate receiver processes — one per NIC,
/// since RoCEv2 NICs on different subnets can't reach each other.
///
/// Without the per-PD fix (keying `activeSegments` by `(address, pd)` instead
/// of just `address`), the second buffer would never register with the correct
/// PD, causing the second read to fail.
#[timed_test::async_timed_test(timeout_secs = 60)]
async fn test_multi_pd_segment_registration() -> Result<(), anyhow::Error> {
    assert!(
        monarch_rdma::is_cuda_available(),
        "CUDA not available (required for GPU memory allocation)"
    );
    assert!(
        monarch_rdma::mlx5dv_supported(),
        "mlx5dv not supported (required for indirect mkey creation)"
    );

    let (device_a, device_b) = find_devices_on_different_nics()
        .expect("need at least 2 CUDA devices on different RDMA NICs");

    const BUF_SIZE: usize = 4 * 1024 * 1024; // 4 MB per buffer

    let cx = context().await;
    let instance = cx.actor_instance;
    let mut host_mesh = HostMesh::local().await?;

    let proc_mesh = host_mesh
        .spawn(
            instance,
            "multi_pd_procs",
            hyperactor_mesh::extent!(procs = 3),
            None,
            None,
        )
        .await?;

    let sender_proc = proc_mesh.range("procs", 0..1).unwrap();
    let receiver_a_proc = proc_mesh.range("procs", 1..2).unwrap();
    let receiver_b_proc = proc_mesh.range("procs", 2..3).unwrap();

    // Single RDMA manager for both senders. It uses different PDs internally
    // for each NIC, which is exactly what we're testing.
    let rdma: ActorMesh<RdmaManagerActor> = sender_proc
        .spawn_service(instance, "rdma_manager", &Some(IbvConfig::default()))
        .await?;

    // Each receiver gets an RDMA manager targeting the NIC that matches its
    // sender's device. This ensures the receiver's QP is on the same subnet
    // as the sender's NIC (required for RoCEv2).
    let _rdma_recv_a: ActorMesh<RdmaManagerActor> = receiver_a_proc
        .spawn_service(
            instance,
            "rdma_manager",
            &Some(IbvConfig::targeting(&format!("cuda:{device_a}"))),
        )
        .await?;
    let _rdma_recv_b: ActorMesh<RdmaManagerActor> = receiver_b_proc
        .spawn_service(
            instance,
            "rdma_manager",
            &Some(IbvConfig::targeting(&format!("cuda:{device_b}"))),
        )
        .await?;

    let rdma_ref = rdma.values().next().unwrap().clone();

    // Both senders on the same proc, sharing the RDMA manager.
    let sender_a_mesh: ActorMesh<SenderActor> =
        sender_proc.spawn(instance, "sender_a", &device_a).await?;
    let sender_b_mesh: ActorMesh<SenderActor> =
        sender_proc.spawn(instance, "sender_b", &device_b).await?;
    let receiver_a_mesh: ActorMesh<ReceiverActor> =
        receiver_a_proc.spawn(instance, "receiver_a", &()).await?;
    let receiver_b_mesh: ActorMesh<ReceiverActor> =
        receiver_b_proc.spawn(instance, "receiver_b", &()).await?;

    let sender_a = sender_a_mesh.values().next().unwrap().clone();
    let sender_b = sender_b_mesh.values().next().unwrap().clone();
    let receiver_a = receiver_a_mesh.values().next().unwrap().clone();
    let receiver_b = receiver_b_mesh.values().next().unwrap().clone();

    // Both senders allocate first, so the scanner sees both segments before
    // either triggers registration.
    sender_a.allocate(instance, BUF_SIZE).await?;
    sender_b.allocate(instance, BUF_SIZE).await?;

    // Now register — each triggers segment scanning and gets its own PD
    // based on the NIC closest to its device.
    let remotes_a = sender_a
        .register(instance, vec![(0, BUF_SIZE)], rdma_ref.clone())
        .await?;
    let remotes_b = sender_b
        .register(instance, vec![(0, BUF_SIZE)], rdma_ref)
        .await?;

    // Each receiver reads using a QP on the matching NIC/subnet.
    let result_a = receiver_a
        .read_remote(instance, remotes_a[0].clone(), BUF_SIZE, 10)
        .await?;
    assert!(
        result_a.is_ok(),
        "RDMA read from device {device_a} failed: {:?}",
        result_a.unwrap_err()
    );

    // On the base revision (address-only key), B's segment would never be
    // registered with the correct PD, causing this read to fail.
    let result_b = receiver_b
        .read_remote(instance, remotes_b[0].clone(), BUF_SIZE, 10)
        .await?;
    assert!(
        result_b.is_ok(),
        "RDMA read from device {device_b} failed (per-PD segment registration bug): {:?}",
        result_b.unwrap_err()
    );

    // Clean up allocations.
    sender_a.free_allocation(instance).await?;
    sender_b.free_allocation(instance).await?;

    let _ = host_mesh.shutdown(instance).await;
    Ok(())
}
