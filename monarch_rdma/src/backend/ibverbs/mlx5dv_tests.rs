/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tests for mlx5dv-specific functionality (indirect mkeys, segment scanning).

use hyperactor_mesh::ActorMesh;
use hyperactor_mesh::context;
use hyperactor_mesh::host_mesh::HostMesh;
use ndslice::ViewExt;

use crate::IbvConfig;
use crate::RdmaManagerActor;
use crate::backend::cuda_test_utils::ReceiverActor;
use crate::backend::cuda_test_utils::ReceiverMessageClient;
use crate::backend::cuda_test_utils::SenderActor;
use crate::backend::cuda_test_utils::SenderMessageClient;

/// Regression test for an integer overflow bug in rdma-core's
/// `umr_sg_list_create` (providers/mlx5/qp.c) where `int byte_count` was used
/// to accumulate KLM entry sizes for an indirect mkey. When the total segment
/// size exceeded ~2 GB, the 32-bit signed sum overflowed, corrupting the
/// hardware mkey's total length field (`mk->len`). This caused RDMA reads at
/// offsets beyond the truncated length to fail with remote access error
/// (status=10, vendor_err=136).
///
/// The test allocates a >2 GB GPU memory segment via the CUDA virtual memory
/// API, registers it through the segment scanner (which triggers indirect mkey
/// creation via `mlx5dv_wr_mr_list`), then performs RDMA reads at both offset 0
/// and a large offset (0x66000000 = 1.7 GB). Without the rdma-core fix, the
/// second read fails because the NIC thinks the mkey only covers ~1.4 GB.
///
/// See also: D84387295 (internal discovery of the same bug by dstaay),
/// upstream fix in rdma-core v61.0.
#[timed_test::async_timed_test(timeout_secs = 60)]
async fn test_indirect_mkey_read_at_large_offset() -> Result<(), anyhow::Error> {
    use crate::backend::ibverbs::primitives::mlx5dv_supported;

    if !crate::is_cuda_available() {
        panic!("SKIPPED: CUDA not available (required for GPU memory allocation)");
    }
    if !mlx5dv_supported() {
        panic!("SKIPPED: mlx5dv not supported (required for indirect mkey creation)");
    }

    // These constants reproduce the real-world failure scenario.
    // The segment must be large enough (>2 GB) to trigger the overflow.
    const BUF0_SIZE: usize = 8016 * 8192 * 2; // ~131 MB at offset 0
    const BUF1_SIZE: usize = 512 * 8192 * 2; // ~8 MB at offset 0x66000000
    const BUF1_OFFSET: usize = 0x66000000; // 1.71 GB — beyond the truncated mkey length
    const SEGMENT_SIZE: usize = 14_302_576_640; // ~14.3 GB total segment

    let cx = context().await;
    let instance = cx.actor_instance;
    let mut host_mesh = HostMesh::local().await?;
    let proc_mesh = host_mesh
        .spawn(
            instance,
            "mkey_test_procs",
            hyperactor_mesh::extent!(procs = 2),
            None,
            None,
        )
        .await?;

    let sender_proc = proc_mesh.range("procs", 0..1).unwrap();
    let receiver_proc = proc_mesh.range("procs", 1..2).unwrap();

    let sender_rdma: ActorMesh<RdmaManagerActor> = sender_proc
        .spawn_service(instance, "rdma_manager", &Some(IbvConfig::default()))
        .await?;
    let _receiver_rdma: ActorMesh<RdmaManagerActor> = receiver_proc
        .spawn_service(instance, "rdma_manager", &Some(IbvConfig::default()))
        .await?;

    let sender_rdma_ref = sender_rdma.values().next().unwrap().clone();

    let sender_mesh: ActorMesh<SenderActor> = sender_proc.spawn(instance, "sender", &0_i32).await?;
    let receiver_mesh: ActorMesh<ReceiverActor> =
        receiver_proc.spawn(instance, "receiver", &()).await?;

    let sender = sender_mesh.values().next().unwrap().clone();
    let receiver = receiver_mesh.values().next().unwrap().clone();

    sender.allocate(instance, SEGMENT_SIZE).await?;
    let remotes = sender
        .register(
            instance,
            vec![(0, BUF0_SIZE), (BUF1_OFFSET, BUF1_SIZE)],
            sender_rdma_ref,
        )
        .await?;

    // Read at offset 0 — should always work.
    let buf0_result = receiver
        .read_remote(instance, remotes[0].clone(), BUF0_SIZE, 10)
        .await?;
    assert!(
        buf0_result.is_ok(),
        "RDMA read at offset 0 failed: {:?}",
        buf0_result.unwrap_err()
    );

    // Read at offset 0x66000000 (1.71 GB) — fails without the rdma-core fix.
    let buf1_result = receiver
        .read_remote(instance, remotes[1].clone(), BUF1_SIZE, 10)
        .await?;
    assert!(
        buf1_result.is_ok(),
        "RDMA read at offset 0x{:x} failed (likely rdma-core umr_sg_list_create \
         int overflow bug — see D84387295): {:?}",
        BUF1_OFFSET,
        buf1_result.unwrap_err()
    );

    sender.free_allocation(instance).await?;
    let _ = host_mesh.shutdown(instance).await;
    Ok(())
}
