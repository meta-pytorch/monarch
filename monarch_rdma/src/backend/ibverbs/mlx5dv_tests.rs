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

    const PATTERN: u8 = 0xa5;
    let alloc_idx = sender
        .allocate(instance, SEGMENT_SIZE, SEGMENT_SIZE)
        .await?;
    let remotes = sender
        .register(
            instance,
            alloc_idx,
            vec![(0, BUF0_SIZE), (BUF1_OFFSET, BUF1_SIZE)],
            PATTERN,
            sender_rdma_ref,
        )
        .await?;

    // Read at offset 0 — should always work.
    let buf0_result = receiver
        .read_remote(instance, remotes[0].clone(), BUF0_SIZE, PATTERN, 10)
        .await?;
    assert!(
        buf0_result.is_ok(),
        "RDMA read at offset 0 failed: {:?}",
        buf0_result.unwrap_err()
    );

    // Read at offset 0x66000000 (1.71 GB) — fails without the rdma-core fix.
    let buf1_result = receiver
        .read_remote(instance, remotes[1].clone(), BUF1_SIZE, PATTERN, 10)
        .await?;
    assert!(
        buf1_result.is_ok(),
        "RDMA read at offset 0x{:x} failed (likely rdma-core umr_sg_list_create \
         int overflow bug — see D84387295): {:?}",
        BUF1_OFFSET,
        buf1_result.unwrap_err()
    );

    sender.free_allocations(instance).await?;
    let _ = host_mesh.shutdown(instance).await;
    Ok(())
}

/// Extract the ibverbs `rkey` from a remote buffer. These tests pin one NIC,
/// so the buffer carries exactly one registration.
fn ibv_rkey_of(remote: &crate::RdmaRemoteBuffer) -> Result<u32, anyhow::Error> {
    let ctx = remote.resolve_mlx().expect("remote buffer is Mellanox");
    let [view] = ctx.buffers.as_slice() else {
        anyhow::bail!(
            "expected one registration on the pinned NIC, got {:?}",
            ctx.buffers,
        );
    };
    Ok(view.rkey)
}

/// Integration test for the DevX indirect-mkey segment-growth path.
///
/// Temporarily overrides the maximum MR size to 2 MiB and reserves two 256 MiB
/// GPU memory segments, S1 and S2. S1 initially allocates 128 MiB of its address
/// range, and an 8 MiB buffer is registered at the beginning of this range. S2
/// allocates and registers an 8 MiB buffer as well. The test asserts that the
/// buffers in S1 and S2 use distinct DevX keys. Then S1 expands its allocation
/// to the full 256 MiB, and a new buffer is registered starting at 128 MiB into
/// the range. S1 now covers 128 KLMs -- far exceeding the old limit of 32 --
/// with the second buffer starting at the 65th KLM. S1's growth must rotate the
/// segment onto a fresh DevX key without invalidating the old DevX key, and
/// round-tripping all three registered buffers must succeed.
///
/// Hardware-gated: needs CUDA + mlx5dv.
#[timed_test::async_timed_test(timeout_secs = 60)]
async fn test_devx_mkey_grows_from_64_to_128_klms() -> Result<(), anyhow::Error> {
    use crate::backend::ibverbs::primitives::mlx5dv_supported;

    if !crate::is_cuda_available() {
        panic!("SKIPPED: CUDA not available (required for GPU memory allocation)");
    }
    if !mlx5dv_supported() {
        panic!("SKIPPED: mlx5dv not supported (required for indirect mkey rebinding)");
    }

    const TEST_MR_SIZE: usize = 2 * 1024 * 1024;
    const INITIAL_KLMS: usize = 64;
    const FINAL_KLMS: usize = 128;
    const CHUNK: usize = INITIAL_KLMS * TEST_MR_SIZE;
    const RESERVED: usize = FINAL_KLMS * TEST_MR_SIZE;
    const BUF: usize = 8 * 1024 * 1024; // 8 MiB per registered buffer

    let cx = context().await;
    let instance = cx.actor_instance;
    let mut host_mesh = HostMesh::local().await?;
    let proc_mesh = host_mesh
        .spawn(
            instance,
            "rebind_grow_procs",
            hyperactor_mesh::extent!(procs = 2),
            None,
            None,
        )
        .await?;

    let sender_proc = proc_mesh.range("procs", 0..1).unwrap();
    let receiver_proc = proc_mesh.range("procs", 1..2).unwrap();

    let sender_config = IbvConfig {
        mkey_max_entries_override: FINAL_KLMS,
        max_mr_size_override: TEST_MR_SIZE,
        require_devx_mkeys: true,
        ..IbvConfig::default()
    };
    let sender_rdma: ActorMesh<RdmaManagerActor> = sender_proc
        .spawn_service(instance, "rdma_manager", &Some(sender_config))
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

    const PATTERN_A: u8 = 0xa1;
    const PATTERN_B: u8 = 0xb1;
    const PATTERN_C: u8 = 0xc1;
    const PATTERN_OVERWRITE: u8 = 0x5a;

    let s1 = sender.allocate(instance, RESERVED, CHUNK).await?;
    let s2 = sender.allocate(instance, RESERVED, BUF).await?;

    let buf_a = sender
        .register(
            instance,
            s1,
            vec![(0, BUF)],
            PATTERN_A,
            sender_rdma_ref.clone(),
        )
        .await?
        .into_iter()
        .next()
        .expect("buf A");
    let buf_b = sender
        .register(
            instance,
            s2,
            vec![(0, BUF)],
            PATTERN_B,
            sender_rdma_ref.clone(),
        )
        .await?
        .into_iter()
        .next()
        .expect("buf B");

    let rkey_a = ibv_rkey_of(&buf_a)?;
    let rkey_b = ibv_rkey_of(&buf_b)?;
    assert_ne!(
        rkey_a, rkey_b,
        "buffers in distinct segments must have distinct rkeys",
    );

    // Expand S1 in place; the next miss triggers a register_segments
    // rebind that grows S1's mkey to cover both chunks.
    sender.expand(instance, s1, CHUNK).await?;
    let buf_c = sender
        .register(instance, s1, vec![(CHUNK, BUF)], PATTERN_C, sender_rdma_ref)
        .await?
        .into_iter()
        .next()
        .expect("buf C");

    let rkey_c = ibv_rkey_of(&buf_c)?;
    // Growth rotates the segment onto a fresh indirect mkey (parking the prior
    // one), so buf C — carved after the grow — carries a new key, distinct from
    // buf A's pre-grow key; the round-trips below confirm A's parked key stays
    // valid. It is also distinct from buf B's separate segment.
    assert_ne!(
        rkey_c, rkey_a,
        "growth rotates the expandable segment onto a new rkey",
    );
    assert_ne!(
        rkey_c, rkey_b,
        "buffers in distinct segments must have distinct rkeys",
    );

    // Each buffer was filled with its own pattern at registration;
    // RDMA reads should return exactly those bytes.
    for (label, buf, pattern) in [
        ("A", &buf_a, PATTERN_A),
        ("B", &buf_b, PATTERN_B),
        ("C", &buf_c, PATTERN_C),
    ] {
        let result = receiver
            .read_remote(instance, buf.clone(), BUF, pattern, 10)
            .await?;
        assert!(
            result.is_ok(),
            "RDMA read of buf {label} failed: {:?}",
            result.unwrap_err()
        );
    }

    // RDMA-write a fresh pattern, read back, confirm it landed.
    for (label, buf) in [("A", &buf_a), ("B", &buf_b), ("C", &buf_c)] {
        let write = receiver
            .write_remote(instance, buf.clone(), BUF, PATTERN_OVERWRITE, 10)
            .await?;
        assert!(
            write.is_ok(),
            "RDMA write to buf {label} failed: {:?}",
            write.unwrap_err()
        );
        let read = receiver
            .read_remote(instance, buf.clone(), BUF, PATTERN_OVERWRITE, 10)
            .await?;
        assert!(
            read.is_ok(),
            "RDMA read-back of buf {label} after write failed: {:?}",
            read.unwrap_err()
        );
    }

    sender.free_allocations(instance).await?;
    let _ = host_mesh.shutdown(instance).await;
    Ok(())
}

/// Integration test for the max KLM fallback path. The sender's DevX key is
/// capped at one KLM: its initial CUDA segment chunk is indirect-key backed,
/// but when that segment expands, the expanded range would introduce a second
/// KLM and therefore cannot be bound to the same DevX key. New buffers in that
/// expanded range must fall back to independent dmabuf MRs rather than reusing
/// the same DevX key with an out-of-range offset.
///
/// Hardware-gated: needs CUDA + mlx5dv.
#[timed_test::async_timed_test(timeout_secs = 60)]
async fn test_indirect_mkey_rebind_falls_back_to_dmabuf_at_capacity() -> Result<(), anyhow::Error> {
    use crate::backend::ibverbs::primitives::mlx5dv_supported;

    if !crate::is_cuda_available() {
        panic!("SKIPPED: CUDA not available (required for GPU memory allocation)");
    }
    if !mlx5dv_supported() {
        panic!("SKIPPED: mlx5dv not supported (required for indirect mkey rebinding)");
    }

    const RESERVED: usize = 1024 * 1024 * 1024;
    const CHUNK: usize = 256 * 1024 * 1024;
    const BUF: usize = 8 * 1024 * 1024;

    let cx = context().await;
    let instance = cx.actor_instance;
    let mut host_mesh = HostMesh::local().await?;
    let proc_mesh = host_mesh
        .spawn(
            instance,
            "rebind_failover_procs",
            hyperactor_mesh::extent!(procs = 2),
            None,
            None,
        )
        .await?;

    let sender_proc = proc_mesh.range("procs", 0..1).unwrap();
    let receiver_proc = proc_mesh.range("procs", 1..2).unwrap();

    let sender_config = IbvConfig {
        mkey_max_entries_override: 1,
        ..IbvConfig::default()
    };
    let sender_rdma: ActorMesh<RdmaManagerActor> = sender_proc
        .spawn_service(instance, "rdma_manager", &Some(sender_config))
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

    const PATTERN_A: u8 = 0xa1;
    const PATTERN_B: u8 = 0xb1;
    const PATTERN_C: u8 = 0xc1;
    const PATTERN_OVERWRITE: u8 = 0x5a;

    let seg = sender.allocate(instance, RESERVED, CHUNK).await?;
    let buf_a = sender
        .register(
            instance,
            seg,
            vec![(0, BUF)],
            PATTERN_A,
            sender_rdma_ref.clone(),
        )
        .await?
        .into_iter()
        .next()
        .expect("buf A");
    let read_a = receiver
        .read_remote(instance, buf_a.clone(), BUF, PATTERN_A, 10)
        .await?;
    assert!(
        read_a.is_ok(),
        "RDMA read of buf A within the initial chunk failed: {:?}",
        read_a.unwrap_err()
    );

    sender.expand(instance, seg, CHUNK).await?;

    // The one-entry DevX key is full. Registering buf B in the expanded range
    // records the unregistered tail and falls back to dmabuf; registering buf C
    // subsequently observes that known tail and also falls back without
    // rescanning or reusing the indirect key.
    let buf_b = sender
        .register(
            instance,
            seg,
            vec![(CHUNK, BUF)],
            PATTERN_B,
            sender_rdma_ref.clone(),
        )
        .await?
        .into_iter()
        .next()
        .expect("buf B");
    let buf_c = sender
        .register(
            instance,
            seg,
            vec![(CHUNK + BUF, BUF)],
            PATTERN_C,
            sender_rdma_ref,
        )
        .await?
        .into_iter()
        .next()
        .expect("buf C");

    let rkey_a = ibv_rkey_of(&buf_a)?;
    let rkey_b = ibv_rkey_of(&buf_b)?;
    let rkey_c = ibv_rkey_of(&buf_c)?;
    // The two tail buffers took the dmabuf path, so neither may carry buf A's
    // indirect key. Reusing it would address beyond the one KLM it covers.
    assert_ne!(
        rkey_a, rkey_b,
        "the first tail buffer must use the dmabuf fallback"
    );
    assert_ne!(
        rkey_a, rkey_c,
        "a later tail buffer must not reuse the prefix's indirect key"
    );

    // Round-trip every buffer after expansion, including the original buffer
    // backed by the still-live indirect key and both fallback registrations.
    for (label, buf, pattern) in [
        ("A", &buf_a, PATTERN_A),
        ("B", &buf_b, PATTERN_B),
        ("C", &buf_c, PATTERN_C),
    ] {
        let read = receiver
            .read_remote(instance, buf.clone(), BUF, pattern, 10)
            .await?;
        assert!(
            read.is_ok(),
            "RDMA read of buf {label} failed: {:?}",
            read.unwrap_err()
        );
        let write = receiver
            .write_remote(instance, buf.clone(), BUF, PATTERN_OVERWRITE, 10)
            .await?;
        assert!(
            write.is_ok(),
            "RDMA write to buf {label} failed: {:?}",
            write.unwrap_err()
        );
        let read_back = receiver
            .read_remote(instance, buf.clone(), BUF, PATTERN_OVERWRITE, 10)
            .await?;
        assert!(
            read_back.is_ok(),
            "RDMA read-back of buf {label} failed: {:?}",
            read_back.unwrap_err()
        );
    }

    sender.free_allocations(instance).await?;
    let _ = host_mesh.shutdown(instance).await;
    Ok(())
}
