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

/// Extract the ibverbs `(lkey, rkey)` from a remote buffer.
fn ibv_keys_of(remote: &crate::RdmaRemoteBuffer) -> Result<(u32, u32), anyhow::Error> {
    let (_mgr, buf) = remote
        .resolve_nic()
        .expect("remote buffer has a NIC backend")
        .into_mlx()
        .expect("remote buffer is Mellanox");
    Ok((buf.lkey, buf.rkey))
}

/// Integration test for the indirect-mkey segment-growth path.
///
/// Allocates two segments S1 and S2 and registers a buffer in each (distinct
/// mkeys). Then expands S1 and registers another buffer in S1's new chunk:
/// growth rotates S1 onto a fresh mkey (parking the prior one rather than
/// mutating an in-flight key), so the new buffer carries a key distinct from
/// both S1's pre-grow buffer and S2's. Round-trips every buffer — including the
/// pre-grow one — to confirm the parked key stays valid and payloads land.
///
/// Hardware-gated: needs CUDA + mlx5dv.
#[timed_test::async_timed_test(timeout_secs = 60)]
async fn test_indirect_mkey_rebind_grows_existing_segment() -> Result<(), anyhow::Error> {
    use crate::backend::ibverbs::primitives::mlx5dv_supported;

    if !crate::is_cuda_available() {
        panic!("SKIPPED: CUDA not available (required for GPU memory allocation)");
    }
    if !mlx5dv_supported() {
        panic!("SKIPPED: mlx5dv not supported (required for indirect mkey rebinding)");
    }

    // Sized to leave plenty of room for granularity rounding while
    // staying small enough to fit on a single GPU.
    const RESERVED: usize = 1024 * 1024 * 1024; // 1 GiB VA per segment
    const CHUNK: usize = 256 * 1024 * 1024; // 256 MiB committed per chunk
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

    const PATTERN_A: u8 = 0xa1;
    const PATTERN_B: u8 = 0xb1;
    const PATTERN_C: u8 = 0xc1;
    const PATTERN_OVERWRITE: u8 = 0x5a;

    let s1 = sender.allocate(instance, RESERVED, CHUNK).await?;
    let s2 = sender.allocate(instance, RESERVED, CHUNK).await?;

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

    let (lkey_a, rkey_a) = ibv_keys_of(&buf_a)?;
    let (lkey_b, rkey_b) = ibv_keys_of(&buf_b)?;
    assert_ne!(
        (lkey_a, rkey_a),
        (lkey_b, rkey_b),
        "buffers in distinct segments must have distinct (lkey, rkey)",
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

    let (lkey_c, rkey_c) = ibv_keys_of(&buf_c)?;
    // Growth rotates the segment onto a fresh indirect mkey (parking the prior
    // one), so buf C — carved after the grow — carries a new key, distinct from
    // buf A's pre-grow key; the round-trips below confirm A's parked key stays
    // valid. It is also distinct from buf B's separate segment.
    assert_ne!(
        (lkey_c, rkey_c),
        (lkey_a, rkey_a),
        "growth rotates the expandable segment onto a new (lkey, rkey)",
    );
    assert_ne!(
        (lkey_c, rkey_c),
        (lkey_b, rkey_b),
        "buffers in distinct segments must have distinct (lkey, rkey)",
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

/// Integration test for the rebind failure path. Forces the second
/// `register_segments` call to fail with `RDMAXCEL_MKEY_REG_LIMIT`
/// by setting `IbvConfig::max_sge_override = 1`, leaving the
/// segment table in a `phys_size > mr_size` state. Subsequent
/// buffer registrations whose addresses fall in the
/// `[mr_size, phys_size)` gap must fall through to per-buffer
/// dmabuf rather than reuse the indirect mkey at an offset past
/// the bound.
///
/// Hardware-gated: needs CUDA + mlx5dv.
#[timed_test::async_timed_test(timeout_secs = 60)]
async fn test_indirect_mkey_rebind_falls_back_to_dmabuf_at_max_sge() -> Result<(), anyhow::Error> {
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
        max_sge_override: 1,
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
        "RDMA read of buf A (within initial chunk) failed: {:?}",
        read_a.unwrap_err()
    );

    sender.expand(instance, seg, CHUNK).await?;

    // Buf B's registration hits the max_sge cap, leaving the
    // segment table with phys_size = 2*CHUNK but mr_size = CHUNK.
    // Buf C is the registration whose lookup observes that gap.
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

    // Buf B took the dmabuf path (the override forced
    // register_segments to fail), so its lkey differs from buf A's
    // indirect mkey — sanity check that the override took effect.
    // Buf C lands at an address inside the [mr_size, phys_size) gap;
    // it must also fall through to dmabuf and get a distinct lkey.
    let (lkey_a, _) = ibv_keys_of(&buf_a)?;
    let (lkey_b, _) = ibv_keys_of(&buf_b)?;
    let (lkey_c, _) = ibv_keys_of(&buf_c)?;
    assert_ne!(
        lkey_a, lkey_b,
        "buf B should be registered via the dmabuf fallback after \
         register_segments hit the max_sge override and therefore have \
         a different lkey from buf A's indirect mkey"
    );
    assert_ne!(
        lkey_a, lkey_c,
        "buf C lands in the [mr_size, phys_size) gap and must fall \
         through to dmabuf; reusing buf A's indirect mkey here would \
         hand the NIC a stale (lkey, offset) past the bound"
    );

    // Round-trip every buffer: read the registration pattern, write
    // a fresh one, read it back. Even if the lkey check above
    // somehow passed, a stale (lkey, offset) on buf C would fail at
    // the NIC with LOC_PROT_ERR.
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
            "RDMA read-back of buf {label} after write failed: {:?}",
            read_back.unwrap_err()
        );
    }

    sender.free_allocations(instance).await?;
    let _ = host_mesh.shutdown(instance).await;
    Ok(())
}
