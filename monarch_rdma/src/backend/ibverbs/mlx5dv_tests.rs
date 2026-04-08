/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tests for mlx5dv-specific functionality (indirect mkeys, segment scanning).

use std::sync::Arc;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor::reference;
use hyperactor_config::Flattrs;
use hyperactor_mesh::ActorMesh;
use hyperactor_mesh::context;
use hyperactor_mesh::host_mesh::HostMesh;
use ndslice::ViewExt;

#[cfg(test_8_gpus)]
use super::test_utils::find_devices_on_different_nics;
use crate::IbvConfig;
use crate::RdmaManagerActor;
use crate::RdmaManagerMessageClient;
use crate::RdmaRemoteBuffer;
use crate::backend::cuda_test_utils::CudaAllocation;
use crate::backend::cuda_test_utils::CudaAllocator;
use crate::backend::cuda_test_utils::cuda_allocator_scanner;
use crate::local_memory::RdmaLocalMemory;
use crate::local_memory::UnsafeLocalMemory;
use crate::register_segment_scanner;

// ---------------------------------------------------------------------------
// Helpers: sender actor, receiver actor
// ---------------------------------------------------------------------------

/// Runs in the sender's child process. Allocates GPU memory via CudaAllocator,
/// registers sub-buffers with the RDMA manager, and can free the allocation on
/// request. Allocate and register are separate messages so that multiple senders
/// can allocate first (making all segments visible to the scanner) before any
/// of them trigger registration.
#[hyperactor::export(spawn = true, handlers = [SenderMessage])]
#[derive(Debug)]
struct SenderActor {
    device: i32,
    allocation: Option<CudaAllocation>,
}

impl Actor for SenderActor {}

#[async_trait]
impl RemoteSpawn for SenderActor {
    type Params = i32;

    async fn new(device_id: i32, _env: Flattrs) -> Result<Self, anyhow::Error> {
        register_segment_scanner(Some(cuda_allocator_scanner));
        Ok(Self {
            device: device_id,
            allocation: None,
        })
    }
}

#[derive(
    Handler,
    RefClient,
    typeuri::Named,
    serde::Serialize,
    serde::Deserialize,
    Debug
)]
enum SenderMessage {
    /// Allocate GPU memory for later registration.
    Allocate {
        total_size: usize,
        #[reply]
        reply: reference::OncePortRef<()>,
    },
    /// Register sub-buffers from the previous allocation with the RDMA manager.
    Register {
        buffers: Vec<(usize, usize)>,
        rdma_manager: reference::ActorRef<RdmaManagerActor>,
        #[reply]
        reply: reference::OncePortRef<Vec<RdmaRemoteBuffer>>,
    },
    FreeAllocation {
        #[reply]
        reply: reference::OncePortRef<()>,
    },
}

#[async_trait]
#[hyperactor::handle(SenderMessage)]
impl SenderMessageHandler for SenderActor {
    async fn allocate(
        &mut self,
        _cx: &Context<Self>,
        total_size: usize,
    ) -> Result<(), anyhow::Error> {
        self.allocation = Some(CudaAllocator::get().allocate(self.device, total_size));
        Ok(())
    }

    async fn register(
        &mut self,
        cx: &Context<Self>,
        buffers: Vec<(usize, usize)>,
        rdma_manager: reference::ActorRef<RdmaManagerActor>,
    ) -> Result<Vec<RdmaRemoteBuffer>, anyhow::Error> {
        let alloc = self
            .allocation
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("register called before allocate"))?;

        for (i, &(offset, size)) in buffers.iter().enumerate() {
            anyhow::ensure!(
                offset + size <= alloc.size(),
                "buffer {i} exceeds allocation: offset=0x{offset:x} size={size} padded={}",
                alloc.size()
            );
        }

        // CudaAllocator::allocate already retained the primary context; set it
        // as current so subsequent CUDA calls on this thread use the right device.
        unsafe {
            let mut dev: rdmaxcel_sys::CUdevice = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuDeviceGet(&mut dev, self.device));
            let mut ctx: rdmaxcel_sys::CUcontext = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuDevicePrimaryCtxRetain(
                &mut ctx, dev
            ));
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(ctx));
        }

        let handle = rdma_manager
            .downcast_handle(cx)
            .ok_or_else(|| anyhow::anyhow!("failed to get rdma handle"))?;

        let mut remotes = Vec::with_capacity(buffers.len());
        for &(offset, size) in &buffers {
            let local: Arc<dyn RdmaLocalMemory> =
                Arc::new(UnsafeLocalMemory::new(alloc.ptr() + offset, size));
            remotes.push(handle.request_buffer(cx, local).await?);
        }

        Ok(remotes)
    }

    async fn free_allocation(&mut self, _cx: &Context<Self>) -> Result<(), anyhow::Error> {
        if let Some(alloc) = self.allocation.take() {
            alloc.try_free();
        }
        Ok(())
    }
}

/// Runs in the receiver's child process. Allocates a CPU buffer and performs
/// an RDMA read from the sender's GPU memory.
#[hyperactor::export(spawn = true, handlers = [ReceiverMessage])]
#[derive(Debug)]
struct ReceiverActor;

impl Actor for ReceiverActor {}

#[async_trait]
impl RemoteSpawn for ReceiverActor {
    type Params = ();

    async fn new((): (), _env: Flattrs) -> Result<Self, anyhow::Error> {
        Ok(Self)
    }
}

#[derive(
    Handler,
    RefClient,
    typeuri::Named,
    serde::Serialize,
    serde::Deserialize,
    Debug
)]
enum ReceiverMessage {
    ReadRemote {
        remote: RdmaRemoteBuffer,
        size: usize,
        timeout_secs: u64,
        #[reply]
        reply: reference::OncePortRef<Result<(), String>>,
    },
}

#[async_trait]
#[hyperactor::handle(ReceiverMessage)]
impl ReceiverMessageHandler for ReceiverActor {
    async fn read_remote(
        &mut self,
        cx: &Context<Self>,
        remote: RdmaRemoteBuffer,
        size: usize,
        timeout_secs: u64,
    ) -> Result<Result<(), String>, anyhow::Error> {
        let buf = vec![0u8; size].into_boxed_slice();
        let ptr = Box::into_raw(buf) as *mut u8 as usize;
        let local: Arc<dyn RdmaLocalMemory> = Arc::new(UnsafeLocalMemory::new(ptr, size));

        let result = remote
            .read_into_local(cx, local, timeout_secs)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string());

        // SAFETY: ptr was obtained from Box::into_raw above with the same size.
        unsafe {
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                ptr as *mut u8,
                size,
            )));
        }

        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
#[cfg(not(test_8_gpus))]
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
#[cfg(test_8_gpus)]
#[timed_test::async_timed_test(timeout_secs = 60)]
async fn test_multi_pd_segment_registration() -> Result<(), anyhow::Error> {
    use crate::backend::ibverbs::primitives::mlx5dv_supported;

    if !crate::is_cuda_available() {
        panic!("SKIPPED: CUDA not available (required for GPU memory allocation)");
    }
    if !mlx5dv_supported() {
        panic!("SKIPPED: mlx5dv not supported (required for indirect mkey creation)");
    }

    let (device_a, device_b) = match find_devices_on_different_nics() {
        Some(pair) => pair,
        None => panic!("SKIPPED: need at least 2 CUDA devices on different RDMA NICs"),
    };

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
