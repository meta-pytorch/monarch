/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tests for mlx5dv-specific functionality (indirect mkeys, segment scanning).

use std::sync::Arc;
use std::sync::OnceLock;

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

use crate::IbvConfig;
use crate::RdmaManagerActor;
use crate::RdmaManagerMessageClient;
use crate::RdmaRemoteBuffer;
use crate::local_memory::RdmaLocalMemory;
use crate::local_memory::UnsafeLocalMemory;
use crate::register_segment_scanner;

// ---------------------------------------------------------------------------
// Helpers: segment scanner, sender actor, receiver actor
// ---------------------------------------------------------------------------

static SCANNER_CFG: OnceLock<(usize, usize, i32)> = OnceLock::new();

unsafe extern "C" fn test_scanner(
    out: *mut rdmaxcel_sys::rdmaxcel_scanned_segment_t,
    max: usize,
) -> usize {
    let Some(&(base, size, device)) = SCANNER_CFG.get() else {
        return 0;
    };
    if max == 0 || out.is_null() {
        return 1;
    }
    // SAFETY: caller guarantees `out` points to a buffer of at least `max` entries.
    unsafe {
        *out = rdmaxcel_sys::rdmaxcel_scanned_segment_t {
            address: base,
            size,
            device,
            is_expandable: 0,
        };
    }
    1
}

/// Runs in the sender's child process. Initializes CUDA, allocates GPU memory,
/// registers the segment scanner, and registers sub-buffers with the RDMA manager.
#[hyperactor::export(spawn = true, handlers = [SenderMessage])]
#[derive(Debug)]
struct SenderActor {
    device: i32,
    // CUcontext stored as usize so we don't need a Send+Sync newtype wrapper.
    cuda_ctx: usize,
}

impl Actor for SenderActor {}

#[async_trait]
impl RemoteSpawn for SenderActor {
    type Params = i32;

    async fn new(device_id: i32, _env: Flattrs) -> Result<Self, anyhow::Error> {
        unsafe {
            cu_check!(rdmaxcel_sys::rdmaxcel_cuInit(0));
            let mut dev: rdmaxcel_sys::CUdevice = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuDeviceGet(&mut dev, device_id));
            let mut ctx: rdmaxcel_sys::CUcontext = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxCreate_v2(
                &mut ctx, 0, device_id
            ));
            Ok(Self {
                device: device_id,
                cuda_ctx: ctx as usize,
            })
        }
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
    AllocateAndRegister {
        total_size: usize,
        buf0_offset: usize,
        buf0_size: usize,
        buf1_offset: usize,
        buf1_size: usize,
        rdma_manager: reference::ActorRef<RdmaManagerActor>,
        #[reply]
        reply: reference::OncePortRef<(RdmaRemoteBuffer, RdmaRemoteBuffer)>,
    },
}

#[async_trait]
#[hyperactor::handle(SenderMessage)]
impl SenderMessageHandler for SenderActor {
    async fn allocate_and_register(
        &mut self,
        cx: &Context<Self>,
        total_size: usize,
        buf0_offset: usize,
        buf0_size: usize,
        buf1_offset: usize,
        buf1_size: usize,
        rdma_manager: reference::ActorRef<RdmaManagerActor>,
    ) -> Result<(RdmaRemoteBuffer, RdmaRemoteBuffer), anyhow::Error> {
        let (dptr, padded_size) = unsafe {
            let ctx = self.cuda_ctx as rdmaxcel_sys::CUcontext;
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(ctx));

            let mut granularity: usize = 0;
            let mut prop: rdmaxcel_sys::CUmemAllocationProp = std::mem::zeroed();
            prop.type_ = rdmaxcel_sys::CU_MEM_ALLOCATION_TYPE_PINNED;
            prop.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
            prop.location.id = self.device;
            prop.allocFlags.gpuDirectRDMACapable = 1;
            prop.requestedHandleTypes = rdmaxcel_sys::CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;

            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemGetAllocationGranularity(
                &mut granularity,
                &prop,
                rdmaxcel_sys::CU_MEM_ALLOC_GRANULARITY_MINIMUM,
            ));

            let padded = ((total_size - 1) / granularity + 1) * granularity;

            let mut handle: rdmaxcel_sys::CUmemGenericAllocationHandle = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemCreate(
                &mut handle,
                padded,
                &prop,
                0
            ));

            let mut dptr: rdmaxcel_sys::CUdeviceptr = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemAddressReserve(
                &mut dptr, padded, 0, 0, 0,
            ));

            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemMap(dptr, padded, 0, handle, 0));

            let mut access: rdmaxcel_sys::CUmemAccessDesc = std::mem::zeroed();
            access.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
            access.location.id = self.device;
            access.flags = rdmaxcel_sys::CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemSetAccess(
                dptr, padded, &access, 1
            ));

            (dptr, padded)
        };

        let base = dptr as usize;
        SCANNER_CFG
            .set((base, padded_size, self.device))
            .expect("scanner config already set");
        register_segment_scanner(Some(test_scanner));

        anyhow::ensure!(
            buf0_offset + buf0_size <= padded_size && buf1_offset + buf1_size <= padded_size,
            "buffer offsets exceed allocation: padded={padded_size} \
             buf0=[0x{buf0_offset:x},{buf0_size}] buf1=[0x{buf1_offset:x},{buf1_size}]"
        );

        let handle = rdma_manager
            .downcast_handle(cx)
            .ok_or_else(|| anyhow::anyhow!("failed to get rdma handle"))?;

        let buf0_local: Arc<dyn RdmaLocalMemory> =
            Arc::new(UnsafeLocalMemory::new(base + buf0_offset, buf0_size));
        let remote0 = handle.request_buffer(cx, buf0_local).await?;

        let buf1_local: Arc<dyn RdmaLocalMemory> =
            Arc::new(UnsafeLocalMemory::new(base + buf1_offset, buf1_size));
        let remote1 = handle.request_buffer(cx, buf1_local).await?;

        Ok((remote0, remote1))
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

    let (remote_buf0, remote_buf1) = sender
        .allocate_and_register(
            instance,
            SEGMENT_SIZE,
            0,
            BUF0_SIZE,
            BUF1_OFFSET,
            BUF1_SIZE,
            sender_rdma_ref,
        )
        .await?;

    // Read at offset 0 — should always work.
    let buf0_result = receiver
        .read_remote(instance, remote_buf0, BUF0_SIZE, 10)
        .await?;
    assert!(
        buf0_result.is_ok(),
        "RDMA read at offset 0 failed: {:?}",
        buf0_result.unwrap_err()
    );

    // Read at offset 0x66000000 (1.71 GB) — fails without the rdma-core fix.
    let buf1_result = receiver
        .read_remote(instance, remote_buf1, BUF1_SIZE, 10)
        .await?;
    assert!(
        buf1_result.is_ok(),
        "RDMA read at offset 0x{:x} failed (likely rdma-core umr_sg_list_create \
         int overflow bug — see D84387295): {:?}",
        BUF1_OFFSET,
        buf1_result.unwrap_err()
    );

    let _ = host_mesh.shutdown(instance).await;
    Ok(())
}
