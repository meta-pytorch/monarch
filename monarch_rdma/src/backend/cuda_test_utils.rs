/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Reusable CUDA VMM allocator, segment scanner, and sender/receiver test
//! actors for RDMA tests.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor_config::Flattrs;

use crate::RdmaManagerActor;
use crate::RdmaManagerMessageClient;
use crate::RdmaRemoteBuffer;
use crate::local_memory::Keepalive;
use crate::local_memory::KeepaliveLocalMemory;
use crate::local_memory::RdmaLocalMemory;
use crate::register_segment_scanner;

#[derive(Debug)]
struct CudaAllocationInner {
    ptr: usize,
    size: usize,
    device: i32,
    handle: u64,
}

impl Drop for CudaAllocationInner {
    fn drop(&mut self) {
        CudaAllocator::get().free(self);
    }
}

/// A CUDA device allocation that keeps its backing memory alive via
/// reference counting. When the last clone is dropped, the backing
/// CUDA resources are released through [`CudaAllocator`].
#[derive(Clone, Debug)]
pub struct CudaAllocation {
    inner: Arc<CudaAllocationInner>,
}

impl CudaAllocation {
    pub fn ptr(&self) -> usize {
        self.inner.ptr
    }

    pub fn size(&self) -> usize {
        self.inner.size
    }
}

impl CudaAllocation {
    /// Try to free the backing CUDA memory. Returns `true` if the
    /// resources were released, or `false` if other clones still exist.
    pub fn try_free(self) -> bool {
        match Arc::into_inner(self.inner) {
            Some(inner) => {
                CudaAllocator::get().free(&inner);
                // Prevent Drop from re-entering free.
                std::mem::forget(inner);
                true
            }
            None => false,
        }
    }
}

impl Keepalive for CudaAllocation {}

pub struct CudaAllocator {
    allocations: Mutex<HashMap<usize, Weak<CudaAllocationInner>>>,
}

static CUDA_ALLOCATOR: OnceLock<CudaAllocator> = OnceLock::new();

unsafe fn cuda_err(result: rdmaxcel_sys::CUresult) -> String {
    unsafe {
        let mut s: *const std::os::raw::c_char = std::ptr::null();
        rdmaxcel_sys::rdmaxcel_cuGetErrorString(result, &mut s);
        if s.is_null() {
            format!("CUDA error {result}")
        } else {
            std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
        }
    }
}

impl CudaAllocator {
    pub fn get() -> &'static CudaAllocator {
        CUDA_ALLOCATOR.get_or_init(|| {
            unsafe {
                cu_check!(rdmaxcel_sys::rdmaxcel_cuInit(0));
            }
            CudaAllocator {
                allocations: Mutex::new(HashMap::new()),
            }
        })
    }

    /// Allocate GPU memory on `device` of at least `size` bytes via the CUDA
    /// VMM API. Returns the device pointer. Panics on failure, cleaning up
    /// any partially-allocated resources.
    pub fn allocate(&self, device: i32, size: usize) -> CudaAllocation {
        unsafe {
            // Context setup — shared primary context, nothing to roll back.
            let mut dev: rdmaxcel_sys::CUdevice = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuDeviceGet(&mut dev, device));

            let mut ctx: rdmaxcel_sys::CUcontext = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuDevicePrimaryCtxRetain(
                &mut ctx, dev
            ));
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(ctx));

            let mut granularity: usize = 0;
            let mut prop: rdmaxcel_sys::CUmemAllocationProp = std::mem::zeroed();
            prop.type_ = rdmaxcel_sys::CU_MEM_ALLOCATION_TYPE_PINNED;
            prop.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
            prop.location.id = device;
            prop.allocFlags.gpuDirectRDMACapable = 1;
            prop.requestedHandleTypes = rdmaxcel_sys::CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;

            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemGetAllocationGranularity(
                &mut granularity,
                &prop,
                rdmaxcel_sys::CU_MEM_ALLOC_GRANULARITY_MINIMUM,
            ));

            let padded = size.div_ceil(granularity) * granularity;

            // From here each step acquires a resource; clean up predecessors
            // before panicking if a later step fails.
            let mut handle: rdmaxcel_sys::CUmemGenericAllocationHandle = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemCreate(
                &mut handle,
                padded,
                &prop,
                0
            ));

            let mut dptr: rdmaxcel_sys::CUdeviceptr = std::mem::zeroed();
            let r = rdmaxcel_sys::rdmaxcel_cuMemAddressReserve(&mut dptr, padded, 0, 0, 0);
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                rdmaxcel_sys::rdmaxcel_cuMemRelease(handle);
                panic!("cuMemAddressReserve: {}", cuda_err(r));
            }

            let r = rdmaxcel_sys::rdmaxcel_cuMemMap(dptr, padded, 0, handle, 0);
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                rdmaxcel_sys::rdmaxcel_cuMemRelease(handle);
                rdmaxcel_sys::rdmaxcel_cuMemAddressFree(dptr, padded);
                panic!("cuMemMap: {}", cuda_err(r));
            }

            let mut access: rdmaxcel_sys::CUmemAccessDesc = std::mem::zeroed();
            access.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
            access.location.id = device;
            access.flags = rdmaxcel_sys::CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
            let r = rdmaxcel_sys::rdmaxcel_cuMemSetAccess(dptr, padded, &access, 1);
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                rdmaxcel_sys::rdmaxcel_cuMemUnmap(dptr, padded);
                rdmaxcel_sys::rdmaxcel_cuMemRelease(handle);
                rdmaxcel_sys::rdmaxcel_cuMemAddressFree(dptr, padded);
                panic!("cuMemSetAccess: {}", cuda_err(r));
            }

            let ptr = dptr as usize;
            let inner = Arc::new(CudaAllocationInner {
                ptr,
                size: padded,
                device,
                handle,
            });
            self.allocations
                .lock()
                .unwrap()
                .insert(ptr, Arc::downgrade(&inner));
            CudaAllocation { inner }
        }
    }

    /// Remove the tracking entry for `inner` and release its CUDA
    /// resources. Validates that the tracked [`Weak`] has no remaining
    /// strong references.
    fn free(&self, inner: &CudaAllocationInner) {
        let weak = self
            .allocations
            .lock()
            .unwrap()
            .remove(&inner.ptr)
            .unwrap_or_else(|| panic!("unknown allocation 0x{:x}", inner.ptr));
        assert!(
            weak.strong_count() == 0,
            "allocation 0x{:x} still has live references",
            inner.ptr
        );
        // SAFETY: inner.ptr is a valid CUDA device pointer from allocate.
        let _ctx_guard = unsafe { crate::local_memory::set_ctx_for_ptr(inner.ptr) }
            .expect("failed to set CUDA context for deallocation");
        unsafe {
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemUnmap(
                inner.ptr as rdmaxcel_sys::CUdeviceptr,
                inner.size
            ));
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemRelease(inner.handle));
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemAddressFree(
                inner.ptr as rdmaxcel_sys::CUdeviceptr,
                inner.size
            ));
        }
    }
}

/// Number of CUDA devices visible to the driver, or 0 on failure.
#[cfg(test)]
pub(crate) fn cuda_device_count() -> i32 {
    unsafe {
        if rdmaxcel_sys::rdmaxcel_cuInit(0) != rdmaxcel_sys::CUDA_SUCCESS {
            return 0;
        }
        let mut count: i32 = 0;
        if rdmaxcel_sys::rdmaxcel_cuDeviceGetCount(&mut count) != rdmaxcel_sys::CUDA_SUCCESS {
            return 0;
        }
        count
    }
}

/// Segment scanner callback compatible with `rdmaxcel_segment_scanner_fn`.
/// Reports all allocations tracked by `CudaAllocator`.
pub unsafe extern "C" fn cuda_allocator_scanner(
    out: *mut rdmaxcel_sys::rdmaxcel_scanned_segment_t,
    max: usize,
) -> usize {
    let Some(allocator) = CUDA_ALLOCATOR.get() else {
        return 0;
    };
    let allocs = allocator.allocations.lock().unwrap();
    let mut written = 0;
    for weak in allocs.values() {
        let Some(inner) = weak.upgrade() else {
            continue;
        };
        if !out.is_null() && written < max {
            // SAFETY: caller guarantees `out` points to a buffer of at least `max` entries.
            unsafe {
                *out.add(written) = rdmaxcel_sys::rdmaxcel_scanned_segment_t {
                    address: inner.ptr,
                    size: inner.size,
                    device: inner.device,
                    is_expandable: 1,
                };
            }
        }
        written += 1;
    }
    written
}

/// Runs in the sender's child process. Allocates GPU memory via CudaAllocator,
/// registers sub-buffers with the RDMA manager, and can free the allocation on
/// request. Allocate and register are separate messages so that multiple senders
/// can allocate first (making all segments visible to the scanner) before any
/// of them trigger registration.
#[hyperactor::export(handlers = [SenderMessage])]
#[hyperactor::spawnable]
#[derive(Debug)]
pub struct SenderActor {
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
pub enum SenderMessage {
    /// Allocate GPU memory for later registration.
    Allocate {
        total_size: usize,
        #[reply]
        reply: OncePortRef<()>,
    },
    /// Register sub-buffers from the previous allocation with the RDMA manager.
    Register {
        buffers: Vec<(usize, usize)>,
        rdma_manager: ActorRef<RdmaManagerActor>,
        #[reply]
        reply: OncePortRef<Vec<RdmaRemoteBuffer>>,
    },
    FreeAllocation {
        #[reply]
        reply: OncePortRef<()>,
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
        rdma_manager: ActorRef<RdmaManagerActor>,
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

        let handle = rdma_manager
            .downcast_handle(cx)
            .ok_or_else(|| anyhow::anyhow!("failed to get rdma handle"))?;

        let mut remotes = Vec::with_capacity(buffers.len());
        for &(offset, size) in &buffers {
            let local: Arc<dyn RdmaLocalMemory> = Arc::new(KeepaliveLocalMemory::new(
                alloc.ptr() + offset,
                size,
                Arc::new(alloc.clone()),
            ));
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
#[hyperactor::export(handlers = [ReceiverMessage])]
#[hyperactor::spawnable]
#[derive(Debug)]
pub struct ReceiverActor;

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
pub enum ReceiverMessage {
    ReadRemote {
        remote: RdmaRemoteBuffer,
        size: usize,
        timeout_secs: u64,
        #[reply]
        reply: OncePortRef<Result<(), String>>,
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
        let buf: Box<[u8]> = vec![0u8; size].into_boxed_slice();
        let addr = buf.as_ptr() as usize;
        let local: Arc<dyn RdmaLocalMemory> =
            Arc::new(KeepaliveLocalMemory::new(addr, size, Arc::new(buf)));

        let result = remote
            .read_into_local(cx, local, timeout_secs)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string());

        Ok(result)
    }
}
