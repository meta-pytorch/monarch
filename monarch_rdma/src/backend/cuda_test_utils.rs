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
use crate::register_segment_scanner;

/// One physical chunk mapped into an expandable segment's VA
/// reservation. Owns a single `cuMemCreate` handle.
#[derive(Debug)]
struct MappedChunk {
    offset: usize,
    size: usize,
    handle: u64,
}

#[derive(Debug)]
struct CudaAllocationInner {
    /// Start of the VA reservation; stable for the allocation's life.
    ptr: usize,
    /// Total VA extent reserved; `mapped_size` grows up to this.
    reserved_size: usize,
    device: i32,
    /// Driver-reported granularity; commit/expand sizes round up to
    /// the next multiple.
    granularity: usize,
    state: Mutex<MappedState>,
}

#[derive(Debug)]
struct MappedState {
    /// Ordered chunks; each must be unmapped before the VA reservation
    /// is freed.
    chunks: Vec<MappedChunk>,
    /// Sum of chunk sizes — what the scanner reports and what
    /// `CudaAllocation::size` returns.
    mapped_size: usize,
}

impl Drop for CudaAllocationInner {
    fn drop(&mut self) {
        CudaAllocator::get().free(self);
    }
}

/// CUDA VMM allocation modelled on PyTorch's expandable segments:
/// a large VA range reserved up front, with physical memory mapped
/// in on demand via [`Self::expand`]. `ptr` is stable; `size()`
/// grows. Refcounted; dropped on last clone.
#[derive(Clone, Debug)]
pub struct CudaAllocation {
    inner: Arc<CudaAllocationInner>,
}

impl CudaAllocation {
    pub fn ptr(&self) -> usize {
        self.inner.ptr
    }

    /// Currently mapped extent. Use this — not [`Self::reserved_size`]
    /// — to bounds-check buffer offsets; addresses past `ptr + size()`
    /// are reserved but unmapped.
    pub fn size(&self) -> usize {
        self.inner.state.lock().unwrap().mapped_size
    }

    /// Total reserved VA extent. Always >= [`Self::size`].
    pub fn reserved_size(&self) -> usize {
        self.inner.reserved_size
    }

    /// Driver-reported allocation granularity. [`Self::expand`]
    /// rounds `additional_size` up to a multiple of this.
    pub fn granularity(&self) -> usize {
        self.inner.granularity
    }

    /// Map another `additional_size` bytes (rounded up to a granularity
    /// multiple) into the trailing reserved VA region and return the
    /// new total mapped size. Errors if `additional_size` is zero or
    /// the rounded total would exceed `reserved_size`.
    ///
    /// Scanner sees the same `ptr`, just a larger `size`.
    pub fn expand(&self, additional_size: usize) -> Result<usize, anyhow::Error> {
        anyhow::ensure!(
            additional_size > 0,
            "expand: additional_size must be positive (got 0)",
        );
        let granularity = self.inner.granularity;
        let padded = additional_size.div_ceil(granularity) * granularity;

        let mut state = self.inner.state.lock().unwrap();
        let new_total = state
            .mapped_size
            .checked_add(padded)
            .ok_or_else(|| anyhow::anyhow!("expand: mapped_size overflow"))?;
        anyhow::ensure!(
            new_total <= self.inner.reserved_size,
            "expand: would map {} bytes past reservation ({} reserved, {} already mapped)",
            padded,
            self.inner.reserved_size,
            state.mapped_size,
        );

        let offset = state.mapped_size;
        let chunk_addr = self.inner.ptr + offset;

        // SAFETY: [offset, offset + padded) is within the live VA
        // reservation and currently unmapped (the inner `Arc` keeps
        // the reservation alive past every chunk).
        unsafe {
            let _ctx_guard = crate::local_memory::set_ctx_for_ptr(self.inner.ptr)
                .map_err(|e| anyhow::anyhow!("set CUDA context for expand: {e}"))?;

            let mut prop: rdmaxcel_sys::CUmemAllocationProp = std::mem::zeroed();
            prop.type_ = rdmaxcel_sys::CU_MEM_ALLOCATION_TYPE_PINNED;
            prop.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
            prop.location.id = self.inner.device;
            prop.allocFlags.gpuDirectRDMACapable = 1;
            prop.requestedHandleTypes = rdmaxcel_sys::CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;

            let mut handle: rdmaxcel_sys::CUmemGenericAllocationHandle = std::mem::zeroed();
            let r = rdmaxcel_sys::rdmaxcel_cuMemCreate(&mut handle, padded, &prop, 0);
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                anyhow::bail!("cuMemCreate (expand): {}", cuda_err(r));
            }

            let r = rdmaxcel_sys::rdmaxcel_cuMemMap(
                chunk_addr as rdmaxcel_sys::CUdeviceptr,
                padded,
                0,
                handle,
                0,
            );
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                rdmaxcel_sys::rdmaxcel_cuMemRelease(handle);
                anyhow::bail!("cuMemMap (expand): {}", cuda_err(r));
            }

            let mut access: rdmaxcel_sys::CUmemAccessDesc = std::mem::zeroed();
            access.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
            access.location.id = self.inner.device;
            access.flags = rdmaxcel_sys::CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
            let r = rdmaxcel_sys::rdmaxcel_cuMemSetAccess(
                chunk_addr as rdmaxcel_sys::CUdeviceptr,
                padded,
                &access,
                1,
            );
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                rdmaxcel_sys::rdmaxcel_cuMemUnmap(chunk_addr as rdmaxcel_sys::CUdeviceptr, padded);
                rdmaxcel_sys::rdmaxcel_cuMemRelease(handle);
                anyhow::bail!("cuMemSetAccess (expand): {}", cuda_err(r));
            }

            state.chunks.push(MappedChunk {
                offset,
                size: padded,
                handle,
            });
            state.mapped_size = new_total;
        }
        Ok(new_total)
    }

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

    /// Allocate an expandable CUDA segment on `device`: reserve
    /// `reserved_size` bytes of VA and commit `initial_committed_size`
    /// at offset 0. [`CudaAllocation::expand`] commits more later.
    ///
    /// Both sizes must be positive and round up to the device
    /// granularity, with `initial_committed_size <= reserved_size`
    /// after rounding. Panics on FFI failure.
    pub fn allocate(
        &self,
        device: i32,
        reserved_size: usize,
        initial_committed_size: usize,
    ) -> CudaAllocation {
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

            assert!(reserved_size > 0, "reserved_size must be positive");
            assert!(
                initial_committed_size > 0,
                "initial_committed_size must be positive",
            );
            let padded_reserved = reserved_size.div_ceil(granularity) * granularity;
            let padded_initial = initial_committed_size.div_ceil(granularity) * granularity;
            assert!(
                padded_initial <= padded_reserved,
                "initial_committed_size {initial_committed_size} (padded {padded_initial}) > \
                 reserved_size {reserved_size} (padded {padded_reserved})",
            );

            // Reserve the full VA range up front so `expand` can
            // contiguously fill in.
            let mut dptr: rdmaxcel_sys::CUdeviceptr = std::mem::zeroed();
            let r = rdmaxcel_sys::rdmaxcel_cuMemAddressReserve(&mut dptr, padded_reserved, 0, 0, 0);
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                panic!("cuMemAddressReserve: {}", cuda_err(r));
            }

            // Commit the initial chunk at offset 0.
            let mut handle: rdmaxcel_sys::CUmemGenericAllocationHandle = std::mem::zeroed();
            let r = rdmaxcel_sys::rdmaxcel_cuMemCreate(&mut handle, padded_initial, &prop, 0);
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                rdmaxcel_sys::rdmaxcel_cuMemAddressFree(dptr, padded_reserved);
                panic!("cuMemCreate: {}", cuda_err(r));
            }

            let r = rdmaxcel_sys::rdmaxcel_cuMemMap(dptr, padded_initial, 0, handle, 0);
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                rdmaxcel_sys::rdmaxcel_cuMemRelease(handle);
                rdmaxcel_sys::rdmaxcel_cuMemAddressFree(dptr, padded_reserved);
                panic!("cuMemMap: {}", cuda_err(r));
            }

            let mut access: rdmaxcel_sys::CUmemAccessDesc = std::mem::zeroed();
            access.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
            access.location.id = device;
            access.flags = rdmaxcel_sys::CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
            let r = rdmaxcel_sys::rdmaxcel_cuMemSetAccess(dptr, padded_initial, &access, 1);
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                rdmaxcel_sys::rdmaxcel_cuMemUnmap(dptr, padded_initial);
                rdmaxcel_sys::rdmaxcel_cuMemRelease(handle);
                rdmaxcel_sys::rdmaxcel_cuMemAddressFree(dptr, padded_reserved);
                panic!("cuMemSetAccess: {}", cuda_err(r));
            }

            let ptr = dptr as usize;
            let inner = Arc::new(CudaAllocationInner {
                ptr,
                reserved_size: padded_reserved,
                device,
                granularity,
                state: Mutex::new(MappedState {
                    chunks: vec![MappedChunk {
                        offset: 0,
                        size: padded_initial,
                        handle,
                    }],
                    mapped_size: padded_initial,
                }),
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

        // Unmap each chunk individually (the unmapped portion of the
        // reservation isn't safe to touch), then free the VA range.
        let chunks = std::mem::take(&mut inner.state.lock().unwrap().chunks);
        unsafe {
            for chunk in chunks {
                cu_check!(rdmaxcel_sys::rdmaxcel_cuMemUnmap(
                    (inner.ptr + chunk.offset) as rdmaxcel_sys::CUdeviceptr,
                    chunk.size,
                ));
                cu_check!(rdmaxcel_sys::rdmaxcel_cuMemRelease(chunk.handle));
            }
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemAddressFree(
                inner.ptr as rdmaxcel_sys::CUdeviceptr,
                inner.reserved_size,
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
        // Report the *currently mapped* extent, not the reserved
        // VA. Expandable segments grow this on each `expand` call.
        let mapped_size = inner.state.lock().unwrap().mapped_size;
        if !out.is_null() && written < max {
            // SAFETY: caller guarantees `out` points to a buffer of at least `max` entries.
            unsafe {
                *out.add(written) = rdmaxcel_sys::rdmaxcel_scanned_segment_t {
                    address: inner.ptr,
                    size: mapped_size,
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
    allocations: Vec<CudaAllocation>,
}

impl Actor for SenderActor {}

#[async_trait]
impl RemoteSpawn for SenderActor {
    type Params = i32;

    async fn new(device_id: i32, _env: Flattrs) -> Result<Self, anyhow::Error> {
        register_segment_scanner(Some(cuda_allocator_scanner));
        Ok(Self {
            device: device_id,
            allocations: Vec::new(),
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
    /// Allocate an expandable segment and return its index for use
    /// with `Register` and `Expand`.
    Allocate {
        reserved_size: usize,
        initial_committed_size: usize,
        #[reply]
        reply: OncePortRef<usize>,
    },
    /// Commit another `additional_size` bytes into the segment at
    /// `allocation_idx`; returns the new mapped extent.
    Expand {
        allocation_idx: usize,
        additional_size: usize,
        #[reply]
        reply: OncePortRef<usize>,
    },
    /// Register sub-buffers carved out of allocation `allocation_idx`
    /// with the RDMA manager. Each buffer's backing bytes are pre-
    /// filled with `pattern` so reads return a known sequence.
    Register {
        allocation_idx: usize,
        buffers: Vec<(usize, usize)>,
        pattern: u8,
        rdma_manager: ActorRef<RdmaManagerActor>,
        #[reply]
        reply: OncePortRef<Vec<RdmaRemoteBuffer>>,
    },
    FreeAllocations {
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
        reserved_size: usize,
        initial_committed_size: usize,
    ) -> Result<usize, anyhow::Error> {
        let idx = self.allocations.len();
        self.allocations.push(CudaAllocator::get().allocate(
            self.device,
            reserved_size,
            initial_committed_size,
        ));
        Ok(idx)
    }

    async fn expand(
        &mut self,
        _cx: &Context<Self>,
        allocation_idx: usize,
        additional_size: usize,
    ) -> Result<usize, anyhow::Error> {
        let alloc = self.allocations.get(allocation_idx).ok_or_else(|| {
            anyhow::anyhow!(
                "expand called with allocation_idx={allocation_idx} but only \
                 {} allocations exist",
                self.allocations.len()
            )
        })?;
        alloc.expand(additional_size)
    }

    async fn register(
        &mut self,
        cx: &Context<Self>,
        allocation_idx: usize,
        buffers: Vec<(usize, usize)>,
        pattern: u8,
        rdma_manager: ActorRef<RdmaManagerActor>,
    ) -> Result<Vec<RdmaRemoteBuffer>, anyhow::Error> {
        let alloc = self
            .allocations
            .get(allocation_idx)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "register called with allocation_idx={allocation_idx} but only \
                     {} allocations exist",
                    self.allocations.len()
                )
            })?
            .clone();

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
            let local: Arc<KeepaliveLocalMemory> = Arc::new(KeepaliveLocalMemory::new(
                alloc.ptr() + offset,
                size,
                Arc::new(alloc.clone()),
            ));
            // Pre-fill so reads return a known sequence; write_at
            // routes to the GPU path for CUDA-backed memory.
            let fill = vec![pattern; size];
            // SAFETY: `local` is freshly constructed and not yet
            // registered, so no other thread holds a view of this
            // sub-range of `alloc` while we initialize it.
            unsafe { local.write_at(0, &fill) }?;
            remotes.push(handle.request_buffer(cx, local).await?);
        }

        Ok(remotes)
    }

    async fn free_allocations(&mut self, _cx: &Context<Self>) -> Result<(), anyhow::Error> {
        for alloc in self.allocations.drain(..) {
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
    /// RDMA-read `size` bytes from `remote` and verify every byte of
    /// the local destination equals `expected_pattern`. Catches both
    /// WR-level failures and silent data corruption.
    ReadRemote {
        remote: RdmaRemoteBuffer,
        size: usize,
        expected_pattern: u8,
        timeout_secs: u64,
        #[reply]
        reply: OncePortRef<Result<(), String>>,
    },
    /// RDMA-write `size` bytes of `pattern` into `remote`.
    WriteRemote {
        remote: RdmaRemoteBuffer,
        size: usize,
        pattern: u8,
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
        expected_pattern: u8,
        timeout_secs: u64,
    ) -> Result<Result<(), String>, anyhow::Error> {
        // Pre-fill with the bitwise-NOT of `expected_pattern` so an
        // unwritten destination is distinguishable from a successful
        // read.
        let buf: Box<[u8]> = vec![!expected_pattern; size].into_boxed_slice();
        let addr = buf.as_ptr() as usize;
        let local: Arc<KeepaliveLocalMemory> =
            Arc::new(KeepaliveLocalMemory::new(addr, size, Arc::new(buf)));

        let read_result = remote
            .read_into_local(cx, Arc::clone(&local), timeout_secs)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string());
        if let Err(e) = read_result {
            return Ok(Err(e));
        }

        let mut got = vec![0u8; size];
        // SAFETY: `local` is the sole handle to this fresh CPU
        // allocation; the `read_into_local` call above has already
        // completed.
        if let Err(e) = unsafe { local.read_at(0, &mut got) } {
            return Ok(Err(format!("post-read inspect failed: {e}")));
        }
        if let Some(idx) = got.iter().position(|&b| b != expected_pattern) {
            return Ok(Err(format!(
                "pattern mismatch at byte {idx}: expected 0x{expected_pattern:02x}, \
                 got 0x{:02x}",
                got[idx]
            )));
        }
        Ok(Ok(()))
    }

    async fn write_remote(
        &mut self,
        cx: &Context<Self>,
        remote: RdmaRemoteBuffer,
        size: usize,
        pattern: u8,
        timeout_secs: u64,
    ) -> Result<Result<(), String>, anyhow::Error> {
        let buf: Box<[u8]> = vec![pattern; size].into_boxed_slice();
        let addr = buf.as_ptr() as usize;
        let local: Arc<KeepaliveLocalMemory> =
            Arc::new(KeepaliveLocalMemory::new(addr, size, Arc::new(buf)));

        let result = remote
            .write_from_local(cx, local, timeout_secs)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string());

        Ok(result)
    }
}
