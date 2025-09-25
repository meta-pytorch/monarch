/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This file contains the implementation of the MR Registry, which manages the caching, reusing and registration of memory regions.
//!
//! - `RdmaMemoryRegion`: Represents a memory region that is registered with an RDMA device for direct memory access operations
//! - `RdmaMemoryRegionView`: Represents a view to a registered memory region, allowing multiple views into a single large MR registration.
//!   memory access operations. This retains shared ownership of the underlying `RdmaMemoryRegion` and is used to access different parts of the same memory region.

use std::collections::HashMap;
use std::ops::Drop;
use std::result::Result;
use std::sync::Arc;

use hyperactor::Actor;
use serde::Deserialize;
use serde::Serialize;

use crate::rdma_components::RdmaDomain;
use crate::utils::SerializablePointer;

/// Handles the caching, reusing and registration of memory regions.
#[derive(Debug, Clone)]
pub struct MemoryRegionRegistry {
    /// The RDMA domain that this registry is associated with.
    domain: Arc<RdmaDomain>,
    // (addr, size) -> RdmaMemoryRegionView
    mrs: HashMap<(usize, usize), RdmaMemoryRegionView>,
}

impl MemoryRegionRegistry {
    pub fn new(domain: Arc<RdmaDomain>) -> Self {
        Self {
            domain,
            mrs: HashMap::new(),
        }
    }
    /// Creates and registers a new `RdmaMemoryRegion` with the given address and size.
    pub fn create_memory_region(
        &self,
        addr: usize,
        size: usize,
    ) -> Result<Arc<RdmaMemoryRegion>, anyhow::Error> {
        tracing::debug!("Creating memory region for addr: {addr:#x}, size: {size}");
        let region = unsafe {
            let mut mem_type: i32 = 0;
            let ptr = addr as cuda_sys::CUdeviceptr;
            let err = cuda_sys::cuPointerGetAttribute(
                &mut mem_type as *mut _ as *mut std::ffi::c_void,
                cuda_sys::CUpointer_attribute_enum::CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
                ptr,
            );
            let is_cuda = err == cuda_sys::CUresult::CUDA_SUCCESS;

            let access = rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;

            let mut mr: *mut rdmaxcel_sys::ibv_mr = std::ptr::null_mut();
            if is_cuda {
                let mut fd: i32 = -1;
                cuda_sys::cuMemGetHandleForAddressRange(
                    &mut fd as *mut i32 as *mut std::ffi::c_void,
                    addr as cuda_sys::CUdeviceptr,
                    size,
                    cuda_sys::CUmemRangeHandleType::CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
                    0,
                );
                mr = rdmaxcel_sys::ibv_reg_dmabuf_mr(
                    self.domain.pd,
                    0,
                    size,
                    0,
                    fd,
                    access.0 as i32,
                );
            } else {
                // CPU memory path
                mr = rdmaxcel_sys::ibv_reg_mr(
                    self.domain.pd,
                    addr as *mut std::ffi::c_void,
                    size,
                    access.0 as i32,
                );
            }

            if mr.is_null() {
                return Err(anyhow::anyhow!("failed to register memory region (MR)"));
            }

            RdmaMemoryRegion {
                virtual_addr: addr,
                rdma_addr: (*mr).addr as usize,
                size,
                lkey: (*mr).lkey,
                rkey: (*mr).rkey,
                ibv_mr: SerializablePointer::new(mr),
            }
        };
        tracing::debug!("Created memory region for addr: {addr:#x}, size: {size}, mr: {region:#?}");
        Ok(Arc::new(region))
    }
    /// Returns a RdmaMemoryRegionView for the given address and size.
    /// This handles reusing/caching of memory regions (not implemented yet).
    pub fn request_mrv(
        &mut self,
        addr: usize,
        size: usize,
    ) -> Result<RdmaMemoryRegionView, anyhow::Error> {
        // TODO: Add caching logic that reuses existing memory regions if possible.
        if let Some(mrv) = self.mrs.get(&(addr, size)) {
            Ok(mrv.clone())
        } else {
            let mr = self.create_memory_region(addr, size)?;
            let mrv = RdmaMemoryRegionView {
                memory_region: mr,
                offset: 0,
                size,
            };
            self.mrs.insert((addr, size), mrv.clone());
            Ok(mrv)
        }
    }
}

/// Represents a memory region that is registered with the RDMA device.
/// This struct shouldn't be created directly, instead use `MemoryRegionRegistry::create()`
/// and should always be wrapped in an `Arc` to ensure proper ownership and lifetime management.
#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct RdmaMemoryRegion {
    /// Virtual address in the process address space.
    /// This is the pointer/address as seen by the local process.
    pub virtual_addr: usize,
    pub rdma_addr: usize,
    pub size: usize,
    pub lkey: u32,
    pub rkey: u32,
    pub ibv_mr: SerializablePointer<rdmaxcel_sys::ibv_mr>,
}

impl std::fmt::Debug for RdmaMemoryRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaMemoryRegion")
            .field("virtual_addr", &format_args!("{:#x}", self.virtual_addr))
            .field("rdma_addr", &format_args!("{:#x}", self.rdma_addr))
            .field("size", &self.size)
            .field("lkey", &self.lkey)
            .field("rkey", &self.rkey)
            .field("ibv_mr", &self.ibv_mr)
            .finish()
    }
}

// SAFETY: RdmaMemoryRegion can be safely sent and shared between threads because it only
// contains address and size information without any thread-local state. However,
// this DOES NOT provide any protection against data races in the underlying memory.
// If one thread initiates an RDMA operation while another thread modifies the same
// memory region, undefined behavior will occur. The caller is responsible for proper
// synchronization of access to the underlying memory.
unsafe impl Send for RdmaMemoryRegion {}
unsafe impl Sync for RdmaMemoryRegion {}

impl Drop for RdmaMemoryRegion {
    fn drop(&mut self) {
        if !self.ibv_mr.is_null() {
            unsafe {
                rdmaxcel_sys::ibv_dereg_mr(self.ibv_mr.as_ptr());
            }
            tracing::debug!("Deregistered MR: {:#?}", self);
        } else {
            tracing::debug!("MR already deregistered: {:#?}", self);
        }
    }
}

/// This is a 'view' of a registered Memory Region, allowing multiple views into a single
/// large MR registration. This is commonly used with PyTorch's caching allocator, which
/// reserves large memory blocks and provides different data pointers into that space.
///
/// # Example
/// PyTorch Caching Allocator creates a 16GB segment at virtual address `0x01000000`.
/// The underlying Memory Region registers 16GB but at RDMA address `0x0`.
/// To access virtual address `0x01100000`, we return a view at RDMA address `0x100000`.
///
/// # Safety
/// The caller must ensure the memory remains valid and is not freed, moved, or
/// overwritten while RDMA operations are in progress.

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct RdmaMemoryRegionView {
    pub memory_region: Arc<RdmaMemoryRegion>,
    // Offest of this view from the base of the MR address space.
    pub offset: usize,
    pub size: usize,
}
