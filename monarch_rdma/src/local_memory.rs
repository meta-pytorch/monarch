/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Local memory abstractions for RDMA operations.
//!
//! This module defines the [`RdmaLocalMemory`] trait and its implementations:
//!
//! - [`KeepaliveLocalMemory`] – wraps a raw pointer with a keepalive guard
//!   and dispatches reads/writes to CPU or CUDA paths.
//! - [`UnsafeLocalMemory`] – raw pointer-based handle where the caller is
//!   responsible for lifetime management.

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::RwLock;

use serde::Deserialize;
use serde::Serialize;

/// Returns `true` when `addr` is a CUDA device pointer.
///
/// Probes the CUDA driver via `cuPointerGetAttribute`; returns `false`
/// when CUDA is unavailable or the pointer is not device memory.
pub fn is_device_ptr(addr: usize) -> bool {
    // SAFETY: FFI call that queries pointer metadata without accessing
    // the pointed-to memory.
    unsafe {
        let mut mem_type: u32 = 0;
        let err = rdmaxcel_sys::rdmaxcel_cuPointerGetAttribute(
            &mut mem_type as *mut _ as *mut std::ffi::c_void,
            rdmaxcel_sys::CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
            addr as rdmaxcel_sys::CUdeviceptr,
        );
        err == rdmaxcel_sys::CUDA_SUCCESS && mem_type == rdmaxcel_sys::CU_MEMORYTYPE_DEVICE
    }
}

/// Handle to a contiguous region of local memory.
///
/// Implementations must guarantee the underlying allocation is valid for the
/// lifetime of the implementor.
pub trait RdmaLocalMemory: Send + Sync + Debug {
    /// Starting virtual address of the memory region.
    fn addr(&self) -> usize;

    /// Size of the memory region in bytes.
    fn size(&self) -> usize;

    /// Copy `dst.len()` bytes from this memory region starting at `offset` into `dst`.
    fn read_at(&self, offset: usize, dst: &mut [u8]) -> Result<(), anyhow::Error>;

    /// Copy `src.len()` bytes from `src` into this memory region starting at `offset`.
    fn write_at(&self, offset: usize, src: &[u8]) -> Result<(), anyhow::Error>;
}

/// Verify that an access at `offset` with `len` bytes fits within `size`.
fn check_bounds(offset: usize, len: usize, size: usize) -> Result<(), anyhow::Error> {
    anyhow::ensure!(
        offset.checked_add(len).is_some_and(|end| end <= size),
        "access at offset {offset} with length {len} exceeds region size {size}"
    );
    Ok(())
}

/// Copy `dst.len()` bytes from host memory at `addr + offset` into `dst`.
///
/// # Safety
///
/// The caller must ensure that `addr` points to a valid host allocation of
/// at least `offset + dst.len()` bytes.
unsafe fn read_cpu(addr: usize, offset: usize, dst: &mut [u8]) {
    unsafe {
        std::ptr::copy_nonoverlapping((addr + offset) as *const u8, dst.as_mut_ptr(), dst.len());
    }
}

/// Copy `src.len()` bytes from `src` into host memory at `addr + offset`.
///
/// # Safety
///
/// The caller must ensure that `addr` points to a valid host allocation of
/// at least `offset + src.len()` bytes.
unsafe fn write_cpu(addr: usize, offset: usize, src: &[u8]) {
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), (addr + offset) as *mut u8, src.len());
    }
}

/// Copy `dst.len()` bytes from device memory at `addr + offset` into `dst`.
///
/// # Safety
///
/// The caller must ensure that `addr` is a valid CUDA device pointer to an
/// allocation of at least `offset + dst.len()` bytes.
unsafe fn read_gpu(addr: usize, offset: usize, dst: &mut [u8]) -> Result<(), anyhow::Error> {
    let rc = unsafe {
        rdmaxcel_sys::rdmaxcel_cuMemcpyDtoH_v2(
            dst.as_mut_ptr() as *mut std::ffi::c_void,
            (addr + offset) as rdmaxcel_sys::CUdeviceptr,
            dst.len(),
        )
    };
    anyhow::ensure!(
        rc == rdmaxcel_sys::CUDA_SUCCESS,
        "cuMemcpyDtoH failed with error code {rc}"
    );
    Ok(())
}

/// Copy `src.len()` bytes from `src` into device memory at `addr + offset`.
///
/// # Safety
///
/// The caller must ensure that `addr` is a valid CUDA device pointer to an
/// allocation of at least `offset + src.len()` bytes.
unsafe fn write_gpu(addr: usize, offset: usize, src: &[u8]) -> Result<(), anyhow::Error> {
    let rc = unsafe {
        rdmaxcel_sys::rdmaxcel_cuMemcpyHtoD_v2(
            (addr + offset) as rdmaxcel_sys::CUdeviceptr,
            src.as_ptr() as *const std::ffi::c_void,
            src.len(),
        )
    };
    anyhow::ensure!(
        rc == rdmaxcel_sys::CUDA_SUCCESS,
        "cuMemcpyHtoD failed with error code {rc}"
    );
    Ok(())
}

/// Marker trait: the implementor keeps a backing memory allocation alive.
///
/// As long as a value implementing this trait exists, the memory region
/// described by the containing [`KeepaliveLocalMemory`] is guaranteed to
/// remain valid.
pub trait Keepalive: Send + Sync {}

/// Local memory handle that keeps its backing allocation alive via an
/// [`Arc<dyn Keepalive>`].
///
/// Detects at construction time whether the address is a CUDA device
/// pointer and dispatches `read_at`/`write_at` accordingly.
///
/// The `direct_access_host_bandwidth` and `direct_access_device_bandwidth`
/// fields indicate the speed of reading the memory via pointer dereference
/// on a host or device thread, respectively. A value of `None` means the
/// memory is not directly accessible from that context.
#[derive(Clone)]
pub struct KeepaliveLocalMemory {
    addr: usize,
    size: usize,
    /// Bandwidth (bytes/s) for direct host-thread pointer access, or `None`
    /// if the memory is not host-accessible.
    direct_access_host_bandwidth: Option<u64>,
    /// Bandwidth (bytes/s) for direct device-thread pointer access, or
    /// `None` if the memory is not device-accessible.
    direct_access_device_bandwidth: Option<u64>,
    _keepalive: Arc<dyn Keepalive>,
    guard: Arc<RwLock<()>>,
}

impl Debug for KeepaliveLocalMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeepaliveLocalMemory")
            .field("addr", &self.addr)
            .field("size", &self.size)
            .field(
                "direct_access_host_bandwidth",
                &self.direct_access_host_bandwidth,
            )
            .field(
                "direct_access_device_bandwidth",
                &self.direct_access_device_bandwidth,
            )
            .finish_non_exhaustive()
    }
}

impl KeepaliveLocalMemory {
    /// Create a new handle. Probes the CUDA driver to determine whether
    /// `addr` is a device pointer and sets the bandwidth fields
    /// accordingly.
    pub fn new(addr: usize, size: usize, keepalive: Arc<dyn Keepalive>) -> Self {
        // TODO(slurye): Using placeholder values for now. Fill in with real values.
        let (host_bw, device_bw) = if is_device_ptr(addr) {
            (None, Some(1))
        } else {
            (Some(1), None)
        };
        Self {
            addr,
            size,
            direct_access_host_bandwidth: host_bw,
            direct_access_device_bandwidth: device_bw,
            _keepalive: keepalive,
            guard: Arc::new(RwLock::new(())),
        }
    }
}

impl RdmaLocalMemory for KeepaliveLocalMemory {
    fn addr(&self) -> usize {
        self.addr
    }

    fn size(&self) -> usize {
        self.size
    }

    fn read_at(&self, offset: usize, dst: &mut [u8]) -> Result<(), anyhow::Error> {
        let _lock = self.guard.read().expect("lock poisoned");
        check_bounds(offset, dst.len(), self.size)?;
        // SAFETY: The keepalive guard guarantees the allocation is live, and
        // check_bounds verified the access is in range.
        unsafe {
            if self.direct_access_host_bandwidth.is_some() {
                read_cpu(self.addr, offset, dst);
                Ok(())
            } else {
                read_gpu(self.addr, offset, dst)
            }
        }
    }

    fn write_at(&self, offset: usize, src: &[u8]) -> Result<(), anyhow::Error> {
        let _lock = self.guard.write().expect("lock poisoned");
        check_bounds(offset, src.len(), self.size)?;
        // SAFETY: The keepalive guard guarantees the allocation is live, and
        // check_bounds verified the access is in range.
        unsafe {
            if self.direct_access_host_bandwidth.is_some() {
                write_cpu(self.addr, offset, src);
                Ok(())
            } else {
                write_gpu(self.addr, offset, src)
            }
        }
    }
}

/// Raw pointer-based local memory handle that supports both CPU and GPU memory.
///
/// Wraps a virtual address and size. The caller is responsible for
/// ensuring the underlying allocation outlives this handle. Uses
/// `is_device_ptr` to dispatch reads/writes to the appropriate CPU or CUDA
/// path, just like [`KeepaliveLocalMemory`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsafeLocalMemory {
    pub addr: usize,
    pub size: usize,
}

impl UnsafeLocalMemory {
    pub fn new(addr: usize, size: usize) -> Self {
        Self { addr, size }
    }
}

impl RdmaLocalMemory for UnsafeLocalMemory {
    fn addr(&self) -> usize {
        self.addr
    }

    fn size(&self) -> usize {
        self.size
    }

    fn read_at(&self, offset: usize, dst: &mut [u8]) -> Result<(), anyhow::Error> {
        check_bounds(offset, dst.len(), self.size)?;
        // SAFETY: The caller is responsible for ensuring the allocation is
        // live; check_bounds verified the access is in range.
        unsafe {
            if is_device_ptr(self.addr) {
                read_gpu(self.addr, offset, dst)
            } else {
                read_cpu(self.addr, offset, dst);
                Ok(())
            }
        }
    }

    fn write_at(&self, offset: usize, src: &[u8]) -> Result<(), anyhow::Error> {
        check_bounds(offset, src.len(), self.size)?;
        // SAFETY: The caller is responsible for ensuring the allocation is
        // live; check_bounds verified the access is in range.
        unsafe {
            if is_device_ptr(self.addr) {
                write_gpu(self.addr, offset, src)
            } else {
                write_cpu(self.addr, offset, src);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- KeepaliveLocalMemory (host) --

    impl Keepalive for Vec<u8> {}

    fn host_keepalive_mem(data: Vec<u8>) -> KeepaliveLocalMemory {
        let addr = data.as_ptr() as usize;
        let size = data.len();
        KeepaliveLocalMemory::new(addr, size, Arc::new(data))
    }

    #[test]
    fn keepalive_host_read_at() {
        let mem = host_keepalive_mem(vec![1, 2, 3, 4, 5]);
        let mut buf = [0u8; 3];
        mem.read_at(1, &mut buf).unwrap();
        assert_eq!(buf, [2, 3, 4]);
    }

    #[test]
    fn keepalive_host_write_then_read() {
        let mem = host_keepalive_mem(vec![0; 5]);
        mem.write_at(1, &[7, 8, 9]).unwrap();
        let mut buf = [0u8; 5];
        mem.read_at(0, &mut buf).unwrap();
        assert_eq!(buf, [0, 7, 8, 9, 0]);
    }

    #[test]
    fn keepalive_host_out_of_bounds() {
        let mem = host_keepalive_mem(vec![0; 3]);
        let mut buf = [0u8; 3];
        assert!(mem.read_at(1, &mut buf).is_err());
        assert!(mem.write_at(1, &[7, 8, 9]).is_err());
    }
}
