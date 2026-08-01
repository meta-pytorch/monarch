/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Local memory abstractions for RDMA operations.
//!
//! [`KeepaliveLocalMemory`] wraps a raw pointer with a [`Keepalive`]
//! guard and dispatches reads/writes to CPU or CUDA paths.

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::OnceLock;

use crate::backend::ibverbs::memory_region::IbvMemoryRegionView;

/// Returns `true` when `addr` is a CUDA device pointer.
///
/// Probes the CUDA driver via `cuPointerGetAttribute`; returns `false`
/// when CUDA is unavailable or the pointer is not device memory.
pub fn is_device_ptr(addr: usize) -> bool {
    // On ROCm, a primary HIP context is created even if the pointer is in host memory.
    // We only want to init primary HIP context if the pointer is actually on the device
    static RUNTIME_READY: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);
    if !RUNTIME_READY.load(std::sync::atomic::Ordering::Relaxed) {
        // SAFETY: queries runtime/context state only; creates no context and
        // does not access `addr`.
        if unsafe { rdmaxcel_sys::rdmaxcel_cuPrimaryCtxActive() } == 0 {
            return false;
        }
        RUNTIME_READY.store(true, std::sync::atomic::Ordering::Relaxed);
    }
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

/// RAII guard that restores the previous CUDA context on drop and, if a
/// primary context was retained, releases it.
pub(crate) struct CudaCtxGuard {
    prev: rdmaxcel_sys::CUcontext,
    /// Set when the fallback path called `cuDevicePrimaryCtxRetain`.
    retained_device: Option<rdmaxcel_sys::CUdevice>,
}

impl Drop for CudaCtxGuard {
    fn drop(&mut self) {
        unsafe {
            rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(self.prev);
            if let Some(device) = self.retained_device {
                rdmaxcel_sys::rdmaxcel_cuDevicePrimaryCtxRelease(device);
            }
        }
    }
}

/// Make the CUDA context that owns `addr` current on the calling
/// thread, returning a guard that restores the previous context on
/// drop.
///
/// First tries `CU_POINTER_ATTRIBUTE_CONTEXT` to get the exact context
/// the allocation belongs to.  When that returns null (runtime-API or
/// memory-pool allocations such as PyTorch's caching allocator), falls
/// back to the device's primary context via
/// `CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL` + `cuDevicePrimaryCtxRetain`.
///
/// # Safety
///
/// `addr` must be a valid CUDA device pointer.
pub(crate) unsafe fn set_ctx_for_ptr(addr: usize) -> Result<CudaCtxGuard, anyhow::Error> {
    let mut prev: rdmaxcel_sys::CUcontext = std::ptr::null_mut();
    unsafe {
        rdmaxcel_sys::rdmaxcel_cuCtxGetCurrent(&mut prev);
    }

    let mut ctx: rdmaxcel_sys::CUcontext = std::ptr::null_mut();
    let rc = unsafe {
        rdmaxcel_sys::rdmaxcel_cuPointerGetAttribute(
            &mut ctx as *mut _ as *mut std::ffi::c_void,
            rdmaxcel_sys::CU_POINTER_ATTRIBUTE_CONTEXT,
            addr as rdmaxcel_sys::CUdeviceptr,
        )
    };

    // Null context: allocation came from the runtime API or a memory
    // pool.  Fall back to the owning device's primary context.
    let mut retained_device = None;
    if rc != rdmaxcel_sys::CUDA_SUCCESS || ctx.is_null() {
        let mut ordinal: i32 = -1;
        let rc = unsafe {
            rdmaxcel_sys::rdmaxcel_cuPointerGetAttribute(
                &mut ordinal as *mut _ as *mut std::ffi::c_void,
                rdmaxcel_sys::CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL,
                addr as rdmaxcel_sys::CUdeviceptr,
            )
        };
        anyhow::ensure!(
            rc == rdmaxcel_sys::CUDA_SUCCESS,
            "cuPointerGetAttribute(DEVICE_ORDINAL) failed with error code {rc}"
        );

        let mut device: rdmaxcel_sys::CUdevice = 0;
        let rc = unsafe { rdmaxcel_sys::rdmaxcel_cuDeviceGet(&mut device, ordinal) };
        anyhow::ensure!(
            rc == rdmaxcel_sys::CUDA_SUCCESS,
            "cuDeviceGet({ordinal}) failed with error code {rc}"
        );

        let rc = unsafe { rdmaxcel_sys::rdmaxcel_cuDevicePrimaryCtxRetain(&mut ctx, device) };
        anyhow::ensure!(
            rc == rdmaxcel_sys::CUDA_SUCCESS,
            "cuDevicePrimaryCtxRetain failed with error code {rc}"
        );
        retained_device = Some(device);
    }

    let rc = unsafe { rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(ctx) };
    anyhow::ensure!(
        rc == rdmaxcel_sys::CUDA_SUCCESS,
        "cuCtxSetCurrent failed with error code {rc}"
    );

    Ok(CudaCtxGuard {
        prev,
        retained_device,
    })
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
    let _guard = unsafe { set_ctx_for_ptr(addr)? };
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
    let _guard = unsafe { set_ctx_for_ptr(addr)? };
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

/// Three-mode access lock used by [`KeepaliveLocalMemory`] to coordinate
/// concurrent reads, exclusive writes, and parallel "disjoint" writes
/// (writers that the caller has promised target disjoint ranges).
///
/// - [`AccessLock::read`] returns when no exclusive writer and no
///   disjoint writer is active. Multiple readers are permitted to hold
///   the lock at the same time.
/// - [`AccessLock::disjoint_write`] returns when no reader and no
///   exclusive writer is active. Multiple disjoint writers are
///   permitted to hold the lock at the same time.
/// - [`AccessLock::exclusive`] returns only when no one else holds the
///   lock.
///
/// Read mode and disjoint-write mode are mutually exclusive, which is
/// what gives readers a torn-free view of memory in the presence of
/// disjoint parallel writers.
#[derive(Debug, Default)]
struct AccessLock {
    state: Mutex<AccessState>,
    cond: Condvar,
}

#[derive(Debug, Default)]
enum AccessState {
    #[default]
    Idle,
    Reading(usize),
    DisjointWriting(usize),
    Exclusive,
}

impl AccessLock {
    fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> AccessReadGuard<'_> {
        let mut state = self.state.lock().expect("AccessLock poisoned");
        loop {
            match &mut *state {
                AccessState::Idle => {
                    *state = AccessState::Reading(1);
                    return AccessReadGuard(self);
                }
                AccessState::Reading(n) => {
                    *n += 1;
                    return AccessReadGuard(self);
                }
                AccessState::DisjointWriting(_) | AccessState::Exclusive => {
                    state = self.cond.wait(state).expect("AccessLock poisoned");
                }
            }
        }
    }

    fn disjoint_write(&self) -> AccessDisjointWriteGuard<'_> {
        let mut state = self.state.lock().expect("AccessLock poisoned");
        loop {
            match &mut *state {
                AccessState::Idle => {
                    *state = AccessState::DisjointWriting(1);
                    return AccessDisjointWriteGuard(self);
                }
                AccessState::DisjointWriting(n) => {
                    *n += 1;
                    return AccessDisjointWriteGuard(self);
                }
                AccessState::Reading(_) | AccessState::Exclusive => {
                    state = self.cond.wait(state).expect("AccessLock poisoned");
                }
            }
        }
    }

    fn exclusive(&self) -> AccessExclusiveGuard<'_> {
        let mut state = self.state.lock().expect("AccessLock poisoned");
        loop {
            if matches!(*state, AccessState::Idle) {
                *state = AccessState::Exclusive;
                return AccessExclusiveGuard(self);
            }
            state = self.cond.wait(state).expect("AccessLock poisoned");
        }
    }
}

struct AccessReadGuard<'a>(&'a AccessLock);
impl Drop for AccessReadGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("AccessLock poisoned");
        match &mut *state {
            AccessState::Reading(1) => {
                *state = AccessState::Idle;
                self.0.cond.notify_all();
            }
            AccessState::Reading(n) => *n -= 1,
            other => unreachable!("AccessReadGuard dropped in non-Reading state: {other:?}"),
        }
    }
}

struct AccessDisjointWriteGuard<'a>(&'a AccessLock);
impl Drop for AccessDisjointWriteGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("AccessLock poisoned");
        match &mut *state {
            AccessState::DisjointWriting(1) => {
                *state = AccessState::Idle;
                self.0.cond.notify_all();
            }
            AccessState::DisjointWriting(n) => *n -= 1,
            other => unreachable!(
                "AccessDisjointWriteGuard dropped in non-DisjointWriting state: {other:?}"
            ),
        }
    }
}

struct AccessExclusiveGuard<'a>(&'a AccessLock);
impl Drop for AccessExclusiveGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("AccessLock poisoned");
        debug_assert!(matches!(*state, AccessState::Exclusive));
        *state = AccessState::Idle;
        self.0.cond.notify_all();
    }
}

/// Trait for values that keep a backing memory allocation alive and
/// know its address and size.
///
/// As long as a value implementing this trait exists, the memory region
/// it describes is guaranteed to remain valid.
pub trait Keepalive: Send + Sync {
    /// Start address of the memory region this keepalive pins.
    fn addr(&self) -> usize;

    /// Size in bytes of the memory region this keepalive pins.
    fn size(&self) -> usize;

    /// Produce a [`WeakKeepalive`] pointing at the same underlying
    /// resource. Defaults to `None` for impls with no weak form.
    fn downgrade(&self) -> Option<Arc<dyn WeakKeepalive>> {
        None
    }
}

/// Counterpart to [`Keepalive`]: a non-pinning reference to the same
/// underlying resource that can be re-promoted to a [`Keepalive`] as
/// long as the resource is still alive.
pub trait WeakKeepalive: Send + Sync {
    /// Re-acquire a strong [`Keepalive`] for the underlying resource,
    /// or `None` if the referent has gone away.
    fn upgrade(&self) -> Option<Arc<dyn Keepalive>>;
}

impl Keepalive for Box<[u8]> {
    fn addr(&self) -> usize {
        self.as_ptr() as usize
    }

    fn size(&self) -> usize {
        self.len()
    }
}

/// Backing state of a [`KeepaliveLocalMemory`].
///
/// Holds the addressing/bandwidth metadata, the access-coordination
/// lock, and a single-slot home for an [`IbvMemoryRegionView`]
/// registered against this region. Cloning shares the slot and the
/// access lock by `Arc`, so every handle derived from the same
/// allocation observes the same registered MR and the same
/// reader/writer coordination.
///
/// All access goes through methods on [`KeepaliveLocalMemory`];
/// nothing outside the module pokes at these fields directly.
#[derive(Clone)]
pub(crate) struct LocalMemoryInner {
    addr: usize,
    size: usize,
    /// Bandwidth (bytes/s) for direct host-thread pointer access, or `None`
    /// if the memory is not host-accessible.
    direct_access_host_bandwidth: Option<u64>,
    /// Bandwidth (bytes/s) for direct device-thread pointer access, or
    /// `None` if the memory is not device-accessible.
    direct_access_device_bandwidth: Option<u64>,
    /// Per-allocation slot for the [`IbvMemoryRegionView`] registered
    /// against this region. Populated lazily by
    /// `IbvManagerActor::resolve_local_mr` on first use.
    mr_slot: Arc<OnceLock<IbvMemoryRegionView>>,
    /// Coordinates concurrent reads, exclusive writes, and parallel
    /// disjoint writes against this region.
    access: Arc<AccessLock>,
}

impl LocalMemoryInner {
    fn new(addr: usize, size: usize) -> Self {
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
            mr_slot: Arc::new(OnceLock::new()),
            access: Arc::new(AccessLock::new()),
        }
    }
}

/// Local memory handle that keeps its backing allocation alive via an
/// [`Arc<dyn Keepalive>`].
///
/// Detects at construction time whether the address is a CUDA device
/// pointer and dispatches `read_at`/`write_at` accordingly.
///
/// All three access methods are `unsafe`: the [`Keepalive`] only
/// guarantees the allocation stays mapped, not that this handle has
/// unique ownership. The internal [`AccessLock`] coordinates concurrent
/// callers that share the same clone of this handle (readers run in
/// parallel, exclusive writers run alone, disjoint writers run in
/// parallel with one another but exclude readers and exclusive
/// writers), but callers must additionally rule out concurrent access
/// through other views of the same allocation.
///
/// The `direct_access_host_bandwidth` and `direct_access_device_bandwidth`
/// fields indicate the speed of reading the memory via pointer dereference
/// on a host or device thread, respectively. A value of `None` means the
/// memory is not directly accessible from that context.
#[derive(Clone)]
pub struct KeepaliveLocalMemory {
    inner: LocalMemoryInner,
    _keepalive: Arc<dyn Keepalive>,
}

impl Debug for KeepaliveLocalMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeepaliveLocalMemory")
            .field("addr", &self.inner.addr)
            .field("size", &self.inner.size)
            .field(
                "direct_access_host_bandwidth",
                &self.inner.direct_access_host_bandwidth,
            )
            .field(
                "direct_access_device_bandwidth",
                &self.inner.direct_access_device_bandwidth,
            )
            .finish_non_exhaustive()
    }
}

impl KeepaliveLocalMemory {
    /// Create a new handle. Derives `addr` and `size` from the
    /// `keepalive` via [`Keepalive::addr`] /
    /// [`Keepalive::size`], then probes the CUDA driver to
    /// determine whether the address is a device pointer and sets the
    /// bandwidth fields accordingly.
    pub fn new(keepalive: Arc<dyn Keepalive>) -> Self {
        let addr = keepalive.addr();
        let size = keepalive.size();
        Self {
            inner: LocalMemoryInner::new(addr, size),
            _keepalive: keepalive,
        }
    }

    /// Starting virtual address of the memory region.
    pub fn addr(&self) -> usize {
        self.inner.addr
    }

    /// Size of the memory region in bytes.
    pub fn size(&self) -> usize {
        self.inner.size
    }

    /// Shared slot for the [`IbvMemoryRegionView`] registered against
    /// this region. Populated lazily by
    /// [`IbvManagerActor::resolve_local_mr`] on first use; the slot
    /// is cloned `Arc` so every handle derived from the same
    /// allocation sees the same registered MR.
    pub fn mr_slot(&self) -> &Arc<OnceLock<IbvMemoryRegionView>> {
        &self.inner.mr_slot
    }

    /// Copy `dst.len()` bytes from this memory region starting at `offset`
    /// into `dst`.
    ///
    /// Mutually exclusive with both `write_at` and `write_at_disjoint`
    /// *across clones of this handle*: the [`AccessLock`] guarantees a
    /// reader and any writer (exclusive or disjoint) that share the
    /// same lock never observe each other's partial state. Multiple
    /// concurrent `read_at` calls on shared clones are permitted and
    /// run in parallel.
    ///
    /// # Safety
    ///
    /// The [`Keepalive`] guarantees the allocation stays mapped, but it
    /// does *not* imply unique ownership: another component may hold its
    /// own view of the same allocation and read or write it concurrently
    /// outside this handle's [`AccessLock`]. The caller must ensure that
    /// no such external access produces a torn read of
    /// `offset..offset + dst.len()` for the duration of this call.
    pub unsafe fn read_at(&self, offset: usize, dst: &mut [u8]) -> Result<(), anyhow::Error> {
        let _guard = self.inner.access.read();
        check_bounds(offset, dst.len(), self.inner.size)?;
        // SAFETY: the `_keepalive` field keeps the allocation live, the
        // read guard above excludes concurrent exclusive and disjoint
        // writers that share this lock, `check_bounds` verified the access
        // is in range, and the caller upholds the no-external-writer
        // obligation documented on this method.
        unsafe {
            if self.inner.direct_access_host_bandwidth.is_some() {
                read_cpu(self.inner.addr, offset, dst);
                Ok(())
            } else {
                read_gpu(self.inner.addr, offset, dst)
            }
        }
    }

    /// Copy `src.len()` bytes from `src` into this memory region starting
    /// at `offset`.
    ///
    /// Mutually exclusive with every other read and write against this
    /// region *across clones of this handle*: the [`AccessLock`] blocks
    /// concurrent readers and writers that share the same lock. Use
    /// [`KeepaliveLocalMemory::write_at_disjoint`] when multiple writers
    /// can be proven to target disjoint byte ranges.
    ///
    /// # Safety
    ///
    /// See [`KeepaliveLocalMemory::read_at`]. The [`Keepalive`] guarantee
    /// covers liveness only; the caller must ensure no concurrent
    /// external reader or writer observes an overlapping byte range.
    pub unsafe fn write_at(&self, offset: usize, src: &[u8]) -> Result<(), anyhow::Error> {
        let _guard = self.inner.access.exclusive();
        check_bounds(offset, src.len(), self.inner.size)?;
        // SAFETY: the `_keepalive` field keeps the allocation live, the
        // exclusive guard above excludes every other reader and writer
        // that shares this lock, `check_bounds` verified the access is
        // in range, and the caller upholds the no-external-access
        // obligation documented on this method.
        unsafe {
            if self.inner.direct_access_host_bandwidth.is_some() {
                write_cpu(self.inner.addr, offset, src);
                Ok(())
            } else {
                write_gpu(self.inner.addr, offset, src)
            }
        }
    }

    /// Like [`KeepaliveLocalMemory::write_at`], but allows other
    /// concurrent `write_at_disjoint` calls (across clones of this
    /// handle) to proceed in parallel. Still mutually exclusive with
    /// `read_at` and `write_at` through the [`AccessLock`].
    ///
    /// # Safety
    ///
    /// In addition to the obligations of
    /// [`KeepaliveLocalMemory::write_at`] (no external concurrent
    /// reader or writer of the same byte range), the caller must
    /// ensure that no other concurrent call to this method targets a
    /// byte range that overlaps `offset..offset + src.len()`. Disjoint
    /// byte ranges across concurrent disjoint callers are sound.
    pub unsafe fn write_at_disjoint(&self, offset: usize, src: &[u8]) -> Result<(), anyhow::Error> {
        let _guard = self.inner.access.disjoint_write();
        check_bounds(offset, src.len(), self.inner.size)?;
        // SAFETY: the `_keepalive` field keeps the allocation live, the
        // disjoint-write guard above excludes concurrent readers and
        // exclusive writers that share this lock, `check_bounds`
        // verified the access is in range, and the caller upholds both
        // safety obligations documented on this method (no external access,
        // no overlap with other concurrent disjoint writers).
        unsafe {
            if self.inner.direct_access_host_bandwidth.is_some() {
                write_cpu(self.inner.addr, offset, src);
                Ok(())
            } else {
                write_gpu(self.inner.addr, offset, src)
            }
        }
    }

    /// Pair off a [`WeakLocalMemory`] that shares this handle's
    /// [`LocalMemoryInner`] (and therefore the same MR slot and
    /// access lock). Returns `None` when the underlying [`Keepalive`]
    /// does not provide a weak form.
    pub fn downgrade(&self) -> Option<WeakLocalMemory> {
        let weak_keepalive = self._keepalive.downgrade()?;
        Some(WeakLocalMemory {
            inner: self.inner.clone(),
            weak_keepalive,
        })
    }
}

/// Non-pinning counterpart of [`KeepaliveLocalMemory`].
///
/// Holds the shared [`LocalMemoryInner`] (so a re-promoted strong
/// handle sees the same MR slot and access lock) plus a
/// [`WeakKeepalive`] that can be upgraded to a fresh
/// [`Arc<dyn Keepalive>`] as long as the referent is still alive.
#[derive(Clone)]
pub struct WeakLocalMemory {
    inner: LocalMemoryInner,
    weak_keepalive: Arc<dyn WeakKeepalive>,
}

impl WeakLocalMemory {
    /// Starting virtual address of the memory region.
    pub fn addr(&self) -> usize {
        self.inner.addr
    }

    /// Size of the memory region in bytes.
    pub fn size(&self) -> usize {
        self.inner.size
    }

    /// Materialize a strong [`KeepaliveLocalMemory`] sharing this
    /// handle's [`LocalMemoryInner`]. Returns `None` if the
    /// referent has gone away **or** if its currently-computed
    /// `(addr, size)` no longer matches the values stored on this
    /// handle — the latter guarding against the live referent
    /// describing a different memory region than the one this weak
    /// handle was paired with at downgrade time.
    pub fn upgrade(&self) -> Option<KeepaliveLocalMemory> {
        let keepalive = self.weak_keepalive.upgrade()?;
        let new_addr = keepalive.addr();
        let new_size = keepalive.size();
        if new_addr != self.inner.addr || new_size != self.inner.size {
            tracing::warn!(
                expected_addr = self.inner.addr,
                actual_addr = new_addr,
                expected_size = self.inner.size,
                actual_size = new_size,
                "WeakLocalMemory upgrade rejected: backing keepalive's (addr, size) changed since downgrade",
            );
            return None;
        }
        Some(KeepaliveLocalMemory {
            inner: self.inner.clone(),
            _keepalive: keepalive,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- KeepaliveLocalMemory (host) --

    fn host_keepalive_mem(data: Box<[u8]>) -> KeepaliveLocalMemory {
        KeepaliveLocalMemory::new(Arc::new(data))
    }

    #[test]
    fn keepalive_host_read_at() {
        let mem = host_keepalive_mem(Box::from([1, 2, 3, 4, 5]));
        let mut buf = [0u8; 3];
        // SAFETY: `mem` is the sole handle to the allocation, no other
        // thread or component holds a view of it.
        unsafe { mem.read_at(1, &mut buf) }.unwrap();
        assert_eq!(buf, [2, 3, 4]);
    }

    #[test]
    fn keepalive_host_write_then_read() {
        let mem = host_keepalive_mem(vec![0; 5].into_boxed_slice());
        // SAFETY: `mem` is the sole handle to the allocation, no other
        // thread or component holds a view of it.
        unsafe { mem.write_at(1, &[7, 8, 9]) }.unwrap();
        let mut buf = [0u8; 5];
        // SAFETY: same as above.
        unsafe { mem.read_at(0, &mut buf) }.unwrap();
        assert_eq!(buf, [0, 7, 8, 9, 0]);
    }

    #[test]
    fn keepalive_host_out_of_bounds() {
        let mem = host_keepalive_mem(vec![0; 3].into_boxed_slice());
        let mut buf = [0u8; 3];
        // SAFETY: `mem` is the sole handle to the allocation; the
        // bounds check fires before any pointer dereference.
        assert!(unsafe { mem.read_at(1, &mut buf) }.is_err());
        // SAFETY: same as above.
        assert!(unsafe { mem.write_at(1, &[7, 8, 9]) }.is_err());
    }
}
