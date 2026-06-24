/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ibverbs RDMA domain.
//!
//! An [`IbvDomain`] manages the device context and protection domain (PD)
//! required for RDMA operations. It provides the foundation for creating
//! queue pairs and establishing connections between RDMA devices.

use std::ffi::c_void;
use std::io::Error;
use std::mem::ManuallyDrop;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::result::Result;
use std::sync::Arc;

use super::device::IbvContext;
use super::memory_region::IbvMemoryRegion;
use super::memory_region::IbvMemoryRegionView;
use super::primitives::IbvConfig;
use super::primitives::IbvDeviceInfo;
use super::queue_pair::IbvQueuePair;
use crate::local_memory::KeepaliveLocalMemory;
use crate::local_memory::is_device_ptr;

/// Manages RDMA resources including context and protection domain.
///
/// # Fields
///
/// * `context`: The owning [`Arc<IbvContext>`]; held (rather than a raw
///   pointer) so the device context outlives the PD by construction. The PD is
///   deallocated in `Drop` before this `Arc` is released.
/// * `pd`: The protection domain pointer (private; read via [`Self::as_ptr`]),
///   which provides isolation between connections.
/// * `device_info`: Metadata for the device this PD is allocated on.
/// * `domain_impl`: The backend [`IbvDomainImpl`] strategy for this PD. It may
///   own FFI resources allocated against the PD, so `Drop` releases it before
///   deallocating the PD. Held in a [`ManuallyDrop`] so that ordering can be
///   enforced by hand.
///
/// `I` is the backend [`IbvDomainImpl`] strategy parameterizing per-PD
/// behavior.
///
/// `IbvDomain` is not `Clone`: its `Drop` runs `ibv_dealloc_pd` and copying the
/// pointer would lead to a double-free. Share the domain across owners via
/// `Arc<IbvDomain>` instead.
pub struct IbvDomain<I: IbvDomainImpl> {
    pub context: Arc<IbvContext>,
    pd: *mut rdmaxcel_sys::ibv_pd,
    pub device_info: IbvDeviceInfo,
    domain_impl: ManuallyDrop<I>,
}

impl<I: IbvDomainImpl> std::fmt::Debug for IbvDomain<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IbvDomain")
            .field("context", &format!("{:p}", self.context.as_ptr()))
            .field("pd", &format!("{:p}", self.pd))
            .field("device_info", &self.device_info)
            .field("domain_impl", &self.domain_impl)
            .finish()
    }
}

// SAFETY:
// IbvDomain is `Send` because the raw pointers to ibverbs structs can be
// accessed from any thread, and it is safe to drop `IbvDomain` (and run the
// ibverbs destructors) from any thread.
unsafe impl<I: IbvDomainImpl> Send for IbvDomain<I> {}

// SAFETY:
// IbvDomain is `Sync` because the underlying ibverbs APIs are thread-safe.
unsafe impl<I: IbvDomainImpl> Sync for IbvDomain<I> {}

/// Type-erased keepalive for an [`IbvDomain`] of any backend. Lets holders that
/// only need to keep a domain's PD (and context) alive — standalone MR guards,
/// queue pairs — hold an `Arc<dyn IbvDomainKeepalive>` without being generic
/// over the backend [`IbvDomainImpl`].
pub(super) trait IbvDomainKeepalive: std::fmt::Debug + Send + Sync {}

impl<I: IbvDomainImpl> IbvDomainKeepalive for IbvDomain<I> {}

impl<I: IbvDomainImpl> Drop for IbvDomain<I> {
    fn drop(&mut self) {
        // Drop the backend strategy first: it may own FFI resources allocated
        // against `pd`, which must be released before the PD is deallocated.
        // SAFETY: `domain_impl` is dropped exactly once — here — and never
        // accessed afterward (this is `IbvDomain`'s own `Drop`).
        unsafe {
            ManuallyDrop::drop(&mut self.domain_impl);
        }
        if self.pd.is_null() {
            return;
        }
        // SAFETY: `pd` was returned by `ibv_alloc_pd` and is deallocated exactly
        // once (`IbvDomain` is not `Clone`). `context` is a separate field,
        // dropped only after this returns, so the device is still open here.
        let result = unsafe { rdmaxcel_sys::ibv_dealloc_pd(self.pd) };
        if result != 0 {
            tracing::error!(
                "failed to deallocate protection domain {:p}: error code {}",
                self.pd,
                result
            );
        }
    }
}

impl<I: IbvDomainImpl> IbvDomain<I> {
    /// Creates an `IbvDomain` over an already-opened device `context`.
    ///
    /// Builds the backend [`IbvDomainImpl`] strategy `I` from `config`, then
    /// allocates the protection domain against `context`. The PD is allocated
    /// only *after* the strategy is built, so a panicking
    /// [`IbvDomainImpl::new`] never leaks a PD.
    ///
    /// Note:
    /// Our memory region (MR) registration uses implicit ODP for RDMA access, which maps large virtual
    /// address ranges without explicit pinning. This is convenient, but it broadens the memory footprint
    /// exposed to the NIC and introduces a security liability.
    ///
    /// We currently assume a trusted, single-environment and are not enforcing finer-grained memory isolation
    /// at this layer. We plan to investigate mitigations - such as memory windows or tighter registration
    /// boundaries in future follow-ups.
    ///
    /// # Safety
    ///
    /// `context` must wrap a valid, live `ibv_context`: the `Arc<IbvContext>`
    /// keeps it open for this call and for the returned domain's lifetime (the
    /// PD allocated here is freed against that context on `Drop`).
    ///
    /// # Panics
    ///
    /// Panics if `context` is null.
    ///
    /// # Errors
    ///
    /// Returns an error if protection-domain allocation fails.
    pub unsafe fn new(
        context: Arc<IbvContext>,
        device_info: IbvDeviceInfo,
        config: &IbvConfig,
    ) -> Result<Self, anyhow::Error> {
        assert!(
            !context.as_ptr().is_null(),
            "IbvDomain::new requires a non-null ibv_context"
        );
        // Build the strategy first; the PD below is allocated only if this
        // returns, so a panicking `IbvDomainImpl::new` never leaks a PD.
        // SAFETY: per this function's contract `context` wraps a valid, live
        // `ibv_context`, which is what `I::new` requires to query the device.
        let domain_impl = unsafe { I::new(&context, &device_info, config) };
        // SAFETY: `context.as_ptr()` is non-null (asserted above) and, per this
        // function's contract, a valid live `ibv_context` owned by the
        // `Arc<IbvContext>` for the duration of this call.
        let pd = unsafe { rdmaxcel_sys::ibv_alloc_pd(context.as_ptr()) };
        if pd.is_null() {
            anyhow::bail!("ibv_alloc_pd failed: {}", Error::last_os_error());
        }
        Ok(Self {
            context,
            pd,
            device_info,
            domain_impl: ManuallyDrop::new(domain_impl),
        })
    }

    /// Test-only constructor assembling a domain from raw parts without
    /// allocating a PD, so unit tests can fabricate a domain (typically with a
    /// null `pd`/`context` whose `Drop` is a no-op).
    ///
    /// # Safety
    ///
    /// If `pd` is non-null it must be a protection domain allocated against
    /// `context` and owned solely by the returned domain (its `Drop` calls
    /// `ibv_dealloc_pd` once); `context` must satisfy [`IbvContext`]'s validity
    /// contract.
    #[cfg(test)]
    pub(super) unsafe fn for_test(
        context: Arc<IbvContext>,
        pd: *mut rdmaxcel_sys::ibv_pd,
        device_info: IbvDeviceInfo,
        domain_impl: I,
    ) -> Self {
        Self {
            context,
            pd,
            device_info,
            domain_impl: ManuallyDrop::new(domain_impl),
        }
    }

    /// The protection domain pointer. Valid for the lifetime of `&self`.
    /// Prefer this over touching the field directly (which is private).
    pub fn as_ptr(&self) -> *mut rdmaxcel_sys::ibv_pd {
        self.pd
    }

    /// The backend [`IbvDomainImpl`] strategy for this domain.
    pub(super) fn domain_impl(&self) -> &I {
        &self.domain_impl
    }

    /// Metadata for the device this domain's PD is allocated on.
    pub fn device_info(&self) -> &IbvDeviceInfo {
        &self.device_info
    }

    /// Access flags used when registering memory regions on this domain,
    /// from the backend [`IbvDomainImpl`] strategy.
    pub fn mr_access_flags(&self) -> i32 {
        self.domain_impl().mr_access_flags()
    }

    /// Register `mem` against this domain's PD, dispatching to the backend
    /// [`IbvDomainImpl`] strategy.
    pub fn register_mr(
        self: Arc<Self>,
        mem: &KeepaliveLocalMemory,
    ) -> anyhow::Result<IbvMemoryRegionView> {
        // SAFETY: a fully-constructed `IbvDomain` holds a null-or-live PD per its
        // construction contract, and `KeepaliveLocalMemory` keeps `mem`'s backing
        // memory alive for the registration.
        unsafe { I::register_mr(self, mem) }
    }

    /// Create a queue pair against this domain, dispatching to the backend
    /// [`IbvDomainImpl`] strategy.
    pub fn create_queue_pair(self: Arc<Self>, config: &IbvConfig) -> anyhow::Result<IbvQueuePair> {
        I::create_queue_pair(self, config)
    }
}

/// Per-backend strategy for a protection domain: how memory regions are
/// registered and how queue pairs are built against the PD.
///
/// One strategy is constructed per domain via [`Self::new`], which
/// inspects the device behind the context to decide backend-specific behavior
/// up front, and is then stored in the [`IbvDomain`] it drives. The per-op
/// methods are associated functions taking the owning `Arc<IbvDomain<Self>>`
/// and reach the strategy itself through [`IbvDomain::domain_impl`].
pub trait IbvDomainImpl: std::fmt::Debug + Send + Sync + 'static + Sized {
    /// Build the strategy for the device behind `context` (whose queried
    /// metadata is `device_info`), using `config` for any setup it performs.
    ///
    /// # Safety
    ///
    /// If `context` is non-null it must wrap a valid, live `ibv_context`;
    /// implementations may query the device behind it.
    unsafe fn new(context: &IbvContext, device_info: &IbvDeviceInfo, config: &IbvConfig) -> Self;

    /// Access flags used when registering memory regions on this domain.
    fn mr_access_flags(&self) -> i32;

    /// Register `mem` against `domain`'s PD and return a view of the
    /// resulting memory region.
    ///
    /// The default implementation covers host memory (`ibv_reg_mr`) and
    /// the device-memory dmabuf path (`ibv_reg_dmabuf_mr`); backends
    /// override to add hardware-specific registration, falling back to
    /// this default for the cases they do not special-case.
    ///
    /// # Safety
    ///
    /// `domain`'s PD (`domain.as_ptr()`) must be null or a live protection
    /// domain; `mem`'s backing memory must stay valid for the returned MR's
    /// lifetime.
    unsafe fn register_mr(
        domain: Arc<IbvDomain<Self>>,
        mem: &KeepaliveLocalMemory,
    ) -> anyhow::Result<IbvMemoryRegionView> {
        // SAFETY: `domain.as_ptr()` is null or a live PD (per this method's
        // contract; `register_host_or_dmabuf_mr` errors on null), and the caller
        // keeps `mem`'s backing memory valid for the MR's lifetime.
        unsafe { register_host_or_dmabuf_mr(domain, mem) }
    }

    /// Create a queue pair against `domain`.
    fn create_queue_pair(
        domain: Arc<IbvDomain<Self>>,
        config: &IbvConfig,
    ) -> anyhow::Result<IbvQueuePair> {
        IbvQueuePair::new(domain, config.clone())
    }
}

/// Register host memory as a standard MR via `ibv_reg_mr`. Returns the raw
/// `ibv_mr`; the caller wraps it in an [`IbvMemoryRegion`] guard.
///
/// # Safety
///
/// If `pd` is non-null it must be a live protection domain whose context
/// outlives this call. `[addr, addr + size)` must name a host mapping that
/// stays valid for the lifetime of the returned MR.
pub(super) unsafe fn register_host_mr(
    pd: *mut rdmaxcel_sys::ibv_pd,
    addr: usize,
    size: usize,
    access_flags: i32,
) -> anyhow::Result<*mut rdmaxcel_sys::ibv_mr> {
    if pd.is_null() {
        anyhow::bail!("register_host_mr called with a null protection domain");
    }
    // SAFETY: `pd` is non-null (checked above) and, per this function's
    // contract, a live protection domain; `[addr, addr + size)` is a
    // caller-guaranteed valid mapping. `ibv_reg_mr` returns null on failure,
    // which we check before returning the pointer.
    let mr = unsafe { rdmaxcel_sys::ibv_reg_mr(pd, addr as *mut c_void, size, access_flags) };
    if mr.is_null() {
        anyhow::bail!("failed to register standard MR");
    }
    Ok(mr)
}

/// Register exactly `[addr, addr + size)` of device memory as a dmabuf MR via
/// `ibv_reg_dmabuf_mr`, mapped at iova 0. Returns the raw `ibv_mr`.
///
/// `cuMemGetHandleForAddressRange` requires both `addr` and `size` to be
/// host-page aligned, so this errors if either is not.
///
/// # Safety
///
/// If `pd` is non-null it must be a live protection domain whose context
/// outlives this call. `[addr, addr + size)` must name device memory that
/// stays valid for the lifetime of the returned MR.
pub(super) unsafe fn register_dmabuf_range(
    pd: *mut rdmaxcel_sys::ibv_pd,
    addr: usize,
    size: usize,
    access_flags: i32,
) -> anyhow::Result<*mut rdmaxcel_sys::ibv_mr> {
    if pd.is_null() {
        anyhow::bail!("register_dmabuf_range called with a null protection domain");
    }
    // SAFETY: `sysconf` reads a process-global parameter and takes no pointers.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    anyhow::ensure!(
        page_size > 0,
        "sysconf(_SC_PAGESIZE) failed: {}",
        Error::last_os_error()
    );
    let host_page_size = page_size as usize;
    anyhow::ensure!(
        addr.is_multiple_of(host_page_size) && size.is_multiple_of(host_page_size),
        "dmabuf range is not host-page aligned (addr: 0x{addr:x}, size: {size}, page size: {host_page_size})"
    );

    let mut fd: i32 = -1;
    // SAFETY: `rdmaxcel_cuMemGetHandleForAddressRange` writes the dmabuf fd for
    // `[addr, addr + size)` into `fd` and touches no Rust memory beyond the
    // `&mut fd` out-param; it reports failure via its return code, checked next.
    let cu_err = unsafe {
        rdmaxcel_sys::rdmaxcel_cuMemGetHandleForAddressRange(
            &mut fd,
            addr as rdmaxcel_sys::CUdeviceptr,
            size,
            rdmaxcel_sys::CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
            0,
        )
    };
    if cu_err != rdmaxcel_sys::CUDA_SUCCESS || fd < 0 {
        anyhow::bail!(
            "failed to get dmabuf handle for CUDA memory (addr: 0x{:x}, size: {}, cu_err: {}, fd: {})",
            addr,
            size,
            cu_err,
            fd
        );
    }
    // SAFETY: `fd >= 0` (checked above) is a fresh dmabuf descriptor we
    // exclusively own; wrapping it in `OwnedFd` closes it on drop and keeps it
    // open across the registration below.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    // SAFETY: `pd` is a non-null protection domain (checked above) belonging to
    // a live context; `fd` is a valid dmabuf descriptor kept open by the
    // `OwnedFd` across this call; `size` matches the range queried above.
    // `ibv_reg_dmabuf_mr` returns null on failure, which we check.
    let mr =
        unsafe { rdmaxcel_sys::ibv_reg_dmabuf_mr(pd, 0, size, 0, fd.as_raw_fd(), access_flags) };
    if mr.is_null() {
        anyhow::bail!("failed to register dmabuf MR");
    }
    Ok(mr)
}

/// Register the CUDA allocation containing `addr` as a dmabuf MR.
/// The MR covers the *entire* allocation; the returned `usize` is
/// the offset of `addr` within it.
///
/// The whole-allocation base and size come from `cuMemGetAddressRange`;
/// [`register_dmabuf_range`] then enforces that both are host-page aligned.
///
/// # Safety
///
/// If `pd` is non-null it must be a live protection domain whose context
/// outlives this call. `addr` must belong to a CUDA device allocation
/// that stays valid for the lifetime of the returned MR.
pub(super) unsafe fn register_dmabuf_mr(
    pd: *mut rdmaxcel_sys::ibv_pd,
    addr: usize,
    access_flags: i32,
) -> anyhow::Result<(*mut rdmaxcel_sys::ibv_mr, usize)> {
    // `cuMemGetAddressRange` resolves the pointer in the *current* CUDA context,
    // so make the pointer's own device context current first. Without this, in a
    // multi-GPU process it fails with `CUDA_ERROR_NOT_FOUND` whenever the active
    // context belongs to a different device than `addr`.
    // SAFETY: this path is only taken for device memory (`is_device_ptr(addr)`
    // in `register_host_or_dmabuf_mr`), so `addr` is a valid CUDA device pointer
    // as `set_ctx_for_ptr` requires. The guard restores the prior context on drop.
    let _ctx_guard = unsafe { crate::local_memory::set_ctx_for_ptr(addr)? };

    // Resolve the base and size of the allocation containing `addr`; the dmabuf
    // handle and MR cover the whole allocation.
    let mut base: rdmaxcel_sys::CUdeviceptr = 0;
    let mut alloc_size: usize = 0;
    // SAFETY: `rdmaxcel_cuMemGetAddressRange` writes the allocation's base and
    // size into the out-params and touches no other Rust memory; it reports
    // failure via its return code, checked next.
    let cu_err = unsafe {
        rdmaxcel_sys::rdmaxcel_cuMemGetAddressRange(
            &mut base,
            &mut alloc_size,
            addr as rdmaxcel_sys::CUdeviceptr,
        )
    };
    if cu_err != rdmaxcel_sys::CUDA_SUCCESS {
        anyhow::bail!(
            "failed to get address range for CUDA memory (addr: 0x{:x}, cu_err: {})",
            addr,
            cu_err
        );
    }
    let base = base as usize;

    // SAFETY: forwards this function's contract; `register_dmabuf_range` checks
    // that `base`/`alloc_size` are host-page aligned.
    let mr = unsafe { register_dmabuf_range(pd, base, alloc_size, access_flags)? };
    Ok((mr, addr - base))
}

/// Default MR registration: host memory via [`register_host_mr`], device
/// memory via [`register_dmabuf_mr`]. Shared by the [`IbvDomainImpl`]
/// default `register_mr` and by backends as the fallback for memory they
/// do not special-case.
///
/// # Safety
///
/// `domain.as_ptr()` must be null or a live protection domain whose context
/// outlives this call (a null PD yields an error). `mem`'s
/// `[addr, addr + size)` must stay valid for the lifetime of the returned
/// view's MR — the MR keepalive maintains the `ibv_mr` but does not keep the
/// backing memory mapped.
pub(super) unsafe fn register_host_or_dmabuf_mr<I: IbvDomainImpl>(
    domain: Arc<IbvDomain<I>>,
    mem: &KeepaliveLocalMemory,
) -> anyhow::Result<IbvMemoryRegionView> {
    let addr = mem.addr();
    let size = mem.size();
    let access_flags = domain.mr_access_flags();
    // `mr_offset` is the offset of `addr` within the MR. For device memory the
    // MR covers the whole allocation, so the requested range starts partway in;
    // for host memory the MR is the requested range itself, so the offset is 0.
    // SAFETY: per this function's contract `domain.as_ptr()` is null or a live
    // PD (the helpers error on null), and `[addr, addr + size)` stays valid for
    // the returned MR's lifetime.
    let (mr, mr_offset) = unsafe {
        if is_device_ptr(addr) {
            register_dmabuf_mr(domain.as_ptr(), addr, access_flags)?
        } else {
            (
                register_host_mr(domain.as_ptr(), addr, size, access_flags)?,
                0,
            )
        }
    };

    // SAFETY: `mr` is non-null — `register_dmabuf_mr`/`register_host_mr` only
    // return `Ok` with a non-null, freshly-registered `ibv_mr` — so reading its
    // `addr`/`lkey`/`rkey` here is sound.
    let (mr_addr, lkey, rkey) = unsafe { ((*mr).addr as usize, (*mr).lkey, (*mr).rkey) };
    // The view addresses the requested sub-range, which sits at `mr_offset`
    // within the MR's zero-based address space.
    let rdma_addr = mr_addr + mr_offset;
    let device_name = domain.device_info().name().to_string();
    // The `IbvMemoryRegion` guard owns the MR (deregistered on its `Drop`) and anchors
    // the PD past that deregistration; it coerces to `Arc<dyn IbvMemoryRegionKeepalive>`.
    let guard = Arc::new(IbvMemoryRegion {
        mr,
        _domain: domain,
    });
    Ok(IbvMemoryRegionView::new(
        addr,
        rdma_addr,
        size,
        lkey,
        rkey,
        device_name,
        guard,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::cuda_test_utils::CudaAllocator;
    use crate::backend::ibverbs::device::IbvDevice;
    use crate::backend::ibverbs::device::IbvDeviceImpl;
    use crate::backend::ibverbs::device_selection::get_cuda_device_to_ibv_device;
    use crate::backend::ibverbs::mlx_device::MlxDevice;
    use crate::backend::ibverbs::mlx_domain::MlxDomain;

    fn host_page_size() -> usize {
        // SAFETY: `sysconf` reads a process-global parameter and takes no pointers.
        unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
    }

    /// Open an [`IbvDomain`] on the NIC mapped to CUDA `device`. These tests
    /// require a GPU with a mapped RDMA NIC, so a missing device, missing NIC, or
    /// open/creation failure panics. The returned domain owns its context, so it
    /// (and its PD) stays valid after the local [`IbvDevice`] drops.
    fn open_domain_for_cuda_device(device: i32) -> Arc<IbvDomain<MlxDomain>> {
        let nic = get_cuda_device_to_ibv_device::<MlxDevice>()
            .get(device as usize)
            .and_then(|nic| nic.as_ref())
            .expect("CUDA device should map to RDMA NIC")
            .name()
            .clone();
        let mut config = IbvConfig::default();
        MlxDevice::apply_config_defaults(&mut config);
        let mut dev = IbvDevice::<MlxDevice>::open(&nic, config).expect("mapped NIC should open");
        dev.get_or_create_domain("test")
            .expect("domain creation should succeed")
    }

    /// A 4 MiB allocation, fully committed (reserved == committed) so it is
    /// entirely mapped and `cuMemGetAddressRange` reports the whole extent.
    fn committed_allocation() -> crate::backend::cuda_test_utils::CudaAllocation {
        CudaAllocator::get().allocate(0, 4 * 1024 * 1024, 4 * 1024 * 1024)
    }

    /// `addr` (iova) and `length` of an `ibv_mr`.
    ///
    /// # Safety
    ///
    /// `mr` must be a live MR (e.g. owned by a not-yet-dropped
    /// [`IbvMemoryRegion`]).
    unsafe fn mr_extent(mr: *mut rdmaxcel_sys::ibv_mr) -> (usize, usize) {
        // SAFETY: per this function's contract `mr` is a live MR.
        unsafe { ((*mr).addr as usize, (*mr).length) }
    }

    // `register_dmabuf_mr` registers the whole enclosing allocation and reports
    // the requested address's offset within it — even when that address is not
    // host-page aligned, which is the case the bug fix targets.
    #[test]
    fn register_dmabuf_mr_covers_whole_allocation() {
        let domain = open_domain_for_cuda_device(0);
        let pd = domain.as_ptr();
        let access = domain.mr_access_flags();
        let alloc = committed_allocation();
        let alloc_size = alloc.size();

        // At the allocation base: offset 0, MR spans the whole allocation at iova
        // 0.
        // SAFETY: `pd` is a live PD; `alloc.ptr()` is a live CUDA allocation kept
        // mapped by `alloc` for the MR's lifetime.
        let (mr, offset) = unsafe { register_dmabuf_mr(pd, alloc.ptr(), access) }.unwrap();
        // Wrap immediately so the MR is deregistered on drop regardless of what
        // the assertions below do.
        let region = IbvMemoryRegion {
            mr,
            _domain: domain.clone(),
        };
        // SAFETY: `region` owns `mr` and has not been dropped.
        let (iova, length) = unsafe { mr_extent(region.as_ptr()) };
        assert_eq!(offset, 0, "base address sits at offset 0");
        assert_eq!(iova, 0, "dmabuf MR is mapped at iova 0");
        assert_eq!(length, alloc_size, "MR covers the whole allocation");

        // At an unaligned interior address: the MR still spans the whole
        // allocation and the offset locates the requested address.
        let unaligned: usize = 257;
        assert!(!unaligned.is_multiple_of(host_page_size()));
        // SAFETY: as above; `alloc.ptr() + unaligned` is inside the allocation.
        let (mr, offset) =
            unsafe { register_dmabuf_mr(pd, alloc.ptr() + unaligned, access) }.unwrap();
        let region = IbvMemoryRegion {
            mr,
            _domain: domain.clone(),
        };
        // SAFETY: `region` owns `mr` and has not been dropped.
        let (iova, length) = unsafe { mr_extent(region.as_ptr()) };
        assert_eq!(offset, unaligned, "offset locates the requested address");
        assert_eq!(iova, 0, "dmabuf MR is mapped at iova 0");
        assert_eq!(length, alloc_size, "MR covers the whole allocation");
    }

    // `register_dmabuf_range` registers exactly the requested range — a strict
    // page-aligned sub-range of the allocation here, not the whole thing.
    #[test]
    fn register_dmabuf_range_registers_exact_range() {
        let domain = open_domain_for_cuda_device(0);
        let pd = domain.as_ptr();
        let access = domain.mr_access_flags();
        let alloc = committed_allocation();

        let offset = 2 * host_page_size();
        let size = 1024 * 1024;
        assert!(offset + size < alloc.size(), "sub-range must fit, strictly");

        // SAFETY: `pd` is a live PD; `[alloc.ptr() + offset, ... + size)` is a
        // host-page-aligned range within the fully mapped allocation.
        let mr = unsafe { register_dmabuf_range(pd, alloc.ptr() + offset, size, access) }.unwrap();
        let region = IbvMemoryRegion {
            mr,
            _domain: domain.clone(),
        };
        // SAFETY: `region` owns `mr` and has not been dropped.
        let (iova, length) = unsafe { mr_extent(region.as_ptr()) };
        assert_eq!(iova, 0, "dmabuf MR is mapped at iova 0");
        assert_eq!(length, size, "MR covers exactly the requested sub-range");
    }

    // `register_dmabuf_range` rejects an unaligned address, gracefully, before
    // touching the driver.
    #[test]
    fn register_dmabuf_range_rejects_unaligned_addr() {
        let domain = open_domain_for_cuda_device(0);
        let pd = domain.as_ptr();
        let access = domain.mr_access_flags();
        let alloc = committed_allocation();

        // One byte past the (aligned) base is not host-page aligned.
        // SAFETY: `pd` is a live PD; the call errors on the alignment check
        // before touching the driver.
        let err = unsafe { register_dmabuf_range(pd, alloc.ptr() + 1, host_page_size(), access) }
            .unwrap_err();
        assert!(
            err.to_string().contains("host-page aligned"),
            "unexpected error: {err}"
        );
    }

    // `register_dmabuf_range` rejects an unaligned size, gracefully, before
    // touching the driver.
    #[test]
    fn register_dmabuf_range_rejects_unaligned_size() {
        let domain = open_domain_for_cuda_device(0);
        let pd = domain.as_ptr();
        let access = domain.mr_access_flags();
        let alloc = committed_allocation();

        // A size that is not a multiple of the host page size.
        // SAFETY: as above.
        let err = unsafe { register_dmabuf_range(pd, alloc.ptr(), host_page_size() + 1, access) }
            .unwrap_err();
        assert!(
            err.to_string().contains("host-page aligned"),
            "unexpected error: {err}"
        );
    }

    // A null PD is rejected before any driver call.
    #[test]
    fn register_dmabuf_range_rejects_null_pd() {
        let page = host_page_size();
        // SAFETY: a null PD is the documented error path; no memory is touched.
        let err =
            unsafe { register_dmabuf_range(std::ptr::null_mut(), page, page, 0) }.unwrap_err();
        assert!(
            err.to_string().contains("null protection domain"),
            "unexpected error: {err}"
        );
    }

    // The full `register_host_or_dmabuf_mr` path over device memory covering the
    // whole allocation: the view reports the allocation's address and size, and
    // an `rdma_addr` of 0 (the request sits at the MR's base).
    #[test]
    fn register_host_or_dmabuf_mr_covers_whole_allocation() {
        let domain = open_domain_for_cuda_device(0);
        let alloc = committed_allocation();
        let mem = alloc.keepalive_slice(0, alloc.size());

        // SAFETY: `domain`'s PD is live; `mem` keeps the allocation mapped for
        // the view's MR lifetime.
        let view = unsafe { register_host_or_dmabuf_mr(domain.clone(), &mem) }.unwrap();
        // Tie the view's lifetime to the allocation's lifetime so that the safety contract
        // above holds.
        mem.mr_slot()
            .set(view.clone())
            .expect("mr_slot not already set");
        assert_eq!(view.virtual_addr, alloc.ptr());
        assert_eq!(view.size, alloc.size());
        assert_eq!(view.rdma_addr, 0, "whole allocation starts at MR offset 0");
    }

    // The full path over a page-aligned sub-range: the underlying MR still spans
    // the whole allocation, so `rdma_addr` is the sub-range's offset within it.
    #[test]
    fn register_host_or_dmabuf_mr_covers_subrange() {
        let domain = open_domain_for_cuda_device(0);
        let alloc = committed_allocation();
        let offset = 2 * host_page_size();
        let size = 1024 * 1024;
        assert!(
            size < alloc.size(),
            "sub-range ({}) must be strictly smaller than allocation ({})",
            size,
            alloc.size()
        );
        let mem = alloc.keepalive_slice(offset, size);

        // SAFETY: as above.
        let view = unsafe { register_host_or_dmabuf_mr(domain.clone(), &mem) }.unwrap();
        // Tie the view's lifetime to the allocation's lifetime so that the safety contract
        // above holds.
        mem.mr_slot()
            .set(view.clone())
            .expect("mr_slot not already set");
        assert_eq!(view.virtual_addr, alloc.ptr() + offset);
        assert_eq!(view.size, size);
        assert_eq!(view.rdma_addr, offset, "rdma_addr is the sub-range offset");
    }

    // The full path tolerates an unaligned sub-range address: registering the
    // whole (aligned) allocation means the unaligned request still succeeds, and
    // `rdma_addr` locates it.
    #[test]
    fn register_host_or_dmabuf_mr_handles_unaligned_addr() {
        let domain = open_domain_for_cuda_device(0);
        let alloc = committed_allocation();
        let offset: usize = 257;
        assert!(!offset.is_multiple_of(host_page_size()));
        let size = 1024 * 1024;
        let mem = alloc.keepalive_slice(offset, size);

        // SAFETY: as above.
        let view = unsafe { register_host_or_dmabuf_mr(domain.clone(), &mem) }.unwrap();
        // Tie the view's lifetime to the allocation's lifetime so that the safety contract
        // above holds.
        mem.mr_slot()
            .set(view.clone())
            .expect("mr_slot not already set");
        assert_eq!(view.virtual_addr, alloc.ptr() + offset);
        assert_eq!(view.size, size);
        assert_eq!(view.rdma_addr, offset);
    }

    // The full path tolerates an unaligned size for the same reason.
    #[test]
    fn register_host_or_dmabuf_mr_handles_unaligned_size() {
        let domain = open_domain_for_cuda_device(0);
        let alloc = committed_allocation();
        let size: usize = 1000;
        assert!(!size.is_multiple_of(host_page_size()));
        let mem = alloc.keepalive_slice(0, size);

        // SAFETY: as above.
        let view = unsafe { register_host_or_dmabuf_mr(domain.clone(), &mem) }.unwrap();
        // Tie the view's lifetime to the allocation's lifetime so that the safety contract
        // above holds.
        mem.mr_slot()
            .set(view.clone())
            .expect("mr_slot not already set");
        assert_eq!(view.virtual_addr, alloc.ptr());
        assert_eq!(view.size, size);
        assert_eq!(view.rdma_addr, 0);
    }
}
