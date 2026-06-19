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
///   pointer) so the device context outlives the PD by construction. The
///   PD is freed in `Drop` before this `Arc` is released.
/// * `pd`: The protection domain pointer (private; read via [`Self::as_ptr`]),
///   which provides isolation between connections.
/// * `device_info`: Metadata for the device this PD is allocated on.
///
/// `IbvDomain` is not `Clone`: the `Drop` impl runs
/// `ibv_dealloc_pd` and copying the pointer would lead to a
/// double-free. Share the domain across owners via
/// `Arc<IbvDomain>` instead.
pub struct IbvDomain {
    pub context: Arc<IbvContext>,
    pd: *mut rdmaxcel_sys::ibv_pd,
    pub device_info: IbvDeviceInfo,
}

impl std::fmt::Debug for IbvDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IbvDomain")
            .field("context", &format!("{:p}", self.context.as_ptr()))
            .field("pd", &format!("{:p}", self.pd))
            .field("device_info", &self.device_info)
            .finish()
    }
}

// SAFETY:
// IbvDomain is `Send` because the raw pointers to ibverbs structs can be
// accessed from any thread, and it is safe to drop `IbvDomain` (and run the
// ibverbs destructors) from any thread.
unsafe impl Send for IbvDomain {}

// SAFETY:
// IbvDomain is `Sync` because the underlying ibverbs APIs are thread-safe.
unsafe impl Sync for IbvDomain {}

impl Drop for IbvDomain {
    fn drop(&mut self) {
        if self.pd.is_null() {
            return;
        }
        unsafe {
            rdmaxcel_sys::ibv_dealloc_pd(self.pd);
        }
    }
}

impl IbvDomain {
    /// Creates an `IbvDomain` over an already-opened device `context`,
    /// allocating its protection domain.
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
    ) -> Result<Self, anyhow::Error> {
        assert!(
            !context.as_ptr().is_null(),
            "IbvDomain::new requires a non-null ibv_context"
        );
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
        })
    }

    /// Test-only constructor assembling a domain from raw parts (typically with
    /// a null `pd`/`context` whose `Drop` is a no-op) for use as a keepalive.
    ///
    /// # Safety
    ///
    /// If `pd` is non-null it must be a protection domain allocated against
    /// `context` and owned solely by the returned domain (its `Drop` calls
    /// `ibv_dealloc_pd` once); `context` must satisfy [`IbvContext`]'s validity
    /// contract.
    #[cfg(test)]
    pub(super) unsafe fn from_parts(
        context: Arc<IbvContext>,
        pd: *mut rdmaxcel_sys::ibv_pd,
        device_info: IbvDeviceInfo,
    ) -> Self {
        Self {
            context,
            pd,
            device_info,
        }
    }

    /// The protection domain pointer. Valid for the lifetime of `&self`.
    /// Prefer this over touching the field directly (which is private).
    pub fn as_ptr(&self) -> *mut rdmaxcel_sys::ibv_pd {
        self.pd
    }

    /// Metadata for the device this domain's PD is allocated on.
    pub fn device_info(&self) -> &IbvDeviceInfo {
        &self.device_info
    }
}

/// Per-backend strategy for a protection domain: how memory regions are
/// registered and how queue pairs are built against the PD.
///
/// One strategy is constructed per opened device via [`Self::new`], which
/// inspects the device behind the context to decide backend-specific
/// behavior up front. The per-op methods take the owning `Arc<IbvDomain>`
/// so the returned resources can anchor the PD's lifetime as a keepalive; a
/// follow-up commit makes this the backend-generic `Arc<IbvDomain<Self>>`.
pub trait IbvDomainImpl: Send + Sync + 'static + Sized {
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
        &self,
        domain: Arc<IbvDomain>,
        mem: &KeepaliveLocalMemory,
    ) -> anyhow::Result<IbvMemoryRegionView> {
        // SAFETY: `domain.as_ptr()` is null or a live PD (per this method's
        // contract; `register_host_or_dmabuf_mr` errors on null), and the caller
        // keeps `mem`'s backing memory valid for the MR's lifetime.
        unsafe { register_host_or_dmabuf_mr(domain, self.mr_access_flags(), mem) }
    }

    /// Create a queue pair against `domain`.
    fn create_queue_pair(
        &self,
        domain: Arc<IbvDomain>,
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

/// Register device memory as a dmabuf MR via `ibv_reg_dmabuf_mr`. Returns
/// the raw `ibv_mr`.
///
/// # Safety
///
/// If `pd` is non-null it must be a live protection domain whose context
/// outlives this call. `[addr, addr + size)` must name a CUDA device allocation
/// that stays valid for the lifetime of the returned MR.
pub(super) unsafe fn register_dmabuf_mr(
    pd: *mut rdmaxcel_sys::ibv_pd,
    addr: usize,
    size: usize,
    access_flags: i32,
) -> anyhow::Result<*mut rdmaxcel_sys::ibv_mr> {
    if pd.is_null() {
        anyhow::bail!("register_dmabuf_mr called with a null protection domain");
    }
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
pub(super) unsafe fn register_host_or_dmabuf_mr(
    domain: Arc<IbvDomain>,
    access_flags: i32,
    mem: &KeepaliveLocalMemory,
) -> anyhow::Result<IbvMemoryRegionView> {
    let addr = mem.addr();
    let size = mem.size();
    // SAFETY: per this function's contract `domain.as_ptr()` is null or a live
    // PD (the helpers error on null), and `[addr, addr + size)` stays valid for
    // the returned MR's lifetime.
    let mr = unsafe {
        if is_device_ptr(addr) {
            register_dmabuf_mr(domain.as_ptr(), addr, size, access_flags)?
        } else {
            register_host_mr(domain.as_ptr(), addr, size, access_flags)?
        }
    };

    // SAFETY: `mr` is non-null — `register_dmabuf_mr`/`register_host_mr` only
    // return `Ok` with a non-null, freshly-registered `ibv_mr` — so reading its
    // `addr`/`lkey`/`rkey` here is sound.
    let (rdma_addr, lkey, rkey) = unsafe { ((*mr).addr as usize, (*mr).lkey, (*mr).rkey) };
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
