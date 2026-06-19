/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Mellanox (mlx5) domain strategy for [`IbvDomainImpl`].
//!
//! Host and non-mlx5dv device memory use the default host/dmabuf
//! registration. On an mlx5dv-capable device, CUDA device memory is bound
//! to an indirect mlx5dv memory key so a whole allocator segment is
//! addressable through a single key.
//!
//! This commit introduces the segment-binding core — the CUDA segment
//! scanner, the per-segment bookkeeping and binding ([`RegisteredSegment`]),
//! and the device/FFI seam ([`MlxDomainOps`]) they depend on — with
//! mock-backed unit tests. [`MlxDomain`] still stubs the mlx5dv path (callers
//! fall back to a dmabuf MR); a follow-up wires it to drive this core.

// The binding core below (scanner, `RegisteredSegment`, `MlxDomainOps`) is
// exercised by this module's unit tests but only driven by `MlxDomain` in the
// next commit, so it is unused in non-test builds.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "binding core is unit-tested here; MlxDomain drives it in the next commit"
    )
)]

use std::sync::Arc;
use std::sync::Mutex;

use super::device::IbvContext;
use super::domain::IbvDomain;
use super::domain::IbvDomainImpl;
use super::domain::register_host_or_dmabuf_mr;
use super::memory_region::IbvMemoryRegionKeepalive;
use super::memory_region::IbvMemoryRegionView;
use super::primitives::IbvConfig;
use super::primitives::IbvDeviceInfo;
use crate::local_memory::KeepaliveLocalMemory;
use crate::local_memory::is_device_ptr;

/// A single MR must be 2 MiB aligned and covers at most 4 GiB (one page
/// under). Larger segments are split across multiple MRs bound to one key.
const MR_ALIGNMENT: usize = 2 * 1024 * 1024;
const MAX_MR_SIZE: usize = 4 * 1024 * 1024 * 1024 - MR_ALIGNMENT;

// ===========================================================================
// CUDA segment scanner
// ===========================================================================

/// A CUDA memory segment reported by a registered scanner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScannedSegment {
    /// Base virtual address of the segment.
    pub address: usize,
    /// Size of the segment in bytes.
    pub size: usize,
    /// CUDA device ordinal the segment is allocated on.
    pub cuda_ordinal: i32,
    /// Whether the segment is an expandable (growable) allocation.
    pub is_expandable: bool,
}

/// Enumerates the currently-live CUDA segments.
pub type CudaSegmentScanner = Box<dyn Fn() -> Vec<ScannedSegment> + Send + Sync>;

static CUDA_SEGMENT_SCANNER: Mutex<Option<CudaSegmentScanner>> = Mutex::new(None);

/// Register the process-wide CUDA segment scanner, replacing any previously
/// registered one.
pub fn register_cuda_segment_scanner(scanner: CudaSegmentScanner) {
    *CUDA_SEGMENT_SCANNER
        .lock()
        .expect("CUDA segment scanner lock poisoned") = Some(scanner);
}

/// Invoke the registered CUDA segment scanner, returning an empty list when
/// none is registered.
fn scan_cuda_segments() -> Vec<ScannedSegment> {
    CUDA_SEGMENT_SCANNER
        .lock()
        .expect("CUDA segment scanner lock poisoned")
        .as_ref()
        .map(|scan| scan())
        .unwrap_or_default()
}

// ===========================================================================
// MlxDomainOps: the device/FFI surface the binding core delegates to
// ===========================================================================

/// Device/FFI operations the mlx5dv binding logic depends on. Tests
/// substitute a mock so the scan/bind/teardown algorithm can be exercised
/// without hardware. The raw FFI pointer types are used directly — the mock
/// fabricates fake pointers.
///
/// This commit defines the subset [`RegisteredSegment`] needs; a follow-up
/// adds the rest (segment scan, loopback QP) alongside the production
/// implementation.
pub(super) trait MlxDomainOps: Send + Sync + 'static {
    /// Name of the RDMA device this domain drives.
    fn device_name(&self) -> String;

    /// Register `[addr, addr + size)` of device memory as a dmabuf MR.
    ///
    /// # Safety
    ///
    /// If `pd` is non-null it must be a live protection domain whose context
    /// outlives this call.
    unsafe fn register_dmabuf_mr(
        &self,
        pd: *mut rdmaxcel_sys::ibv_pd,
        addr: usize,
        size: usize,
        access: i32,
    ) -> anyhow::Result<*mut rdmaxcel_sys::ibv_mr>;

    /// Deregister an MR returned by [`Self::register_dmabuf_mr`].
    ///
    /// # Safety
    ///
    /// `mr` must be null or an MR returned by [`Self::register_dmabuf_mr`] that
    /// has not already been deregistered.
    unsafe fn dereg_mr(&self, mr: *mut rdmaxcel_sys::ibv_mr);

    /// Bind `mrs` to a freshly created indirect key using `qp`'s work-request
    /// builder, returning the new key.
    ///
    /// # Safety
    ///
    /// `pd` (if non-null) must be a live protection domain, `qp` a valid queue
    /// pair, and every element of `mrs` a live MR — all valid for this call.
    unsafe fn bind_mr_list(
        &self,
        pd: *mut rdmaxcel_sys::ibv_pd,
        qp: *mut rdmaxcel_sys::rdmaxcel_qp,
        access: i32,
        mrs: &[*mut rdmaxcel_sys::ibv_mr],
    ) -> anyhow::Result<*mut rdmaxcel_sys::mlx5dv_mkey>;

    /// Destroy a key created by [`Self::bind_mr_list`].
    ///
    /// # Safety
    ///
    /// `mkey` must be null or a key returned by [`Self::bind_mr_list`] that has
    /// not already been destroyed.
    unsafe fn destroy_mkey(&self, mkey: *mut rdmaxcel_sys::mlx5dv_mkey);

    /// The `(lkey, rkey)` of a bound key, or `None` if `mkey` is null.
    ///
    /// # Safety
    ///
    /// If `mkey` is non-null it must be a live key returned by
    /// [`Self::bind_mr_list`].
    unsafe fn mkey_keys(&self, mkey: *mut rdmaxcel_sys::mlx5dv_mkey) -> Option<(u32, u32)>;
}

// ===========================================================================
// RegisteredSegment: a CUDA segment bound to an indirect key
// ===========================================================================

/// [`IbvMemoryRegionKeepalive`] for a segment-bound mlx5dv MR: a no-op (no `Drop`). It pins the
/// owning [`RegisteredSegment`] — which deregisters its MRs and destroys its
/// keys on its own `Drop` — and the domain (PD keepalive).
///
/// `_segment` is declared before `_domain` so it drops first (struct fields
/// drop in declaration order). A follow-up commit stores an [`IbvDomainImpl`]
/// in each [`IbvDomain`], so `IbvDomain<MlxDomain>` will itself hold a strong
/// reference to every `Arc<RegisteredSegment>`. Were `_domain` dropped before
/// `_segment`, the PD could be deallocated before a segment registered against
/// it — wrong. Dropping `_segment` first won't actually free the segment (the
/// domain still references it), but it ensures that once the last `_domain`
/// reference drops, the segment is freed before the PD is deallocated.
#[derive(Debug)]
struct Mlx5dvMemoryRegion {
    _segment: Arc<RegisteredSegment>,
    _domain: Arc<IbvDomain>,
}

impl IbvMemoryRegionKeepalive for Mlx5dvMemoryRegion {}

/// The mutable state of a [`RegisteredSegment`], behind one `Mutex` so the MRs,
/// current key, size, and superseded keys all change together.
#[derive(Debug)]
struct RegisteredSegmentState {
    /// Active dmabuf MRs covering `[base_virtual_addr, base_virtual_addr +
    /// size)`, in order; all bound to `mkey`. Appended to (never replaced) as
    /// the segment grows.
    mrs: Vec<*mut rdmaxcel_sys::ibv_mr>,
    /// Current indirect key over `mrs`. Swapped on growth; the prior key moves
    /// to `stale_mkeys`.
    mkey: *mut rdmaxcel_sys::mlx5dv_mkey,
    /// Bytes currently covered by `mrs` and bound to `mkey`.
    size: usize,
    /// Keys superseded by growth, kept alive so views built against an earlier
    /// key stay valid; all destroyed when the segment drops.
    stale_mkeys: Vec<*mut rdmaxcel_sys::mlx5dv_mkey>,
}

/// A CUDA segment bound to the device via an indirect mlx5dv key, covering
/// `[base_virtual_addr, base_virtual_addr + size)`. Shared as
/// `Arc<RegisteredSegment>` by [`MlxDomain`] and every view over it. On growth
/// it reuses its existing MRs (registering only the new tail), binds those plus
/// the new MRs to a fresh key, and parks the prior key in `stale_mkeys` so
/// views built against it stay valid; the MRs and every key are released on
/// [`Drop`] (keys first), once nothing — neither [`MlxDomain`] nor any live
/// view — references the segment.
///
/// All mutation is serialized by the owning [`MlxDomain`]'s `segments` lock;
/// `state` is behind a `Mutex` only to allow mutation through the shared `Arc`.
struct RegisteredSegment {
    ops: Arc<dyn MlxDomainOps>,
    base_virtual_addr: usize,
    state: Mutex<RegisteredSegmentState>,
}

impl std::fmt::Debug for RegisteredSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredSegment")
            .field(
                "base_virtual_addr",
                &format!("0x{:x}", self.base_virtual_addr),
            )
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

// SAFETY: the only members that aren't already `Send`/`Sync` are the raw
// `ibv_mr`/`mlx5dv_mkey` pointers in `state`. The ibverbs/mlx5dv objects they
// name are not thread-affine — any thread may use or destroy them (Send) — and
// all access to them is serialized behind `state`'s `Mutex` (Sync). The sole
// lock-free use reads `lkey`/`rkey` off a bound mkey, which are immutable once
// bound (growth binds a new key and parks the old one rather than mutating it),
// so that read is race-free.
unsafe impl Send for RegisteredSegment {}
unsafe impl Sync for RegisteredSegment {}

impl RegisteredSegment {
    /// An unbound segment for `base_virtual_addr`: no MRs, no key, zero size.
    /// [`Self::grow`] binds its first generation.
    fn empty(ops: Arc<dyn MlxDomainOps>, base_virtual_addr: usize) -> Self {
        Self {
            ops,
            base_virtual_addr,
            state: Mutex::new(RegisteredSegmentState {
                mrs: Vec::new(),
                mkey: std::ptr::null_mut(),
                size: 0,
                stale_mkeys: Vec::new(),
            }),
        }
    }

    /// Bytes currently covered by the segment's MRs and bound to its key.
    fn size(&self) -> usize {
        self.state.lock().expect("segment state lock poisoned").size
    }

    /// True when `[addr, addr + size)` lies entirely within this segment.
    fn covers(&self, addr: usize, size: usize) -> bool {
        let end = self.base_virtual_addr + self.size();
        addr >= self.base_virtual_addr && addr <= end && size <= end - addr
    }

    /// Grow the segment to `scanned_seg.size` (must exceed its current size):
    /// register the new tail — the whole range on the first call from
    /// [`Self::empty`] — bind the existing + new MRs to a fresh key, and retire
    /// the prior key to `stale_mkeys` (rather than rebinding the in-use key,
    /// which could race in-flight ops); the initial null key is not retired. On
    /// any failure the freshly-registered tail MRs are deregistered and the
    /// segment is left unchanged.
    ///
    /// # Safety
    ///
    /// If `pd` is non-null it must be a live protection domain and `qp` a valid
    /// queue pair, both valid for this call.
    unsafe fn grow(
        &self,
        pd: *mut rdmaxcel_sys::ibv_pd,
        qp: *mut rdmaxcel_sys::rdmaxcel_qp,
        access: i32,
        scanned_seg: &ScannedSegment,
    ) -> anyhow::Result<()> {
        let mut state = self.state.lock().expect("segment state lock poisoned");
        assert!(
            scanned_seg.address == self.base_virtual_addr,
            "grow called for base 0x{:x} on a segment based at 0x{:x}",
            scanned_seg.address,
            self.base_virtual_addr
        );
        // A segment only ever grows; shrinking would underflow
        // `scanned_seg.size - state.size` below.
        assert!(
            scanned_seg.size >= state.size,
            "segment at 0x{:x} shrank from {} to {} bytes; segments must never shrink",
            self.base_virtual_addr,
            state.size,
            scanned_seg.size
        );
        // Growing to the current size has nothing to add.
        if scanned_seg.size == state.size {
            return Ok(());
        }
        // SAFETY: per this function's contract `pd` is null or a live PD and
        // `qp` a valid QP; the tail MRs registered here belong to this segment.
        let new_mrs = unsafe {
            register_range(
                &self.ops,
                pd,
                access,
                self.base_virtual_addr + state.size,
                scanned_seg.size - state.size,
            )
        }?;

        // Bind the existing + new MRs to a brand-new key; clean up the new MRs
        // (and leave the segment untouched) if it fails.
        let mut all = state.mrs.clone();
        all.extend(new_mrs.iter().copied());
        // SAFETY: same contract; `all` are this segment's live MRs (existing +
        // freshly registered above).
        let new_mkey = match unsafe { self.ops.bind_mr_list(pd, qp, access, &all) } {
            Ok(mkey) => mkey,
            Err(e) => {
                // SAFETY: `new_mrs` were just returned by `register_range` and
                // are deregistered exactly once here.
                new_mrs
                    .iter()
                    .for_each(|mr| unsafe { self.ops.dereg_mr(*mr) });
                return Err(e);
            }
        };

        // Commit: keep the new MRs, retire the prior key (the first bind from
        // `empty` has none), advance the size.
        state.mrs.extend(new_mrs);
        let prior = std::mem::replace(&mut state.mkey, new_mkey);
        if !prior.is_null() {
            state.stale_mkeys.push(prior);
        }
        state.size = scanned_seg.size;
        Ok(())
    }

    /// Build a view anchored at `addr`. The view's guard pins `seg` (and
    /// `domain`) alive for its lifetime, so the bound key stays valid while the
    /// view is in use. Indirect keys present their MRs as a flat zero-based
    /// space, so `rdma_addr` is the offset from the segment base.
    fn view(
        seg: &Arc<Self>,
        addr: usize,
        size: usize,
        domain: Arc<IbvDomain>,
    ) -> IbvMemoryRegionView {
        let mkey = seg.state.lock().expect("segment state lock poisoned").mkey;
        // SAFETY: `view` is only called on a segment that covers the request,
        // which means it has been bound, so `mkey` is a live key from
        // `bind_mr_list`. If `mkey` is null, `mkey_keys` will just return `None`.
        let (lkey, rkey) =
            unsafe { seg.ops.mkey_keys(mkey) }.expect("view of a segment with no bound key");
        // `Arc<Mlx5dvMemoryRegion>` coerces to `Arc<dyn IbvMemoryRegionKeepalive>`; it pins this segment
        // (which owns the MRs + keys) and the domain (PD) for the view's life.
        let guard = Arc::new(Mlx5dvMemoryRegion {
            _segment: Arc::clone(seg),
            _domain: domain,
        });
        IbvMemoryRegionView::new(
            addr,
            addr - seg.base_virtual_addr,
            size,
            lkey,
            rkey,
            seg.ops.device_name(),
            guard,
        )
    }
}

impl Drop for RegisteredSegment {
    fn drop(&mut self) {
        // Destroy the current and every superseded key before the MRs they
        // reference, so no key briefly points at a deregistered MR.
        // TODO: invalidate keys before destroying them, to fence in-flight
        // remote access.
        let state = self.state.get_mut().expect("segment state lock poisoned");
        // SAFETY: `state.mkey`, `stale_mkeys`, and `mrs` are this segment's own
        // keys/MRs from `bind_mr_list`/`register_dmabuf_mr` (or null), each
        // destroyed/deregistered exactly once here.
        unsafe {
            self.ops.destroy_mkey(state.mkey);
            for mkey in state.stale_mkeys.drain(..) {
                self.ops.destroy_mkey(mkey);
            }
            for mr in state.mrs.drain(..) {
                self.ops.dereg_mr(mr);
            }
        }
    }
}

/// Register `[start, start + len)` of device memory as dmabuf MRs in
/// `<= MAX_MR_SIZE` chunks. All-or-nothing: on any failure the MRs registered
/// so far are deregistered before returning the error.
///
/// # Safety
///
/// If `pd` is non-null it must be a live protection domain whose context
/// outlives this call.
unsafe fn register_range(
    ops: &Arc<dyn MlxDomainOps>,
    pd: *mut rdmaxcel_sys::ibv_pd,
    access: i32,
    start: usize,
    len: usize,
) -> anyhow::Result<Vec<*mut rdmaxcel_sys::ibv_mr>> {
    let mut mrs: Vec<*mut rdmaxcel_sys::ibv_mr> = Vec::new();
    let mut chunk_start = start;
    let mut remaining = len;
    while remaining > 0 {
        let chunk = remaining.min(MAX_MR_SIZE);
        let result = if chunk.is_multiple_of(MR_ALIGNMENT) {
            // SAFETY: `pd` is null or a live PD per this function's contract.
            unsafe { ops.register_dmabuf_mr(pd, chunk_start, chunk, access) }
        } else {
            Err(anyhow::anyhow!(
                "CUDA chunk size {} is not a multiple of {}",
                chunk,
                MR_ALIGNMENT
            ))
        };
        match result {
            Ok(mr) => mrs.push(mr),
            Err(e) => {
                // SAFETY: the MRs in `mrs` were returned by `register_dmabuf_mr`
                // above and are deregistered exactly once here.
                mrs.iter().for_each(|mr| unsafe { ops.dereg_mr(*mr) });
                return Err(e);
            }
        }
        chunk_start += chunk;
        remaining -= chunk;
    }
    Ok(mrs)
}

// ===========================================================================
// MlxDomain (mlx5dv path stubbed; wired to the binding core in a follow-up)
// ===========================================================================

/// Mellanox [`IbvDomainImpl`].
pub struct MlxDomain {
    mlx5dv_enabled: bool,
}

impl IbvDomainImpl for MlxDomain {
    unsafe fn new(context: &IbvContext, _device_info: &IbvDeviceInfo, _config: &IbvConfig) -> Self {
        let ctx = context.as_ptr();
        // A null context (e.g. a test double) has no device to query, so treat
        // it as mlx5dv-unsupported rather than dereferencing null.
        let mlx5dv_enabled = if ctx.is_null() {
            false
        } else {
            // SAFETY: `ctx` is a non-null, live `ibv_context`; its `device`
            // field is the `ibv_device` we query for mlx5dv support.
            unsafe { rdmaxcel_sys::mlx5dv_is_supported((*ctx).device) }
        };
        Self { mlx5dv_enabled }
    }

    fn mr_access_flags(&self) -> i32 {
        (rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC)
            .0 as i32
    }

    unsafe fn register_mr(
        &self,
        domain: Arc<IbvDomain>,
        mem: &KeepaliveLocalMemory,
    ) -> anyhow::Result<IbvMemoryRegionView> {
        if self.mlx5dv_enabled && is_device_ptr(mem.addr()) {
            // SAFETY: `domain.as_ptr()` is null or a live PD, per this method's
            // contract.
            match unsafe { self.register_cuda_mlx5dv_mr(domain.as_ptr(), mem) } {
                Ok(view) => return Ok(view),
                Err(e) => {
                    tracing::warn!("mlx5dv CUDA registration failed, falling back to dmabuf: {e}")
                }
            }
        }
        // SAFETY: `domain.as_ptr()` is null or a live PD (per this method's
        // contract; `register_host_or_dmabuf_mr` errors on null), and the caller
        // keeps `mem`'s backing memory valid for the MR's lifetime.
        unsafe { register_host_or_dmabuf_mr(domain, self.mr_access_flags(), mem) }
    }
}

impl MlxDomain {
    /// Bind CUDA memory via an indirect mlx5dv memory key. Stub: the binding
    /// core ([`RegisteredSegment`]) lands in this commit, but is driven by
    /// [`MlxDomain`] only in a follow-up; today this always errors so
    /// [`Self::register_mr`] falls back to a dmabuf MR.
    ///
    /// # Safety
    ///
    /// If `pd` is non-null it must be a live protection domain whose context
    /// outlives this call.
    unsafe fn register_cuda_mlx5dv_mr(
        &self,
        _pd: *mut rdmaxcel_sys::ibv_pd,
        _mem: &KeepaliveLocalMemory,
    ) -> anyhow::Result<IbvMemoryRegionView> {
        anyhow::bail!("mlx5dv CUDA registration not yet implemented")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::sync::MutexGuard;

    use super::super::device::IbvContext;
    use super::super::primitives::IbvDeviceInfo;
    use super::*;

    const MIB2: usize = 2 * 1024 * 1024;
    const SERVED_NIC: &str = "mlx5_0";

    fn null_pd() -> *mut rdmaxcel_sys::ibv_pd {
        std::ptr::null_mut()
    }

    fn null_qp() -> *mut rdmaxcel_sys::rdmaxcel_qp {
        std::ptr::null_mut()
    }

    /// A test domain whose `pd`/`context` are null, so its `Drop` is a no-op.
    /// Segment views hold it only as a (never-read) keepalive.
    fn fake_domain() -> Arc<IbvDomain> {
        // SAFETY: null `ibv_context*`/`pd` are explicitly allowed — both
        // `IbvContext` and `IbvDomain` skip their FFI destructors for null.
        unsafe {
            Arc::new(IbvDomain::from_parts(
                Arc::new(IbvContext::new(std::ptr::null_mut())),
                std::ptr::null_mut(),
                IbvDeviceInfo::for_test_named("test"),
            ))
        }
    }

    fn seg(address: usize, size: usize, cuda_ordinal: i32) -> ScannedSegment {
        ScannedSegment {
            address,
            size,
            cuda_ordinal,
            is_expandable: false,
        }
    }

    /// A recorded `register_dmabuf_mr` call.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct DmabufCall {
        addr: usize,
        size: usize,
        access: i32,
    }

    /// A recorded (successful) `bind_mr_list` call: the MR handles bound, as
    /// `usize`.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct BindCall {
        mrs: Vec<usize>,
    }

    /// Recorded state + scripted behavior for [`MockOps`]. FFI pointers are
    /// fabricated as incrementing integers cast to the real pointer types.
    /// `live_*` track outstanding registrations for leak assertions; the
    /// `*_calls` / `*_log` vectors record each call's arguments in order so
    /// tests can assert on them.
    #[derive(Default)]
    struct MockState {
        device_name: String,
        next_handle: usize,
        live_mrs: HashSet<usize>,
        live_mkeys: HashSet<usize>,
        fail_dmabuf_after: Option<usize>,
        fail_bind: bool,
        /// Every `register_dmabuf_mr` call, including a failing one.
        dmabuf_calls: Vec<DmabufCall>,
        /// Every successful `bind_mr_list` call.
        bind_calls: Vec<BindCall>,
        /// `mr` handle passed to each `dereg_mr`, in order.
        dereg_log: Vec<usize>,
        /// `mkey` handle passed to each `destroy_mkey`, in order.
        destroy_log: Vec<usize>,
        /// Teardown events in order: "mkey" on `destroy_mkey`, "mr" on
        /// `dereg_mr`. Lets a test assert keys are destroyed before MRs.
        teardown_order: Vec<&'static str>,
    }

    impl MockState {
        fn mint(&mut self) -> usize {
            self.next_handle += 0x1000;
            self.next_handle
        }
    }

    struct MockOps {
        state: Mutex<MockState>,
    }

    impl MockOps {
        fn new(device_name: &str) -> Arc<Self> {
            Arc::new(Self {
                state: Mutex::new(MockState {
                    device_name: device_name.to_string(),
                    next_handle: 0x1_0000,
                    ..Default::default()
                }),
            })
        }

        fn lock(&self) -> MutexGuard<'_, MockState> {
            self.state.lock().expect("mock state poisoned")
        }
    }

    impl MlxDomainOps for MockOps {
        fn device_name(&self) -> String {
            self.lock().device_name.clone()
        }

        unsafe fn register_dmabuf_mr(
            &self,
            _pd: *mut rdmaxcel_sys::ibv_pd,
            addr: usize,
            size: usize,
            access: i32,
        ) -> anyhow::Result<*mut rdmaxcel_sys::ibv_mr> {
            let mut s = self.lock();
            s.dmabuf_calls.push(DmabufCall { addr, size, access });
            if let Some(n) = s.fail_dmabuf_after
                && s.dmabuf_calls.len() > n
            {
                anyhow::bail!("mock dmabuf registration failure");
            }
            let h = s.mint();
            s.live_mrs.insert(h);
            Ok(h as *mut rdmaxcel_sys::ibv_mr)
        }

        unsafe fn dereg_mr(&self, mr: *mut rdmaxcel_sys::ibv_mr) {
            let mut s = self.lock();
            let v = mr as usize;
            s.live_mrs.remove(&v);
            s.dereg_log.push(v);
            s.teardown_order.push("mr");
        }

        unsafe fn bind_mr_list(
            &self,
            _pd: *mut rdmaxcel_sys::ibv_pd,
            _qp: *mut rdmaxcel_sys::rdmaxcel_qp,
            _access: i32,
            mrs: &[*mut rdmaxcel_sys::ibv_mr],
        ) -> anyhow::Result<*mut rdmaxcel_sys::mlx5dv_mkey> {
            let mut s = self.lock();
            if s.fail_bind {
                anyhow::bail!("mock bind failure");
            }
            s.bind_calls.push(BindCall {
                mrs: mrs.iter().map(|mr| *mr as usize).collect(),
            });
            let h = s.mint();
            s.live_mkeys.insert(h);
            Ok(h as *mut rdmaxcel_sys::mlx5dv_mkey)
        }

        unsafe fn destroy_mkey(&self, mkey: *mut rdmaxcel_sys::mlx5dv_mkey) {
            if mkey.is_null() {
                return;
            }
            let mut s = self.lock();
            let v = mkey as usize;
            s.live_mkeys.remove(&v);
            s.destroy_log.push(v);
            s.teardown_order.push("mkey");
        }

        unsafe fn mkey_keys(&self, mkey: *mut rdmaxcel_sys::mlx5dv_mkey) -> Option<(u32, u32)> {
            if mkey.is_null() {
                return None;
            }
            let v = mkey as usize as u32;
            Some((v, v ^ 0xffff))
        }
    }

    /// Upcast the mock to the `Arc<dyn MlxDomainOps>` `RegisteredSegment` takes.
    fn dyn_ops(ops: &Arc<MockOps>) -> Arc<dyn MlxDomainOps> {
        ops.clone()
    }

    /// Bind a fresh segment `[base, base + size)`: an empty segment grown once.
    fn bind_fresh(ops: &Arc<MockOps>, base: usize, size: usize) -> Arc<RegisteredSegment> {
        let segment = Arc::new(RegisteredSegment::empty(dyn_ops(ops), base));
        // SAFETY: `MockOps` ignores the raw `pd`/`qp`; the nulls are never deref'd.
        unsafe { segment.grow(null_pd(), null_qp(), 0, &seg(base, size, 0)) }
            .expect("fresh bind should succeed");
        segment
    }

    #[test]
    fn test_scanner_registration_round_trips() {
        register_cuda_segment_scanner(Box::new(|| vec![seg(0x1000, MIB2, 3)]));
        assert_eq!(
            scan_cuda_segments(),
            vec![seg(0x1000, MIB2, 3)],
            "the registered scanner's output is returned"
        );
    }

    #[test]
    fn test_fresh_bind_registers_one_mr_and_key() {
        let ops = MockOps::new(SERVED_NIC);
        let base = 0x10_0000_0000;
        let rs = bind_fresh(&ops, base, MIB2);

        assert_eq!(rs.size(), MIB2);
        assert!(
            rs.state.lock().unwrap().stale_mkeys.is_empty(),
            "a fresh segment has no superseded keys"
        );
        let s = ops.lock();
        assert_eq!(
            s.dmabuf_calls,
            vec![DmabufCall {
                addr: base,
                size: MIB2,
                access: 0,
            }],
            "the whole segment is registered as one MR at its base"
        );
        assert_eq!(s.bind_calls.len(), 1);
        assert_eq!(s.bind_calls[0].mrs.len(), 1, "one MR bound");
        assert_eq!(s.live_mrs.len(), 1);
        assert_eq!(s.live_mkeys.len(), 1, "one mkey created");
    }

    #[test]
    fn test_large_segment_splits_into_chunks() {
        let ops = MockOps::new(SERVED_NIC);
        let base = 0x10_0000_0000;
        // One MR maxes out at MAX_MR_SIZE, so MAX_MR_SIZE + 2 MiB needs two,
        // both bound to a single key.
        let rs = bind_fresh(&ops, base, MAX_MR_SIZE + MIB2);

        assert_eq!(rs.state.lock().unwrap().mrs.len(), 2);
        let s = ops.lock();
        assert_eq!(
            s.dmabuf_calls,
            vec![
                DmabufCall {
                    addr: base,
                    size: MAX_MR_SIZE,
                    access: 0,
                },
                DmabufCall {
                    addr: base + MAX_MR_SIZE,
                    size: MIB2,
                    access: 0,
                },
            ],
            "the range splits into a MAX_MR_SIZE chunk and a 2 MiB tail"
        );
        assert_eq!(s.bind_calls.len(), 1);
        assert_eq!(
            s.bind_calls[0].mrs.len(),
            2,
            "both chunk MRs bound to one key"
        );
        assert_eq!(s.live_mrs.len(), 2);
        assert_eq!(s.live_mkeys.len(), 1);
    }

    #[test]
    fn test_grow_reuses_mrs_and_retires_prior_key() {
        let ops = MockOps::new(SERVED_NIC);
        let base = 0x10_0000_0000;
        let rs = bind_fresh(&ops, base, MIB2);
        let mkey_a = *ops.lock().live_mkeys.iter().next().unwrap();
        let mr_a = ops.lock().bind_calls[0].mrs[0];

        // SAFETY: `MockOps` ignores the raw `pd`/`qp`; the nulls are never deref'd.
        unsafe { rs.grow(null_pd(), null_qp(), 0, &seg(base, 2 * MIB2, 0)) }
            .expect("growth should succeed");

        assert_eq!(rs.size(), 2 * MIB2);
        assert_eq!(
            rs.state.lock().unwrap().mrs.len(),
            2,
            "the original MR is reused and one tail MR appended"
        );
        assert_eq!(
            rs.state.lock().unwrap().stale_mkeys.len(),
            1,
            "the prior key is parked as stale"
        );
        let s = ops.lock();
        // Only the new tail is registered; the original MR is reused.
        assert_eq!(
            s.dmabuf_calls,
            vec![
                DmabufCall {
                    addr: base,
                    size: MIB2,
                    access: 0,
                },
                DmabufCall {
                    addr: base + MIB2,
                    size: MIB2,
                    access: 0,
                },
            ],
            "growth registers only the new tail, at base + MIB2"
        );
        assert_eq!(s.bind_calls.len(), 2);
        assert_eq!(
            s.bind_calls[0].mrs,
            vec![mr_a],
            "the first bind covered just the original MR"
        );
        let mr_tail = s.bind_calls[1].mrs[1];
        assert_eq!(
            s.bind_calls[1].mrs,
            vec![mr_a, mr_tail],
            "the second bind reuses the original MR and adds the tail (a \
             brand-new key, never a rebind)"
        );
        assert_eq!(
            s.live_mrs.len(),
            2,
            "only one new MR registered (tail reuse)"
        );
        assert_eq!(
            s.live_mkeys.len(),
            2,
            "the prior key is kept alongside the new current key"
        );
        assert!(
            s.live_mkeys.contains(&mkey_a),
            "the prior key was parked, not destroyed"
        );
    }

    #[test]
    fn test_grow_failure_leaves_segment_unchanged() {
        let ops = MockOps::new(SERVED_NIC);
        let base = 0x10_0000_0000;
        let rs = bind_fresh(&ops, base, MIB2);
        // The fresh bind used one dmabuf call; fail the second tail chunk.
        ops.lock().fail_dmabuf_after = Some(2);

        // SAFETY: `MockOps` ignores the raw `pd`/`qp`; the nulls are never deref'd.
        let result = unsafe {
            rs.grow(
                null_pd(),
                null_qp(),
                0,
                &seg(base, MAX_MR_SIZE + 2 * MIB2, 0),
            )
        };
        assert!(
            result.is_err(),
            "growth fails when a tail MR fails to register"
        );

        assert_eq!(rs.size(), MIB2, "size unchanged after a failed growth");
        assert_eq!(
            rs.state.lock().unwrap().mrs.len(),
            1,
            "no tail MRs committed"
        );
        assert!(
            rs.state.lock().unwrap().stale_mkeys.is_empty(),
            "no key retired"
        );
        let s = ops.lock();
        assert_eq!(
            s.live_mrs.len(),
            1,
            "the partially-registered tail MR was cleaned up"
        );
        assert_eq!(
            s.bind_calls.len(),
            1,
            "only the original bind happened; the growth bind was never reached"
        );
    }

    #[test]
    fn test_grow_to_equal_size_is_noop() {
        let ops = MockOps::new(SERVED_NIC);
        let base = 0x10_0000_0000;
        let rs = bind_fresh(&ops, base, 2 * MIB2);
        let binds_before = ops.lock().bind_calls.len();

        // A scan reporting the same size must be a no-op rather than re-binding.
        // SAFETY: `MockOps` ignores the raw `pd`/`qp`; the nulls are never deref'd.
        unsafe { rs.grow(null_pd(), null_qp(), 0, &seg(base, 2 * MIB2, 0)) }
            .expect("equal-size grow is a no-op");

        assert_eq!(rs.size(), 2 * MIB2, "size is unchanged");
        let s = ops.lock();
        assert_eq!(s.bind_calls.len(), binds_before, "no new bind performed");
        assert_eq!(s.live_mrs.len(), 1, "no new MR registered");
    }

    #[test]
    fn test_view_pins_segment_and_anchors_at_base() {
        let ops = MockOps::new(SERVED_NIC);
        let base = 0x10_0000_0000;
        let rs = bind_fresh(&ops, base, MIB2);

        assert!(rs.covers(base + 0x400, 4096));
        let view = RegisteredSegment::view(&rs, base + 0x400, 4096, fake_domain());
        assert_eq!(
            view.rdma_addr, 0x400,
            "rdma_addr is the offset from the segment base"
        );
        assert_eq!(view.size, 4096);
        assert_eq!(view.device_name, SERVED_NIC);
        assert_eq!(
            Arc::strong_count(&rs),
            2,
            "the view's guard pins the segment alive (our ref + the view's)"
        );

        drop(view);
        assert_eq!(
            Arc::strong_count(&rs),
            1,
            "dropping the view releases its segment ref"
        );
    }

    #[test]
    fn test_partial_dmabuf_failure_cleans_up() {
        let ops = MockOps::new(SERVED_NIC);
        ops.lock().fail_dmabuf_after = Some(1);
        let base = 0x10_0000_0000;
        let segment = RegisteredSegment::empty(dyn_ops(&ops), base);

        // Two chunks; the second dmabuf registration fails.
        // SAFETY: `MockOps` ignores the raw `pd`/`qp`; the nulls are never deref'd.
        let result =
            unsafe { segment.grow(null_pd(), null_qp(), 0, &seg(base, MAX_MR_SIZE + MIB2, 0)) };
        assert!(
            result.is_err(),
            "the first bind fails when a chunk registration fails"
        );
        assert_eq!(segment.size(), 0, "the segment is left empty");
        let s = ops.lock();
        assert_eq!(s.dereg_log.len(), 1, "the first chunk's MR is cleaned up");
        assert!(s.live_mrs.is_empty(), "no MR leaks after partial failure");
        assert!(s.bind_calls.is_empty(), "no bind performed");
        assert!(s.live_mkeys.is_empty(), "no mkey created");
    }

    #[test]
    fn test_bind_failure_cleans_up() {
        let ops = MockOps::new(SERVED_NIC);
        ops.lock().fail_bind = true;
        let base = 0x10_0000_0000;
        let segment = RegisteredSegment::empty(dyn_ops(&ops), base);

        // SAFETY: `MockOps` ignores the raw `pd`/`qp`; the nulls are never deref'd.
        let result = unsafe { segment.grow(null_pd(), null_qp(), 0, &seg(base, MIB2, 0)) };
        assert!(result.is_err(), "the bind fails when bind_mr_list fails");
        assert_eq!(segment.size(), 0, "the segment is left empty");
        let s = ops.lock();
        assert_eq!(
            s.dereg_log.len(),
            1,
            "the freshly-registered MR is cleaned up"
        );
        assert!(s.live_mrs.is_empty(), "no MR leaks after bind failure");
        assert!(s.live_mkeys.is_empty(), "no mkey created on bind failure");
    }

    #[test]
    fn test_drop_destroys_all_keys_before_mrs() {
        let ops = MockOps::new(SERVED_NIC);
        let base = 0x10_0000_0000;
        let rs = bind_fresh(&ops, base, MIB2);
        // Grow once so the segment carries a parked (stale) key plus its
        // current key, and two MRs.
        // SAFETY: `MockOps` ignores the raw `pd`/`qp`; the nulls are never deref'd.
        unsafe { rs.grow(null_pd(), null_qp(), 0, &seg(base, 2 * MIB2, 0)) }
            .expect("growth should succeed");
        assert_eq!(ops.lock().live_mrs.len(), 2);
        assert_eq!(ops.lock().live_mkeys.len(), 2);

        drop(rs); // last `Arc` ref → `RegisteredSegment::drop`

        let s = ops.lock();
        assert!(s.live_mrs.is_empty(), "drop frees the MRs");
        assert!(
            s.live_mkeys.is_empty(),
            "drop destroys the current + stale keys"
        );
        assert_eq!(
            s.teardown_order,
            vec!["mkey", "mkey", "mr", "mr"],
            "every key is destroyed before any MR it might reference"
        );
    }
}
