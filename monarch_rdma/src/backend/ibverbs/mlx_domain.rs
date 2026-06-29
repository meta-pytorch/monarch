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
//! The segment scanning + binding bookkeeping lives here in Rust. Every
//! device/FFI touch goes through [`MlxDomainOps`] so the (intricate)
//! scan/bind/teardown logic can be unit-tested against a mock; the
//! production implementation ([`ProdMlxDomainOps`]) delegates to the real
//! functions and the [`rdmaxcel_sys::rdmaxcel_bind_mr_list`] shim.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use anyhow::Context;

use super::device::IbvContext;
use super::device_selection::get_cuda_device_to_ibv_device;
use super::domain::IbvDomain;
use super::domain::IbvDomainImpl;
use super::domain::register_dmabuf_range;
use super::domain::register_host_or_dmabuf_mr;
use super::memory_region::IbvMemoryRegionKeepalive;
use super::memory_region::IbvMemoryRegionView;
use super::primitives::IbvConfig;
use super::primitives::IbvDeviceInfo;
use super::queue_pair::legacy::IbvQueuePair;
use crate::backend::ibverbs::mlx_device::MlxDevice;
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

/// Enumerates the currently-live CUDA segments. An `Arc` (not `Box`) so
/// [`scan_cuda_segments`] can clone it out and drop the registry lock before
/// invoking it — see that function.
pub type CudaSegmentScanner = Arc<dyn Fn() -> Vec<ScannedSegment> + Send + Sync>;

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
    // Clone the `Arc` and release the registry lock before invoking: the
    // pytorch scanner takes the Python GIL, and even though scan_cuda_segments
    // is never called concurrently from multiple threads (in the current design),
    // this protects against the possibility of deadlock.
    let scanner = CUDA_SEGMENT_SCANNER
        .lock()
        .expect("CUDA segment scanner lock poisoned")
        .clone();
    scanner.as_deref().map(|scan| scan()).unwrap_or_default()
}

// ===========================================================================
// MlxDomainOps: the device/FFI surface MlxDomain delegates to
// ===========================================================================

/// Device/FFI operations the mlx5dv binding logic depends on. Production
/// uses [`ProdMlxDomainOps`]; tests substitute a mock so the scan/bind/
/// teardown algorithm can be exercised without hardware. The raw FFI
/// pointer types are used directly — the mock fabricates fake pointers.
pub(super) trait MlxDomainOps: Send + Sync + 'static {
    /// Name of the RDMA device this domain drives.
    fn device_name(&self) -> String;

    /// Whether the device supports mlx5dv direct verbs.
    fn mlx5dv_enabled(&self) -> bool;

    /// CUDA ordinals whose optimal NIC is this domain's device; only segments
    /// on these ordinals are bound here.
    fn assigned_cuda_devices(&self) -> Vec<i32>;

    /// Enumerate the currently-live CUDA segments.
    fn scan_segments(&self) -> Vec<ScannedSegment>;

    /// Register `[addr, addr + size)` of device memory as a dmabuf MR.
    ///
    /// # Safety
    ///
    /// If `pd` is non-null it must be a live protection domain whose context
    /// outlives this call.
    unsafe fn register_dmabuf_range(
        &self,
        pd: *mut rdmaxcel_sys::ibv_pd,
        addr: usize,
        size: usize,
        access: i32,
    ) -> anyhow::Result<*mut rdmaxcel_sys::ibv_mr>;

    /// Deregister an MR returned by [`Self::register_dmabuf_range`].
    ///
    /// # Safety
    ///
    /// `mr` must be null or an MR returned by [`Self::register_dmabuf_range`] that
    /// has not already been deregistered.
    unsafe fn dereg_mr(&self, mr: *mut rdmaxcel_sys::ibv_mr);

    /// Create and connect a fresh loopback queue pair against `domain`'s PD,
    /// returning the raw `rdmaxcel_qp` pointer. The caller ([`MlxDomain`])
    /// caches it and is responsible for destroying it via
    /// [`Self::destroy_loopback_qp`]. We manage the raw pointer directly since
    /// we only ever use it for indirect mkey binding.
    fn create_loopback_qp(
        &self,
        domain: Arc<IbvDomain<MlxDomain>>,
        config: &IbvConfig,
    ) -> anyhow::Result<*mut rdmaxcel_sys::rdmaxcel_qp>;

    /// Destroy a loopback QP returned by [`Self::create_loopback_qp`].
    ///
    /// # Safety
    ///
    /// `qp` must be null or a queue pair returned by [`Self::create_loopback_qp`]
    /// that has not already been destroyed.
    unsafe fn destroy_loopback_qp(&self, qp: *mut rdmaxcel_sys::rdmaxcel_qp);

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

/// Production [`MlxDomainOps`] backed by the real ibverbs / mlx5dv FFI.
///
/// The device name (taken from the queried [`IbvDeviceInfo`]) and mlx5dv
/// support are immutable properties of the device, recorded once at
/// construction; the per-op FFI methods operate on the `pd` passed in by
/// [`MlxDomain`], so no context handle needs to be retained here.
pub(super) struct ProdMlxDomainOps {
    device_name: String,
    mlx5dv_enabled: bool,
}

impl ProdMlxDomainOps {
    fn new(context: &IbvContext, device_info: &IbvDeviceInfo) -> Self {
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
        Self {
            device_name: device_info.name().to_string(),
            mlx5dv_enabled,
        }
    }
}

impl MlxDomainOps for ProdMlxDomainOps {
    fn device_name(&self) -> String {
        self.device_name.clone()
    }

    fn mlx5dv_enabled(&self) -> bool {
        self.mlx5dv_enabled
    }

    fn assigned_cuda_devices(&self) -> Vec<i32> {
        get_cuda_device_to_ibv_device::<MlxDevice>()
            .iter()
            .enumerate()
            .filter_map(|(ordinal, nic)| {
                nic.as_ref()
                    .filter(|n| n.name() == &self.device_name)
                    .map(|_| ordinal as i32)
            })
            .collect()
    }

    fn scan_segments(&self) -> Vec<ScannedSegment> {
        scan_cuda_segments()
    }

    unsafe fn register_dmabuf_range(
        &self,
        pd: *mut rdmaxcel_sys::ibv_pd,
        addr: usize,
        size: usize,
        access: i32,
    ) -> anyhow::Result<*mut rdmaxcel_sys::ibv_mr> {
        // SAFETY: forwards this method's contract (non-null `pd` is a live PD).
        unsafe { register_dmabuf_range(pd, addr, size, access) }
    }

    unsafe fn dereg_mr(&self, mr: *mut rdmaxcel_sys::ibv_mr) {
        if mr.is_null() {
            return;
        }
        // SAFETY: `mr` was returned by `register_dmabuf_range` and is dereg'd
        // exactly once.
        let result = unsafe { rdmaxcel_sys::ibv_dereg_mr(mr) };
        if result != 0 {
            tracing::error!("failed to deregister MR at {:p}: error code {}", mr, result);
        }
    }

    fn create_loopback_qp(
        &self,
        domain: Arc<IbvDomain<MlxDomain>>,
        config: &IbvConfig,
    ) -> anyhow::Result<*mut rdmaxcel_sys::rdmaxcel_qp> {
        // Build a full queue pair (QP + mlx5dv structures) and connect it to
        // its own endpoint (loopback) so it can post the key-binding work
        // request, then take ownership of the raw pointer: MlxDomain manages
        // the QP directly and destroys it via destroy_loopback_qp.
        let mut qp = IbvQueuePair::new(domain, config.clone())
            .context("could not create loopback QP for mkey binding")?;
        let info = qp
            .get_qp_info()
            .context("could not query loopback QP info for mkey binding")?;
        qp.connect(&info)
            .context("could not connect loopback QP for mkey binding")?;
        Ok(qp.take_ptr())
    }

    unsafe fn destroy_loopback_qp(&self, qp: *mut rdmaxcel_sys::rdmaxcel_qp) {
        if qp.is_null() {
            return;
        }
        // SAFETY: `qp` is non-null (checked above), was returned by
        // `create_loopback_qp`'s `rdmaxcel_qp_create`, and is destroyed exactly
        // once.
        unsafe { rdmaxcel_sys::rdmaxcel_qp_destroy(qp) };
    }

    unsafe fn bind_mr_list(
        &self,
        pd: *mut rdmaxcel_sys::ibv_pd,
        qp: *mut rdmaxcel_sys::rdmaxcel_qp,
        access: i32,
        mrs: &[*mut rdmaxcel_sys::ibv_mr],
    ) -> anyhow::Result<*mut rdmaxcel_sys::mlx5dv_mkey> {
        if pd.is_null() || qp.is_null() {
            anyhow::bail!("bind_mr_list called with a null protection domain or queue pair");
        }
        // `rdmaxcel_bind_mr_list` creates the indirect key in place when the
        // `mkey` out-param starts null, which it always does here.
        let mut mkey: *mut rdmaxcel_sys::mlx5dv_mkey = std::ptr::null_mut();
        // SAFETY: `pd` and `qp` are non-null (checked above), so reading `qp`'s
        // `ibv_qp` field is sound; `mrs` is a contiguous array of `mrs.len()`
        // non-null MRs; `mkey` is created in place. `rdmaxcel_bind_mr_list`
        // reports any failure via its return code.
        let ret = unsafe {
            let ibv_qp = (*qp).ibv_qp;
            rdmaxcel_sys::rdmaxcel_bind_mr_list(
                pd,
                ibv_qp,
                access,
                mrs.as_ptr() as *mut *mut rdmaxcel_sys::ibv_mr,
                mrs.len(),
                &mut mkey,
            )
        };
        if ret != 0 {
            anyhow::bail!("rdmaxcel_bind_mr_list failed: error code {}", ret);
        }
        Ok(mkey)
    }

    unsafe fn destroy_mkey(&self, mkey: *mut rdmaxcel_sys::mlx5dv_mkey) {
        if mkey.is_null() {
            return;
        }
        // SAFETY: `mkey` was created by `rdmaxcel_bind_mr_list` and is
        // destroyed exactly once.
        unsafe { rdmaxcel_sys::rdmaxcel_destroy_mkey(mkey) };
    }

    unsafe fn mkey_keys(&self, mkey: *mut rdmaxcel_sys::mlx5dv_mkey) -> Option<(u32, u32)> {
        if mkey.is_null() {
            return None;
        }
        // SAFETY: `mkey` is non-null (checked above), a key bound by `bind_mr_list`.
        Some(unsafe { ((*mkey).lkey, (*mkey).rkey) })
    }
}

// ===========================================================================
// RegisteredSegment: a CUDA segment bound to an indirect key
// ===========================================================================

/// [`IbvMemoryRegionKeepalive`] for a segment-bound mlx5dv MR: a no-op (no
/// `Drop`). It pins the owning [`RegisteredSegment`] — which deregisters its MRs
/// and destroys its keys on its own `Drop` — and the domain (PD keepalive).
///
/// `_segment` is declared before `_domain` so it drops first (struct fields
/// drop in declaration order). Each [`IbvDomain`] stores its [`IbvDomainImpl`]
/// (here, [`MlxDomain`]), so an `IbvDomain<MlxDomain>` itself holds a strong
/// reference to every `Arc<RegisteredSegment>` it has bound. Were `_domain`
/// dropped before `_segment`, the PD could be deallocated before a segment
/// registered against it — wrong. Dropping `_segment` first won't actually free
/// the segment (the domain still references it), but it ensures that once the
/// last `_domain` reference drops, the segment is freed before the PD is
/// deallocated.
#[derive(Debug)]
struct Mlx5dvMemoryRegion {
    _segment: Arc<RegisteredSegment>,
    _domain: Arc<IbvDomain<MlxDomain>>,
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
        domain: Arc<IbvDomain<MlxDomain>>,
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
        // keys/MRs from `bind_mr_list`/`register_dmabuf_range` (or null), each
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
            unsafe { ops.register_dmabuf_range(pd, chunk_start, chunk, access) }
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
                // SAFETY: the MRs in `mrs` were returned by `register_dmabuf_range`
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
// MlxDomain
// ===========================================================================

/// Mellanox [`IbvDomainImpl`].
pub struct MlxDomain {
    ops: Arc<dyn MlxDomainOps>,
    /// Config for the loopback binding QP.
    config: IbvConfig,
    mlx5dv_enabled: bool,
    /// CUDA ordinals whose optimal NIC is this device. Only segments on
    /// these ordinals are bound here.
    cuda_ordinals: Vec<i32>,
    /// Lazily-created loopback QP used to post key-binding work requests,
    /// held as a raw `rdmaxcel_qp` pointer (stored as `usize` for `Send`)
    /// owned by this domain and destroyed in [`Drop`].
    loopback_qp: OnceLock<usize>,
    /// Currently-bound segments, keyed by `(base address, CUDA ordinal)`. Each
    /// grows in place (reusing its MRs, retiring superseded keys internally);
    /// a key whose base vanishes from the scan is dropped (a live view keeps
    /// its own `Arc` alive regardless).
    segments: Mutex<HashMap<(usize, i32), Arc<RegisteredSegment>>>,
}

impl std::fmt::Debug for MlxDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MlxDomain")
            .field("mlx5dv_enabled", &self.mlx5dv_enabled)
            .field("cuda_ordinals", &self.cuda_ordinals)
            .finish_non_exhaustive()
    }
}

impl Drop for MlxDomain {
    fn drop(&mut self) {
        if let Some(&qp) = self.loopback_qp.get() {
            // SAFETY: `qp` was returned by `create_loopback_qp` and is destroyed
            // exactly once, here, at the end of this domain's life.
            unsafe {
                self.ops
                    .destroy_loopback_qp(qp as *mut rdmaxcel_sys::rdmaxcel_qp);
            }
        }
    }
}

impl MlxDomain {
    /// Build a domain over the given ops, deriving mlx5dv support and the
    /// served CUDA ordinals from them.
    fn new_with_ops(ops: Arc<dyn MlxDomainOps>, config: IbvConfig) -> Self {
        let mlx5dv_enabled = ops.mlx5dv_enabled();
        let cuda_ordinals = ops.assigned_cuda_devices();
        Self {
            ops,
            config,
            mlx5dv_enabled,
            cuda_ordinals,
            loopback_qp: OnceLock::new(),
            segments: Mutex::new(HashMap::new()),
        }
    }

    /// Get-or-create the loopback QP and return its raw `rdmaxcel_qp`
    /// pointer.
    fn loopback_qp_ptr(
        &self,
        domain: &Arc<IbvDomain<MlxDomain>>,
    ) -> anyhow::Result<*mut rdmaxcel_sys::rdmaxcel_qp> {
        // `OnceLock::get_or_try_init` would fit here but is still unstable
        // (`once_cell_try`); calls are serialized under the `segments` lock,
        // so this check-then-set is race-free.
        if let Some(&qp) = self.loopback_qp.get() {
            return Ok(qp as *mut rdmaxcel_sys::rdmaxcel_qp);
        }
        let qp = self
            .ops
            .create_loopback_qp(Arc::clone(domain), &self.config)?;
        self.loopback_qp
            .set(qp as usize)
            .expect("loopback qp already initialized");
        Ok(qp)
    }

    /// Bind CUDA `[addr, addr + size)` via an indirect mlx5dv key. Scans for
    /// live segments on the ordinals this NIC serves, binding a fresh segment
    /// for any that appeared and growing in place any that expanded, dropping
    /// any whose base address vanished from the scan, then returns a view
    /// anchored at `addr`.
    ///
    /// # Safety
    ///
    /// `domain.as_ptr()` must be null or a live protection domain whose context
    /// outlives this call.
    unsafe fn register_cuda_mlx5dv_mr(
        &self,
        domain: &Arc<IbvDomain<MlxDomain>>,
        addr: usize,
        size: usize,
    ) -> anyhow::Result<IbvMemoryRegionView> {
        let pd = domain.as_ptr();
        let access = self.mr_access_flags();
        let mut segments = self
            .segments
            .lock()
            .expect("mlx domain segments lock poisoned");

        // Fast path: a current binding already covers the request.
        if let Some(seg) = segments.values().find(|s| s.covers(addr, size)) {
            return Ok(RegisteredSegment::view(seg, addr, size, Arc::clone(domain)));
        }

        // Pull live segments and keep only the ones on ordinals we serve.
        let scanned: Vec<ScannedSegment> = self
            .ops
            .scan_segments()
            .into_iter()
            .filter(|s| self.cuda_ordinals.contains(&s.cuda_ordinal))
            .collect();

        let qp = self.loopback_qp_ptr(domain)?;

        let mut snapshot: HashSet<(usize, i32)> = HashSet::new();
        for scanned_seg in &scanned {
            let key = (scanned_seg.address, scanned_seg.cuda_ordinal);
            snapshot.insert(key);
            match segments.get(&key) {
                // Already bound at this extent: nothing to do.
                Some(seg) if seg.size() == scanned_seg.size => {}
                // Grew: extend the existing segment in place (reuses its MRs,
                // retires its prior key internally).
                // SAFETY: `pd`/`qp` satisfy this function's contract (live PD or
                // null; valid loopback QP) and are forwarded unchanged.
                Some(seg) => unsafe { seg.grow(pd, qp, access, scanned_seg) }?,
                // New: create an empty segment and grow it to the full range.
                None => {
                    let fresh = Arc::new(RegisteredSegment::empty(
                        self.ops.clone(),
                        scanned_seg.address,
                    ));
                    // SAFETY: as above.
                    unsafe { fresh.grow(pd, qp, access, scanned_seg) }?;
                    segments.insert(key, fresh);
                }
            }
        }

        // Drop segments whose base address vanished from the scan: the
        // allocator freed that region. A still-live view keeps its own `Arc`
        // alive regardless, so this never frees memory in use.
        segments.retain(|key, _| snapshot.contains(key));

        // Serve the caller's view from a current segment that covers it.
        segments
            .values()
            .find(|s| s.covers(addr, size))
            .map(|s| RegisteredSegment::view(s, addr, size, Arc::clone(domain)))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "CUDA address 0x{:x} + size {} is not covered by any scanned segment on the CUDA ordinals {:?} mapped to this NIC",
                    addr,
                    size,
                    self.cuda_ordinals,
                )
            })
    }
}

impl IbvDomainImpl for MlxDomain {
    type QueuePair = IbvQueuePair;

    unsafe fn new(context: &IbvContext, device_info: &IbvDeviceInfo, config: &IbvConfig) -> Self {
        Self::new_with_ops(
            Arc::new(ProdMlxDomainOps::new(context, device_info)),
            config.clone(),
        )
    }

    fn mr_access_flags(&self) -> i32 {
        (rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC)
            .0 as i32
    }

    fn create_queue_pair(
        domain: Arc<IbvDomain<Self>>,
        config: &IbvConfig,
    ) -> anyhow::Result<Self::QueuePair> {
        // mlx5 builds the legacy single-type queue pair for now; it will
        // become an mlx5-specific queue pair.
        IbvQueuePair::new(domain, config.clone())
    }

    unsafe fn register_mr(
        domain: Arc<IbvDomain<MlxDomain>>,
        mem: &KeepaliveLocalMemory,
    ) -> anyhow::Result<IbvMemoryRegionView> {
        let this = domain.domain_impl();
        if this.mlx5dv_enabled && is_device_ptr(mem.addr()) {
            // SAFETY: `domain.as_ptr()` is null or a live PD, per this method's
            // contract.
            match unsafe { this.register_cuda_mlx5dv_mr(&domain, mem.addr(), mem.size()) } {
                Ok(view) => return Ok(view),
                Err(e) => {
                    tracing::warn!("mlx5dv CUDA registration failed, falling back to dmabuf: {e}")
                }
            }
        }
        // SAFETY: `domain.as_ptr()` is null or a live PD (per this method's
        // contract; `register_host_or_dmabuf_mr` errors on null), and the caller
        // keeps `mem`'s backing memory valid for the MR's lifetime.
        unsafe { register_host_or_dmabuf_mr(domain, mem) }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::sync::MutexGuard;

    use super::super::primitives::IbvDeviceInfo;
    use super::*;

    const MIB2: usize = 2 * 1024 * 1024;
    const SERVED_NIC: &str = "mlx5_0";

    fn seg(address: usize, size: usize, cuda_ordinal: i32) -> ScannedSegment {
        ScannedSegment {
            address,
            size,
            cuda_ordinal,
            is_expandable: false,
        }
    }

    /// A recorded `register_dmabuf_range` call.
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
        mlx5dv_enabled: bool,
        served_ordinals: Vec<i32>,
        scan: Vec<ScannedSegment>,
        next_handle: usize,
        live_mrs: HashSet<usize>,
        live_mkeys: HashSet<usize>,
        live_loopback_qps: HashSet<usize>,
        /// Every loopback QP handle passed to `destroy_loopback_qp`, in order.
        destroyed_loopback_qps: Vec<usize>,
        fail_dmabuf_after: Option<usize>,
        fail_bind: bool,
        scan_calls: usize,
        /// Every `register_dmabuf_range` call, including a failing one.
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
        /// When set, `dereg_mr` records whether this (weakly-held) domain is
        /// still alive at each MR deregistration. Lets a test assert a
        /// segment's MRs are freed while its domain's PD is still alive.
        domain_probe: Option<std::sync::Weak<IbvDomain<MlxDomain>>>,
        /// One entry per `dereg_mr` while `domain_probe` is set: whether the
        /// probed domain was still alive.
        domain_alive_at_dereg: Vec<bool>,
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
        fn new(device_name: &str, mlx5dv_enabled: bool, served_ordinals: &[i32]) -> Arc<Self> {
            Arc::new(Self {
                state: Mutex::new(MockState {
                    device_name: device_name.to_string(),
                    mlx5dv_enabled,
                    served_ordinals: served_ordinals.to_vec(),
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

        fn mlx5dv_enabled(&self) -> bool {
            self.lock().mlx5dv_enabled
        }

        fn assigned_cuda_devices(&self) -> Vec<i32> {
            self.lock().served_ordinals.clone()
        }

        fn scan_segments(&self) -> Vec<ScannedSegment> {
            let mut s = self.lock();
            s.scan_calls += 1;
            s.scan.clone()
        }

        unsafe fn register_dmabuf_range(
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
            let probe = s.domain_probe.clone();
            if let Some(probe) = probe {
                let alive = probe.upgrade().is_some();
                s.domain_alive_at_dereg.push(alive);
            }
        }

        fn create_loopback_qp(
            &self,
            _domain: Arc<IbvDomain<MlxDomain>>,
            _config: &IbvConfig,
        ) -> anyhow::Result<*mut rdmaxcel_sys::rdmaxcel_qp> {
            let mut s = self.lock();
            let h = s.mint();
            s.live_loopback_qps.insert(h);
            Ok(h as *mut rdmaxcel_sys::rdmaxcel_qp)
        }

        unsafe fn destroy_loopback_qp(&self, qp: *mut rdmaxcel_sys::rdmaxcel_qp) {
            let mut s = self.lock();
            let v = qp as usize;
            s.live_loopback_qps.remove(&v);
            s.destroyed_loopback_qps.push(v);
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

    /// A domain wrapping the mock-driven [`MlxDomain`] under test. Its
    /// `pd`/`context` are null (no-op `Drop`); the segment views built against
    /// it hold it only as a keepalive. Drive the strategy via
    /// [`IbvDomain::domain_impl`].
    fn domain(mock: Arc<MockOps>) -> Arc<IbvDomain<MlxDomain>> {
        let mlx = MlxDomain::new_with_ops(mock, IbvConfig::default());
        // SAFETY: null `ibv_context*`/`pd` are explicitly allowed — both
        // `IbvContext` and `IbvDomain` skip their FFI destructors for null.
        unsafe {
            Arc::new(IbvDomain::for_test(
                Arc::new(IbvContext::new(std::ptr::null_mut())),
                std::ptr::null_mut(),
                IbvDeviceInfo::for_test_named("test"),
                mlx,
            ))
        }
    }

    /// Drive `domain`'s own [`MlxDomain`] strategy to register CUDA memory.
    fn register_cuda(
        domain: &Arc<IbvDomain<MlxDomain>>,
        addr: usize,
        size: usize,
    ) -> anyhow::Result<IbvMemoryRegionView> {
        // SAFETY: `domain`'s pd is null and the `MockOps` never dereference it.
        unsafe {
            domain
                .domain_impl()
                .register_cuda_mlx5dv_mr(domain, addr, size)
        }
    }

    fn null_pd() -> *mut rdmaxcel_sys::ibv_pd {
        std::ptr::null_mut()
    }

    fn null_qp() -> *mut rdmaxcel_sys::rdmaxcel_qp {
        std::ptr::null_mut()
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

    // ----- RegisteredSegment binding-core unit tests -----

    #[test]
    fn test_scanner_registration_round_trips() {
        register_cuda_segment_scanner(Arc::new(|| vec![seg(0x1000, MIB2, 3)]));
        assert_eq!(
            scan_cuda_segments(),
            vec![seg(0x1000, MIB2, 3)],
            "the registered scanner's output is returned"
        );
    }

    #[test]
    fn test_fresh_bind_registers_one_mr_and_key() {
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
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
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
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
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
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
            "the second bind reuses the original MR and adds the tail"
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
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
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
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
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
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
        let base = 0x10_0000_0000;
        let rs = bind_fresh(&ops, base, MIB2);

        assert!(rs.covers(base + 0x400, 4096));
        let view = RegisteredSegment::view(&rs, base + 0x400, 4096, domain(ops.clone()));
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
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
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
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
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
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
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

    #[test]
    fn test_view_guard_drops_segment_before_domain() {
        // The `Mlx5dvMemoryRegion` guard inside a view declares `_segment` before
        // `_domain`, so the segment's MRs are deregistered while the domain's
        // PD is still alive. Drive a real view through drop and confirm that at
        // each MR deregistration the weakly-probed domain is still alive.
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
        let base = 0x10_0000_0000;
        let rs = bind_fresh(&ops, base, MIB2);
        let domain = domain(ops.clone());
        ops.lock().domain_probe = Some(Arc::downgrade(&domain));

        let view = RegisteredSegment::view(&rs, base, 4096, Arc::clone(&domain));

        // Make the view's guard the sole owner of both the segment and the
        // domain, so dropping the view drops each exactly once, in field order.
        drop(rs);
        drop(domain);

        drop(view);

        let s = ops.lock();
        assert!(
            !s.domain_alive_at_dereg.is_empty(),
            "dropping the view deregistered the segment's MR"
        );
        assert!(
            s.domain_alive_at_dereg.iter().all(|&alive| alive),
            "the segment's MRs are freed before the domain is dropped"
        );
    }

    // ----- MlxDomain integration tests -----

    #[test]
    fn test_cuda_ordinals_and_mlx5dv_enabled_derived_from_ops() {
        let mock = MockOps::new(SERVED_NIC, true, &[0, 2]);
        let domain = domain(mock);
        assert_eq!(domain.domain_impl().cuda_ordinals, vec![0, 2]);
        assert!(domain.domain_impl().mlx5dv_enabled);
    }

    #[test]
    fn test_fresh_bind() {
        let base = 0x10_0000_0000;
        let mock = MockOps::new(SERVED_NIC, true, &[0]);
        mock.lock().scan = vec![seg(base, MIB2, 0)];
        let domain = domain(mock.clone());

        let view = register_cuda(&domain, base, 4096).expect("fresh bind should succeed");

        // The per-segment binding details are covered by the binding-core unit
        // tests above; here we only check the domain wired scan → bind → view.
        assert_eq!(view.rdma_addr, 0, "view is anchored at the segment base");
        assert_eq!(view.size, 4096);
        assert_eq!(view.device_name, SERVED_NIC);
        assert_eq!(mock.lock().live_mkeys.len(), 1, "one segment bound");
    }

    #[test]
    fn test_fast_path_skips_scan() {
        let mock = MockOps::new(SERVED_NIC, true, &[0]);
        mock.lock().scan = vec![seg(0x10_0000_0000, MIB2, 0)];
        let domain = domain(mock.clone());

        register_cuda(&domain, 0x10_0000_0000, 4096).unwrap();
        let scans_after_first = mock.lock().scan_calls;

        // A second request covered by the existing binding must not rescan
        // or rebind.
        register_cuda(&domain, 0x10_0000_0400, 4096).unwrap();
        let s = mock.lock();
        assert_eq!(s.scan_calls, scans_after_first, "fast path skips the scan");
        assert_eq!(s.bind_calls.len(), 1, "fast path performs no new bind");
    }

    #[test]
    fn test_segment_growth_via_register() {
        let base = 0x10_0000_0000;
        let mock = MockOps::new(SERVED_NIC, true, &[0]);
        mock.lock().scan = vec![seg(base, MIB2, 0)];
        let domain = domain(mock.clone());

        register_cuda(&domain, base, 4096).unwrap();

        // The scanner now reports the segment has grown; a request into the new
        // tail grows the mapped segment in place and serves a view from it. (MR
        // reuse / key retirement are covered by the binding-core unit tests.)
        mock.lock().scan = vec![seg(base, 2 * MIB2, 0)];
        let view =
            register_cuda(&domain, base + MIB2, 4096).expect("growth registration should succeed");

        assert_eq!(view.rdma_addr, MIB2, "the view anchors into the grown tail");
        let s = mock.lock();
        assert_eq!(
            s.live_mkeys.len(),
            2,
            "growth created a new key and retired (kept) the prior one"
        );
        assert!(
            s.destroy_log.is_empty(),
            "nothing freed while the segment lives"
        );
    }

    #[test]
    fn test_unserved_ordinal_is_not_covered() {
        let mock = MockOps::new(SERVED_NIC, true, &[0]);
        // Segment is on ordinal 5, which this NIC does not serve.
        mock.lock().scan = vec![seg(0x10_0000_0000, MIB2, 5)];
        let domain = domain(mock.clone());

        let result = register_cuda(&domain, 0x10_0000_0000, 4096);
        assert!(
            result.is_err(),
            "memory on an unserved ordinal is not bound"
        );
        assert!(
            mock.lock().bind_calls.is_empty(),
            "no bind attempted for an unserved ordinal"
        );
    }

    #[test]
    fn test_drop_frees_segment_keys_and_loopback_qp() {
        let base = 0x10_0000_0000;
        let mock = MockOps::new(SERVED_NIC, true, &[0]);
        mock.lock().scan = vec![seg(base, MIB2, 0)];
        let domain = domain(mock.clone());

        // Bind then grow, so the segment carries a retired key plus its current
        // key, and two MRs. Views are dropped immediately.
        register_cuda(&domain, base, 4096).unwrap();
        mock.lock().scan = vec![seg(base, 2 * MIB2, 0)];
        register_cuda(&domain, base + MIB2, 4096).unwrap();

        assert_eq!(mock.lock().live_mrs.len(), 2);
        assert_eq!(mock.lock().live_mkeys.len(), 2, "current + retired key");
        assert_eq!(
            mock.lock().live_loopback_qps.len(),
            1,
            "the loopback binding QP was created"
        );

        drop(domain);

        let s = mock.lock();
        assert!(
            s.live_mrs.is_empty(),
            "dropping the domain frees all segment MRs"
        );
        assert!(
            s.live_mkeys.is_empty(),
            "dropping the domain destroys the current + retired keys"
        );
        assert!(
            s.live_loopback_qps.is_empty(),
            "dropping the domain destroys its loopback QP"
        );
    }

    #[test]
    fn test_loopback_qp_created_once() {
        let base = 0x10_0000_0000;
        let other = 0x20_0000_0000;
        let mock = MockOps::new(SERVED_NIC, true, &[0]);
        mock.lock().scan = vec![seg(base, MIB2, 0)];
        let domain = domain(mock.clone());

        // The first binding creates the loopback QP used to post key-binding
        // work requests; capture its handle.
        register_cuda(&domain, base, 4096).unwrap();
        let qp = {
            let s = mock.lock();
            assert_eq!(
                s.live_loopback_qps.len(),
                1,
                "the first binding creates the loopback QP"
            );
            *s.live_loopback_qps.iter().next().unwrap()
        };

        // Later scans — a growth, then a brand-new segment — each consult the
        // loopback QP, but must reuse the very same cached one. After each, the
        // single live QP is unchanged (same handle), never recreated.
        mock.lock().scan = vec![seg(base, 2 * MIB2, 0)];
        register_cuda(&domain, base + MIB2, 4096).unwrap();
        assert_eq!(
            mock.lock().live_loopback_qps,
            HashSet::from([qp]),
            "growth reuses the same loopback QP handle"
        );

        mock.lock().scan = vec![seg(base, 2 * MIB2, 0), seg(other, MIB2, 0)];
        register_cuda(&domain, other, 4096).unwrap();
        assert_eq!(
            mock.lock().live_loopback_qps,
            HashSet::from([qp]),
            "binding a new segment reuses the same loopback QP handle"
        );

        // No loopback QP was ever destroyed across the domain's lifetime.
        assert!(
            mock.lock().destroyed_loopback_qps.is_empty(),
            "no loopback QP is destroyed while the domain lives"
        );
    }

    #[test]
    fn test_multi_device_subset_growth_new_segments_and_boundaries() {
        // Three CUDA devices are visible, but this NIC only serves ordinals
        // 0 and 2; ordinal 1's segment must never be bound.
        let a = 0x10_0000_0000usize; // ordinal 0
        let a2 = 0x18_0000_0000usize; // ordinal 0, appears later
        let b = 0x20_0000_0000usize; // ordinal 1 (unserved)
        let c = 0x30_0000_0000usize; // ordinal 2
        let mock = MockOps::new(SERVED_NIC, true, &[0, 2]);
        mock.lock().scan = vec![seg(a, MIB2, 0), seg(b, MIB2, 1), seg(c, 2 * MIB2, 2)];
        let domain = domain(mock.clone());
        // The registration path registers MRs with the domain's own access
        // flags; capture them to assert the dmabuf registration arguments.
        let access = domain.domain_impl().mr_access_flags();

        // Validate every field of a returned view: its address fields, its
        // size, the serving NIC's name, and the keys. The mock's `mkey_keys`
        // derives `(lkey, rkey)` from the segment's current mkey as
        // `(handle, handle ^ 0xffff)`, so a bound view always has a non-zero
        // lkey and an rkey that is its lkey xor 0xffff.
        let assert_view =
            |view: &IbvMemoryRegionView, virtual_addr: usize, rdma_addr: usize, size: usize| {
                assert_eq!(
                    view.virtual_addr, virtual_addr,
                    "virtual_addr is the requested address"
                );
                assert_eq!(
                    view.rdma_addr, rdma_addr,
                    "rdma_addr is the offset from the segment base"
                );
                assert_eq!(view.size, size, "size matches the request");
                assert_eq!(view.device_name, SERVED_NIC, "view names the serving NIC");
                assert_ne!(view.lkey, 0, "a bound view carries a real lkey");
                assert_eq!(
                    view.rkey,
                    view.lkey ^ 0xffff,
                    "rkey and lkey are derived from the same mkey"
                );
            };

        // First registration binds every served segment in the scan (ordinals
        // 0 and 2), skipping the unserved ordinal-1 segment, then serves the
        // requested view from ordinal 0's segment.
        let view_a = register_cuda(&domain, a, 0x2000).expect("served ordinal binds");
        assert_view(&view_a, a, 0, 0x2000);
        let (mr_a, mr_c) = {
            let s = mock.lock();
            assert_eq!(s.live_mkeys.len(), 2, "only the two served segments bound");
            assert_eq!(s.live_mrs.len(), 2);
            assert_eq!(s.bind_calls.len(), 2, "one bind per served segment");
            // Each served segment fits in a single MR, so each bind got a
            // one-element MR list.
            assert_eq!(s.bind_calls[0].mrs.len(), 1, "segment a binds a single MR");
            assert_eq!(s.bind_calls[1].mrs.len(), 1, "segment c binds a single MR");
            // Each served segment is registered as one MR covering its whole
            // extent, with the domain's access flags; the unserved ordinal-1
            // segment is absent.
            assert_eq!(
                s.dmabuf_calls,
                vec![
                    DmabufCall {
                        addr: a,
                        size: MIB2,
                        access,
                    },
                    DmabufCall {
                        addr: c,
                        size: 2 * MIB2,
                        access,
                    },
                ],
                "a and c are each registered once, covering their full extent"
            );
            assert_eq!(s.scan_calls, 1);
            let (mr_a, mr_c) = (s.bind_calls[0].mrs[0], s.bind_calls[1].mrs[0]);
            assert_ne!(mr_a, mr_c, "a and c are backed by distinct MRs");
            (mr_a, mr_c)
        };

        // A request into the other served segment (ordinal 2) resolves to it,
        // not segment a, and is served from the fast path (no rescan).
        let view_c =
            register_cuda(&domain, c + 0x800, 0x4000).expect("segment c covers the request");
        assert_view(&view_c, c + 0x800, 0x800, 0x4000);
        assert_ne!(
            view_c.lkey, view_a.lkey,
            "distinct segments are backed by distinct keys"
        );
        assert_eq!(
            mock.lock().scan_calls,
            1,
            "fast path serves c without a scan"
        );

        // Boundary handling: a request ending exactly at segment c's end is
        // covered (and shares c's key, since c has not been rebound); one that
        // crosses the end is not.
        let tail = register_cuda(&domain, c + 2 * MIB2 - 0x6000, 0x6000)
            .expect("a request ending exactly at the boundary is covered");
        assert_view(&tail, c + 2 * MIB2 - 0x6000, 2 * MIB2 - 0x6000, 0x6000);
        assert_eq!(
            (tail.lkey, tail.rkey),
            (view_c.lkey, view_c.rkey),
            "both views of segment c share its current key"
        );
        assert!(
            register_cuda(&domain, c + 2 * MIB2 - 2048, 4096).is_err(),
            "a request straddling the segment's end boundary is rejected"
        );

        // The scanner now reports segment a grew, segment c is unchanged, and a
        // brand-new segment a2 appeared on ordinal 0. A single request into a's
        // grown tail processes all of that: grow a, skip c, bind a2.
        mock.lock().scan = vec![
            seg(a, 3 * MIB2, 0),
            seg(b, MIB2, 1),
            seg(c, 2 * MIB2, 2),
            seg(a2, MIB2, 0),
        ];
        let grown =
            register_cuda(&domain, a + 2 * MIB2, 0x3000).expect("a's grown tail is now covered");
        assert_view(&grown, a + 2 * MIB2, 2 * MIB2, 0x3000);
        assert_ne!(
            grown.lkey, view_a.lkey,
            "growing segment a rotated it onto a new key"
        );
        {
            let s = mock.lock();
            // a: current + retired key (2 MRs); c: unchanged (1); a2: new (1).
            assert_eq!(
                s.live_mkeys.len(),
                4,
                "a retired+current, c, and the new a2 segment"
            );
            assert_eq!(s.live_mrs.len(), 4, "a's reused MR + tail, c, a2");
            assert!(
                s.destroy_log.is_empty(),
                "nothing vanished, so nothing is freed"
            );
            // Two binds added: a's growth rebind and a2's fresh bind (c was
            // already full-size, so it is not rebound).
            assert_eq!(s.bind_calls.len(), 4);
            // a's growth rebinds its reused original MR followed by the one new
            // tail MR.
            assert_eq!(
                s.bind_calls[2].mrs.len(),
                2,
                "a's growth binds the reused original MR plus the new tail"
            );
            assert_eq!(
                s.bind_calls[2].mrs[0], mr_a,
                "the original MR is reused, not re-registered"
            );
            let mr_a_tail = s.bind_calls[2].mrs[1];
            // a2's fresh bind passes a single new MR.
            assert_eq!(
                s.bind_calls[3].mrs.len(),
                1,
                "the new segment a2 binds a single MR"
            );
            let mr_a2 = s.bind_calls[3].mrs[0];
            // a's original + tail, c, and a2 are four distinct MR handles.
            let distinct: HashSet<usize> = [mr_a, mr_c, mr_a_tail, mr_a2].into_iter().collect();
            assert_eq!(
                distinct.len(),
                4,
                "a, a-tail, c, and a2 are four distinct MRs"
            );
            // Growth registered only a's new tail (at a + MIB2, covering the 2
            // MiB it grew by) and a2's single MR; c was left untouched and the
            // first two registrations are unchanged. MR extents track segment
            // sizes, never the (varied) request sizes.
            assert_eq!(
                s.dmabuf_calls,
                vec![
                    DmabufCall {
                        addr: a,
                        size: MIB2,
                        access,
                    },
                    DmabufCall {
                        addr: c,
                        size: 2 * MIB2,
                        access,
                    },
                    DmabufCall {
                        addr: a + MIB2,
                        size: 2 * MIB2,
                        access,
                    },
                    DmabufCall {
                        addr: a2,
                        size: MIB2,
                        access,
                    },
                ],
                "growth registers only a's new tail and a2; c is not re-registered"
            );
        }

        // The brand-new segment is independently usable, carries its own
        // distinct key, and has correct boundary handling of its own.
        let view_a2 = register_cuda(&domain, a2 + 0x400, 0x800)
            .expect("the new segment a2 covers the request");
        assert_view(&view_a2, a2 + 0x400, 0x400, 0x800);
        assert_ne!(
            view_a2.lkey, grown.lkey,
            "the new segment a2 has a key distinct from a's"
        );
        assert_ne!(
            view_a2.lkey, view_c.lkey,
            "the new segment a2 has a key distinct from c's"
        );
        assert!(
            register_cuda(&domain, a2 + MIB2 - 512, 1024).is_err(),
            "a request past a2's end boundary is rejected"
        );

        // Even after all the rebinding, memory on the unserved ordinal is still
        // never bound or served.
        assert!(
            register_cuda(&domain, b, 4096).is_err(),
            "the unserved ordinal-1 segment is never served"
        );
        assert!(
            !mock.lock().dmabuf_calls.iter().any(|call| call.addr == b),
            "the unserved ordinal-1 segment was never registered"
        );
    }
}
