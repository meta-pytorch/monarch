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
//! The segment scanning + binding bookkeeping lives here in Rust. Resource
//! creation goes through [`MlxDomainOps`] so the (intricate) scan/bind logic
//! can be unit-tested against a mock; the production implementation
//! ([`ProdMlxDomainOps`]) delegates to the real functions and the
//! [`rdmaxcel_sys::rdmaxcel_bind_mr_list`] shim. Teardown is the `Drop` of the
//! owning RAII wrappers ([`IbvMr`], [`Mlx5dvMkey`]), correct by construction.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use anyhow::Context;

use super::cq_pool::cq_entries_for;
use super::device_selection::get_cuda_device_to_ibv_devices;
use super::domain::IbvDomain;
use super::domain::IbvDomainImpl;
use super::domain::register_dmabuf_range;
use super::domain::register_host_or_dmabuf_mr;
use super::memory_region::IbvMemoryRegionKeepalive;
use super::memory_region::IbvMemoryRegionView;
use super::mlx_queue_pair::MlxQueuePair;
use super::primitives::GidScope;
use super::primitives::GidType;
use super::primitives::IbvConfig;
use super::primitives::IbvContext;
use super::primitives::IbvCq;
use super::primitives::IbvDeviceInfo;
use super::primitives::IbvMr;
use super::primitives::IbvPd;
use super::primitives::IbvQp;
use super::queue_pair::connect;
use super::queue_pair::get_qp_info;
use crate::backend::ibverbs::mlx_device::MlxDevice;
use crate::device_selection::MemoryLocation;
use crate::local_memory::KeepaliveLocalMemory;

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
/// uses [`ProdMlxDomainOps`]; tests substitute a mock so the scan/bind
/// algorithm can be exercised without hardware. Creation returns the owning
/// RAII wrappers ([`IbvMr`], [`Mlx5dvMkey`]), which free their resources in the
/// right order on `Drop`; the mock fabricates null-handle wrappers whose `Drop`
/// is a no-op.
pub(super) trait MlxDomainOps: Send + Sync + 'static {
    /// Name of the RDMA device this domain drives.
    fn device_name(&self) -> String;

    /// Whether the device supports mlx5dv direct verbs.
    fn mlx5dv_enabled(&self) -> bool;

    /// CUDA ordinals whose optimal NIC is this domain's device; only segments
    /// on these ordinals are bound here.
    fn assigned_cuda_devices(&self) -> anyhow::Result<Vec<i32>>;

    /// Enumerate the currently-live CUDA segments.
    fn scan_segments(&self) -> Vec<ScannedSegment>;

    /// Register `[addr, addr + size)` of device memory as a dmabuf MR, returned
    /// as an [`IbvMr`] (owning `pd`) that deregisters it on drop.
    ///
    /// # Safety
    ///
    /// `pd`, if non-null, must wrap a live protection domain.
    unsafe fn register_dmabuf_range(
        &self,
        pd: &Arc<IbvPd>,
        addr: usize,
        size: usize,
        access: i32,
    ) -> anyhow::Result<IbvMr>;

    /// Creates a loopback-connected queue pair against `domain`'s PD, returned
    /// as an [`IbvQp`] owning its completion queues and PD. The caller stores it
    /// for the domain's lifetime.
    ///
    /// # Safety
    ///
    /// `domain`'s context and PD, if non-null, must be live. A null context or
    /// PD yields `Err`.
    unsafe fn create_loopback_qp(
        &self,
        domain: &IbvDomain<MlxDomain>,
        config: &IbvConfig,
    ) -> anyhow::Result<IbvQp>;

    /// Bind `mrs` to a freshly created indirect key (sized to hold
    /// `mkey_max_entries` MRs) using `qp`'s work-request builder, returning it
    /// as a [`Mlx5dvMkey`] owning those MR references. On failure the `mrs` are
    /// dropped.
    ///
    /// # Safety
    ///
    /// `pd` (if non-null) must be a live protection domain, `qp` a valid queue
    /// pair, and each MR in `mrs` null or a live MR allocated against `pd` — all
    /// valid for this call.
    unsafe fn bind_mr_list(
        &self,
        pd: &IbvPd,
        qp: &IbvQp,
        access: i32,
        mrs: Vec<Arc<IbvMr>>,
        mkey_max_entries: usize,
    ) -> anyhow::Result<Mlx5dvMkey>;
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

    fn assigned_cuda_devices(&self) -> anyhow::Result<Vec<i32>> {
        Ok(get_cuda_device_to_ibv_devices::<MlxDevice>()?
            .into_iter()
            .enumerate()
            .filter_map(|(ordinal, nics)| {
                nics.iter()
                    .any(|nic| nic.name() == &self.device_name)
                    .then_some(ordinal as i32)
            })
            .collect())
    }

    fn scan_segments(&self) -> Vec<ScannedSegment> {
        scan_cuda_segments()
    }

    unsafe fn register_dmabuf_range(
        &self,
        pd: &Arc<IbvPd>,
        addr: usize,
        size: usize,
        access: i32,
    ) -> anyhow::Result<IbvMr> {
        // SAFETY: forwards this method's contract (non-null `pd` is a live PD).
        unsafe { register_dmabuf_range(pd, addr, size, access) }
    }

    unsafe fn create_loopback_qp(
        &self,
        domain: &IbvDomain<MlxDomain>,
        config: &IbvConfig,
    ) -> anyhow::Result<IbvQp> {
        // This QP is private to mkey binding and its completions are polled
        // here, not by a `QueuePairActor`, so it gets its own completion queue
        // rather than drawing on the device's pool -- sized to hold everything
        // its one owner can have outstanding. One queue serves both sides, since
        // it posts no receives.
        let cq_entries = cq_entries_for(1, config.max_send_wr, domain.device_info().max_cqe())?;
        // SAFETY: an `IbvDomain` holds a null-or-live context; `IbvCq::create`
        // rejects a null context.
        let cq = Arc::new(unsafe { IbvCq::create(domain.context().clone(), cq_entries) }?);
        // The `IbvQp` holds a clone of the CQ and of the PD, so an early return
        // or panic in the connect below still tears everything down in order.
        // SAFETY: an `IbvDomain` holds a null-or-live context and PD, and the CQ
        // was just created on that context.
        let qp = unsafe { MlxQueuePair::create_ibv_qp(domain, config, Arc::clone(&cq), cq) }
            .context("could not create loopback QP for mkey binding")?;
        let context = qp.context().as_ptr();
        let access_flags = domain.access_flags();
        let gid = domain.device_info().select_gid(
            config.port_num,
            Some(GidScope::Global),
            Some(GidType::RoCEv2),
        )?;

        // Connect the QP to itself (loopback) so it reaches RTS, the state
        // required to post work requests.
        // SAFETY: `qp` wraps the live QP just created above and `context` is its
        // live device context.
        let info = unsafe { get_qp_info(qp.as_ptr(), context, config, gid) }
            .context("could not query loopback QP info for mkey binding")?;
        // SAFETY: as above.
        unsafe { connect(qp.as_ptr(), config, access_flags, &info, gid.index()) }
            .context("could not connect loopback QP for mkey binding")?;

        Ok(qp)
    }

    unsafe fn bind_mr_list(
        &self,
        pd: &IbvPd,
        qp: &IbvQp,
        access: i32,
        mrs: Vec<Arc<IbvMr>>,
        mkey_max_entries: usize,
    ) -> anyhow::Result<Mlx5dvMkey> {
        if pd.as_ptr().is_null() || qp.as_ptr().is_null() {
            anyhow::bail!("bind_mr_list called with a null protection domain or queue pair");
        }
        let ptrs: Vec<*mut rdmaxcel_sys::ibv_mr> = mrs
            .iter()
            .map(|m| {
                let p = m.as_ptr();
                if p.is_null() {
                    Err(anyhow::anyhow!(
                        "bind_mr_list called with a null memory region"
                    ))
                } else {
                    Ok(p)
                }
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        // A null out-param makes `rdmaxcel_bind_mr_list` create the key in place.
        let mut mkey: *mut rdmaxcel_sys::mlx5dv_mkey = std::ptr::null_mut();
        // SAFETY: `pd`/`qp` are non-null (checked above) and valid, `ptrs` holds
        // the MRs' pointers, and failure is reported via the return code.
        let ret = unsafe {
            rdmaxcel_sys::rdmaxcel_bind_mr_list(
                pd.as_ptr(),
                qp.as_ptr(),
                access,
                ptrs.as_ptr() as *mut *mut rdmaxcel_sys::ibv_mr,
                ptrs.len(),
                mkey_max_entries,
                &mut mkey,
            )
        };
        if ret != 0 {
            anyhow::bail!("rdmaxcel_bind_mr_list failed: error code {}", ret);
        }
        // SAFETY: `mkey` is a live key freshly bound over `mrs` (`ret == 0`).
        Ok(unsafe { Mlx5dvMkey::from_raw(mkey, mrs) })
    }
}

/// Owns an `mlx5dv_mkey` together with the [`Arc<IbvMr>`]s it binds, destroying
/// the key on drop (a no-op if null). Caches the `(lkey, rkey)` read at bind
/// time so a view can address through the key without further FFI.
///
/// The MRs are `Arc` because successive keys over a growing segment bind
/// overlapping sets; an MR is deregistered only once the last key referencing it
/// drops.
#[derive(Debug)]
pub(super) struct Mlx5dvMkey {
    mkey: *mut rdmaxcel_sys::mlx5dv_mkey,
    lkey: u32,
    rkey: u32,
    mrs: Vec<Arc<IbvMr>>,
}

// SAFETY: the only raw member is the `mlx5dv_mkey` pointer (the MRs are already
// `Send`/`Sync`), which the mlx5dv API treats as usable and destroyable from any
// thread (`Send`); `Mlx5dvMkey` exposes no operation that mutates the key
// through a shared `&` (`keys` reads immutable fields), so sharing a
// `&Mlx5dvMkey` cannot race (`Sync`).
unsafe impl Send for Mlx5dvMkey {}
// SAFETY: as for `Send` above.
unsafe impl Sync for Mlx5dvMkey {}

impl Mlx5dvMkey {
    /// Takes ownership of a raw `mlx5dv_mkey` bound over `mrs`, reading its
    /// `(lkey, rkey)` and destroying it on drop.
    ///
    /// # Safety
    ///
    /// `mkey` must be a live key returned by `rdmaxcel_bind_mr_list` over `mrs`,
    /// owned solely by the returned value.
    pub(super) unsafe fn from_raw(
        mkey: *mut rdmaxcel_sys::mlx5dv_mkey,
        mrs: Vec<Arc<IbvMr>>,
    ) -> Self {
        // SAFETY: per this function's contract `mkey` is a live bound key, so its
        // `lkey`/`rkey` fields are initialized.
        let (lkey, rkey) = unsafe { ((*mkey).lkey, (*mkey).rkey) };
        Self {
            mkey,
            lkey,
            rkey,
            mrs,
        }
    }

    /// The `(lkey, rkey)` to address memory through this key.
    fn keys(&self) -> (u32, u32) {
        (self.lkey, self.rkey)
    }

    /// The MRs this key binds.
    fn mrs(&self) -> &Vec<Arc<IbvMr>> {
        &self.mrs
    }

    /// Fabricates a key with the given `(lkey, rkey)` over a null `mkey` (whose
    /// `Drop` is a no-op), binding `mrs`. Lets the mock hand back a usable key
    /// without touching the FFI.
    #[cfg(test)]
    pub(super) fn with_test_keys(lkey: u32, rkey: u32, mrs: Vec<Arc<IbvMr>>) -> Self {
        Self {
            mkey: std::ptr::null_mut(),
            lkey,
            rkey,
            mrs,
        }
    }
}

impl Drop for Mlx5dvMkey {
    fn drop(&mut self) {
        if !self.mkey.is_null() {
            // SAFETY: a non-null `self.mkey` came from `rdmaxcel_bind_mr_list`
            // and, since `Mlx5dvMkey` is not `Clone`, is destroyed exactly once.
            unsafe { rdmaxcel_sys::rdmaxcel_destroy_mkey(self.mkey) };
        }
    }
}

// ===========================================================================
// RegisteredSegment: a CUDA segment bound to an indirect key
// ===========================================================================

/// The mutable state of a [`RegisteredSegment`], behind one `Mutex` so the
/// current key, superseded keys, and size all change together.
#[derive(Debug)]
struct RegisteredSegmentState {
    /// Keys superseded by growth, each still owning the MRs it bound, kept alive
    /// so views built against an earlier key stay valid.
    stale_mkeys: Vec<Mlx5dvMkey>,
    /// Current indirect key over the segment's MRs. `None` until the first bind;
    /// swapped on growth, the prior key moved to `stale_mkeys`.
    mkey: Option<Mlx5dvMkey>,
    /// Bytes currently covered by `mkey`.
    size: usize,
    /// Bytes the most recent scan reported for this segment. Equal to `size`
    /// while the key spans the whole segment, and larger once growth is refused
    /// because the segment needs more MRs than one key can bind.
    scanned_size: usize,
}

/// A CUDA segment bound to the device via an indirect mlx5dv key, covering
/// `[base_virtual_addr, base_virtual_addr + size)`. Shared as
/// `Arc<RegisteredSegment>` by [`MlxDomain`] and every view over it, so it (and
/// the keys, MRs, and PD it owns) lives until nothing references it.
///
/// All mutation is serialized by the owning [`MlxDomain`]'s `segments` lock;
/// `state` is behind a `Mutex` only to allow mutation through the shared `Arc`.
struct RegisteredSegment {
    ops: Arc<dyn MlxDomainOps>,
    base_virtual_addr: usize,
    /// The most MRs that can bind to this segment's indirect key.
    mkey_max_entries: usize,
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

impl RegisteredSegment {
    /// An unbound segment for `base_virtual_addr`: no MRs, no key, zero size.
    /// [`Self::grow`] binds its first generation. `mkey_max_entries` caps the
    /// MRs bound to the segment's key.
    fn empty(
        ops: Arc<dyn MlxDomainOps>,
        base_virtual_addr: usize,
        mkey_max_entries: usize,
    ) -> Self {
        Self {
            ops,
            base_virtual_addr,
            mkey_max_entries,
            state: Mutex::new(RegisteredSegmentState {
                stale_mkeys: Vec::new(),
                mkey: None,
                size: 0,
                scanned_size: 0,
            }),
        }
    }

    /// Bytes currently covered by the segment's MRs and bound to its key.
    #[cfg(test)]
    fn size(&self) -> usize {
        self.state.lock().expect("segment state lock poisoned").size
    }

    /// True when `[addr, addr + size)` lies entirely within the extent bound to
    /// this segment's key.
    fn covers(&self, addr: usize, size: usize) -> bool {
        let state = self.state.lock().expect("segment state lock poisoned");
        if state.mkey.is_none() {
            return false;
        }
        let end = self.base_virtual_addr + state.size;
        addr >= self.base_virtual_addr && addr <= end && size <= end - addr
    }

    /// Bytes the most recent scan reported for this segment.
    fn scanned_size(&self) -> usize {
        self.state
            .lock()
            .expect("segment state lock poisoned")
            .scanned_size
    }

    /// True when both conditions are true:
    /// 1. The scanned size of the segment is larger than the registered
    ///    size, which occurs only when the mkey for this segment cannot
    ///    grow anymore.
    /// 2. [addr, addr + size) overlaps the unregistered part of the scanned
    ///    segment.
    fn in_unregistered_tail(&self, addr: usize, size: usize) -> bool {
        let state = self.state.lock().expect("segment state lock poisoned");
        let registered_end = self.base_virtual_addr + state.size;
        let scanned_end = self.base_virtual_addr + state.scanned_size;
        addr >= self.base_virtual_addr
            && addr <= scanned_end
            && size <= scanned_end - addr
            && addr + size > registered_end
    }

    /// Grow the segment towards `scanned_seg.size` (must not be below its
    /// current size): register the new tail — the whole range on the first call
    /// from [`Self::empty`] — bind the existing + new MRs to a fresh key, and
    /// retire the prior key to `stale_mkeys` (rather than rebinding the in-use
    /// key, which could race in-flight ops); the initial null key is not
    /// retired. On any failure, any freshly registered tail MRs will be cleaned
    /// up. However, the segment's `scanned_size` remains at the updated value.
    /// This is necessary to prevent future registrations from repeatedly triggering
    /// an expensive rescan only to fail again inside this function.
    ///
    /// A key binds at most `mkey_max_entries` MRs. When the tail needs more than
    /// the key has room for, as much of it as fits is registered and the rest is
    /// left out, so the segment still covers its prefix; [`Self::covers`] then
    /// reports what was bound and [`Self::in_unregistered_tail`] the remainder.
    ///
    /// # Safety
    ///
    /// If `pd` is non-null it must be a live protection domain and `qp` a valid
    /// queue pair, both valid for this call. Nothing else may poll `qp`'s send
    /// completion queue meanwhile: binding posts a work request and polls that
    /// queue until it completes.
    unsafe fn grow(
        &self,
        pd: &Arc<IbvPd>,
        qp: &IbvQp,
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
        state.scanned_size = scanned_seg.size;

        // `register_range` splits the tail into `MAX_MR_SIZE` chunks, so how many
        // MRs it needs is known before any of it is registered. Take only as many
        // chunks as the key has room for.
        let bound = state.mkey.as_ref().map_or(0, |k| k.mrs().len());
        let room = self
            .mkey_max_entries
            .saturating_sub(bound)
            .saturating_mul(MAX_MR_SIZE);
        let grew = scanned_seg.size - state.size;
        let tail = grew.min(room);

        if tail < grew {
            tracing::warn!(
                "CUDA segment at 0x{:x} grew from {} bytes to {} bytes, \
            but its mkey only has room for {tail} more bytes. This segment's tail \
            will remain unregistered, and some tensors inside it may use the dmabuf \
            fallback.",
                self.base_virtual_addr,
                state.size,
                state.scanned_size
            );
        }

        // The key is full: the segment keeps the prefix it already covers.
        if tail == 0 {
            return Ok(());
        }

        // SAFETY: per this function's contract `pd` is null or a live PD and
        // `qp` a valid QP; the tail MRs registered here belong to this segment.
        let new_tail = unsafe {
            register_range(
                &self.ops,
                pd,
                access,
                self.base_virtual_addr + state.size,
                tail,
            )
        }?;

        // Bind the existing MRs (reused from the current key) plus the new tail
        // to a fresh key. On failure `all` is dropped by `bind_mr_list`: the new
        // tail's MRs deregister while the existing ones survive via the current
        // key, leaving the segment unchanged.
        let mut all = state
            .mkey
            .as_ref()
            .map(|k| k.mrs().clone())
            .unwrap_or_default();
        all.extend(new_tail);
        debug_assert!(
            all.len() <= self.mkey_max_entries,
            "mkey max entries is {}, but got {} MRs",
            self.mkey_max_entries,
            all.len()
        );
        // SAFETY: same contract; `all` are this segment's live MRs.
        let new_mkey = unsafe {
            self.ops
                .bind_mr_list(pd, qp, access, all, self.mkey_max_entries)
        }?;

        // Retire the prior key (kept for in-flight ops built against it) and
        // install the new one.
        if let Some(prior) = state.mkey.replace(new_mkey) {
            state.stale_mkeys.push(prior);
        }
        state.size += tail;
        Ok(())
    }

    /// Build a view anchored at `addr`. The view's guard is the segment itself,
    /// pinning it (and thus its current key, the key's MRs, and the PD they own)
    /// alive for the view's lifetime, so the bound key stays valid while the view
    /// is in use. Indirect keys present their MRs as a flat zero-based space, so
    /// `rdma_addr` is the offset from the segment base.
    fn view(seg: &Arc<Self>, addr: usize, size: usize) -> IbvMemoryRegionView {
        let (lkey, rkey) = {
            let state = seg.state.lock().expect("segment state lock poisoned");
            // `view` is only called on a segment that covers the request, which
            // means it has been bound, so the current key is `Some`.
            state
                .mkey
                .as_ref()
                .map(Mlx5dvMkey::keys)
                .expect("view of a segment with no bound key")
        };
        // The segment is its own keepalive: cloning the `Arc<RegisteredSegment>`
        // pins the current key (which owns the MRs, which own the PD) for the
        // view's life.
        let guard: Arc<dyn IbvMemoryRegionKeepalive> = seg.clone();
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

// A `RegisteredSegment` is its own MR keepalive: it owns the current key, which
// owns the bound MRs, which own the PD — all freed in order on its `Drop`.
impl IbvMemoryRegionKeepalive for RegisteredSegment {}

/// Register `[start, start + len)` of device memory as dmabuf MRs in
/// `<= MAX_MR_SIZE` chunks. All-or-nothing: on any failure the MRs registered
/// so far drop (deregistering) as the returned `Vec` unwinds.
///
/// # Safety
///
/// If `pd` is non-null it must be a live protection domain whose context
/// outlives this call.
unsafe fn register_range(
    ops: &Arc<dyn MlxDomainOps>,
    pd: &Arc<IbvPd>,
    access: i32,
    start: usize,
    len: usize,
) -> anyhow::Result<Vec<Arc<IbvMr>>> {
    let mut mrs: Vec<Arc<IbvMr>> = Vec::new();
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
        // On error, return early; the MRs collected so far drop here,
        // deregistering them.
        mrs.push(Arc::new(result?));
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
    /// Caps the MRs bound to each segment's indirect key.
    mkey_max_entries: usize,
    /// Lazily-created loopback QP (an [`IbvQp`] owning its completion queues and
    /// PD) used to post key-binding work requests, destroyed when this domain
    /// drops.
    loopback: OnceLock<IbvQp>,
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
            .finish_non_exhaustive()
    }
}

impl MlxDomain {
    /// Build a domain over the given ops, deriving mlx5dv support from them.
    /// `mkey_max_entries` caps the MRs bound to every segment's indirect key.
    fn new_with_ops(
        ops: Arc<dyn MlxDomainOps>,
        config: IbvConfig,
        mkey_max_entries: usize,
    ) -> Self {
        let mlx5dv_enabled = ops.mlx5dv_enabled();
        Self {
            ops,
            config,
            mlx5dv_enabled,
            mkey_max_entries,
            loopback: OnceLock::new(),
            segments: Mutex::new(HashMap::new()),
        }
    }

    /// Get-or-create the loopback QP.
    fn loopback_qp_ptr(&self, domain: &IbvDomain<MlxDomain>) -> anyhow::Result<&IbvQp> {
        // `OnceLock::get_or_try_init` would fit here but is still unstable
        // (`once_cell_try`); calls are serialized under the `segments` lock,
        // so this check-then-set is race-free.
        if let Some(qp) = self.loopback.get() {
            return Ok(qp);
        }
        // SAFETY: an `IbvDomain` guarantees its context and PD are null or live;
        // `create_loopback_qp` rejects null.
        let qp = unsafe { self.ops.create_loopback_qp(domain, &self.config) }?;
        let _ = self.loopback.set(qp);
        Ok(self.loopback.get().expect("loopback just set"))
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
        domain: &IbvDomain<MlxDomain>,
        addr: usize,
        size: usize,
    ) -> anyhow::Result<IbvMemoryRegionView> {
        let pd = domain.pd();
        let access = self.access_flags();
        let mut segments = self
            .segments
            .lock()
            .expect("mlx domain segments lock poisoned");

        // Fast path: an existing scanned segment already fully covers the new
        // range (`Ok` when the scanned segment's registered portion covers the
        // range, `Err` when the range overlaps the segment's unregistered tail).
        if let Some(result) = Self::lookup(&segments, addr, size) {
            return result;
        }

        // Which ordinals this NIC serves, resolved now rather than at domain
        // construction: the caller holds a CUDA address, so CUDA is initialized
        // and the ordinal → NIC map is resolvable. If this were instead called
        // only once at construction, if CUDA were not already initialized,
        // this domain would permanently have an empty list of ordinals.
        let cuda_ordinals = self.ops.assigned_cuda_devices()?;

        // Pull live segments and keep only the ones on ordinals we serve.
        let scanned: Vec<ScannedSegment> = self
            .ops
            .scan_segments()
            .into_iter()
            .filter(|s| cuda_ordinals.contains(&s.cuda_ordinal))
            .collect();

        let qp = self.loopback_qp_ptr(domain)?;

        let mut snapshot: HashSet<(usize, i32)> = HashSet::new();
        for scanned_seg in &scanned {
            let key = (scanned_seg.address, scanned_seg.cuda_ordinal);
            snapshot.insert(key);
            match segments.get(&key) {
                // Already grown against this extent: nothing to do.
                Some(seg) if seg.scanned_size() == scanned_seg.size => {}
                // Grew: extend the existing segment in place (reuses its MRs,
                // retires its prior key internally).
                // SAFETY: `pd`/`qp` satisfy this function's contract (live PD or
                // null; valid loopback QP) and are forwarded unchanged. The
                // segments lock held here serializes every use of the domain's
                // one loopback QP, so nothing else polls its completion queue.
                Some(seg) => unsafe { seg.grow(pd, qp, access, scanned_seg) }?,
                // New: create an empty segment and grow it towards the full
                // range. It is kept even when the growth covers only part of
                // that range, so the scanned extent is recorded and later
                // requests past the covered prefix are answered without another
                // scan.
                None => {
                    let fresh = Arc::new(RegisteredSegment::empty(
                        self.ops.clone(),
                        scanned_seg.address,
                        self.mkey_max_entries,
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
        Self::lookup(&segments, addr, size).unwrap_or_else(|| {
            Err(anyhow::anyhow!(
                "CUDA address 0x{:x} + size {} is not covered by any scanned segment on the CUDA ordinals {:?} mapped to this NIC",
                addr,
                size,
                cuda_ordinals,
            ))
        })
    }

    /// Look up how `[addr, addr + size)` relates to the already scanned
    /// and registered segments. There are 3 possibilities:
    /// 1. `[addr, addr + size)` does not fall within any scanned segment.
    ///    Return `None`.
    /// 2. `[addr, addr + size)` is fully covered by the registered portion
    ///    of a scanned segment. Return `Some(Ok(IbvMemoryRegionView))`.
    /// 3. `[addr, addr + size)` is fully covered by a scanned segment, but
    ///    part of it extends into that scanned segment's unregistered tail.
    ///    Return `Some(Err(anyhow::Error))`.
    fn lookup(
        segments: &HashMap<(usize, i32), Arc<RegisteredSegment>>,
        addr: usize,
        size: usize,
    ) -> Option<anyhow::Result<IbvMemoryRegionView>> {
        if let Some(seg) = segments.values().find(|s| s.covers(addr, size)) {
            return Some(Ok(RegisteredSegment::view(seg, addr, size)));
        }
        let seg = segments
            .values()
            .find(|s| s.in_unregistered_tail(addr, size))?;
        Some(Err(anyhow::anyhow!(
            "CUDA address 0x{:x} + size {} is in the unregistered tail of the segment at 0x{:x}, whose key has no more room",
            addr,
            size,
            seg.base_virtual_addr,
        )))
    }
}

impl IbvDomainImpl for MlxDomain {
    type QueuePair = MlxQueuePair;

    unsafe fn new(context: &IbvContext, device_info: &IbvDeviceInfo, config: &IbvConfig) -> Self {
        Self::new_with_ops(
            Arc::new(ProdMlxDomainOps::new(context, device_info)),
            config.clone(),
            // An indirect mkey cannot bind more MRs than its device's `max_sge`.
            device_info.max_sge().max(0) as usize,
        )
    }

    fn access_flags(&self) -> i32 {
        (rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | rdmaxcel_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC)
            .0 as i32
    }

    unsafe fn register_mr(
        domain: &IbvDomain<MlxDomain>,
        mem: &KeepaliveLocalMemory,
    ) -> anyhow::Result<IbvMemoryRegionView> {
        let this = domain.domain_impl();
        if this.mlx5dv_enabled && matches!(mem.location(), MemoryLocation::Gpu(_)) {
            // SAFETY: `domain.as_ptr()` is null or a live PD, per this method's
            // contract.
            match unsafe { this.register_cuda_mlx5dv_mr(domain, mem.addr(), mem.size()) } {
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

    /// A recorded (successful) `bind_mr_list` call: the MRs it bound (compared
    /// by `Arc::ptr_eq`, since the mock's MRs wrap null pointers).
    #[derive(Debug, Clone)]
    struct BindCall {
        mrs: Vec<Arc<IbvMr>>,
    }

    /// Recorded state + scripted behavior for [`MockOps`]. Creation calls
    /// (`dmabuf_calls`, `bind_calls`, `scan_calls`, `loopback_created`) are
    /// recorded so tests can assert on the scan/bind algorithm. The returned
    /// [`IbvMr`]/[`Mlx5dvMkey`]/[`IbvQp`] wrap null handles, so their `Drop` is a
    /// no-op and FFI teardown is not observed here — that ordering is
    /// structurally guaranteed by ownership and exercised by the hardware tests.
    #[derive(Default)]
    struct MockState {
        device_name: String,
        mlx5dv_enabled: bool,
        served_ordinals: Vec<i32>,
        scan: Vec<ScannedSegment>,
        next_handle: usize,
        /// Number of `create_loopback_qp` calls; tests check the QP is
        /// created once and reused.
        loopback_created: usize,
        fail_dmabuf_after: Option<usize>,
        fail_bind: bool,
        scan_calls: usize,
        /// Every `register_dmabuf_range` call, including a failing one.
        dmabuf_calls: Vec<DmabufCall>,
        /// Every successful `bind_mr_list` call.
        bind_calls: Vec<BindCall>,
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

        fn assigned_cuda_devices(&self) -> anyhow::Result<Vec<i32>> {
            Ok(self.lock().served_ordinals.clone())
        }

        fn scan_segments(&self) -> Vec<ScannedSegment> {
            let mut s = self.lock();
            s.scan_calls += 1;
            s.scan.clone()
        }

        unsafe fn register_dmabuf_range(
            &self,
            _pd: &Arc<IbvPd>,
            addr: usize,
            size: usize,
            access: i32,
        ) -> anyhow::Result<IbvMr> {
            let mut s = self.lock();
            s.dmabuf_calls.push(DmabufCall { addr, size, access });
            if let Some(n) = s.fail_dmabuf_after
                && s.dmabuf_calls.len() > n
            {
                anyhow::bail!("mock dmabuf registration failure");
            }
            // A null MR: its `Drop` is a no-op, so the mock need not track it.
            Ok(IbvMr::null())
        }

        unsafe fn create_loopback_qp(
            &self,
            _domain: &IbvDomain<MlxDomain>,
            _config: &IbvConfig,
        ) -> anyhow::Result<IbvQp> {
            // A null QP placeholder (its `Drop` is a no-op); just count the call.
            self.lock().loopback_created += 1;
            Ok(IbvQp::null())
        }

        unsafe fn bind_mr_list(
            &self,
            _pd: &IbvPd,
            _qp: &IbvQp,
            _access: i32,
            mrs: Vec<Arc<IbvMr>>,
            _mkey_max_entries: usize,
        ) -> anyhow::Result<Mlx5dvMkey> {
            let mut s = self.lock();
            if s.fail_bind {
                anyhow::bail!("mock bind failure");
            }
            s.bind_calls.push(BindCall { mrs: mrs.clone() });
            // Derive distinct keys from a freshly minted handle so tests can tell
            // segments and generations apart; the key wraps a null `mkey`.
            let v = s.mint() as u32;
            Ok(Mlx5dvMkey::with_test_keys(v, v ^ 0xffff, mrs))
        }
    }

    /// `mkey_max_entries` for tests: large enough that the small MR counts here
    /// never trip the segment cap (that limit has its own test).
    const TEST_MKEY_MAX_ENTRIES: usize = 32;

    /// A domain wrapping the mock-driven [`MlxDomain`] under test. Its `pd`
    /// (and, through it, its context) is null (no-op `Drop`). Drive the strategy
    /// via [`IbvDomain::domain_impl`].
    fn domain(mock: Arc<MockOps>) -> Arc<IbvDomain<MlxDomain>> {
        domain_with_cap(mock, TEST_MKEY_MAX_ENTRIES)
    }

    /// A [`domain`] whose segments cap their keys at `mkey_max_entries` MRs.
    fn domain_with_cap(mock: Arc<MockOps>, mkey_max_entries: usize) -> Arc<IbvDomain<MlxDomain>> {
        let mlx = MlxDomain::new_with_ops(mock, IbvConfig::default(), mkey_max_entries);
        // SAFETY: `IbvPd::null()` holds a null PD (and, through it, a null
        // context) whose `Drop`s are no-ops.
        unsafe {
            Arc::new(IbvDomain::for_test(
                Arc::new(IbvPd::null()),
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

    /// A null `Arc<IbvPd>` for driving [`RegisteredSegment::grow`] directly; the
    /// `MockOps` never dereference it.
    fn null_pd() -> Arc<IbvPd> {
        Arc::new(IbvPd::null())
    }

    fn null_qp() -> IbvQp {
        IbvQp::null()
    }

    /// Upcast the mock to the `Arc<dyn MlxDomainOps>` `RegisteredSegment` takes.
    fn dyn_ops(ops: &Arc<MockOps>) -> Arc<dyn MlxDomainOps> {
        ops.clone()
    }

    /// Bind a fresh segment `[base, base + size)`: an empty segment grown once.
    fn bind_fresh(ops: &Arc<MockOps>, base: usize, size: usize) -> Arc<RegisteredSegment> {
        let segment = Arc::new(RegisteredSegment::empty(
            dyn_ops(ops),
            base,
            TEST_MKEY_MAX_ENTRIES,
        ));
        // SAFETY: `MockOps` ignores the `pd`/`qp`; the nulls are never deref'd.
        unsafe { segment.grow(&null_pd(), &null_qp(), 0, &seg(base, size, 0)) }
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
        assert_eq!(
            rs.state.lock().unwrap().mkey.as_ref().unwrap().mrs().len(),
            1,
            "the current key binds the one MR"
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
    }

    #[test]
    fn test_large_segment_splits_into_chunks() {
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
        let base = 0x10_0000_0000;
        // One MR maxes out at MAX_MR_SIZE, so MAX_MR_SIZE + 2 MiB needs two,
        // both bound to a single key.
        let rs = bind_fresh(&ops, base, MAX_MR_SIZE + MIB2);

        assert_eq!(
            rs.state.lock().unwrap().mkey.as_ref().unwrap().mrs().len(),
            2
        );
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
    }

    #[test]
    fn test_grow_reuses_mrs_and_retires_prior_key() {
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
        let base = 0x10_0000_0000;
        let rs = bind_fresh(&ops, base, MIB2);
        let mkey_a = rs.state.lock().unwrap().mkey.as_ref().unwrap().keys();
        let mr_a = ops.lock().bind_calls[0].mrs[0].clone();

        // SAFETY: `MockOps` ignores the `pd`/`qp`; the nulls are never deref'd.
        unsafe { rs.grow(&null_pd(), &null_qp(), 0, &seg(base, 2 * MIB2, 0)) }
            .expect("growth should succeed");

        assert_eq!(rs.size(), 2 * MIB2);
        assert_eq!(
            rs.state.lock().unwrap().mkey.as_ref().unwrap().mrs().len(),
            2,
            "the original MR is reused and one tail MR appended"
        );
        assert_eq!(
            rs.state.lock().unwrap().stale_mkeys.len(),
            1,
            "the prior key is parked as stale"
        );
        assert_eq!(
            rs.state.lock().unwrap().stale_mkeys[0].keys(),
            mkey_a,
            "the parked key is the original, not a fresh one"
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
        assert_eq!(s.bind_calls[0].mrs.len(), 1);
        assert!(
            Arc::ptr_eq(&s.bind_calls[0].mrs[0], &mr_a),
            "the first bind covered just the original MR"
        );
        assert_eq!(s.bind_calls[1].mrs.len(), 2);
        assert!(
            Arc::ptr_eq(&s.bind_calls[1].mrs[0], &mr_a)
                && !Arc::ptr_eq(&s.bind_calls[1].mrs[1], &mr_a),
            "the second bind reuses the original MR and adds a new tail"
        );
    }

    #[test]
    fn test_grow_failure_leaves_segment_unchanged() {
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
        let base = 0x10_0000_0000;
        let rs = bind_fresh(&ops, base, MIB2);
        // The fresh bind used one dmabuf call; fail the second tail chunk.
        ops.lock().fail_dmabuf_after = Some(2);

        // SAFETY: `MockOps` ignores the `pd`/`qp`; the nulls are never deref'd.
        let result = unsafe {
            rs.grow(
                &null_pd(),
                &null_qp(),
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
            rs.state.lock().unwrap().mkey.as_ref().unwrap().mrs().len(),
            1,
            "no tail MRs committed"
        );
        assert!(
            rs.state.lock().unwrap().stale_mkeys.is_empty(),
            "no key retired"
        );
        assert_eq!(
            ops.lock().bind_calls.len(),
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
        // SAFETY: `MockOps` ignores the `pd`/`qp`; the nulls are never deref'd.
        unsafe { rs.grow(&null_pd(), &null_qp(), 0, &seg(base, 2 * MIB2, 0)) }
            .expect("equal-size grow is a no-op");

        assert_eq!(rs.size(), 2 * MIB2, "size is unchanged");
        let s = ops.lock();
        assert_eq!(s.bind_calls.len(), binds_before, "no new bind performed");
        assert_eq!(s.dmabuf_calls.len(), 1, "no new MR registered");
    }

    #[test]
    fn test_view_pins_segment_and_anchors_at_base() {
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
        let base = 0x10_0000_0000;
        let rs = bind_fresh(&ops, base, MIB2);

        assert!(rs.covers(base + 0x400, 4096));
        let view = RegisteredSegment::view(&rs, base + 0x400, 4096);
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
        let segment = RegisteredSegment::empty(dyn_ops(&ops), base, TEST_MKEY_MAX_ENTRIES);

        // Two chunks; the second dmabuf registration fails.
        // SAFETY: `MockOps` ignores the `pd`/`qp`; the nulls are never deref'd.
        let result =
            unsafe { segment.grow(&null_pd(), &null_qp(), 0, &seg(base, MAX_MR_SIZE + MIB2, 0)) };
        assert!(
            result.is_err(),
            "the first bind fails when a chunk registration fails"
        );
        assert_eq!(segment.size(), 0, "the segment is left empty");
        assert!(
            segment.state.lock().unwrap().mkey.is_none(),
            "no key installed"
        );
        let s = ops.lock();
        assert!(s.bind_calls.is_empty(), "no bind performed");
    }

    #[test]
    fn test_bind_failure_cleans_up() {
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
        ops.lock().fail_bind = true;
        let base = 0x10_0000_0000;
        let segment = RegisteredSegment::empty(dyn_ops(&ops), base, TEST_MKEY_MAX_ENTRIES);

        // SAFETY: `MockOps` ignores the `pd`/`qp`; the nulls are never deref'd.
        let result = unsafe { segment.grow(&null_pd(), &null_qp(), 0, &seg(base, MIB2, 0)) };
        assert!(result.is_err(), "the bind fails when bind_mr_list fails");
        assert_eq!(segment.size(), 0, "the segment is left empty");
        assert!(
            segment.state.lock().unwrap().mkey.is_none(),
            "no key installed on bind failure"
        );
    }

    #[test]
    fn test_grow_past_mkey_max_entries_covers_the_prefix_that_fits() {
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
        let base = 0x10_0000_0000;
        // Cap at one MR against a two-chunk segment: the first chunk binds and
        // the second is left out rather than pushing the key over its cap.
        let segment = RegisteredSegment::empty(dyn_ops(&ops), base, 1);
        // SAFETY: `MockOps` ignores the `pd`/`qp`; the nulls are never deref'd.
        let result =
            unsafe { segment.grow(&null_pd(), &null_qp(), 0, &seg(base, MAX_MR_SIZE + MIB2, 0)) };
        assert!(result.is_ok(), "grow binds the chunks that fit the cap");
        assert_eq!(segment.size(), MAX_MR_SIZE, "one chunk is bound");
        assert_eq!(
            segment.scanned_size(),
            MAX_MR_SIZE + MIB2,
            "the whole scanned extent is recorded"
        );
        assert!(
            segment.in_unregistered_tail(base + MAX_MR_SIZE, MIB2),
            "the chunk left out is reported as unregistered tail"
        );
        let s = ops.lock();
        assert_eq!(s.dmabuf_calls.len(), 1, "only the fitting chunk registers");
        assert_eq!(s.bind_calls.len(), 1, "the key binds that one chunk");
    }

    #[test]
    fn test_grow_with_a_full_key_leaves_the_segment_bound() {
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
        let base = 0x10_0000_0000;
        let segment = RegisteredSegment::empty(dyn_ops(&ops), base, 1);
        // SAFETY: `MockOps` ignores the `pd`/`qp`; the nulls are never deref'd.
        unsafe { segment.grow(&null_pd(), &null_qp(), 0, &seg(base, MAX_MR_SIZE, 0)) }
            .expect("the first chunk fills the one-MR key");
        let registered = ops.lock().dmabuf_calls.len();

        // SAFETY: as above.
        let result =
            unsafe { segment.grow(&null_pd(), &null_qp(), 0, &seg(base, MAX_MR_SIZE + MIB2, 0)) };
        assert!(result.is_ok(), "a full key is not an error");
        assert_eq!(segment.size(), MAX_MR_SIZE, "the bound prefix is unchanged");
        assert_eq!(
            segment.scanned_size(),
            MAX_MR_SIZE + MIB2,
            "the new scanned extent is recorded"
        );
        assert_eq!(
            ops.lock().dmabuf_calls.len(),
            registered,
            "nothing registers once the key is full"
        );
    }

    #[test]
    fn test_segment_too_large_for_one_key_still_covers_its_prefix() {
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
        let base = 0x10_0000_0000;
        // Two chunks against a one-MR cap: the first binds, the second is left
        // out.
        ops.lock().scan = vec![seg(base, MAX_MR_SIZE + MIB2, 0)];
        let domain = domain_with_cap(ops.clone(), 1);

        assert!(
            register_cuda(&domain, base, MIB2).is_ok(),
            "a request inside the bound prefix is served by the key"
        );
        let scans = ops.lock().scan_calls;

        let tail = base + MAX_MR_SIZE;
        assert!(
            register_cuda(&domain, tail, MIB2).is_err(),
            "a request in the chunk left out cannot use the key"
        );
        assert_eq!(
            ops.lock().scan_calls,
            scans,
            "the unregistered tail does not trigger another scan"
        );
    }

    #[test]
    fn test_growth_past_the_cap_refuses_the_new_tail_without_rescanning() {
        let ops = MockOps::new(SERVED_NIC, true, &[0]);
        let base = 0x10_0000_0000;
        // One chunk binds under a one-MR cap; a second would not.
        ops.lock().scan = vec![seg(base, MAX_MR_SIZE, 0)];
        let domain = domain_with_cap(ops.clone(), 1);
        assert!(
            register_cuda(&domain, base, MIB2).is_ok(),
            "the first chunk fits the cap"
        );

        ops.lock().scan = vec![seg(base, MAX_MR_SIZE + MIB2, 0)];
        let tail = base + MAX_MR_SIZE;
        assert!(
            register_cuda(&domain, tail, MIB2).is_err(),
            "growing into a second chunk would exceed the cap"
        );
        let (scans, dmabufs) = {
            let s = ops.lock();
            (s.scan_calls, s.dmabuf_calls.len())
        };

        assert!(
            register_cuda(&domain, tail, MIB2).is_err(),
            "the tail stays unservable"
        );
        let s = ops.lock();
        assert_eq!(s.scan_calls, scans, "no rescan for a known tail");
        assert_eq!(
            s.dmabuf_calls.len(),
            dmabufs,
            "no registration attempted for a refused tail"
        );
    }

    // ----- MlxDomain integration tests -----

    #[test]
    fn test_mlx5dv_enabled_derived_from_ops() {
        let mock = MockOps::new(SERVED_NIC, true, &[0, 2]);
        let domain = domain(mock);
        assert!(domain.domain_impl().mlx5dv_enabled);
    }

    #[test]
    fn test_served_ordinals_resolved_per_use_not_at_construction() {
        // A domain is normally opened by a host registration, before CUDA is
        // initialized and so before any ordinal resolves to a NIC. Capturing the
        // served set at construction would exclude every CUDA segment for the
        // domain's lifetime, so it must be re-read per registration.
        let base = 0x10_0000_0000;
        let mock = MockOps::new(SERVED_NIC, true, &[]);
        let domain = domain(mock.clone());
        mock.lock().scan = vec![seg(base, MIB2, 0)];

        assert!(
            register_cuda(&domain, base, 4096).is_err(),
            "ordinal 0 is not served yet, so its segment must not bind"
        );

        // CUDA comes up; ordinal 0 now resolves to this NIC.
        mock.lock().served_ordinals = vec![0];

        let view = register_cuda(&domain, base, 4096)
            .expect("the segment must bind once its ordinal becomes served");
        assert_eq!(view.size, 4096);
        assert_eq!(mock.lock().bind_calls.len(), 1, "one segment bound");
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
        assert_eq!(mock.lock().bind_calls.len(), 1, "one segment bound");
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
        assert_eq!(
            mock.lock().bind_calls.len(),
            2,
            "growth created a new key (a second bind), retiring the prior one"
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
    fn test_loopback_qp_created_once() {
        let base = 0x10_0000_0000;
        let other = 0x20_0000_0000;
        let mock = MockOps::new(SERVED_NIC, true, &[0]);
        mock.lock().scan = vec![seg(base, MIB2, 0)];
        let domain = domain(mock.clone());

        // The first binding creates the loopback QP used to post key-binding
        // work requests.
        register_cuda(&domain, base, 4096).unwrap();
        assert_eq!(
            mock.lock().loopback_created,
            1,
            "the first binding creates the loopback QP"
        );

        // Later scans — a growth, then a brand-new segment — each consult the
        // loopback QP, but must reuse the cached one rather than create another.
        mock.lock().scan = vec![seg(base, 2 * MIB2, 0)];
        register_cuda(&domain, base + MIB2, 4096).unwrap();
        assert_eq!(
            mock.lock().loopback_created,
            1,
            "growth reuses the cached loopback QP"
        );

        mock.lock().scan = vec![seg(base, 2 * MIB2, 0), seg(other, MIB2, 0)];
        register_cuda(&domain, other, 4096).unwrap();
        assert_eq!(
            mock.lock().loopback_created,
            1,
            "binding a new segment reuses the cached loopback QP"
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
        let access = domain.domain_impl().access_flags();

        // Validate every field of a returned view: its address fields, its
        // size, the serving NIC's name, and the keys. The mock binds each key
        // with `(lkey, rkey) = (handle, handle ^ 0xffff)` for a freshly minted
        // handle, so a bound view always has a non-zero lkey and an rkey that is
        // its lkey xor 0xffff.
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
            let (mr_a, mr_c) = (
                s.bind_calls[0].mrs[0].clone(),
                s.bind_calls[1].mrs[0].clone(),
            );
            assert!(!Arc::ptr_eq(&mr_a, &mr_c), "a and c are distinct MRs");
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
            assert!(
                Arc::ptr_eq(&s.bind_calls[2].mrs[0], &mr_a),
                "the original MR is reused, not re-registered"
            );
            let mr_a_tail = s.bind_calls[2].mrs[1].clone();
            // a2's fresh bind passes a single new MR.
            assert_eq!(
                s.bind_calls[3].mrs.len(),
                1,
                "the new segment a2 binds a single MR"
            );
            let mr_a2 = s.bind_calls[3].mrs[0].clone();
            // a's original + tail, c, and a2 are four distinct MRs.
            let all = [&mr_a, &mr_c, &mr_a_tail, &mr_a2];
            for (i, x) in all.iter().enumerate() {
                for y in &all[i + 1..] {
                    assert!(
                        !Arc::ptr_eq(x, y),
                        "a, a-tail, c, and a2 are four distinct MRs"
                    );
                }
            }
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
