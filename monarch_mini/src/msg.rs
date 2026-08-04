/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ffi::c_void;
use std::mem::ManuallyDrop;
use std::os::fd::OwnedFd;
use std::os::fd::RawFd;

use crate::shm::MapperHandle;

struct OwnedBytes(Vec<u8>);

/// A message part that owns its bytes via a C-style deleter (the original,
/// inline representation): either Rust-allocated bytes or a buffer handed in from
/// C. Calls the deleter on drop.
pub(crate) struct OwnedPart {
    data: *const c_void,
    len: usize,
    deleter: Option<unsafe extern "C" fn(*mut c_void)>,
    deleter_ctx: *mut c_void,
}

impl OwnedPart {
    fn as_bytes(&self) -> &[u8] {
        if self.data.is_null() || self.len == 0 {
            &[]
        } else {
            // SAFETY: `data`/`len` describe a valid immutable buffer owned by this
            // part for its lifetime (from `from_bytes`/`from_c`); we only read it.
            unsafe { std::slice::from_raw_parts(self.data as *const u8, self.len) }
        }
    }

    /// Transfer ownership of the buffer back to C, suppressing this part's drop so
    /// the deleter runs once, on the C side.
    fn into_c(self) -> CMsgPart {
        let me = ManuallyDrop::new(self);
        CMsgPart {
            data: me.data,
            len: me.len,
            deleter: me.deleter,
            deleter_ctx: me.deleter_ctx,
        }
    }
}

impl Drop for OwnedPart {
    fn drop(&mut self) {
        if let Some(deleter) = self.deleter {
            // SAFETY: `deleter_ctx` was paired with `deleter` at construction and
            // this is the sole owner, so the deleter runs exactly once here.
            unsafe { deleter(self.deleter_ctx) };
        }
    }
}

/// A message part living in a shared-memory slab: unmapped metadata in flight.
/// It is mapped only when its bytes are actually needed — at the source (to
/// `memcpy` in, before sending) and at the destination (to hand bytes to the
/// user). Intermediate hops forward `(offset, len, token)` without mapping.
pub(crate) struct ShmPart {
    /// The context's mapper, used to turn `(slab_fd, offset, len)` into a pointer
    /// on demand. Shared so every part that maps the same slab reuses one mapping.
    mapper: MapperHandle,
    /// The slab object this part lives in (kept open for the process lifetime).
    slab_fd: RawFd,
    /// The liveness token (a read end of the grant pipe). Holding it keeps the
    /// slab block alive; dropping it releases this part's reference.
    token: OwnedFd,
    /// Offset and length of the part within the slab.
    offset: u64,
    len: u64,
}

impl ShmPart {
    /// Map the slab range and return a pointer to its bytes. Panics only on a
    /// mapping failure, which for an already-file-backed slab range is effectively
    /// out-of-address-space — not reachable through the public interface.
    fn map(&self) -> *mut u8 {
        // SAFETY: `slab_fd` is the live slab this part was granted from, and
        // `[offset, offset+len)` is within a region the allocator grew the file to
        // cover, so mapping it is sound (see `ShmMapper::map`).
        unsafe {
            self.mapper
                .lock()
                .expect("shm mapper mutex should not be poisoned")
                .map(self.slab_fd, self.offset, self.len as usize)
                .expect("mapping a granted slab range should succeed")
        }
    }

    fn as_bytes(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        let ptr = self.map();
        // SAFETY: `map` returned a pointer to `len` readable bytes valid for the
        // mapper's (context's) lifetime, which outlives this part.
        unsafe { std::slice::from_raw_parts(ptr, self.len as usize) }
    }

    /// Materialize into a C part: map the bytes and hand C a pointer into the slab
    /// mapping, with a deleter that drops the liveness token (releasing this
    /// part's reference) when C is done. The mapping itself stays (the mapper owns
    /// it for the context's lifetime).
    fn into_c(self) -> CMsgPart {
        let data: *const c_void = if self.len == 0 {
            std::ptr::null()
        } else {
            self.map() as *const c_void
        };
        let token = Box::new(self.token);
        CMsgPart {
            data,
            len: self.len as usize,
            deleter: Some(drop_owned_fd),
            deleter_ctx: Box::into_raw(token).cast(),
        }
    }
}

/// A multipart-message part. `Owned` is the inline bytes+deleter representation;
/// `Shm` is a reference to bytes in a shared-memory slab (used for large parts
/// between processes on one machine). The enum itself has no `Drop` — each
/// variant owns and releases its own resources — so it can be moved out of in a
/// `match` (needed to relay an `Shm` part's token).
pub(crate) enum MsgPart {
    Owned(OwnedPart),
    Shm(ShmPart),
}

/// C-layout struct mirroring mm_msg_part_t. Used only at the FFI boundary in lib.rs.
#[repr(C)]
pub struct CMsgPart {
    pub data: *const c_void,
    pub len: usize,
    pub deleter: Option<unsafe extern "C" fn(*mut c_void)>,
    pub deleter_ctx: *mut c_void,
}

/// C-layout struct mirroring mm_msg_t. Used only at the FFI boundary in lib.rs.
#[repr(C)]
pub struct CMsg {
    pub parts: *mut CMsgPart,
    pub n_parts: usize,
}

impl MsgPart {
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Self {
        let owned = Box::new(OwnedBytes(bytes));
        let data = owned.0.as_ptr().cast();
        let len = owned.0.len();
        MsgPart::Owned(OwnedPart {
            data,
            len,
            deleter: Some(drop_owned_bytes),
            deleter_ctx: Box::into_raw(owned).cast(),
        })
    }

    /// Take ownership of a CMsgPart passed in from C.
    pub(crate) unsafe fn from_c(part: CMsgPart) -> Self {
        MsgPart::Owned(OwnedPart {
            data: part.data,
            len: part.len,
            deleter: part.deleter,
            deleter_ctx: part.deleter_ctx,
        })
    }

    /// Construct a shared-memory part from a received descriptor (unmapped).
    pub(crate) fn new_shm(
        mapper: MapperHandle,
        slab_fd: RawFd,
        token: OwnedFd,
        offset: u64,
        len: u64,
    ) -> Self {
        MsgPart::Shm(ShmPart {
            mapper,
            slab_fd,
            token,
            offset,
            len,
        })
    }

    /// The part's byte length, without materializing a shared-memory part.
    pub(crate) fn len(&self) -> usize {
        match self {
            MsgPart::Owned(o) => o.len,
            MsgPart::Shm(s) => s.len as usize,
        }
    }

    /// Whether this is already a shared-memory part (so it is relayed by
    /// descriptor, never copied or mapped).
    pub(crate) fn is_shm(&self) -> bool {
        matches!(self, MsgPart::Shm(_))
    }

    /// Take this shared-memory part's `(offset, len, token)` for relay, moving the
    /// liveness token out. Panics if called on an `Owned` part (guard with
    /// [`is_shm`](Self::is_shm)).
    pub(crate) fn into_shm(self) -> (u64, u64, OwnedFd) {
        match self {
            MsgPart::Shm(s) => (s.offset, s.len, s.token),
            MsgPart::Owned(_) => panic!("into_shm called on an owned part"),
        }
    }

    /// The bytes of the part. For a shared-memory part this maps the slab range
    /// (source/destination use); intermediates relay without calling this.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            MsgPart::Owned(o) => o.as_bytes(),
            MsgPart::Shm(s) => s.as_bytes(),
        }
    }

    /// Transfer ownership back to C. For a shared-memory part this maps the bytes
    /// and hands C a pointer into the slab, with a deleter that releases the
    /// liveness token when C is done.
    pub(crate) fn into_c(self) -> CMsgPart {
        match self {
            MsgPart::Owned(o) => o.into_c(),
            MsgPart::Shm(s) => s.into_c(),
        }
    }
}

unsafe extern "C" fn drop_owned_bytes(ctx: *mut c_void) {
    // SAFETY: `ctx` was created by `MsgPart::from_bytes` with
    // `Box::into_raw(Box<OwnedBytes>)`, and this deleter is installed exactly
    // once on the owning `OwnedPart`.
    unsafe {
        drop(Box::from_raw(ctx.cast::<OwnedBytes>()));
    }
}

unsafe extern "C" fn drop_owned_fd(ctx: *mut c_void) {
    // SAFETY: `ctx` was created by `ShmPart::into_c` with
    // `Box::into_raw(Box<OwnedFd>)`, and this deleter is installed exactly once on
    // the produced part; dropping the box closes the liveness token fd.
    unsafe {
        drop(Box::from_raw(ctx.cast::<OwnedFd>()));
    }
}

// SAFETY: `MsgPart` transfers opaque C-owned memory (Owned) and shared-memory
// metadata (Shm) between threads. For Owned, ownership is unique and the bytes
// are only read immutably; the deleter runs only from drop. For Shm, mapping may
// happen on whichever thread consumes the part (e.g. the poller's caller thread),
// which is sound: the mapper's state is behind a `Mutex` and the mapping it
// returns is process-global, so the resulting pointer is valid from any thread.
unsafe impl Send for MsgPart {}
// SAFETY: shared references to `MsgPart` expose only immutable byte slices (and
// mapping, which is serialized by the mapper's mutex). Mutation and destruction
// require ownership of the `MsgPart`.
unsafe impl Sync for MsgPart {}
