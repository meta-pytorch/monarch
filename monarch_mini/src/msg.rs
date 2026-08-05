/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ffi::c_void;

struct OwnedBytes(Vec<u8>);

/// Owning Rust message part. Calls the C deleter on drop.
/// The C caller must not free or use the data after passing it in.
pub struct MsgPart {
    data: *const c_void,
    pub len: usize,
    deleter: Option<unsafe extern "C" fn(*mut c_void)>,
    deleter_ctx: *mut c_void,
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
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let owned = Box::new(OwnedBytes(bytes));
        let data = owned.0.as_ptr().cast();
        let len = owned.0.len();
        MsgPart {
            data,
            len,
            deleter: Some(drop_owned_bytes),
            deleter_ctx: Box::into_raw(owned).cast(),
        }
    }

    /// Take ownership of a CMsgPart passed in from C.
    pub unsafe fn from_c(part: CMsgPart) -> Self {
        MsgPart {
            data: part.data,
            len: part.len,
            deleter: part.deleter,
            deleter_ctx: part.deleter_ctx,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        if self.data.is_null() || self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.data as *const u8, self.len) }
        }
    }

    /// Transfer ownership back to C. self is consumed without calling drop;
    /// the C caller becomes responsible for calling the deleter.
    pub fn into_c(self) -> CMsgPart {
        let part = CMsgPart {
            data: self.data,
            len: self.len,
            deleter: self.deleter,
            deleter_ctx: self.deleter_ctx,
        };
        std::mem::forget(self);
        part
    }
}

unsafe extern "C" fn drop_owned_bytes(ctx: *mut c_void) {
    // SAFETY: `ctx` was created by `MsgPart::from_bytes` with
    // `Box::into_raw(Box<OwnedBytes>)`, and this deleter is installed exactly
    // once on the owning `MsgPart`.
    unsafe {
        drop(Box::from_raw(ctx.cast::<OwnedBytes>()));
    }
}

impl Drop for MsgPart {
    fn drop(&mut self) {
        if let Some(deleter) = self.deleter {
            unsafe { deleter(self.deleter_ctx) };
        }
    }
}

// SAFETY: `MsgPart` transfers opaque C-owned memory between threads without
// dereferencing it except to read immutable bytes. Ownership remains unique,
// and the deleter runs only from `Drop`.
unsafe impl Send for MsgPart {}
// SAFETY: shared references to `MsgPart` expose only immutable byte slices.
// Mutation and destruction require ownership of the `MsgPart`.
unsafe impl Sync for MsgPart {}
