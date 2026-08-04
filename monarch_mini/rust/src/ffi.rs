/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Raw, `unsafe` FFI declarations for the minimonarch C ABI.
//!
//! These mirror `monarch_mini/minimonarch.h` verbatim: same layout, same
//! function signatures, same error codes. This is the *only* place that talks
//! to the C symbols; the safe wrappers in [`crate`] are built exclusively on
//! top of these declarations so the bindings go through the stable C ABI rather
//! than the internal Rust implementation.
//!
//! Nothing here is safe to call directly — the layout contracts (valid
//! pointers, ownership transfer of `mm_msg_part_t`, single-threaded poller use)
//! are documented on the safe wrappers.

#![expect(non_camel_case_types, reason = "mirror the C ABI type names exactly")]

use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_void;

/// A single part of a multipart message (mirrors `mm_msg_part_t`).
///
/// Ownership of the buffer is transferred *by value* into the runtime on
/// `mm_actor_send`/`mm_actor_serve`/etc.: after the call the runtime invokes
/// `deleter(deleter_ctx)` exactly once when the bytes are no longer needed.
#[repr(C)]
pub struct mm_msg_part_t {
    pub data: *const c_void,
    pub len: usize,
    /// Called with `deleter_ctx` when this part's memory may be freed. May be
    /// `None` (NULL in C) for borrowed data with no owned storage.
    pub deleter: Option<unsafe extern "C" fn(ctx: *mut c_void)>,
    pub deleter_ctx: *mut c_void,
}

/// A complete multipart message: an array of `n_parts` parts (mirrors `mm_msg_t`).
#[repr(C)]
pub struct mm_msg_t {
    pub parts: *mut mm_msg_part_t,
    pub n_parts: usize,
}

// Opaque handle types. The C header exposes these only as pointers.
pub enum mm_ctx {}
pub enum mm_actor {}
pub enum mm_poller {}

/// A monitor handle is just an opaque integer id, scoped to its actor (mirrors
/// `mm_monitor_handle_t`). It owns nothing.
pub type mm_monitor_handle_t = u64;

/// This actor's role in a serve/join pair (mirrors `mm_role_t`).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum mm_role_t {
    Child = 0,
    Parent = 1,
}

/// Arguments shared by `mm_actor_serve` and `mm_actor_join` (mirrors
/// `mm_connect_args_t`).
#[repr(C)]
pub struct mm_connect_args_t {
    pub role: mm_role_t,
    /// Optional: assign a name to the remote actor (NULL to skip). Transferred
    /// by value like any other part.
    pub name_for_other: *mut mm_msg_part_t,
    /// Prefix for the connection-established message (NULL for none).
    pub hello_prefix: *const mm_msg_t,
    /// Prefix for connection-failure messages (NULL for none).
    pub failure_prefix: *const mm_msg_t,
}

/// Result code returned by the fallible `mm_*` functions (mirrors `mm_err_t`).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    dead_code,
    reason = "variants are produced by the C ABI across the FFI boundary and matched on, not constructed in Rust"
)]
pub enum mm_err_t {
    /// Success.
    Ok = 0,
    /// No message available (`mm_poller_next`).
    NoMsg = 1,
    /// Parts buffer too small; `n_parts_out` holds the required size.
    BufSz = -1,
    /// Internal error; see `mm_last_error`.
    Internal = -2,
}

unsafe extern "C" {
    pub fn mm_last_error() -> *const c_char;

    pub fn mm_ctx_create(out: *mut *mut mm_ctx) -> mm_err_t;
    pub fn mm_ctx_destroy(ctx: *mut mm_ctx);

    pub fn mm_actor_create(
        ctx: *mut mm_ctx,
        ident: *mut mm_msg_part_t,
        gateway: bool,
        out: *mut *mut mm_actor,
    ) -> mm_err_t;
    pub fn mm_actor_destroy(actor: *mut mm_actor);
    pub fn mm_actor_send(
        actor: *mut mm_actor,
        receiver_ident: mm_msg_part_t,
        msg: *const mm_msg_t,
    ) -> mm_err_t;
    pub fn mm_actor_serve(
        actor: *mut mm_actor,
        url: *const c_char,
        args: *const mm_connect_args_t,
    ) -> mm_err_t;
    pub fn mm_actor_join(
        actor: *mut mm_actor,
        url: *const c_char,
        args: *const mm_connect_args_t,
    ) -> mm_err_t;
    pub fn mm_actor_die(actor: *mut mm_actor, reason: mm_msg_part_t);
    pub fn mm_actor_monitor(
        actor: *mut mm_actor,
        to_monitor_ident: mm_msg_part_t,
        failure_prefix: *const mm_msg_t,
        timeout_for_nonexistence: u64,
        out: *mut mm_monitor_handle_t,
    ) -> mm_err_t;

    pub fn mm_monitor_handle_cancel(actor: *mut mm_actor, handle: mm_monitor_handle_t);

    pub fn mm_poller_create(
        ctx: *mut mm_ctx,
        fd_out: *mut c_int,
        out: *mut *mut mm_poller,
    ) -> mm_err_t;
    pub fn mm_poller_destroy(poller: *mut mm_poller);
    pub fn mm_poller_subscribe(
        poller: *mut mm_poller,
        index: usize,
        actor: *mut mm_actor,
    ) -> mm_err_t;
    pub fn mm_poller_unsubscribe(poller: *mut mm_poller, index: usize);
    pub fn mm_poller_next(
        poller: *mut mm_poller,
        index_out: *mut usize,
        parts: *mut mm_msg_part_t,
        parts_cap: usize,
        n_parts_out: *mut usize,
    ) -> mm_err_t;
}
