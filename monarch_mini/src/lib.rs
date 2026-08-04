/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod actor;
mod connection;
mod ctx;
mod framing;
mod inproc_transport;
mod matcher;
mod msg;
mod poller;
mod quic_transport;
mod shm;
mod transport;
mod unix_framing;
mod unix_transport;

use std::cell::RefCell;
use std::ffi::CString;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_uint;
use std::ffi::c_void;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use connection::ConnectRequest;
use ctx::Command;
use ctx::CtxHandle;
use ctx::Key;
use ctx::PollerKey;
use msg::CMsg;
use msg::CMsgPart;
use msg::MsgPart;
use poller::Delivered;

const EFD_CLOEXEC: c_int = 0o2000000;
const EFD_NONBLOCK: c_int = 0o4000;

unsafe extern "C" {
    fn eventfd(initval: c_uint, flags: c_int) -> c_int;
    fn dup(fd: c_int) -> c_int;
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    pub(crate) fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
}

// ---------------------------------------------------------------------------
// C wrapper types (only used at the FFI boundary)
// ---------------------------------------------------------------------------

pub struct CCtx(CtxHandle);

pub struct CActor {
    ctx: CtxHandle,
    key: Key,
    // Monitor ids are allocated here, on the caller's side, so mm_actor_monitor
    // never has to round-trip the event loop for one. Ids only need to be unique
    // among this actor's own monitors (they key its per-actor `monitors` map).
    next_monitor_id: AtomicU64,
}

pub struct CPoller {
    ctx: CtxHandle,
    key: PollerKey,
    event_fd: OwnedFd,
    rx: tokio::sync::mpsc::UnboundedReceiver<Delivered>,
    pending: Option<Delivered>,
    consumed_count: u64,
    // The consumed_count at which we last armed the poller, if still armed. The
    // server side is one-shot: it fires (writes the eventfd) once and disarms,
    // and firing always enqueues a message first. So while try_recv() keeps
    // returning Empty at the same consumed_count, the arm we sent is still live
    // and there is no need to drain the eventfd or re-send ArmPoller. This makes
    // a busy poll loop (repeated mm_poller_next without waiting) cheap: it only
    // arms once per consumed message instead of on every empty poll.
    armed_at: Option<u64>,
}

pub struct CMonitorHandle {
    ctx: CtxHandle,
    actor: Key,
    id: u64,
}

// ---------------------------------------------------------------------------
// C-compatible enums and structs (mirror minimonarch.h)
// ---------------------------------------------------------------------------

#[repr(i32)]
#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize
)]
pub enum Role {
    Child = 0,
    Parent = 1,
}

#[repr(C)]
pub struct CConnectArgs {
    pub role: Role,
    pub name_for_other: *mut CMsgPart,
    pub hello_prefix: *const CMsg,
    pub failure_prefix: *const CMsg,
}

#[repr(i32)]
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Ok = 0,
    NoMsg = 1,
    BufSz = -1,
    Internal = -2,
}

// ---------------------------------------------------------------------------
// Thread-local error string
// ---------------------------------------------------------------------------

thread_local! {
    static LAST_ERROR: RefCell<CString> =
        RefCell::new(CString::new("").unwrap());
}

fn set_last_error(msg: &str) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg).unwrap_or_default();
    });
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        set_last_error(&format!("{:#}", e));
        Error::Internal
    }
}

impl From<anyhow::Result<()>> for Error {
    fn from(r: anyhow::Result<()>) -> Self {
        match r {
            Ok(()) => Error::Ok,
            Err(e) => e.into(),
        }
    }
}

fn actor_from_create(ctx: CtxHandle, key: Key) -> CActor {
    CActor {
        ctx,
        key,
        next_monitor_id: AtomicU64::new(0),
    }
}

fn create_eventfd() -> anyhow::Result<OwnedFd> {
    let fd = unsafe { eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK) };
    if fd < 0 {
        anyhow::bail!("eventfd: {}", std::io::Error::last_os_error());
    }

    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn duplicate_fd(fd: &OwnedFd) -> anyhow::Result<OwnedFd> {
    let duplicate = unsafe { dup(fd.as_raw_fd()) };
    if duplicate < 0 {
        anyhow::bail!("dup: {}", std::io::Error::last_os_error());
    }

    Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
}

fn drain_eventfd(fd: &OwnedFd) {
    let mut value = 0_u64;
    unsafe {
        let _ = read(
            fd.as_raw_fd(),
            (&mut value as *mut u64).cast(),
            std::mem::size_of::<u64>(),
        );
    }
}

fn poller_write_message(
    delivered: Delivered,
    index_out: *mut usize,
    parts: *mut CMsgPart,
    parts_cap: usize,
    n_parts_out: *mut usize,
) -> Result<(), Delivered> {
    unsafe {
        *index_out = delivered.index;
        *n_parts_out = delivered.msg.len();
    }

    if delivered.msg.len() > parts_cap {
        return Err(delivered);
    }

    for (i, part) in delivered.msg.into_iter().enumerate() {
        unsafe {
            parts.add(i).write(part.into_c());
        }
    }
    Ok(())
}

#[no_mangle]
pub unsafe extern "C" fn mm_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ptr())
}

// ---------------------------------------------------------------------------
// Conversion: *const CMsg -> Vec<MsgPart>
// ---------------------------------------------------------------------------

unsafe fn opt_name(p: *mut CMsgPart) -> Option<MsgPart> {
    if p.is_null() {
        None
    } else {
        Some(MsgPart::from_c(std::ptr::read(p)))
    }
}

unsafe fn parts_from_cmsg(msg: *const CMsg) -> Vec<MsgPart> {
    if msg.is_null() {
        return vec![];
    }
    let m = &*msg;
    (0..m.n_parts)
        .map(|i| MsgPart::from_c(std::ptr::read(m.parts.add(i))))
        .collect()
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn mm_ctx_create(out: *mut *mut CCtx) -> Error {
    match CtxHandle::new() {
        Ok(ctx) => {
            *out = Box::into_raw(Box::new(CCtx(ctx)));
            Error::Ok
        }
        Err(e) => e.into(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn mm_ctx_destroy(ctx: *mut CCtx) {
    let ctx = Box::from_raw(ctx);
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let _ = ctx.0.send_command(Command::Shutdown { done: done_tx });
    if let Ok(thread) = done_rx.blocking_recv() {
        thread.join().expect("monarch-mini thread should join");
    }
}

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn mm_actor_create(
    ctx: *mut CCtx,
    ident: *mut CMsgPart,
    gateway: bool,
    out: *mut *mut CActor,
) -> Error {
    let handle = (*ctx).0.clone();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    match handle
        .send_command(Command::CreateActor {
            ident: opt_name(ident),
            gateway,
            done: done_tx,
        })
        .and_then(|()| done_rx.blocking_recv().map_err(anyhow::Error::from))
    {
        Ok(key) => {
            *out = Box::into_raw(Box::new(actor_from_create(handle, key)));
            Error::Ok
        }
        Err(e) => e.into(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn mm_actor_destroy(actor: *mut CActor) {
    let actor = Box::from_raw(actor);
    let _ = actor
        .ctx
        .send_command(Command::DestroyActor { key: actor.key });
}

#[no_mangle]
pub unsafe extern "C" fn mm_actor_send(
    actor: *mut CActor,
    receiver_ident: CMsgPart,
    msg: *const CMsg,
) -> Error {
    let a = &*actor;
    a.ctx
        .send_command(Command::Send {
            sender: a.key,
            destination_ident: MsgPart::from_c(receiver_ident),
            parts: parts_from_cmsg(msg),
        })
        .into()
}

#[no_mangle]
pub unsafe extern "C" fn mm_actor_serve(
    actor: *mut CActor,
    url: *const c_char,
    args: *const CConnectArgs,
) -> Error {
    let url = std::ffi::CStr::from_ptr(url)
        .to_str()
        .unwrap_or("")
        .to_owned();
    let a = &*actor;
    let args = &*args;
    a.ctx
        .send_command(Command::Serve {
            actor: a.key,
            url,
            request: ConnectRequest {
                role: args.role,
                name_for_other: opt_name(args.name_for_other),
                hello_prefix: parts_from_cmsg(args.hello_prefix),
                failure_prefix: parts_from_cmsg(args.failure_prefix),
            },
        })
        .into()
}

#[no_mangle]
pub unsafe extern "C" fn mm_actor_join(
    actor: *mut CActor,
    url: *const c_char,
    args: *const CConnectArgs,
) -> Error {
    let url = std::ffi::CStr::from_ptr(url)
        .to_str()
        .unwrap_or("")
        .to_owned();
    let a = &*actor;
    let args = &*args;
    a.ctx
        .send_command(Command::Join {
            actor: a.key,
            url,
            request: ConnectRequest {
                role: args.role,
                name_for_other: opt_name(args.name_for_other),
                hello_prefix: parts_from_cmsg(args.hello_prefix),
                failure_prefix: parts_from_cmsg(args.failure_prefix),
            },
        })
        .into()
}

#[no_mangle]
pub unsafe extern "C" fn mm_actor_die(actor: *mut CActor, reason: CMsgPart) {
    let a = &*actor;
    let _ = a.ctx.send_command(Command::Die {
        actor: a.key,
        reason: MsgPart::from_c(reason),
    });
}

#[no_mangle]
pub unsafe extern "C" fn mm_actor_monitor(
    actor: *mut CActor,
    to_monitor_ident: CMsgPart,
    failure_prefix: *const CMsg,
    timeout_for_nonexistence: u64,
    out: *mut *mut CMonitorHandle,
) -> Error {
    let a = &*actor;
    // Allocate the id locally and fire the command without waiting: this runs in
    // messaging loops and must never block on the event loop thread.
    let id = a.next_monitor_id.fetch_add(1, Ordering::Relaxed);
    match a.ctx.send_command(Command::Monitor {
        actor: a.key,
        id,
        to_monitor: MsgPart::from_c(to_monitor_ident),
        failure_prefix: parts_from_cmsg(failure_prefix),
        timeout_ms: timeout_for_nonexistence,
    }) {
        Ok(()) => {
            *out = Box::into_raw(Box::new(CMonitorHandle {
                ctx: a.ctx.clone(),
                actor: a.key,
                id,
            }));
            Error::Ok
        }
        Err(e) => e.into(),
    }
}

// ---------------------------------------------------------------------------
// MonitorHandle
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn mm_monitor_handle_cancel(handle: *mut CMonitorHandle) {
    let handle = Box::from_raw(handle);
    let _ = handle.ctx.send_command(Command::CancelMonitor {
        actor: handle.actor,
        id: handle.id,
    });
}

// ---------------------------------------------------------------------------
// Poller
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn mm_poller_create(
    ctx: *mut CCtx,
    fd_out: *mut c_int,
    out: *mut *mut CPoller,
) -> Error {
    let handle = (*ctx).0.clone();
    let event_fd = match create_eventfd() {
        Ok(fd) => fd,
        Err(e) => return e.into(),
    };
    let context_event_fd = match duplicate_fd(&event_fd) {
        Ok(fd) => fd,
        Err(e) => return e.into(),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    match handle
        .send_command(Command::CreatePoller {
            tx,
            event_fd: context_event_fd,
            done: done_tx,
        })
        .and_then(|()| done_rx.blocking_recv().map_err(anyhow::Error::from))
    {
        Ok(key) => {
            if !fd_out.is_null() {
                *fd_out = event_fd.as_raw_fd();
            }
            *out = Box::into_raw(Box::new(CPoller {
                ctx: handle,
                key,
                event_fd,
                rx,
                pending: None,
                consumed_count: 0,
                // The poller is created armed at 0 (PollerEntry::new), so mirror
                // that here to avoid a redundant first arm.
                armed_at: Some(0),
            }));
            Error::Ok
        }
        Err(e) => e.into(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn mm_poller_destroy(poller: *mut CPoller) {
    let poller = Box::from_raw(poller);
    let _ = poller
        .ctx
        .send_command(Command::DestroyPoller { poller: poller.key });
}

#[no_mangle]
pub unsafe extern "C" fn mm_poller_subscribe(
    poller: *mut CPoller,
    index: usize,
    actor: *mut CActor,
) -> Error {
    let p = &*poller;
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    p.ctx
        .send_command(Command::Subscribe {
            poller: p.key,
            index,
            actor: (*actor).key,
            done: done_tx,
        })
        .and_then(|()| done_rx.blocking_recv().map_err(anyhow::Error::from))
        .and_then(|result| result)
        .into()
}

#[no_mangle]
pub unsafe extern "C" fn mm_poller_unsubscribe(poller: *mut CPoller, index: usize) {
    let p = &*poller;
    let _ = p.ctx.send_command(Command::Unsubscribe {
        poller: p.key,
        index,
    });
}

#[no_mangle]
pub unsafe extern "C" fn mm_poller_next(
    poller: *mut CPoller,
    index_out: *mut usize,
    parts: *mut CMsgPart,
    parts_cap: usize,
    n_parts_out: *mut usize,
) -> Error {
    let p = &mut *poller;
    if let Some(delivered) = p.pending.take() {
        return match poller_write_message(delivered, index_out, parts, parts_cap, n_parts_out) {
            Ok(()) => {
                p.consumed_count += 1;
                Error::Ok
            }
            Err(delivered) => {
                p.pending = Some(delivered);
                Error::BufSz
            }
        };
    }

    let delivered = match p.rx.try_recv() {
        Ok(delivered) => delivered,
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return Error::NoMsg,
        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
            // Only drain + arm if we are not already armed at this count. A
            // repeated empty poll (busy loop, or a spurious fd wakeup) needs
            // neither, since the prior arm is still live.
            if p.armed_at != Some(p.consumed_count) {
                drain_eventfd(&p.event_fd);
                let _ = p.ctx.send_command(Command::ArmPoller {
                    poller: p.key,
                    wake_after: p.consumed_count,
                });
                p.armed_at = Some(p.consumed_count);
            }
            return Error::NoMsg;
        }
    };

    match poller_write_message(delivered, index_out, parts, parts_cap, n_parts_out) {
        Ok(()) => {
            p.consumed_count += 1;
            Error::Ok
        }
        Err(delivered) => {
            p.pending = Some(delivered);
            Error::BufSz
        }
    }
}
