/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Safe Rust bindings for the minimonarch C ABI.
//!
//! This crate is the Rust analogue of the Python bindings (`minimonarch.pyi`):
//! a thin, idiomatic wrapper over the stable C ABI declared in
//! [`minimonarch.h`]. Every call goes through the C ABI (see [`ffi`]) — the
//! bindings never reach into the internal Rust implementation — so the ABI
//! contract, and long-term compatibility, is the same one C and Python callers
//! depend on.
//!
//! # Shape
//!
//! Unlike Python, Rust has no ambient event loop, so the context and poller the
//! Python API hides are explicit here:
//!
//! - [`Context`] owns the runtime (one background event loop). Handles are
//!   plain owned values with no borrows tying them to it; dropping the context
//!   tears the runtime down, and any later use of an [`Actor`] simply returns
//!   an error rather than misbehaving.
//! - [`Actor`] is an addressable endpoint: [`send`](Actor::send),
//!   [`serve`](Actor::serve), [`join`](Actor::join), [`die`](Actor::die),
//!   [`monitor`](Actor::monitor), and [`recv`](Actor::recv) to await this
//!   actor's own next message (the analogue of Python's `actor.next()`).
//! - [`Poller`] drives message delivery for *several* actors at once. Subscribe
//!   actors to it, then drain with [`try_recv`](Poller::try_recv) (non-blocking)
//!   or [`recv`](Poller::recv) (blocks on the wakeup fd), integrating with any
//!   fd-based event loop via [`fd`](Poller::fd). [`Actor::recv`] is the
//!   single-actor convenience built on top: it lazily creates and stores a
//!   private poller the first time it is called.
//! - [`Part`] is a multipart-message segment. It owns its bytes (like
//!   `minimonarch.bytearray`) and can be *moved* into a message zero-copy —
//!   including forwarding a received part without copying.
//!
//! # Example
//!
//! ```no_run
//! use monarch_mini_rs::Context;
//! use monarch_mini_rs::Part;
//! use monarch_mini_rs::Role;
//!
//! # async fn example() -> Result<(), monarch_mini_rs::Error> {
//! let ctx = Context::new()?;
//! let actor = ctx.actor(Some(b"hello-actor"), /*gateway=*/ true)?;
//!
//! actor.send(b"hello-actor", vec![Part::copy_from(b"hello, self")])?;
//! let parts = actor.recv().await?; // lazily creates this actor's own poller
//! assert_eq!(parts[0].as_bytes(), b"hello, self");
//! # Ok(())
//! # }
//! ```

mod ffi;

// The `mm_*` C ABI symbols are resolved by statically linking minimonarch's
// self-contained native archive (`libmonarch_mini-*.a`); see build.rs. It is an
// opaque native library — its std and tokio are bundled and internalized, so
// from this crate's point of view it is indistinguishable from a C library, and
// its runtime is fully isolated from ours (no shared allocator, panic runtime,
// or thread-local state).

use std::ffi::CString;
use std::ffi::c_void;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;

use ffi::mm_msg_part_t;
use ffi::mm_msg_t;
use tokio::io::unix::AsyncFd;

/// An error from the minimonarch runtime, carrying the C ABI's last-error
/// string for the failing call.
#[derive(Debug, thiserror::Error)]
#[error("minimonarch: {0}")]
pub struct Error(String);

/// Result alias for the fallible bindings API.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Capture the current thread's last-error string from the C ABI.
    fn last() -> Self {
        // SAFETY: `mm_last_error` returns a valid NUL-terminated pointer owned
        // by the runtime, valid until the next `mm_*` call on this thread; we
        // copy it out immediately.
        let ptr = unsafe { ffi::mm_last_error() };
        let msg = if ptr.is_null() {
            String::new()
        } else {
            // SAFETY: `ptr` is a valid C string per the ABI contract above.
            unsafe { std::ffi::CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned()
        };
        Error(msg)
    }
}

/// Translate a `mm_err_t` from a fallible non-poller call into a `Result`.
fn check(err: ffi::mm_err_t) -> Result<()> {
    match err {
        ffi::mm_err_t::Ok => Ok(()),
        _ => Err(Error::last()),
    }
}

// A few C calls (`mm_ctx_destroy`, `mm_actor_create`, `mm_poller_create`,
// `mm_poller_subscribe`) block briefly for a round-trip to minimonarch's own
// runtime thread. minimonarch is linked as an opaque native library (a static
// archive with its own std/tokio; see build.rs), so that wait is invisible to
// the caller's async runtime — there is no shared thread-local runtime context to
// trip over. These calls therefore just block the calling thread, exactly like
// any synchronous C function, on any runtime flavor and with no special
// handling. (The same round-trip is what the Python bindings block the asyncio
// loop thread for.)

/// This actor's role in a serve/join pair. Exactly one side of a connection
/// must be [`Role::Parent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// This actor becomes the parent of the remote actor.
    Parent,
    /// This actor becomes the child of the remote actor.
    Child,
}

impl From<Role> for ffi::mm_role_t {
    fn from(role: Role) -> Self {
        match role {
            Role::Parent => ffi::mm_role_t::Parent,
            Role::Child => ffi::mm_role_t::Child,
        }
    }
}

// ---------------------------------------------------------------------------
// Part
// ---------------------------------------------------------------------------

/// A single part of a multipart message: an owned byte buffer.
///
/// A `Part` owns its storage through a C-style deleter, mirroring
/// `minimonarch.bytearray`. It is either bytes allocated on the Rust side or a
/// buffer adopted from a received message (possibly backed by a shared-memory
/// slab). Either way, sending a `Part` *moves* it into the message zero-copy —
/// so a received part can be forwarded onward without copying its bytes.
///
/// `Part` is intentionally neither `Send` nor `Sync`: parts are built and
/// consumed inline with a [`send`](Actor::send) or drained from a [`Poller`],
/// so they never need to cross a thread boundary on their own.
pub struct Part {
    data: *const c_void,
    len: usize,
    deleter: Option<unsafe extern "C" fn(*mut c_void)>,
    deleter_ctx: *mut c_void,
}

/// Deleter for a `Part` backed by a Rust `Box<Vec<u8>>`.
unsafe extern "C" fn drop_boxed_vec(ctx: *mut c_void) {
    // SAFETY: `ctx` was produced by `Part::from_vec` via
    // `Box::into_raw(Box<Vec<u8>>)`, installed as the sole deleter, so it runs
    // exactly once here.
    unsafe {
        drop(Box::from_raw(ctx.cast::<Vec<u8>>()));
    }
}

impl Part {
    /// An empty part.
    pub fn empty() -> Self {
        Part {
            data: std::ptr::null(),
            len: 0,
            deleter: None,
            deleter_ctx: std::ptr::null_mut(),
        }
    }

    /// Take ownership of `bytes`, moving it into a part with no copy of the
    /// underlying buffer.
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            return Part::empty();
        }
        let boxed = Box::new(bytes);
        let data = boxed.as_ptr().cast();
        let len = boxed.len();
        Part {
            data,
            len,
            deleter: Some(drop_boxed_vec),
            deleter_ctx: Box::into_raw(boxed).cast(),
        }
    }

    /// Copy `bytes` into a new owned part.
    pub fn copy_from(bytes: &[u8]) -> Self {
        Part::from_vec(bytes.to_vec())
    }

    /// The part's bytes.
    pub fn as_bytes(&self) -> &[u8] {
        if self.data.is_null() || self.len == 0 {
            return &[];
        }
        // SAFETY: `data`/`len` describe a buffer this part owns for its whole
        // lifetime (Rust-allocated or adopted from a received part); we only
        // read it immutably.
        unsafe { std::slice::from_raw_parts(self.data.cast::<u8>(), self.len) }
    }

    /// The part's length in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the part is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Copy the part's bytes into an owned `Vec`.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    /// Adopt a raw part handed back from the C ABI (a received message part),
    /// taking over its buffer and deleter.
    ///
    /// # Safety
    /// `raw` must be a part just produced by the runtime (e.g. via
    /// `mm_poller_next`) whose ownership is being transferred exactly once.
    unsafe fn from_raw(raw: mm_msg_part_t) -> Self {
        Part {
            data: raw.data,
            len: raw.len,
            deleter: raw.deleter,
            deleter_ctx: raw.deleter_ctx,
        }
    }

    /// Transfer ownership of the buffer to the C ABI, suppressing this part's
    /// drop so the deleter runs once, on the runtime side.
    fn into_raw(self) -> mm_msg_part_t {
        let me = std::mem::ManuallyDrop::new(self);
        mm_msg_part_t {
            data: me.data,
            len: me.len,
            deleter: me.deleter,
            deleter_ctx: me.deleter_ctx,
        }
    }
}

impl Drop for Part {
    fn drop(&mut self) {
        if let Some(deleter) = self.deleter {
            // SAFETY: `deleter_ctx` was paired with `deleter` at construction
            // and this is the sole owner, so the deleter runs exactly once.
            unsafe { deleter(self.deleter_ctx) };
        }
    }
}

impl From<Vec<u8>> for Part {
    fn from(bytes: Vec<u8>) -> Self {
        Part::from_vec(bytes)
    }
}

impl From<&[u8]> for Part {
    fn from(bytes: &[u8]) -> Self {
        Part::copy_from(bytes)
    }
}

/// A borrowed multipart message: a temporary `Vec<mm_msg_part_t>` that keeps
/// the parts' ownership until the C call consumes them. Dropping it *without*
/// leaking runs every remaining part's deleter, so it is always leaked
/// (`ManuallyDrop`) once handed to a C call that takes ownership.
struct RawParts {
    parts: Vec<mm_msg_part_t>,
}

impl RawParts {
    /// Move `parts` into their raw C representation.
    fn new(parts: Vec<Part>) -> Self {
        RawParts {
            parts: parts.into_iter().map(Part::into_raw).collect(),
        }
    }

    /// Build owned copies of a slice of byte-slice prefixes.
    fn copied(prefix: &[&[u8]]) -> Self {
        RawParts::new(prefix.iter().map(|b| Part::copy_from(b)).collect())
    }

    fn as_msg(&mut self) -> mm_msg_t {
        mm_msg_t {
            parts: self.parts.as_mut_ptr(),
            n_parts: self.parts.len(),
        }
    }
}

impl Drop for RawParts {
    fn drop(&mut self) {
        // Run each not-yet-consumed part's deleter by rehydrating it as a
        // `Part`. Only reached if a C call was *not* made (e.g. an error before
        // the send); a successful call transfers ownership and this drains an
        // already-empty vec.
        for raw in self.parts.drain(..) {
            // SAFETY: each `raw` came from `Part::into_raw`, so reconstructing
            // the owning `Part` restores the single-owner invariant.
            drop(unsafe { Part::from_raw(raw) });
        }
    }
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// The minimonarch runtime for a process: a single background event loop plus
/// the actors and pollers created from it.
///
/// A `Context` is a handle to that event loop; every operation is dispatched to
/// it over an internal channel, so the handle is safe to move and share across
/// threads.
pub struct Context {
    ptr: *mut ffi::mm_ctx,
}

// SAFETY: a `Context` handle only ever forwards commands to the runtime over an
// internal `Send + Sync` channel and blocks on a reply; it holds no
// thread-affine state, so it is safe to move between threads (`Send`) and to
// share (`Sync`) — concurrent creates each use their own reply channel.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    /// Create a new context and its background event loop.
    pub fn new() -> Result<Self> {
        let mut ptr: *mut ffi::mm_ctx = std::ptr::null_mut();
        // SAFETY: `out` points to a valid slot we own; on `Ok` it is populated
        // with a context pointer we take ownership of.
        check(unsafe { ffi::mm_ctx_create(&mut ptr) })?;
        Ok(Context { ptr })
    }

    /// Create an actor bound to this context.
    ///
    /// `ident` must be unique across the whole run; if `None`, a name must be
    /// assigned by the peer in a later `serve`/`join`. `gateway` declares the
    /// actor as the entry point for its process group (no parent or a network
    /// parent); it is fixed at creation.
    pub fn actor(&self, ident: Option<&[u8]>, gateway: bool) -> Result<Actor> {
        // Keep the ident part alive across the call; the runtime takes ownership
        // of it (like every other part) when a name is supplied.
        let mut ident_raw = ident.map(|b| Part::copy_from(b).into_raw());
        let ident_ptr = ident_raw
            .as_mut()
            .map_or(std::ptr::null_mut(), |p| p as *mut mm_msg_part_t);

        let mut ptr: *mut ffi::mm_actor = std::ptr::null_mut();
        // SAFETY: `self.ptr` is a live context; `ident_ptr` is either null or a
        // valid part whose ownership transfers to the runtime; `out` is ours.
        let err = unsafe { ffi::mm_actor_create(self.ptr, ident_ptr, gateway, &mut ptr) };
        check(err)?;
        Ok(Actor {
            ptr,
            ctx: self.ptr,
            poller: tokio::sync::Mutex::new(None),
        })
    }

    /// Create a [`Poller`] bound to this context.
    pub fn poller(&self) -> Result<Poller> {
        let mut ptr: *mut ffi::mm_poller = std::ptr::null_mut();
        let mut fd: RawFd = -1;
        // SAFETY: `self.ptr` is a live context; `fd_out` and `out` point to slots
        // we own and are populated on success.
        let err = unsafe { ffi::mm_poller_create(self.ptr, &mut fd, &mut ptr) };
        check(err)?;
        Ok(Poller {
            ptr,
            fd,
            async_fd: None,
        })
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` is a live context we own; this consumes it,
        // flushing pending messages and joining minimonarch's runtime thread.
        // Actors/pollers are self-contained C handles that stay safe to drop
        // afterwards (they each hold their own runtime sender), so no
        // drop-ordering relationship with them is required.
        unsafe { ffi::mm_ctx_destroy(self.ptr) };
    }
}

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

/// An addressable messaging endpoint, created from a [`Context`].
///
/// An `Actor` is a plain owned handle: it may outlive its [`Context`], and any
/// method called after the context is dropped returns an error rather than
/// misbehaving.
pub struct Actor {
    ptr: *mut ffi::mm_actor,
    // The context this actor was created from, kept so [`Actor::recv`] can lazily
    // build its own [`Poller`]. A raw pointer (not a `Context` handle) so an
    // actor never keeps the runtime alive; if the context is gone, poller
    // creation simply errors.
    ctx: *mut ffi::mm_ctx,
    // A per-actor poller, created on the first call to [`Actor::recv`] and reused
    // thereafter, so `actor.recv().await` works without the caller ever managing
    // a [`Poller`] — the Rust analogue of the Python bindings' `actor.next()`.
    // Behind a `tokio::sync::Mutex` (whose guard is held across the `await`) so
    // the actor stays `Send + Sync` even though a `Poller` is `!Sync`.
    poller: tokio::sync::Mutex<Option<Poller>>,
}

// SAFETY: every `Actor` method forwards a command to the runtime over its
// internal `Send + Sync` channel (monitor-id allocation uses an atomic), so an
// `Actor` is safe to move between threads (`Send`) and to call concurrently from
// several threads (`Sync`) — the classic "send from many threads" pattern. The
// raw pointers are just runtime handles; the lazily-created per-actor poller is
// guarded by a `tokio::sync::Mutex` (itself `Send + Sync` since `Poller: Send`),
// which serializes the otherwise-`!Sync` poller's use.
unsafe impl Send for Actor {}
unsafe impl Sync for Actor {}

impl Actor {
    /// Send a multipart message to `receiver`. Each [`Part`] is moved into the
    /// message; `receiver` is copied.
    pub fn send(&self, receiver: &[u8], parts: Vec<Part>) -> Result<()> {
        let mut raw = RawParts::new(parts);
        let msg = raw.as_msg();
        let receiver_raw = Part::copy_from(receiver).into_raw();
        // SAFETY: `self.ptr` is a live actor. `receiver_raw` and every part in
        // `msg` are valid owned parts whose ownership transfers to the runtime
        // (which always consumes them, even on error), so we leak `raw` to
        // avoid double-freeing.
        let err = unsafe { ffi::mm_actor_send(self.ptr, receiver_raw, &msg) };
        std::mem::forget(raw);
        check(err)
    }

    /// Serve (listen) on `url`, taking `role` in the resulting pair.
    ///
    /// On success this actor is later delivered `[hello..., self, other]`; on
    /// failure `[failure..., other, reason]`. `name` optionally assigns a name
    /// to the peer. See [`Actor::join`] for the joining side.
    pub fn serve(
        &self,
        url: &str,
        role: Role,
        name: Option<&[u8]>,
        hello: &[&[u8]],
        failure: &[&[u8]],
    ) -> Result<()> {
        self.connect(url, role, name, hello, failure, ffi::mm_actor_serve)
    }

    /// Join (connect to) `url`, taking `role` in the resulting pair. See
    /// [`Actor::serve`] for the semantics of the arguments.
    pub fn join(
        &self,
        url: &str,
        role: Role,
        name: Option<&[u8]>,
        hello: &[&[u8]],
        failure: &[&[u8]],
    ) -> Result<()> {
        self.connect(url, role, name, hello, failure, ffi::mm_actor_join)
    }

    /// Shared implementation of `serve`/`join`, which take identical arguments.
    fn connect(
        &self,
        url: &str,
        role: Role,
        name: Option<&[u8]>,
        hello: &[&[u8]],
        failure: &[&[u8]],
        // `serve` and `join` have the same signature; pick one.
        call: unsafe extern "C" fn(
            *mut ffi::mm_actor,
            *const std::ffi::c_char,
            *const ffi::mm_connect_args_t,
        ) -> ffi::mm_err_t,
    ) -> Result<()> {
        let curl = CString::new(url).map_err(|e| Error(e.to_string()))?;

        let mut name_raw = name.map(|b| Part::copy_from(b).into_raw());
        let name_ptr = name_raw
            .as_mut()
            .map_or(std::ptr::null_mut(), |p| p as *mut mm_msg_part_t);

        let mut hello_raw = RawParts::copied(hello);
        let mut failure_raw = RawParts::copied(failure);
        let hello_msg = hello_raw.as_msg();
        let failure_msg = failure_raw.as_msg();

        let args = ffi::mm_connect_args_t {
            role: role.into(),
            name_for_other: name_ptr,
            hello_prefix: &hello_msg,
            failure_prefix: &failure_msg,
        };
        // SAFETY: `self.ptr` is a live actor; `curl` is a valid NUL-terminated
        // string; `args` and the parts it references are valid for the call,
        // and the runtime takes ownership of the (owned) parts.
        let err = unsafe { call(self.ptr, curl.as_ptr(), &args) };
        std::mem::forget(hello_raw);
        std::mem::forget(failure_raw);
        check(err)
    }

    /// Signal that this actor is dead to its parent, children, and monitors.
    /// `reason` is a UTF-8 string explaining why.
    pub fn die(&self, reason: &[u8]) {
        let reason_raw = Part::copy_from(reason).into_raw();
        // SAFETY: `self.ptr` is a live actor; `reason_raw` is a valid owned part
        // whose ownership transfers to the runtime.
        unsafe { ffi::mm_actor_die(self.ptr, reason_raw) };
    }

    /// Monitor `ident`. If it dies (or is already dead), this actor is sent
    /// `[failure..., ident, b"actor died"]`.
    ///
    /// If `timeout_for_nonexistence` is non-zero and `ident` is still not known
    /// anywhere after that many milliseconds, the monitor fires once with
    /// reason `b"actor does not exist"` and is consumed. `0` disables the
    /// timeout. Only the first monitor on a given target arms a timeout.
    pub fn monitor(
        &self,
        ident: &[u8],
        failure: &[&[u8]],
        timeout_for_nonexistence: u64,
    ) -> Result<MonitorHandle> {
        let ident_raw = Part::copy_from(ident).into_raw();
        let mut failure_raw = RawParts::copied(failure);
        let failure_msg = failure_raw.as_msg();

        let mut handle: ffi::mm_monitor_handle_t = 0;
        // SAFETY: `self.ptr` is a live actor; `ident_raw` and the failure parts
        // are valid owned parts transferred to the runtime; `out` is ours.
        let err = unsafe {
            ffi::mm_actor_monitor(
                self.ptr,
                ident_raw,
                &failure_msg,
                timeout_for_nonexistence,
                &mut handle,
            )
        };
        std::mem::forget(failure_raw);
        check(err)?;
        Ok(MonitorHandle { handle })
    }

    /// Cancel `monitor`, which must have been created by this actor. Its failure
    /// message will no longer be delivered, and any already-queued failure is dropped
    /// before delivery. Consumes the handle; dropping it instead leaves the monitor
    /// active.
    pub fn cancel_monitor(&self, monitor: MonitorHandle) {
        // SAFETY: `self.ptr` is a live actor, and `monitor.handle` is an opaque monitor
        // id consumed by this call. The cancel is addressed by (actor, id) and frees
        // nothing.
        unsafe { ffi::mm_monitor_handle_cancel(self.ptr, monitor.handle) };
    }

    /// Await this actor's next delivered message — the Rust analogue of the
    /// Python bindings' awaitable `actor.next()`.
    ///
    /// On the first call, the actor lazily creates a private [`Poller`],
    /// subscribes itself to it, and stores it on the handle; subsequent calls
    /// reuse it. This is the convenience path for the common "just read this
    /// actor's messages" case, so the caller never has to manage a [`Poller`]
    /// or actor indices. Must be called from within a Tokio runtime.
    ///
    /// Do not also subscribe this actor to a separate [`Poller`]: the C ABI
    /// allows an actor on at most one poller at a time, so mixing the two paths
    /// makes this call (or the manual `subscribe`) fail. For fan-in over many
    /// actors, use an explicit [`Poller`] instead of per-actor `recv`.
    pub async fn recv(&self) -> Result<Vec<Part>> {
        let mut guard = self.poller.lock().await;
        if guard.is_none() {
            *guard = Some(self.make_poller()?);
        }
        // The guard is held across the await so the `!Sync` poller is used by
        // one task at a time; concurrent `recv`s on the same actor serialize.
        let poller = guard.as_mut().expect("poller just created");
        let (_index, parts) = poller.recv().await?;
        Ok(parts)
    }

    /// Build the actor's private poller and subscribe itself at index 0.
    fn make_poller(&self) -> Result<Poller> {
        let mut ptr: *mut ffi::mm_poller = std::ptr::null_mut();
        let mut fd: RawFd = -1;
        // SAFETY: `self.ctx` is the context this actor was created from; `fd_out`
        // and `out` point to slots we own and are populated on success. (If the
        // context has been dropped, this returns an error instead.)
        check(unsafe { ffi::mm_poller_create(self.ctx, &mut fd, &mut ptr) })?;
        let poller = Poller {
            ptr,
            fd,
            async_fd: None,
        };
        poller.subscribe(0, self)?;
        Ok(poller)
    }

    /// The raw actor pointer, for [`Poller::subscribe`].
    fn as_ptr(&self) -> *mut ffi::mm_actor {
        self.ptr
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        // Drop the lazily-created per-actor poller (if any) first: it holds this
        // actor subscribed and owns a wakeup fd, so it must be torn down before
        // the actor. `get_mut` needs no lock — we have exclusive access here.
        self.poller.get_mut().take();
        // SAFETY: `self.ptr` is a live actor we own; this consumes it.
        unsafe { ffi::mm_actor_destroy(self.ptr) };
    }
}

// ---------------------------------------------------------------------------
// MonitorHandle
// ---------------------------------------------------------------------------

/// Handle to a registered monitor, analogous to a [`tokio::task::JoinHandle`]:
/// **dropping it does not cancel the monitor** — it detaches, and the monitor
/// keeps running (it will still fire on the target's death/absence). Keep the
/// handle only if you may want to pass it to [`Actor::cancel_monitor`] later.
///
/// (Consistent with the Python bindings, whose handle also does not cancel on
/// GC.) The handle owns nothing — it is just the monitor's id — so dropping it
/// is free and leaks nothing.
pub struct MonitorHandle {
    handle: ffi::mm_monitor_handle_t,
}

// No `Drop` impl: dropping a `MonitorHandle` detaches (JoinHandle semantics) and
// must not cancel the monitor.

// ---------------------------------------------------------------------------
// Poller
// ---------------------------------------------------------------------------

/// Drives message delivery for a set of subscribed actors, integrating with a
/// Tokio runtime.
///
/// [`recv`](Poller::recv) is `async`: it awaits the poller's wakeup fd on the
/// caller's Tokio reactor (via [`AsyncFd`]), draining delivered messages — the
/// same fd-reader model the Python bindings install on the asyncio loop.
///
/// Unlike [`Context`] and [`Actor`], a `Poller` is `Send` but **not** `Sync`:
/// the C ABI requires that calls on a single poller be externally serialized.
/// It can be owned by, or moved to, one task/thread that drives it, but it
/// cannot be shared for concurrent use. `!Sync` (from the raw pointer) enforces
/// this; draining still goes through `&mut self`.
pub struct Poller {
    ptr: *mut ffi::mm_poller,
    fd: RawFd,
    // The wakeup fd registered with the Tokio reactor, created lazily on the
    // first `recv` (registration must happen inside a runtime).
    async_fd: Option<AsyncFd<WakeupFd>>,
}

// SAFETY: a `Poller` owns only a runtime handle, a `Send` receiver, and a
// non-thread-affine wakeup fd, so it is sound to move to another thread
// (`Send`). It is deliberately left `!Sync` (the `*mut` field withholds the
// auto-impl): the C ABI requires calls on one poller to be externally
// serialized, so it must not be shared for concurrent access.
unsafe impl Send for Poller {}

/// Borrows the poller's wakeup fd for Tokio registration *without owning it* —
/// the fd belongs to the poller (the C ABI says not to close it), and dropping
/// a `RawFd` does nothing, so the `AsyncFd` only registers/deregisters it.
struct WakeupFd(RawFd);

impl AsRawFd for WakeupFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl Poller {
    /// The wakeup file descriptor. It becomes readable when more delivered
    /// messages may exist; suitable for `select`/`poll`/`epoll`. Owned by the
    /// poller — do not close it.
    pub fn fd(&self) -> RawFd {
        self.fd
    }

    /// Watch `actor` for incoming messages, associating it with `index`. An
    /// actor may be subscribed to at most one poller at a time.
    pub fn subscribe(&self, index: usize, actor: &Actor) -> Result<()> {
        // SAFETY: `self.ptr` and `actor` are live handles into the same runtime.
        check(unsafe { ffi::mm_poller_subscribe(self.ptr, index, actor.as_ptr()) })
    }

    /// Stop watching the actor previously assigned `index`.
    pub fn unsubscribe(&self, index: usize) {
        // SAFETY: `self.ptr` is a live poller we own.
        unsafe { ffi::mm_poller_unsubscribe(self.ptr, index) };
    }

    /// Read the next delivered message without blocking, returning
    /// `Ok(Some((index, parts)))` for a message, or `Ok(None)` if none is
    /// available. Arms the wakeup [`fd`](Poller::fd) when it returns `None`.
    pub fn try_recv(&mut self) -> Result<Option<(usize, Vec<Part>)>> {
        // Grow the parts buffer on demand: `mm_poller_next` reports the required
        // size via `n_parts_out` on `BufSz` without consuming the message.
        let mut cap = 8usize;
        loop {
            let mut index_out: usize = 0;
            let mut n_parts_out: usize = 0;
            let mut buf: Vec<mm_msg_part_t> = Vec::with_capacity(cap);
            // SAFETY: `self.ptr` is a live poller; `buf` has `cap` writable
            // slots; the out-params point to locals we own. On `Ok` the runtime
            // writes `n_parts_out` parts whose ownership transfers to us.
            let err = unsafe {
                ffi::mm_poller_next(
                    self.ptr,
                    &mut index_out,
                    buf.as_mut_ptr(),
                    cap,
                    &mut n_parts_out,
                )
            };
            match err {
                ffi::mm_err_t::Ok => {
                    // SAFETY: the runtime initialized `n_parts_out` parts.
                    unsafe { buf.set_len(n_parts_out) };
                    let parts = buf
                        .into_iter()
                        // SAFETY: each part was just produced by the runtime and
                        // its ownership transferred to us exactly once.
                        .map(|raw| unsafe { Part::from_raw(raw) })
                        .collect();
                    return Ok(Some((index_out, parts)));
                }
                ffi::mm_err_t::NoMsg => return Ok(None),
                ffi::mm_err_t::BufSz => {
                    // Nothing was written or consumed; retry with a big-enough
                    // buffer.
                    cap = n_parts_out;
                    continue;
                }
                ffi::mm_err_t::Internal => return Err(Error::last()),
            }
        }
    }

    /// Await the next delivered message — the Rust analogue of the Python API's
    /// awaitable `next()`.
    ///
    /// Drains [`try_recv`](Poller::try_recv); when empty, awaits the poller's
    /// wakeup fd on the current Tokio reactor and retries. Must be called from
    /// within a Tokio runtime (the fd is registered lazily on first use).
    pub async fn recv(&mut self) -> Result<(usize, Vec<Part>)> {
        loop {
            if let Some(message) = self.try_recv()? {
                return Ok(message);
            }
            // `try_recv` drained the wakeup fd and armed the poller; await the
            // runtime signalling it (an enqueued message writes the eventfd).
            let async_fd = self.ensure_async_fd()?;
            let mut guard = async_fd
                .readable()
                .await
                .map_err(|e| Error(format!("await poller wakeup fd: {e}")))?;
            // Readiness may be stale — `try_recv` drains the fd itself, behind
            // the reactor's back — so clear it and re-check the channel; a real
            // message always re-signals via a fresh edge.
            guard.clear_ready();
        }
    }

    /// Register the wakeup fd with the current Tokio reactor on first use.
    fn ensure_async_fd(&mut self) -> Result<&AsyncFd<WakeupFd>> {
        match self.async_fd {
            Some(ref async_fd) => Ok(async_fd),
            None => {
                let async_fd = AsyncFd::new(WakeupFd(self.fd))
                    .map_err(|e| Error(format!("register poller wakeup fd with tokio: {e}")))?;
                Ok(self.async_fd.insert(async_fd))
            }
        }
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        // Deregister the wakeup fd from the Tokio reactor *before* the C side
        // closes it (mm_poller_destroy drops the poller's owned fd). Dropping
        // the `AsyncFd` only removes it from the reactor; it does not close the
        // borrowed fd.
        self.async_fd = None;
        // SAFETY: `self.ptr` is a live poller we own; this consumes it. The fd
        // is owned by the poller, so we do not close it.
        unsafe { ffi::mm_poller_destroy(self.ptr) };
    }
}

#[cfg(test)]
mod tests;
