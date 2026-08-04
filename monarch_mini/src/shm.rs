/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Shared-memory slab machinery for the machine-local large-message transport.
//!
//! Three objects with distinct owners and lifetimes:
//!
//! - [`Allocator`] — the bump + per-size-freelist allocator over a single slab
//!   `memfd`. It owns the slab file, grows it, and hands out offsets; it never
//!   maps anything.
//! - [`ShmServer`] — the allocation *authority* (one per gateway actor). It runs
//!   a unix dgram server that answers allocation requests against an `Allocator`,
//!   and per grant watches a liveness pipe so a slab block is freed exactly once,
//!   when the last holder of its token closes.
//! - [`ShmClient`] — a tiny `Copy` pair of raw fds (the dgram request socket and
//!   the slab object). Per actor; not the authority, just a handle.
//! - [`ShmMapper`] — the context-global address-space manager. It reserves a huge
//!   `PROT_NONE` range per slab once and grows a `MAP_FIXED` mapping into it so
//!   pointers never move, and unmaps everything when the context is destroyed.
//!
//! The bottom of the file holds the libc mechanisms (`memfd_create`, `ftruncate`,
//! `mmap` reserve/grow, `pipe2`, `socketpair`, and the `SCM_RIGHTS`
//! `sendmsg`/`recvmsg` pair), each wrapped in a checked helper that turns the
//! syscall's `-1`/`EAGAIN` into a `Result`/[`io::ErrorKind::WouldBlock`].

// The shared-memory machinery is wired into the transport across later stages, so
// in a non-test library build these APIs are momentarily unused; the unit tests
// exercise them directly. (Removed once non-test callers wire them in.)
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "wired into the unix transport in a later stage of the shared-memory plan"
    )
)]

use std::collections::HashMap;
use std::ffi::CStr;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::BorrowedFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Virtual address space reserved (PROT_NONE) per slab object, once. The slab
/// file's `MAP_FIXED` mapping grows into the front of this reservation so the
/// base never moves as the file grows. 128 GiB is far more than any slab will
/// reach but costs nothing until touched.
const RESERVE: usize = 1 << 37; // 128 GiB
/// Initial slab file size, and the granularity we grow it by. Both a power of two
/// so growth is a handful of `ftruncate`s even for large slabs.
const INITIAL: u64 = 1 << 20; // 1 MiB
const GROW: u64 = 1 << 20; // 1 MiB
/// Every allocation is rounded up to this alignment, which also keys the
/// per-size freelist (so a freed block can only satisfy a same-size request).
const ALIGN: u64 = 64;

/// Parts at least this large are moved through the slab (one memcpy + a tiny
/// descriptor) instead of streamed inline over the socket; smaller parts stay
/// inline, where the per-message fd/liveness overhead would not pay off.
pub(crate) const SHM_THRESHOLD: u64 = 256 * 1024;

/// A per-actor slot holding its gateway's [`ShmClient`] once learned. Lives on
/// the actor and is shared (cloned) with that actor's transport coroutines, so
/// shared memory turns on as soon as the gateway state arrives.
pub(crate) type ShmClientSlot = Arc<Mutex<Option<ShmClient>>>;

/// The slab object's name. `memfd_create` names are purely informational (they
/// appear in `/proc/<pid>/fd` link targets as `memfd:<name>`); nothing collides.
const SLAB_NAME: &CStr = c"monarch_mini_shm";

/// Round `value` up to the next multiple of `align` (a power of two).
fn align_up(value: u64, align: u64) -> u64 {
    (value + align - 1) & !(align - 1)
}

/// Checked end of a range that must fit inside one mapper reservation.
fn checked_range_end(offset: u64, len: usize) -> io::Result<u64> {
    let len = u64::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "shared-memory length overflow"))?;
    offset
        .checked_add(len)
        .filter(|&end| end <= RESERVE as u64)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "shared-memory range exceeds reservation",
            )
        })
}

/// Create an anonymous, *unnamed* in-memory file. Nothing leaks in `/dev/shm`,
/// and the kernel reclaims it once the last fd (this one plus any distributed
/// dups) closes — so no path leaks on any crash.
fn create_slab() -> io::Result<OwnedFd> {
    // SAFETY: `SLAB_NAME` is a valid NUL-terminated C string that outlives the
    // call. `memfd_create` either returns a fresh owned fd or -1; on success we
    // take sole ownership of that fd via `OwnedFd::from_raw_fd`.
    let fd = unsafe { libc::memfd_create(SLAB_NAME.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is a fresh, owned, valid file descriptor returned by
    // `memfd_create` above and not owned by anything else.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Set the slab file's size to `size` bytes (only ever called to grow it).
fn set_slab_size(fd: RawFd, size: u64) -> io::Result<()> {
    // SAFETY: `fd` is the live slab memfd owned by the `Allocator`; `ftruncate`
    // only changes the file's length and has no other effect on the process.
    let r = unsafe { libc::ftruncate(fd, size as libc::off_t) };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Bump + per-size-freelist allocator over a single slab `memfd`. Owned by the
/// allocation-authority side (the future `ShmServer`). It only ever creates and
/// grows the file and hands out offsets; it never maps anything.
struct Allocator {
    /// The slab memfd. `Arc` so the same object can be shared (e.g. handed to a
    /// mapper) without re-opening; the allocator only needs it to `ftruncate`.
    slab_fd: Arc<OwnedFd>,
    /// Current on-disk size of the slab file. `top` never exceeds this.
    file_size: u64,
    /// Bump pointer: the next never-yet-allocated offset.
    top: u64,
    /// Freed blocks, keyed by their aligned size. A request reuses a same-size
    /// freed block before bumping, so steady same-size traffic does not grow the
    /// file unboundedly.
    free: HashMap<u64, Vec<u64>>,
}

impl Allocator {
    /// Create a fresh slab, grown to [`INITIAL`] so the first allocations need no
    /// `ftruncate`.
    fn new() -> io::Result<Self> {
        let slab_fd = create_slab()?;
        set_slab_size(slab_fd.as_raw_fd(), INITIAL)?;
        Ok(Self {
            slab_fd: Arc::new(slab_fd),
            file_size: INITIAL,
            top: 0,
            free: HashMap::new(),
        })
    }

    /// The slab object, for whoever maps or forwards it.
    fn slab_fd(&self) -> &Arc<OwnedFd> {
        &self.slab_fd
    }

    /// Allocate `len` bytes, returning the slab offset. Reuses a freed same-size
    /// block if one exists; otherwise bumps and grows the file (rounded up to
    /// [`GROW`]) so the returned range is backed before the caller maps it. No
    /// `mmap` happens here.
    fn alloc(&mut self, len: u64) -> io::Result<u64> {
        let size = align_up(len.max(1), ALIGN);

        if let Some(offset) = self.free.get_mut(&size).and_then(Vec::pop) {
            return Ok(offset);
        }

        let offset = self.top;
        let new_top = offset + size;
        if new_top > self.file_size {
            let new_size = align_up(new_top, GROW);
            set_slab_size(self.slab_fd.as_raw_fd(), new_size)?;
            self.file_size = new_size;
        }
        self.top = new_top;
        Ok(offset)
    }

    /// Return a previously-allocated block to its size's freelist. `len` must be
    /// the same length passed to the [`alloc`](Self::alloc) that produced
    /// `offset` (it is re-aligned identically, so the freelist key matches).
    fn free(&mut self, offset: u64, len: u64) {
        let size = align_up(len.max(1), ALIGN);
        self.free.entry(size).or_default().push(offset);
    }
}

// ---------------------------------------------------------------------------
// ShmClient — a tiny, Copy handle to a gateway's slab (per actor)
// ---------------------------------------------------------------------------

/// A non-owning pair of raw fds: where to send allocation requests (the dgram
/// socket to the [`ShmServer`]) and which slab to map/forward. Deliberately
/// `Copy` and lifetime-free — the owning `ShmServer` (or, on a child, the process
/// itself) keeps the fds alive. An actor holds one once it learns its gateway.
#[derive(Clone, Copy)]
pub(crate) struct ShmClient {
    /// Dgram socket to the `ShmServer` for `Alloc` requests. Set non-blocking by
    /// its owner; shared by every actor under the same gateway (many writers, one
    /// reader), so a reply cannot be addressed over it — see [`ShmClient::allocate`].
    dgram_fd: RawFd,
    /// The slab object, for the mapper and for forwarding to children.
    slab_fd: RawFd,
}

impl ShmClient {
    /// Reconstruct a client from fds learned via gateway-state propagation (the
    /// dgram request socket and the slab object). The fds are non-owning and must
    /// stay open for the process lifetime (the caller leaks the received owned fds
    /// into raw fds before calling this).
    pub(crate) fn from_raw(dgram_fd: RawFd, slab_fd: RawFd) -> Self {
        Self { dgram_fd, slab_fd }
    }

    /// The slab object fd (for mapping or forwarding).
    pub(crate) fn slab_fd(&self) -> RawFd {
        self.slab_fd
    }

    /// The dgram request socket fd (for forwarding via gateway-state propagation).
    pub(crate) fn dgram_fd(&self) -> RawFd {
        self.dgram_fd
    }

    /// Request `len` bytes from the server, returning `(offset, token)`. Because
    /// the dgram socket is shared (one reader, many writers) the grant cannot ride
    /// back on it; instead we mint a private pipe, hand the server its `write_end`
    /// alongside the request, and read the granted offset back from our `read_end`.
    /// That `read_end` is then the **liveness token**: as long as any copy of it is
    /// open the server holds the block; when the last copy closes (delivered and
    /// consumed, or a holder died) the server frees it.
    pub(crate) async fn allocate(&self, len: u64) -> io::Result<(u64, OwnedFd)> {
        let (read_end, write_end) = make_pipe()?;
        set_nonblocking(read_end.as_raw_fd())?;

        // Send `Alloc{len}` + the pipe's write end; then drop our write end so the
        // server holds the only copy (its hangup watch keys off that).
        let len_bytes = len.to_le_bytes();
        send_dgram_with_fd(self.dgram_fd, &len_bytes, write_end.as_raw_fd()).await?;
        drop(write_end);

        let offset = read_grant(read_end.as_raw_fd()).await?;
        Ok((offset, read_end))
    }
}

// ---------------------------------------------------------------------------
// ShmServer — the allocation authority (one per gateway actor)
// ---------------------------------------------------------------------------

/// Owns the slab `memfd` and the `Allocator`, and runs a dgram server that
/// answers allocation requests and frees each grant when its liveness pipe hangs
/// up. Dropping it aborts the server task and releases the slab.
pub(crate) struct ShmServer {
    /// The server loop; aborted on drop, which drops the `Allocator` and the
    /// server's end of the dgram socket, releasing the slab once all distributed
    /// fds are gone.
    server_task: JoinHandle<()>,
    /// Clonable handle to this gateway's slab, handed to actors / children.
    client: ShmClient,
    /// Number of grants freed (a grant's liveness pipe hung up). Observability for
    /// tests and diagnostics; never affects behavior.
    freed: Arc<std::sync::atomic::AtomicU64>,
    /// Owned client end of the dgram socket. Kept alive here so the raw `dgram_fd`
    /// inside `client` stays valid for the server's lifetime.
    _client_end: OwnedFd,
    /// Owned slab fd. Kept alive here so the raw `slab_fd` inside `client` stays
    /// valid (the allocator inside the task holds another `Arc` clone).
    _slab_fd: Arc<OwnedFd>,
}

impl Drop for ShmServer {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

impl ShmServer {
    /// Build a slab + dgram server and spawn its loop. Must be called on the
    /// context's tokio runtime (it uses `spawn_local` and the io driver).
    pub(crate) fn new() -> io::Result<Self> {
        let allocator = Allocator::new()?;
        let slab_fd = Arc::clone(allocator.slab_fd());

        let (server_end, client_end) = make_dgram_pair()?;
        set_nonblocking(server_end.as_raw_fd())?;
        set_nonblocking(client_end.as_raw_fd())?;

        let client = ShmClient {
            dgram_fd: client_end.as_raw_fd(),
            slab_fd: slab_fd.as_raw_fd(),
        };
        let freed = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let server_task =
            tokio::task::spawn_local(server_loop(server_end, allocator, Arc::clone(&freed)));

        Ok(Self {
            server_task,
            client,
            freed,
            _client_end: client_end,
            _slab_fd: slab_fd,
        })
    }

    /// A `Copy` handle to this gateway's slab.
    pub(crate) fn client(&self) -> ShmClient {
        self.client
    }

    /// Number of grants freed so far (observability).
    fn freed_count(&self) -> u64 {
        self.freed.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// The server loop: read allocation requests off `server_end`, allocate, write the
/// granted offset back down the request's pipe, and spawn a per-grant watcher that
/// frees the block when the pipe hangs up. Frees are funneled back here over an
/// mpsc so the single `Allocator` is only touched from this task.
async fn server_loop(
    server_end: OwnedFd,
    mut allocator: Allocator,
    freed: Arc<std::sync::atomic::AtomicU64>,
) {
    let server_fd = server_end.as_raw_fd();
    let afd = match AsyncFd::with_interest(server_end, Interest::READABLE) {
        Ok(afd) => afd,
        Err(err) => {
            tracing::error!("shm server: registering dgram socket failed: {err}");
            return;
        }
    };
    let (free_tx, mut free_rx) = mpsc::unbounded_channel::<(u64, u64)>();

    loop {
        tokio::select! {
            request = recv_alloc_request(&afd, server_fd) => {
                let Ok((len, write_end)) = request else {
                    return; // socket error: the gateway is going away
                };
                let offset = match allocator.alloc(len) {
                    Ok(offset) => offset,
                    Err(err) => {
                        tracing::error!("shm server: alloc({len}) failed: {err}");
                        // Drop write_end: the client's read_grant sees EOF and errors.
                        continue;
                    }
                };
                // Grant the offset down the private pipe (8 bytes into a fresh pipe
                // never blocks), then watch the write end for the all-tokens-closed
                // hangup that frees the block.
                if write_all(write_end.as_raw_fd(), &offset.to_le_bytes()).is_err() {
                    allocator.free(offset, len);
                    continue;
                }
                tokio::task::spawn_local(watch_grant(write_end, offset, len, free_tx.clone()));
            }
            freed_block = free_rx.recv() => {
                // free_tx is held here too, so recv never returns None.
                let Some((offset, len)) = freed_block else { return };
                allocator.free(offset, len);
                freed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

/// Watch a grant's pipe `write_end` for hangup — i.e. every copy of the matching
/// `read_end` token closing — then report the block free. A write end is always
/// writable, so plain writability never fires; we wait for the edge-triggered
/// readiness whose `is_write_closed()` flags the closed read ends
/// (`EPOLLERR`/`EPOLLHUP`), ignoring the initial writable wake.
async fn watch_grant(
    write_end: OwnedFd,
    offset: u64,
    len: u64,
    free_tx: mpsc::UnboundedSender<(u64, u64)>,
) {
    if set_nonblocking(write_end.as_raw_fd()).is_err() {
        return;
    }
    let afd = match AsyncFd::with_interest(write_end, Interest::WRITABLE) {
        Ok(afd) => afd,
        Err(_) => return,
    };
    loop {
        let mut guard = match afd.ready(Interest::WRITABLE).await {
            Ok(guard) => guard,
            Err(_) => return,
        };
        if guard.ready().is_write_closed() {
            let _ = free_tx.send((offset, len));
            return;
        }
        // The expected initial "writable" edge; clear it and park for the next
        // edge, which (mio is edge-triggered) only comes on hangup.
        guard.clear_ready();
    }
}

/// Await one allocation request off the server dgram socket: `[u64 len]` + one fd
/// (the grant pipe's write end). Loops past `WouldBlock`; returns `Err` on a real
/// socket error or a malformed request.
async fn recv_alloc_request(
    afd: &AsyncFd<OwnedFd>,
    server_fd: RawFd,
) -> io::Result<(u64, OwnedFd)> {
    loop {
        let mut guard = afd.readable().await?;
        let mut buf = [0u8; 8];
        // An allocation request carries exactly one fd: the grant pipe's write end.
        match guard.try_io(|_| recv_with_fds(server_fd, &mut buf, 1)) {
            Ok(Ok((n, mut fds))) => {
                if n != 8 || fds.len() != 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "malformed shm alloc request",
                    ));
                }
                let len = u64::from_le_bytes(buf);
                let write_end = fds.pop().expect("exactly one fd checked above");
                return Ok((len, write_end));
            }
            Ok(Err(err)) => return Err(err),
            Err(_would_block) => continue,
        }
    }
}

/// Send `Alloc` request bytes + the grant pipe's write end over the (non-blocking)
/// dgram socket, awaiting writability on `WouldBlock`. The socket is shared by
/// many writers, so a transient borrowed `AsyncFd` is used only on backpressure.
async fn send_dgram_with_fd(dgram_fd: RawFd, data: &[u8], fd: RawFd) -> io::Result<()> {
    loop {
        match send_with_fds(dgram_fd, data, &[fd]) {
            Ok(_) => return Ok(()),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                // SAFETY: `dgram_fd` is a live socket owned elsewhere for the whole
                // call; the transient `BorrowedFd`/`AsyncFd` only watch readiness
                // and never close it.
                let borrowed = unsafe { BorrowedFd::borrow_raw(dgram_fd) };
                let afd = AsyncFd::with_interest(borrowed, Interest::WRITABLE)?;
                let _ = afd.writable().await?;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Read the 8-byte granted offset back from the request's private (non-blocking)
/// pipe `read_fd`, awaiting readability as needed. EOF before 8 bytes means the
/// server dropped the write end without granting (an allocation error).
async fn read_grant(read_fd: RawFd) -> io::Result<u64> {
    // SAFETY: `read_fd` is the live read end owned by the caller for the whole
    // call; the `AsyncFd` watches readiness over a borrow and never closes it.
    let borrowed = unsafe { BorrowedFd::borrow_raw(read_fd) };
    let afd = AsyncFd::with_interest(borrowed, Interest::READABLE)?;
    let mut buf = [0u8; 8];
    let mut filled = 0;
    while filled < buf.len() {
        let mut guard = afd.readable().await?;
        match guard.try_io(|_| read_fd_bytes(read_fd, &mut buf[filled..])) {
            Ok(Ok(0)) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "shm grant pipe closed before reply",
                ));
            }
            Ok(Ok(n)) => filled += n,
            Ok(Err(err)) => return Err(err),
            Err(_would_block) => continue,
        }
    }
    Ok(u64::from_le_bytes(buf))
}

// ---------------------------------------------------------------------------
// ShmMapper — context-global address-space manager
// ---------------------------------------------------------------------------

/// Shared handle to the per-context [`ShmMapper`].
pub(crate) type MapperHandle = Arc<Mutex<ShmMapper>>;

/// One slab's persistent mapping: a large `PROT_NONE` reservation whose front is
/// progressively backed by the slab file via `MAP_FIXED`, so `base` never moves
/// as the file grows.
struct Reservation {
    /// Owned dup of the slab fd, kept so the mapping can keep growing even after
    /// the client's fd closes. (Existing mappings survive the fd closing; only
    /// growth needs a live fd.)
    _fd: OwnedFd,
    /// Start of the reserved address range.
    base: *mut u8,
    /// How many bytes from `base` are currently backed by the file.
    mapped: u64,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        // SAFETY: `base` came from a `mmap` of `RESERVE` bytes in `ShmMapper::map`
        // and is owned solely by this `Reservation`; unmapping it exactly once on
        // drop is sound. Any pointers handed out from it must not outlive the
        // context (and thus the mapper), which is the mapper's contract.
        unsafe {
            libc::munmap(self.base.cast(), RESERVE);
        }
    }
}

/// Maps slab ranges into this context's address space and keeps them mapped until
/// the context is destroyed. One reservation per distinct slab (keyed by inode),
/// so clones of the same `ShmClient` share a single mapping.
pub(crate) struct ShmMapper {
    reservations: HashMap<u64, Reservation>,
}

impl ShmMapper {
    pub(crate) fn new() -> Self {
        Self {
            reservations: HashMap::new(),
        }
    }

    /// Ensure `[offset, offset+len)` of the slab `slab_fd` is mapped and return a
    /// pointer to `base + offset`. Reserves the slab's address range on first use
    /// and grows the backed extent in place as needed; the base never moves.
    ///
    /// # Safety
    /// `slab_fd` must be a valid fd referring to the slab whose allocator produced
    /// `offset`, and `[offset, offset+len)` must lie within a region the allocator
    /// has grown the file to cover. The returned pointer is valid only while this
    /// mapper (i.e. the context) lives, and only `len` bytes are accessible.
    pub(crate) unsafe fn map(
        &mut self,
        slab_fd: RawFd,
        offset: u64,
        len: usize,
    ) -> io::Result<*mut u8> {
        let need = checked_range_end(offset, len)?;
        let (inode, _size) = fstat_ino_size(slab_fd)?;

        let reservation = match self.reservations.entry(inode) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let base = mmap_reserve(RESERVE)?;
                let fd = dup_fd(slab_fd)?;
                e.insert(Reservation {
                    _fd: fd,
                    base,
                    mapped: 0,
                })
            }
        };

        if need > reservation.mapped {
            let target = align_up(need, page_size());
            // SAFETY: `base` is the start of a `RESERVE`-byte reservation; `target`
            // cannot exceed `RESERVE` because `checked_range_end` bounded `need` and
            // `RESERVE` is page-aligned. `target` and `mapped` are page-aligned, so the
            // `MAP_FIXED` overlay lands inside the reservation at a page boundary backed
            // by the slab file (which the allocator has grown to cover `need`). `_fd` is
            // a live slab fd.
            unsafe {
                mmap_fixed(
                    reservation.base,
                    reservation.mapped,
                    target - reservation.mapped,
                    reservation._fd.as_raw_fd(),
                )?;
            }
            reservation.mapped = target;
        }

        // SAFETY: `base` is a valid mapping of at least `reservation.mapped >=
        // offset + len` bytes (ensured just above), so `base + offset` points
        // within it.
        Ok(unsafe { reservation.base.add(offset as usize) })
    }
}

// SAFETY: a `ShmMapper`'s only non-`Send` field is the raw `base` pointer in each
// `Reservation`. The mapper lives behind `Arc<Mutex<_>>` and is only ever
// dereferenced on the single-threaded context runtime; the `Mutex` serializes the
// (rare) cross-thread access from the handle. The mapping is process-global, so
// the pointer is meaningful from any thread.
unsafe impl Send for ShmMapper {}

// ---------------------------------------------------------------------------
// libc mechanisms (each wrapped to return io::Result; -1/EAGAIN -> Err)
// ---------------------------------------------------------------------------

/// `O_NONBLOCK` the given fd (preserving its other flags).
fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is a live fd owned by the caller; fcntl F_GETFL/F_SETFL only
    // read and write its status flags.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    let r = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// `dup` an fd into an owned handle.
fn dup_fd(fd: RawFd) -> io::Result<OwnedFd> {
    // SAFETY: `fd` is live; `dup` returns a fresh owned fd or -1.
    let dup = unsafe { libc::dup(fd) };
    if dup < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `dup` is a fresh, owned, valid fd not owned by anything else.
    Ok(unsafe { OwnedFd::from_raw_fd(dup) })
}

/// `fstat` for the inode (mapping dedup key) and current size.
fn fstat_ino_size(fd: RawFd) -> io::Result<(u64, u64)> {
    // SAFETY: `st` is fully written by a successful fstat; `fd` is a live fd.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    let r = unsafe { libc::fstat(fd, &mut st) };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((st.st_ino as u64, st.st_size as u64))
}

/// System page size, for aligning `MAP_FIXED` growth.
fn page_size() -> u64 {
    // SAFETY: sysconf with a valid name has no preconditions and no side effects.
    let v = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if v > 0 { v as u64 } else { 4096 }
}

/// Create a `(read_end, write_end)` pipe with `O_CLOEXEC`.
fn make_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0 as RawFd; 2];
    // SAFETY: `fds` is a valid 2-element array; pipe2 fills it on success.
    let r = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both entries are fresh, owned, valid fds from a successful pipe2.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

/// Create a `(server_end, client_end)` `AF_UNIX`/`SOCK_DGRAM` socket pair.
fn make_dgram_pair() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0 as RawFd; 2];
    // SAFETY: `fds` is a valid 2-element array; socketpair fills it on success.
    let r = unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_DGRAM | libc::SOCK_CLOEXEC,
            0,
            fds.as_mut_ptr(),
        )
    };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both entries are fresh, owned, valid fds from a successful socketpair.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

/// Write all of `data` to `fd` (used only for the 8-byte grant into a fresh pipe,
/// which never partials/blocks, but loop on short writes for correctness).
fn write_all(fd: RawFd, data: &[u8]) -> io::Result<()> {
    let mut written = 0;
    while written < data.len() {
        // SAFETY: `fd` is a live writable fd; we pass a valid pointer/length into
        // the remaining slice and only read `len` bytes from it.
        let n = unsafe { libc::write(fd, data[written..].as_ptr().cast(), data.len() - written) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        written += n as usize;
    }
    Ok(())
}

/// One non-blocking `read` into `buf`, mapping `EAGAIN` to `WouldBlock`.
fn read_fd_bytes(fd: RawFd, buf: &mut [u8]) -> io::Result<usize> {
    // SAFETY: `fd` is a live readable fd; `buf` is a valid mutable slice that read
    // fills up to its length.
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(n as usize)
}

/// Reserve `len` bytes of address space (`PROT_NONE`, anonymous, no commit).
fn mmap_reserve(len: usize) -> io::Result<*mut u8> {
    // SAFETY: a fresh anonymous PROT_NONE reservation has no preconditions; we
    // check for MAP_FAILED before returning the pointer.
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_NONE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        )
    };
    if p == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(p.cast())
}

/// Overlay `[at, at+len)` of the reservation at `base` with the slab file's bytes
/// at file offset `at` (read/write, shared). `base+at` and `at` must be page
/// aligned and the range must lie within the reservation.
///
/// # Safety
/// `base` must be the start of a live `RESERVE`-byte reservation, `at + len <=
/// RESERVE`, both `at` and `base` page-aligned, and `fd` a live slab fd whose file
/// is at least `at + len` bytes.
unsafe fn mmap_fixed(base: *mut u8, at: u64, len: u64, fd: RawFd) -> io::Result<()> {
    // SAFETY: per this fn's contract `base.add(at)` is page-aligned and inside the
    // reservation, so MAP_FIXED overlays a valid sub-range; the file is backed to
    // `at + len`. We check MAP_FAILED before returning.
    let p = unsafe {
        libc::mmap(
            base.add(at as usize).cast(),
            len as usize,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_FIXED,
            fd,
            at as libc::off_t,
        )
    };
    if p == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// `sendmsg` `data` plus `fds` (via one `SCM_RIGHTS` control message) on `sock`.
/// `MSG_NOSIGNAL` so a dead peer yields `EPIPE` rather than a signal.
pub(crate) fn send_with_fds(sock: RawFd, data: &[u8], fds: &[RawFd]) -> io::Result<usize> {
    let fd_bytes = std::mem::size_of_val(fds);
    // SAFETY: the whole block builds a single msghdr with one iovec over `data` and
    // one SCM_RIGHTS cmsg holding `fds`, exactly as the cmsg macros require, then
    // calls sendmsg. `data`/`fds` outlive the call; the control buffer is sized via
    // CMSG_SPACE and zeroed; we copy the fds into CMSG_DATA bytewise (no alignment
    // assumption). The result is checked before use.
    unsafe {
        let mut iov = libc::iovec {
            iov_base: data.as_ptr() as *mut libc::c_void,
            iov_len: data.len(),
        };
        let control_len = libc::CMSG_SPACE(fd_bytes as libc::c_uint) as usize;
        let mut control = vec![0u8; control_len];

        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = control_len as _;

        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(fd_bytes as libc::c_uint) as _;
        std::ptr::copy_nonoverlapping(fds.as_ptr().cast::<u8>(), libc::CMSG_DATA(cmsg), fd_bytes);

        let n = libc::sendmsg(sock, &msg, libc::MSG_NOSIGNAL);
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(n as usize)
    }
}

/// `recvmsg` into `data`, collecting any `SCM_RIGHTS` fds as fresh owned fds
/// (`MSG_CMSG_CLOEXEC`). Non-blocking (`MSG_DONTWAIT`); `EAGAIN` -> `WouldBlock`.
/// Returns `(bytes_read, fds)`; `0` bytes means EOF. `max_fds` sizes the control
/// buffer: the caller knows from the frame header exactly how many fds the sender
/// attached, so there is no fixed cap — too small a buffer would truncate the
/// ancillary data (`MSG_CTRUNC`) and the caller's count check would catch it.
pub(crate) fn recv_with_fds(
    sock: RawFd,
    data: &mut [u8],
    max_fds: usize,
) -> io::Result<(usize, Vec<OwnedFd>)> {
    let fd_size = std::mem::size_of::<RawFd>();
    // SAFETY: the block builds a msghdr with one iovec over `data` and a control
    // buffer sized for `max_fds` cmsg fds, calls recvmsg, then walks the returned
    // cmsgs copying out each SCM_RIGHTS fd (installed by the kernel as a fresh fd we
    // take ownership of). All pointers come from the just-filled msghdr.
    unsafe {
        let mut iov = libc::iovec {
            iov_base: data.as_mut_ptr().cast(),
            iov_len: data.len(),
        };
        let control_len = libc::CMSG_SPACE((max_fds * fd_size) as libc::c_uint) as usize;
        let mut control = vec![0u8; control_len];

        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = control_len as _;

        let n = libc::recvmsg(sock, &mut msg, libc::MSG_CMSG_CLOEXEC | libc::MSG_DONTWAIT);
        if n < 0 {
            return Err(io::Error::last_os_error());
        }

        let mut fds = Vec::new();
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let payload = (*cmsg).cmsg_len as usize - libc::CMSG_LEN(0) as usize;
                let count = payload / fd_size;
                let src = libc::CMSG_DATA(cmsg);
                for i in 0..count {
                    let mut raw: RawFd = 0;
                    std::ptr::copy_nonoverlapping(
                        src.add(i * fd_size),
                        (&mut raw as *mut RawFd).cast::<u8>(),
                        fd_size,
                    );
                    fds.push(OwnedFd::from_raw_fd(raw));
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
        Ok((n as usize, fds))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapper_range_must_fit_reservation() {
        assert_eq!(
            checked_range_end(RESERVE as u64 - 1, 1).expect("last byte should fit"),
            RESERVE as u64
        );
        assert_eq!(
            checked_range_end(RESERVE as u64, 1)
                .expect_err("range past reservation should fail")
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            checked_range_end(u64::MAX, 1)
                .expect_err("overflowing range should fail")
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    /// Bumping past the initial size grows the file; a smaller, then larger
    /// request walk the bump pointer with correct alignment.
    #[test]
    fn alloc_bumps_and_grows() {
        let mut alloc = Allocator::new().expect("create slab");
        assert_eq!(alloc.file_size, INITIAL);

        // First two allocations are sub-ALIGN, so each consumes exactly ALIGN and
        // they sit back to back at 0 and ALIGN.
        assert_eq!(alloc.alloc(10).expect("alloc a"), 0);
        assert_eq!(alloc.alloc(1).expect("alloc b"), ALIGN);

        // A request larger than the current file forces a grow. After the two
        // 64-byte allocations the bump pointer is at 2*ALIGN = 128.
        let big = INITIAL + 5;
        let offset = alloc.alloc(big).expect("alloc big");
        assert_eq!(offset, 2 * ALIGN);
        // The file grew to cover offset + aligned(big), rounded up to GROW.
        let expected = align_up(2 * ALIGN + align_up(big, ALIGN), GROW);
        assert_eq!(alloc.file_size, expected);
        assert!(alloc.file_size >= offset + big);
    }

    /// Freeing a block lets the next same-size request reuse its offset instead of
    /// bumping; a different size still bumps.
    #[test]
    fn free_same_size_is_reused() {
        let mut alloc = Allocator::new().expect("create slab");

        let a = alloc.alloc(100).expect("alloc a"); // size = aligned(100) = 128
        let b = alloc.alloc(100).expect("alloc b");
        assert_ne!(a, b);

        alloc.free(a, 100);
        // Same size: reuses the just-freed offset rather than advancing the bump.
        let reused = alloc.alloc(120).expect("alloc reused"); // aligned(120) == 128
        assert_eq!(
            reused, a,
            "a same-size request should reuse the freed block"
        );

        // A different size class has no free block, so it bumps fresh (past b).
        let other = alloc.alloc(500).expect("alloc other");
        assert!(other >= b + align_up(100, ALIGN));
    }

    /// The freelist is keyed by aligned size, so freeing one size never satisfies
    /// a request for another.
    #[test]
    fn free_is_size_classed() {
        let mut alloc = Allocator::new().expect("create slab");
        let small = alloc.alloc(10).expect("alloc small");
        alloc.free(small, 10);

        // A request in a larger size class must not reuse the small freed block.
        let large = alloc.alloc(1000).expect("alloc large");
        assert_ne!(large, small);
    }

    /// The slab fd is shareable and stays valid for growth across clones.
    #[test]
    fn slab_fd_is_shared() {
        let mut alloc = Allocator::new().expect("create slab");
        let shared = Arc::clone(alloc.slab_fd());
        // Force a grow; the shared handle still refers to the same (now larger)
        // file.
        alloc.alloc(INITIAL + 1).expect("alloc forcing grow");
        assert!(shared.as_raw_fd() >= 0);
    }

    /// Run an async body on a fresh current-thread runtime + LocalSet, matching how
    /// the shm tasks run inside the context (spawn_local, io driver).
    fn run_local<F: std::future::Future>(future: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        tokio::task::LocalSet::new().block_on(&rt, future)
    }

    /// The mapper maps a slab range writable, keeps the base fixed across repeated
    /// maps, and the bytes written through it persist (it is the same file).
    #[test]
    fn mapper_maps_slab_writable_and_keeps_base_fixed() {
        let mut alloc = Allocator::new().expect("create slab");
        let len = 4096usize;
        let offset = alloc.alloc(len as u64).expect("alloc");
        let slab = alloc.slab_fd().as_raw_fd();

        let mut mapper = ShmMapper::new();
        // SAFETY: `slab` is the live slab fd and `[offset, offset+len)` was just
        // allocated, so the file is grown to cover it.
        let ptr = unsafe { mapper.map(slab, offset, len).expect("map") };
        // SAFETY: `ptr` is a writable mapping of `len` bytes for the mapper's life.
        unsafe { std::slice::from_raw_parts_mut(ptr, len).fill(0xAB) };

        // Mapping the same range again returns the same pointer (base never moves)
        // and observes the bytes written above (same underlying file).
        // SAFETY: same justification as the first map.
        let ptr2 = unsafe { mapper.map(slab, offset, len).expect("remap") };
        assert_eq!(ptr2, ptr, "the reservation base must not move");
        // SAFETY: `ptr2` maps the same `len` bytes.
        let seen = unsafe { std::slice::from_raw_parts(ptr2, len) };
        assert!(
            seen.iter().all(|&b| b == 0xAB),
            "written bytes should persist"
        );
    }

    /// Full request protocol: allocate a grant, write/read it through the mapper,
    /// then drop the token so the server frees the block — after which a same-size
    /// allocation reuses the freed offset.
    #[test]
    fn allocate_grants_slab_then_frees_and_reuses_on_token_drop() {
        run_local(async {
            let server = ShmServer::new().expect("server");
            let client = server.client();
            let mapper: MapperHandle = Arc::new(Mutex::new(ShmMapper::new()));

            let len = 5000u64;
            let (offset, token) = client.allocate(len).await.expect("allocate");

            // SAFETY: `offset` was just granted against `client`'s slab for `len`
            // bytes, so the file covers it; the pointer lives as long as `mapper`.
            let ptr = unsafe {
                mapper
                    .lock()
                    .expect("mapper lock")
                    .map(client.slab_fd(), offset, len as usize)
                    .expect("map grant")
            };
            // SAFETY: `ptr` is a writable mapping of `len` bytes.
            unsafe { std::slice::from_raw_parts_mut(ptr, len as usize).fill(0x5A) };
            // SAFETY: read back the same range.
            let seen = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
            assert!(seen.iter().all(|&b| b == 0x5A), "slab grant is writable");

            // Dropping the only token closes the read end; the server's hangup watch
            // then frees the block. Wait until that free is processed.
            drop(token);
            for _ in 0..500 {
                if server.freed_count() >= 1 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            assert_eq!(server.freed_count(), 1, "the block should be freed once");

            // A same-size request now reuses the just-freed offset.
            let (offset2, _token2) = client.allocate(len).await.expect("re-allocate");
            assert_eq!(offset2, offset, "a freed block should be reused");
        });
    }

    /// The hangup that frees a block fires only after the *last* copy of its token
    /// closes: an in-transit duplicate keeps the block alive.
    #[test]
    fn liveness_fires_only_after_last_token_closes() {
        run_local(async {
            let (read_end, write_end) = make_pipe().expect("pipe");
            let (free_tx, mut free_rx) = mpsc::unbounded_channel();
            tokio::task::spawn_local(watch_grant(write_end, 42, 100, free_tx));

            // A second holder of the token (as if relayed to an in-transit hop).
            let read_dup = dup_fd(read_end.as_raw_fd()).expect("dup token");

            // Closing one copy must NOT free the block.
            drop(read_end);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert!(
                free_rx.try_recv().is_err(),
                "free must not fire while a duplicate token is open"
            );

            // Closing the last copy frees it, reporting the original (offset, len).
            drop(read_dup);
            let freed = free_rx.recv().await.expect("free reported");
            assert_eq!(freed, (42, 100), "free reports the granted block");
        });
    }
}
