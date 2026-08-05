We are making a change to monarch_mini (/home/zdevito/feature_mini/fbcode/monarch/monarch_mini/IMPLEMENTATION.md).
# Shared-Memory Large-Message Transport — Plan v2

Goal: when an actor sends a **large message part** to an actor in another process on
the same machine, move that part's bytes through a shared-memory slab (one `memcpy`
into the slab + a tiny descriptor) instead of streaming the bytes across each unix
hop. Small parts stay inline. Death-safe via fd liveness.

This document is self-contained — the concrete syscalls and constants are in the
**Appendix** at the end. The design (slab + per-request-pipe liveness + two-hop
relay) has been prototyped and measured: a *persistent* per-context mapping is
mandatory (mapping/unmapping per message plateaus around 2.8 GB/s; a persistent
mapping approaches single-core `memcpy` bandwidth), and the per-message fd-handle
liveness adds only a fixed ~60 µs/msg that amortizes away for large payloads.

---

## Stages (after each stage, you will make a commit. DO NOT ATTEMPT TO LAND ANYTHING,  YOU ARE JUST MAKING THE STACK)

1. **Gateway is an explicit argument.** Add a `gateway: bool` to actor creation
   (`mm_actor_create` in `minimonarch.h` + `lib.rs`, `Command::CreateActor`,
   `ActorEntry`, the Python binding + `.pyi`, the C examples). Replace the
   auto-derived notion entirely: `ActorEntry.gateway` is set once at creation and
   never flips. Enforce at `join`: **a gateway must have no parent or a network
   (tcp/quic) parent** — reject joining a gateway to a unix/inproc parent. (The
   existing topology checks that read `gateway` are effectively dead today; either
   drop the `gateway` term from them or mark test roots as gateways — pick one and
   keep all existing tests passing.)

2. **Decouple `framing.rs` from the unix transport.**  The unix transport gets its **own** wire enums so
   message + shared-memory handling is inlined there; no shm/fd concepts leak into
   the generic framing which will be used for quic/tcp only. It is ok for these objects to have some duplicates, there is not many wire message types.

3. **Slab allocator (no mapping).** A bump + per-size-freelist allocator that
   **only** creates the slab file (`memfd_create`), grows it (`ftruncate`), and
   hands out `(offset)` allocations. It never `mmap`s — mapping is the `ShmMapper`'s
   job. The grant just needs the offset (the file has already been grown to cover
   it).

4. **The three objects** (below): `ShmServer`, `ShmClient`, `ShmMapper`.

5. **Per-part send/receive over unix.** Per *part*: a part `>= SHM_THRESHOLD`
   (~256 KiB) on a context that has a `ShmClient` → allocate, `memcpy` into the slab
   (via the mapper), send a descriptor `{offset, len}` + the liveness token fd;
   smaller parts stream inline. The receiver reconstructs the part as **unmapped
   metadata** `(token, offset, len)`; it is mapped only when handed to the user.
   Intermediates forward the descriptor (relay the token fd) with **no map, no
   copy**. (So a message of small headers + one big payload only shm-ifies the
   payload.)

6. **GatewayState propagation.** When a child connection establishes, the parent
   hands the child its gateway state (the `ShmClient`'s two fds). Propagates down
   **both unix and inproc** edges (an inproc child shares the process but must still
   forward to *its* unix children — `unix → inproc → unix`), never quic. Each actor
   that receives it sets its own `ShmClient` and re-propagates to its children. When a new
   child is actor is connected, and you have a ShmClient, you will also send this client down.
   This handles the case where the child goins after the ShmClient has already been added.

---

## The three objects (distinct owners / lifetimes)

### `ShmServer` — the allocation authority (owned by a gateway actor)

Runs the unix dgram socket that answers allocation requests, owns the slab
allocator and the slab `memfd`. An **optional** member of a gateway `ActorEntry`;
dropping it (when the gateway actor is destroyed) stops the server and releases the
slab. Spawns one task that loops over the dgram socket.

```rust
struct ShmServer {
    server_task: JoinHandle<()>,   // Drop aborts it
    client: ShmClient,             // hands out clones for this gateway's slab
}

struct Allocator {                 // owned by the server task
    slab_fd: Arc<OwnedFd>,         // the memfd; ftruncate to grow
    file_size: u64,
    top: u64,                      // bump pointer
    free: HashMap<u64, Vec<u64>>,  // per-(aligned)size freelist
}
// alloc(len) -> offset: reuse a freed same-size block, else bump + ftruncate. No mmap.
```

### `ShmClient` — a tiny, `Copy` handle (per connection that wants it)

Just two **raw, non-owning** fds: the **dgram fd** (where to send alloc requests) and
the **slab fd** (which slab to map / forward). Not the allocation authority and not
the address-space owner — a plain pair of ints. Each actor *optionally* holds one
(set once it learns its gateway). **Per actor, not per context: two actors in one
context may belong to different gateways.** Passed to the transports when they do a
read/write.

```rust
#[derive(Clone, Copy)]
struct ShmClient {
    dgram_fd: RawFd, // dgram to the ShmServer for alloc requests (NOT owned here)
    slab_fd: RawFd,  // slab object, for the mapper + for forwarding (NOT owned here)
}

```
> Ownership: the gateway side's fds are owned by its `ShmServer`. On a **child**, the
> fds arriving in `GatewayState` are deliberately **not tracked** — extract the
> `RawFd` and leave the fd open for the process lifetime (`into_raw_fd`). Closing them
> in a child would be harmless anyway (the server keeps its own copies, so the
> slab/socket stay alive), and a mapped region survives its fd closing — so this keeps
> `ShmClient` a trivially-`Copy` pair of ints with no lifetime tracking.
>
> Async sends on `dgram_fd`: set it non-blocking once and drive the (small, rare)
> request `sendmsg` under readiness via a transient `AsyncFd` over a `BorrowedFd`
> (or have whoever owns the `OwnedFd` expose one). The grant is read from the
> per-request pipe, which the `allocate` call owns.

### `ShmMapper` — context-global address-space manager

Takes a `(slab fd, range)` and returns a pointer, reserving once per slab object and
growing the mapping in place (so pointers never move). Created for **every** context
(passed to the transport traits at construction); sits idle if shm is unused. Keeps
its ranges mapped and `munmap`s them all when the context is destroyed. Keyed by
fd so registrations from the same ShmClient clones can share the same mapping. A single
context may have actors that have a different gateway, so it is possible to see more than one fd in the ShmMapper.
However, all actors sharing the same ShmClient are always going to have the same fd.

```rust
type MapperHandle = Arc<Mutex<ShmMapper>>;
struct ShmMapper { reservations: HashMap<u64 /*inode*/, Reservation> }
struct Reservation { fd: OwnedFd, base: *mut u8, mapped: u64 } // PROT_NONE reserve + MAP_FIXED grow
impl ShmMapper {
    // ensure [offset, offset+len) of slab `slab_fd` is mapped; return base+offset.
    unsafe fn map(&mut self, slab_fd: RawFd, offset: u64, len: usize) -> Result<*mut u8>;
}
```
> Takes the client's non-owning `RawFd`; We do not close client fds so it is fine to just keep the RawFd
 so it can keep growing the mapping. Only grow the reservations, the only time we unmap entirely is when the context is destroyed.

Wiring: `ShmMapper` is constructed in `Ctx::new` and passed to each transport at
construction (only unix uses it). The `ShmClient` is per-actor, stored as Arc<Mutex<Option<ShmClient>>>. This way we can pass this slot to the transport recv/send coroutines and enable ShmClient as soon as we get the information about it.
`ShmServer` lives on the gateway `ActorEntry`.

---

## Client → server request protocol (dgrams + fd tracking)

The dgram request socket is **shared** (the server's `client_end` is duped to every
child via GatewayState), so it is many-writers → one-reader and **cannot carry an
addressed reply**. The grant therefore comes back down a **private per-request
pipe**, and that same pipe doubles as the liveness token.

Per allocation (in `ShmClient::allocate(len)`):

1. `let (read_end, write_end) = pipe()`.
2. Send on the dgram socket: `Alloc{len}` (8 bytes LE) **+ `write_end`** via
   `SCM_RIGHTS`. Drop the local `write_end`.
3. Server task `recvmsg`s `{len, write_end}`, `alloc(len) -> offset`, writes
   `offset` (8 bytes) **into `write_end`**, and starts watching `write_end` for
   hangup. Reply is read back from `read_end`.
4. `read_end` is now the **liveness token**: it rides with the descriptor toward the
   destination (SCM_RIGHTS per unix hop; a plain move over inproc). Each holder/hop
   closes its copy when done.
5. **Free signal:** the server holds `write_end`; when *every* copy of `read_end` is
   closed (delivered-and-consumed, or any holder died → kernel closes its fds) the
   `write_end` reports hangup → `free(offset, len)`. No explicit free message.

> Liveness orientation matters: **server keeps the write end, the read end travels.**
> The grant must go down this private pipe (the dgram is shared and can't address a
> reply). Detecting the hangup on a write end in tokio: register
> `Interest::WRITABLE`, ignore the initial "writable" wake, return on
> `is_write_closed()` (a write end is always writable, so `readable()`/`writable()`
> alone won't do it; closed read ends surface as `EPOLLERR`/`EPOLLHUP` = write-closed).

Request message format (fixed): `[u64 len]` + 1 fd (write_end). Grant: `[u64 offset]`
down the pipe.

---

## Unix wire format (its own enums; only touch fd syscalls when fds are present)

A frame is `[u64 header_len][header][maybe fd-exchange][inline part bytes]`. The
header is a bincode `UnixFrame`; **the header and inline bytes are read/written with
plain `try_read`/`try_write`.** Only if the header says fds are present do we do one
extra `sendmsg`/`recvmsg` step (a 1-byte payload carrying all the fds). No-fd frames
never touch the fd-passing syscalls.

```rust
enum UnixFrame {
    Establish { .. }, PublishRoutes { .. }, ToAncestor { .. }, Severed { .. },
    Message { destination_ident: Vec<u8>, payload: WirePayload },
    GatewayState,                       // carries 2 fds (slab, dgram)
}
enum WirePayload { ActorMessage { parts: Vec<PartDesc> }, FireMonitor { .. } }
enum PartDesc { Inline { len: u64 }, Shm { offset: u64, len: u64 } } // Shm => 1 fd, in part order
```

Sender: write `[len][header]` (plain); if the header implies fds (count the `Shm`
parts, or `GatewayState`), do one `sendmsg(1 byte, [fds...])`; then write inline part
bytes (plain). Receiver: read `[len][header]` (plain); if the decoded header implies
N fds, `recvmsg` once to collect them; then read inline bytes (plain), popping one fd
per `Shm` part in order.

`MsgPart` becomes an enum: `Owned{..}` (today's bytes+deleter) or
`Shm{ mapper: MapperHandle, slab_fd: Arc<OwnedFd>, token: OwnedFd, offset, len }`.
A `Shm` part is unmapped metadata in flight; `as_bytes`/`into_c` map it (source/dest)
via `mapper.map(slab_fd, offset, len)`; `slab_meta()` exposes `(offset, len, token)`
for relay without mapping. Dropping the part closes `token` (its liveness ref).

---

## Invariants

1. A slab offset is freed **exactly once**, when its pipe write end hangs up.
2. The hangup cannot fire while any holder (sender, any in-transit hop dup, the
   destination's part) still holds a copy of the read-end token.
3. Mapping happens **only** at the source (to `memcpy` in) and the destination (to
   hand bytes to the user). Intermediates forward `(offset, len, token)` only.
4. `ShmServer` lifetime = its gateway actor. `ShmClient` = per actor. `ShmMapper` =
   per context (always present, idle if unused; unmaps everything on context drop).
5. The slab is a `memfd` (no `/dev/shm` name); the kernel reclaims it when the last
   fd (server's + all distributed dups) closes. No path leaks on any crash.

## Test targets (OSS: `cargo test -p monarch_mini --lib`)

- Allocator: alloc/grow/free-reuse (no mapping).
- Request protocol: allocate → grant → write/read slab via mapper → drop token →
  freed → reused (single process, `ShmServer` + `ShmClient` + `ShmMapper`).
- Liveness: hangup fires only after the last token closes; an in-transit dup keeps it
  alive.
- Per-part: a `[small header, big payload]` message — header inline, payload via slab,
  both intact in order.
- End-to-end: gateway → child large message; two-hop relay (`p0 → h → p1`); deep
  `gateway → P → Q` and `unix → inproc → unix` (state forwarded past an inproc hop).
- Keep all existing inproc/unix/quic/monitor tests green.

---

## Appendix: the libc mechanisms (self-contained)

All `unsafe`; wrap each in a checked helper that turns the syscall's `-1`/`EAGAIN`
into `Result`/`io::ErrorKind::WouldBlock` so async callers compose with tokio
`try_io`. Constants:

```rust
const RESERVE: usize = 1 << 37; // 128 GiB virtual reservation per slab (PROT_NONE)
const INITIAL: u64   = 1 << 20; // 1 MiB initial slab file size
const GROW:    u64   = 1 << 20; // ftruncate/map growth granularity
const ALIGN:   u64   = 64;      // allocation alignment
```

**Slab object (allocator).** `memfd_create("monarch_mini_shm\0", MFD_CLOEXEC)` → an
anonymous, *unnamed* file: nothing leaks in `/dev/shm`, and the kernel reclaims it
when the last fd closes. `ftruncate(fd, new_size)` to size/grow. (Portable fallback:
`shm_open` then immediate `shm_unlink`.)

**Persistent mapping (`ShmMapper`, one `Reservation` per slab inode).** Reserve a
large range once, then map the file `MAP_FIXED` over its front so `base` never moves
as the file grows:
- reserve: `mmap(NULL, RESERVE, PROT_NONE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE, -1, 0)` → `base`.
- ensure mapped to `size`: `mmap(base + mapped, size - mapped, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, slab_fd, mapped)`, then `mapped = size`.
- pointer: `base + offset` (after ensuring `mapped >= offset + len`).
- dedup key / size: `fstat(slab_fd)` → `st_ino` (key), `st_size` (initial mapped extent).
- drop: `munmap(base, RESERVE)`. (Existing mappings survive the slab fd closing —
  `mmap` holds the inode — so only *growth* needs an owned dup of the fd.)

**Liveness pipe.** `pipe2(fds, O_CLOEXEC)` → `(read_end = fds[0], write_end = fds[1])`.
The server keeps `write_end` and watches it for hangup; the `read_end` travels.
Hangup watch (tokio): set `write_end` `O_NONBLOCK`, wrap in
`AsyncFd::with_interest(fd, Interest::WRITABLE)`, loop `ready(WRITABLE).await`, return
when `guard.ready().is_write_closed()`, else `guard.clear_ready()` and re-wait.
(`writable()`/`readable()` alone don't work: a write end is always writable, never
readable; closed read ends surface as `EPOLLERR`/`EPOLLHUP` = write-closed.)

**Request socket.** `socketpair(AF_UNIX, SOCK_DGRAM, 0)` → `(g_end, client_end)`. The
server reads `g_end`; `client_end` is duped to children in `GatewayState`. Many
writers, one reader — hence the reply rides the per-request pipe, not the socket.

**Passing fds over a unix socket (one `sendmsg`, N fds via `SCM_RIGHTS`).** Keep a
single-syscall send/recv pair (these are the only fd-touching calls):
- send: `msghdr` with one `iovec` over the data bytes (≥1 byte is required to carry a
  cmsg) and a control buffer holding one `cmsghdr` (`cmsg_level = SOL_SOCKET`,
  `cmsg_type = SCM_RIGHTS`, `cmsg_len = CMSG_LEN(n * size_of::<RawFd>())`); copy the
  `RawFd`s into `CMSG_DATA(cmsg)`; `sendmsg(sock, &mh, MSG_NOSIGNAL)`.
- recv: `recvmsg(sock, &mh, MSG_CMSG_CLOEXEC)`; walk `CMSG_FIRSTHDR`/`CMSG_NXTHDR`,
  and for each `SCM_RIGHTS` cmsg copy out the `RawFd`s (each is a fresh **owned** fd
  the kernel installed). `recvmsg` returning `0` bytes is EOF.
- size the control buffer for the max fds in one frame (e.g. `[u64; N]` via
  `CMSG_SPACE`); cap N (~64) so it's fixed-size.

In the unix transport these are used in exactly two places: the conditional
fd-exchange step (1-byte payload + the frame's fds) and the `ShmClient` request send
(the `Alloc` datagram + the `write_end`). The header/length-prefix and inline part
bytes use plain `try_read`/`try_write` — never `recvmsg`/`sendmsg`.
