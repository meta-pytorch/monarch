# Network Channel Internals

The `channel::net` module implements reliable, ordered message delivery
over TCP, Unix, and TLS sockets. All network transports share the same
session management, framing, and protocol logic. This chapter describes
the internal architecture — the connection lifecycle, multiplexing, and
how simplex and duplex modes compose from the same building blocks.

## Overview

A network channel has two sides: an **initiator** (client/sender) and
an **acceptor** (server/receiver). The initiator dials; the acceptor
listens. Each side creates a `Session` that manages physical
connections through a typestate lifecycle:

```text
Session<Disconnected>  ──connect()──→  Session<Connected>
Session<Connected>     ──release()──→  Session<Disconnected>
```

The protocol loops (`send_connected`, `recv_connected`) run on a
single physical connection. When the connection breaks, the session
transitions back to `Disconnected`, the driver acquires a new
connection, and the protocol resumes from where it left off.

### Simplex vs duplex

- **Simplex** channels are unidirectional: one `Tx`, one `Rx`, one
  logical stream (tag `0x00`). The initiator runs `send_connected`;
  the acceptor runs `recv_connected`.
- **Duplex** channels are bidirectional: each side has a `Tx` and an
  `Rx`, using two logical streams (tags `0x00` and `0x01`) over the
  same physical connection. Both sides run `send_connected` and
  `recv_connected` concurrently via `tokio::select!`.

The key insight is that simplex and duplex are not separate
implementations — they are the same `Session` + `Link` pattern with
different protocol functions bound to different stream tags.

## Link trait

A `Link` represents a source of physical connections:

```rust
trait Link: Send + Sync + Debug + 'static {
    type Stream: Stream;
    fn dest(&self) -> ChannelAddr;
    fn link_id(&self) -> SessionId;
    async fn next(&self) -> Result<Self::Stream, ClientError>;
}
```

- **Initiator links** (`TcpLink`, `UnixLink`, `TlsLink`) dial the
  remote address. `next()` connects, performs TLS negotiation if
  needed, writes a `LinkInit` header, and returns the stream. On
  transient failures, `next()` retries with exponential backoff
  (1ms initial, 2x multiplier, 1s max).

- **Acceptor links** (`AcceptorLink<S>`) receive pre-established
  streams from the server's accept loop via an `MVar<S>`. `next()`
  waits for the next dispatched stream.

`NetLink` is an enum (Tcp/Unix/Tls) that implements `Link` with
`Box<dyn Stream>` as the stream type. The `net::link(addr)` constructor
dispatches based on the channel address variant, so call sites don't
need to match on transports.

## Session lifecycle

`Session<L, State>` is a typestate machine parameterized on a `Link`
type, with two states:

### Disconnected

```rust
let session: Session<L, Disconnected> = Session::new(link);
```

`Session::new(link)` stores the link and returns a disconnected
session. No background task is spawned — the session owns the link
directly.

Transition to connected:

- `session.connect()` — consumes self, calls `link.next()` inline to
  acquire a connection. Returns `Ok(Session<Connected>)` or
  `Err(self)` if the link fails.
- `session.connect_by(deadline)` — calls `link.next()` in a loop
  until a connection succeeds or the deadline fires. Returns
  `Err(self)` on deadline. Used by the simplex sender to bound how
  long it waits when messages are queued.

### Connected

```rust
let connected: Session<L, Connected<L::Stream>> = session.connect().await?;
let stream = connected.stream(INITIATOR_TO_ACCEPTOR); // borrows
// ... run protocol ...
drop(stream); // release borrow
let session = connected.release();
```

- `connected.stream(tag)` — borrows the session and returns a
  `ConnectionStream<'_>`. The borrow prevents `release()` at compile
  time.
- `connected.release()` — consumes the connected session, drops the
  mux, and returns a disconnected session with the link intact.

Safety properties enforced by the type system:

| Invariant | Mechanism |
|-----------|-----------|
| Cannot use a stream on a disconnected session | `stream()` only on `Session<Connected>` |
| Cannot release while streams are borrowed | `release(self)` consumes; `stream()` borrows |
| Cannot connect an already-connected session | `connect(self)` only on `Session<Disconnected>` |

## Multiplexing (Mux)

Each physical connection is wrapped in a `Mux` that provides tagged
streams over a single reader/writer pair:

```text
Physical stream
    │
    ├─ DemuxFrameReader ──→ per-tag buffered reads
    │     tag 0x00 ──→ INITIATOR_TO_ACCEPTOR
    │     tag 0x01 ──→ ACCEPTOR_TO_INITIATOR
    │
    └─ SharedWriter ──→ mutex-protected writes
          any tag ──→ frames prefixed with [tag, len, data]
```

- **DemuxFrameReader**: Reads length-prefixed frames, each tagged with
  a leading byte. Demuxes into per-tag buffers. A reader for tag `t`
  first checks `buffered[t]`; if empty, it locks the underlying reader
  and reads frames until one matches tag `t` (buffering others).
- **SharedWriter**: Mutex-protected writer. Writes are tagged with a
  leading byte so the remote demuxer can route them.

Simplex uses one tag (`0x00`); duplex uses two (`0x00` and `0x01`).

## Protocol loops

### recv_connected

Reads message frames from a `ConnectionStream`, validates sequence
numbers, delivers to an `mpsc::Sender<M>`, and periodically sends
acks:

```text
loop {
    select! {
        biased;
        drive pending ack write to completion;
        ack timer tick (loop back to check ack_behind);
        read next frame → {
            seq < next.seq → ignore retransmit
            seq == next.seq → deliver, advance next.seq
            seq > next.seq → SequenceError (fatal)
        }
    }
}
```

Returns `Ok(())` on EOF, or `Err(RecvLoopError)` on I/O error,
cancellation, or sequence violation.

### send_connected

Accepts messages from an `mpsc::UnboundedReceiver`, serializes them,
writes via cancel-safe `Completion`, reads ack responses, and manages
outbox/unacked buffers:

```text
loop {
    if idle and outbox non-empty → begin write
    select! {
        biased;
        read ack/reject/closed from peer;
        delivery timeout on oldest unacked;
        drive frame write to completion → move outbox→unacked;
        accept new message from app (only when outbox empty);
    }
}
```

Returns `Ok(())` on EOF, or `Err(SendLoopError)` for various
terminal/recoverable conditions.

## Simplex mode

### Client (initiator)

`net::spawn(link) -> NetTx<M>`:

```text
create session with link
lazy connect: wait for first message
loop {
    connect_by(deadline)
    ┌ connected ─────────────────────┐
    │ stream(INITIATOR_TO_ACCEPTOR)  │
    │ send_connected(stream, ...)    │
    └────────────────────────────────┘
    release()
    if terminal error → break
    requeue unacked for retransmission
}
```

The client waits for the first message before connecting (lazy
connect). On each connected session it runs `send_connected` until
an error occurs. I/O errors are recoverable (reconnect); sequence
errors, delivery timeouts, and app closures are terminal.

### Server (acceptor)

`server::serve(addr) -> (ChannelAddr, NetRx<M>)`:

The server binds a listener, then runs an accept loop:

```text
accept_loop:
    accept connection
    prepare(stream) → TLS handshake if needed, read LinkInit
    dispatch(session_id, stream):
        sessions[session_id].put(stream)    // MVar
        (first time: create AcceptorLink + Session::new)
```

Each session runs:

```text
loop {
    connect()  // calls link.next(), waits for MVar
    ┌ connected ──────────────────────────────┐
    │ stream(INITIATOR_TO_ACCEPTOR)           │
    │ select! {                               │
    │     recv_connected(stream, tx, next)     │
    │     cancel_token.cancelled()             │
    │ }                                       │
    └─────────────────────────────────────────┘
    flush remaining acks
    send Reject/Closed if appropriate
    release()
}
```

## Duplex mode

### Client (initiator)

`duplex::spawn(link) -> (DuplexTx<Out>, DuplexRx<In>)`:

```text
create session with link
loop {
    connect()
    ┌ connected ──────────────────────────────────┐
    │ send_stream = stream(INITIATOR_TO_ACCEPTOR) │
    │ recv_stream = stream(ACCEPTOR_TO_INITIATOR) │
    │ select! {                                   │
    │     send_connected(send_stream, ...)         │
    │     recv_connected(recv_stream, ...)          │
    │ }                                           │
    └─────────────────────────────────────────────┘
    release()
}
```

Unlike simplex, the duplex client connects immediately (no lazy
connect) and runs both send and recv concurrently. When either
direction errors, the `select!` returns and the session reconnects.

### Server (acceptor)

`duplex::serve(addr)` produces pairs of `(DuplexRx<In>, DuplexTx<Out>)`
for each new session. The server session runs the same shape but with
reversed stream tags:

```text
loop {
    connect()  // calls link.next(), waits for MVar
    ┌ connected ──────────────────────────────────┐
    │ recv_stream = stream(INITIATOR_TO_ACCEPTOR) │
    │ send_stream = stream(ACCEPTOR_TO_INITIATOR) │
    │ select! {                                   │
    │     recv_connected(recv_stream, ...)          │
    │     send_connected(send_stream, ...)          │
    │     cancel_token.cancelled()                 │
    │ }                                           │
    └─────────────────────────────────────────────┘
    release()
}
```

The stream tags are swapped: the acceptor receives on
`INITIATOR_TO_ACCEPTOR` and sends on `ACCEPTOR_TO_INITIATOR`.

## Connection identity: LinkInit

Every physical connection starts with a 12-byte `LinkInit` header:

```text
[magic: 4B "LNK\0"] [session_id: 8B u64 BE]
```

The `SessionId` is generated randomly by the initiator and identifies
a logical session. On reconnection, the initiator sends the same
`SessionId` so the acceptor routes the new connection to the existing
session, which resumes from the last acknowledged sequence number.

## Server-side dispatch

The server's accept loop reads `LinkInit` from each new connection and
routes it by `SessionId`:

```text
DashMap<SessionId, MVar<S>>  ←── sessions table

incoming connection:
    read LinkInit → session_id
    if session_id is new:
        create MVar<S>, AcceptorLink, Session::spawn
        spawn protocol task
        sessions.insert(session_id, mvar)
    mvar.put(stream)  ←── wakes the session's connect()
```

`MVar` is a single-slot synchronisation primitive. The accept loop puts
each new stream; the session's `AcceptorLink::next()` takes it. This
serialises reconnections: only one physical connection at a time can be
active for a given session.

## Transport unification

Both client and server use enum dispatch to avoid per-transport
boilerplate:

| Layer | Client (initiator) | Server (acceptor) |
|-------|-------------------|-------------------|
| Transport dispatch | `NetLink` enum | `NetListener` enum |
| Constructor | `net::link(addr)` | `net::listen(addr)` |
| Session entry | `net::spawn(link)` | `server::serve(addr)` |

TLS uses the same TCP listener under the hood — the TLS handshake
happens in the `prepare` callback, not the listener.

## Error handling and recovery

| Error | Recoverable? | Action |
|-------|-------------|--------|
| I/O error (EOF, broken pipe) | Yes | Release unhealthy, reconnect, retransmit unacked |
| Sequence error | No | Send `Reject`, break session loop |
| Delivery timeout | No | Close channel, return undelivered messages |
| App closed (sender dropped) | No | Close channel |
| Server `Closed` response | No | Close channel |
| Server `Reject` response | No | Close channel |
| Oversized frame | No | Return message, close channel |

On reconnection, the send side moves all unacked messages back to the
outbox (`deliveries.requeue_unacked()`) and retransmits them. The
receive side deduplicates by sequence number — messages with
`seq < next.seq` are silently ignored.

## Cancellation safety

The protocol loops are designed to be wrapped in `tokio::select!`:

- `recv_connected` and `send_connected` do not take a
  `CancellationToken`. Instead, call sites compose cancellation
  externally:

  ```rust
  tokio::select! {
      r = recv_connected(&stream, &tx, &mut next) => r,
      _ = cancel_token.cancelled() => Err(RecvLoopError::Cancelled),
  }
  ```

- Frame I/O uses `Completion`, a cancel-safe write primitive that
  resumes from the last written byte on the next poll. Cancelling a
  `select!` branch mid-write does not corrupt the stream.

## Module layout

```text
channel/net.rs
    Link trait, NetLink enum, NetListener enum
    net::spawn() — simplex client
    net::link(), net::listen() — transport constructors
    transport modules: tcp, unix, tls, meta

channel/net/session.rs
    Session<S, State> typestate
    session_driver task
    Mux, DemuxFrameReader, SharedWriter
    send_connected, recv_connected
    Deliveries, Outbox, Unacked

channel/net/server.rs
    AcceptorLink<S>
    dispatch_stream — simplex server session routing
    accept_loop — generic accept loop
    server::serve() — simplex server entry point
    ServerHandle

channel/net/duplex.rs
    dispatch_duplex_stream — duplex server session routing
    duplex::spawn() — duplex client
    duplex::serve() — duplex server entry point
    duplex::dial() — convenience wrapper

channel/net/framed.rs
    FrameReader, FrameWrite — cancel-safe frame I/O
    Completion — cancel-safe write state machine
```
