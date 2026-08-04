# Minimonarch Implementation Notes

This document describes the internal design of minimonarch. The public
behavioral contract (what callers can rely on) lives in `minimonarch.h`.
This document covers *how* that contract is implemented.

Philosophy:

* The point of minimonarch is to be the simplest possible distributed actor messaging and fault monitoring API.
* Monarch is a system with lots of flexibliity in how the pieces are put together.
  Minimonarch is attempt at choosing a coherent set of choices rather than options with the ultimate goal of performance (message speed, binary size, startup time, etc.).

---

## Context

The context is the single runtime state object for a process. It owns:

- A single-threaded tokio async event loop running on a dedicated thread.
  All internal I/O (connections, heartbeats, routing) runs on this loop.
  The loop is isolated from the caller's thread so Python GC pauses,
  language runtimes, or blocking calls in user code cannot interfere with
  keepalive timeouts or heartbeats.
- The set of endpoints currently being served (one acceptor per unique URL).
- The dead-gateway set and propagation state (see Dead Gateway Management).
- The set of actors running in this context, with each actor having some
  number of connection objects and an optional gateway.

`mm_ctx_destroy` initiates a graceful shutdown: it flushes messages already
posted before the shutdown, then tears down the event loop thread. It does
not manage parent-child ancestry — actors within the same context may have
different parents and the context does not own that topology.

---

## Connection

Every parent-child link is a `Connection` object. An actor holds at most one
parent connection and zero or more child connections.

### State machine

```
Unestablished → Established → Closed
```

State only moves forward. The Closed state is terminal.

### Transport implementations

| URL scheme  | Transport    | Closed detection                |
|-------------|--------------|---------------------------------|
| `inproc://` | mpsc channel | Channel closed / sender dropped |
| `unix://`   | UNIX socket  | File descriptor closed (passive)|
| `tcp://`    | QUIC         | Bidirectional heartbeat failure |

`inproc://` and `unix://` detect closure passively — no heartbeat needed.
`tcp://` connections are heartbeated bidirectionally; failure to heartbeat
transitions the connection to Closed and triggers failure message delivery.

TCP connections include reconnect/retry logic before declaring failure, both
for the initial connection and for established connections that drop.

---

## Serving

`mm_actor_serve` registers an acceptor for the given URL on the context's
event loop (one acceptor per unique URL, shared across multiple serves on the
same URL). Serves are queued; when a join arrives the next pending serve is
dequeued and the connection objects are constructed and attached to both actors.
The acceptor socket stays open between pairs.

Transport-specific acceptor behaviour:

- `unix://` — standard `listen()`/`accept()` on a UNIX domain socket.
- `inproc://` — an mpsc channel; a joiner sends a message to establish its
  own private per-pair channel.
- `tcp://` — a QUIC listener negotiates a new stream per join.

---

## Message Routing

### Local routing

Each actor maintains a routing table covering all its descendants on the same
gateway (actor id → the immediate child connection to forward through). When
an actor joins a parent, the parent broadcasts the new actor's id up the
ancestry within the same gateway to populate all ancestor tables.

On `mm_actor_send`, the sender looks up the receiver id in its routing table.
If found, it forwards the message down to the appropriate child. If not found,
it forwards up to its parent. This continues until the message reaches an
actor that has the receiver in its table, or reaches a gateway actor.

Message forwarding and originating a send are identical operations: messages
carry no sender field, so routing is purely destination-driven.

### Gateway routing

Actors whose parent connection is a `tcp://` serve, or who have no parent
(i.e. the root actor), are gateways. A gateway actor has an associated
`Gateway` object that manages QUIC connections to other gateways. When a
message reaches a gateway actor whose routing table does not contain the
destination (i.e. the destination is on a different machine), the gateway
makes a direct QUIC connection to the destination's gateway and sends the
message there. The destination gateway then delivers it to the local actor
via normal local routing.

The gateway address forms the `@endpoint` part of the actor's ident, so no
global DNS lookup is needed to route a message — the address is encoded in
the destination ident.

Messages to the root actor are routed by sending up the parent hierarchy even
when the sender is an endpoint, avoiding the need for the root actor to accept
inbound connections.

### Zero-copy

Messages are `(data_pointer, length, deleter)` segments. Forwarding a message
passes the pointer without copying. The deleter is called once the message is
no longer needed, allowing the originating buffer to be released without ever
copying bytes through intermediate hops.

---

## TCP Scale-out *(design now, implement later)*

A logical parent → many-children topology over TCP hits two limits:
connection count and heartbeat cost. The approach:

1. **Message routing** scales automatically because routing is endpoint-based
   (direct QUIC to the receiver's gateway). No broadcast fan-out at this layer.

2. **Liveness monitoring** for UNIX and inproc is passive (fd close). For TCP,
   once the number of direct joins to a single parent exceeds a threshold, new
   joiners are instead connected indirectly through existing siblings:
   - The parent instructs the new actor to connect to N existing siblings.
   - Those siblings take over heartbeating responsibility for the new child.
   - If a sibling fails, the child attempts to reconnect to the real parent or
     another sibling before declaring a failure.
   - The redundant heartbeating fabric is repaired as failures occur.

   The intent is steady-state hierarchical heartbeating with bounded cost per
   node, accepting that failure detection may take longer during
   re-establishment.

Keep this mechanism in mind during initial implementation so that connection
objects, routing tables, and acceptors can accommodate it without a rewrite.

---

## Dead Gateway Management

Gateways (`tcp://` endpoints) can fail. When they do, all actors at that
gateway are implicitly dead. The system maintains a distributed set of
known-dead gateways and propagates changes during heartbeats.

Key design points:

- Gateway URLs include a pseudo-port in addition to the real port so that a
  recovered host reusing the same IP/port is distinguishable from the original
  dead gateway.
- When a parent gateway observes a child gateway failure, it announces the
  failure up its ancestry so that all nodes eventually learn of it.
- The total number of dead gateways is bounded by the number of machines
  (≤ ~1M), making full replication feasible. Only gateways need to track this
  set; individual actors do not.
- Per-gateway OS-level monitoring runs on the gateway's own host; the
  assumption is that the gateway's death propagates reliably via this
  mechanism.
- Nested gateway topologies are handled by having gateways include their
  ancestry in join/serve announcements, so that observers can detect a nested
  gateway failure even when an intermediate gateway does not announce it.

---

## Join/Serve Supervision

Parent-child connections are supervised differently by transport:

**inproc:// and unix://**

Closure of the underlying fd or channel is a stable, immediate signal that
the connection is broken. On detection, the failure message is delivered and
the child actor is implicitly dead (no further messages will be received).

**tcp://**

The connection is heartbeated bidirectionally. Heartbeat failure causes the
connection to behave as if the pipe closed. Initial connections retry before
declaring failure.

For TCP scale-out (see above): at large fanout, a new child may receive a
redirect response instructing it to connect indirectly through a sibling, with
a backup sibling also provided. The sibling registers the new child and
acknowledges, after which it takes over monitoring. If the delegate sibling
fails, the child first tries to reconnect to the real parent or a failover
sibling before propagating a failure — this keeps the steady-state heartbeat
cost hierarchical.

---

## Monitoring

Two mechanisms work together:

### Registration

When `mm_actor_monitor` is called, a registration message is sent to the
gateway of the actor being monitored. The gateway acknowledges with the full
gateway ancestry of the monitored actor (normally a single parent gateway, but
nesting is possible). From that point:

- If the monitored actor dies while its gateway is alive, the gateway delivers
  the failure message directly to the monitor.
- If the monitored actor's gateway (or any ancestor gateway) is announced as
  dead, the monitor fires based on the dead-gateway set alone — no further
  coordination with the now-dead gateway is needed.

### Failure to register

If the monitoring actor cannot establish a connection to the target gateway to
perform the registration, it declares and propagates that gateway as dead. This
speeds up future monitors targeting the same gateway (fast-fail).

### Lazy cancellation and deduplication

Since monitor state is lightweight, cancellation is deferred rather than
immediate. The context locally deduplicates monitors by remote gateway: rapid
monitor create/cancel cycles to the same endpoint skip the remote
registration/deregistration round-trip entirely. A remote cancel message is
only sent after a monitor has been idle (uncreated) for some grace period.

### Decisions made *(please review)*

The implementation notes contained two overlapping descriptions of monitoring.
The following decisions were made to rationalize them:

- **Local vs. remote tracking**: the context tracks monitors *this actor has
  created* (so they can be cancelled and deduplicated). The *gateway of the
  monitored actor* tracks incoming registrations from remote monitors.
- **OS monitoring and heartbeats are complementary**: OS monitoring (fd close,
  process exit) is the primary signal for inproc/unix; heartbeats are the
  primary signal for tcp. Both feed into the same dead-gateway propagation
  path.
- **Nested gateway ancestry in ack**: the registration ack includes all
  ancestor gateways (not just the immediate parent) so that a monitor can fire
  on any ancestor failure without re-querying.
- **Cancel before delivery**: if the failure message is already in the actor's
  local queue when cancel is called, it is dropped before delivery on the next
  `mm_poller_next` call.

---

## Rust Module Layout

| Module      | Contents                                                           |
|-------------|--------------------------------------------------------------------|
| `lib.rs`    | C FFI layer; `CCtx`/`CActor`/`CPoller`/`CMonitorHandle` wrappers  |
| `ctx.rs`    | `Ctx`: tokio loop, acceptors, actors, pollers, routing table, dead-gateway set |
| `msg.rs`    | `MsgPart`: owning wrapper with C deleter                           |
