# Parallel Data Streams for Large Messages

Design proposal for striping a large message's payload across several physical streams of
one connection, so a bandwidth-sensitive link can fill a fat (esp. cross-region) pipe
instead of being bottlenecked on one flow. The mechanism lives entirely in
[`crate::net_transport`]; ctx/actors observe nothing new, and — thanks to the generic
`stream(index, priority)` seam — the [`Net`] implementations need no changes at all.

> Status: design for review. High-level — it names the pieces and the invariant that
> keeps them consistent, but leaves wire framing details to implementation. It builds
> directly on the completed generic-stream refactor (see below).

---

## 1. Motivation

Today every connection uses two streams: a data/control stream (index 0) and a
heartbeat stream (index 1). One data stream is one flow, and a single flow is
throughput-bound: a tcp socket is core-bound for crypto and, cross-region, cwnd/RTT-bound
to one flow's share of the link; bulk transfer wants *parallel flows* (the GridFTP trick)
to saturate the pipe. So for a connection we judge bandwidth-sensitive, we want to carry
large messages over several physical streams at once.

The generic-stream refactor is what makes this cheap. `NetConn::stream(index, priority)`
now opens (dialer) or awaits (acceptor) the stream addressed by `index`, paired with the
peer's stream of the same index, lazily and in any order:

- **tcp** — each index is its own TLS-over-TCP socket, dialed on demand and routed to the
  connection by the listener demux via its `(connection_id, index)` prefix.
- **quic** — each index is a stream on the one connection; the acceptor's demux coroutine
  pairs them by their index prefix.

So "more physical streams for a connection" is just "more indices" — index 0 is data,
index 1 is heartbeat, and **higher indices are data streams** requested on demand. No
second connection, no new pairing scheme: the existing per-index pairing already handles
arbitrary indices.

## 2. Goals & non-goals

**Goals**

- Stripe a large message's bytes across several data streams; deliver messages **in
  order**; pipeline assembly so a straggler on one message doesn't stall assembling the
  next.
- **Small-message latency must not regress.** A small message must not transit any more
  coroutines or channel hops than it does today. The ordering gate and all assembly logic
  live *inside* the existing control-reader task — never a separate coroutine — so a small
  message is decoded and forwarded to the command loop on exactly today's path; it never
  passes through a data-stream reader or an assembly stage. When nothing is being striped
  the gate is a pure passthrough, adding no work. (A small message is delayed only when
  in-order delivery genuinely requires it — it followed a not-yet-assembled large message
  on the control stream — and even then it waits in a buffer in the reader, not an extra
  hop.)
- **Reader and writer own disjoint, one-way data streams**, so no stream half is ever
  handed between them and neither needs to hold (or share) the connection on the other's
  behalf.
- No new coroutine on the per-connection baseline. Extra tasks/streams exist only for
  connections that actually trip the heuristic — and only for the direction(s) that do.
- No new transport machinery: reuse `stream(index, priority)` and the per-index pairing
  the transports already do.

**Non-goals (first cut)**

- Shrinking back / closing data streams while the connection lives.
- Changing heartbeat, establishment, delegation, or routing.
- Striping small messages. They stay inline on the control stream (§5).

## 3. Fixed stream indices (direction by parity)

A bandwidth-sensitive connection keeps its existing two streams and adds data streams on
the *same* connection. Every stream has a **fixed index**, known to both ends without
negotiation. Data-stream direction is encoded by the index's **parity**:

```
index:   0        1     2, 4, 6, ...      3, 5, 7, ...
stream:  control  hb    td0, td1, td2 ..  ta0, ta1, ta2 ..
                        (even ≥ 2)        (odd ≥ 3)
```

so `td_k = 2 + 2k` and `ta_k = 3 + 2k`.

- **control** (index 0) — as today: the preamble, every `ConnectionCommand` frame, and
  small messages inline. For a large message it also carries a **descriptor** (`seq` +
  `total_len`) that fixes the message's delivery order and how the payload was split.
- **hb** (index 1) — heartbeat stream, unchanged.
- **td** (toward-dialer) data streams, at **even** indices ≥ 2 — carry striped payload
  **to the dialer**: the acceptor writes them, the dialer reads them.
- **ta** (toward-acceptor) data streams, at **odd** indices ≥ 3 — carry striped payload
  **to the acceptor**: the dialer writes them, the acceptor reads them.

Data streams are used one-way and carry **unframed striped payload bytes** only, at a
*lower* priority than the heartbeat so beats still pack ahead of bulk data.

Encoding direction in the parity (rather than contiguous `td` then `ta` blocks) means the
two directions' index spaces never overlap **regardless of how many streams each has**:
`td_k` doesn't depend on the number of `ta` streams and vice-versa. So the counts can
differ — a one-directional bulk transfer can open many streams the busy way and none the
other — with no shared base to agree on.

## 4. Which task opens which streams

The fixed layout means a task can name its own stream indices with no coordination — it
just needs to know whether it is on the dialer or the acceptor, and whether it reads or
writes. The four cases, all with data flowing along the direction's name:

| side     | reader reads | writer writes |
|----------|--------------|---------------|
| dialer   | `td` streams | `ta` streams  |
| acceptor | `ta` streams | `td` streams  |

**Both `reader_task` and `writer_task` hold a clone of the (now `Clone`) `NetConn` and
call `conn.stream(index, priority).await` themselves** to obtain the half they use — the
reader keeps the recv half of the streams it reads, the writer keeps the send half of the
streams it writes. There is no "one task opens a full-duplex stream and hands the other
half across" step, so nothing is shared between the two tasks beyond the cloneable handle.

`stream(index)` already hides who physically dials (the dialer's opens, the acceptor's
awaits; for tcp only the dialer can dial a socket, so even a `td` stream the dialer only
*reads* is still dialed by the dialer's `stream` call). Both ends name the same fixed
index, so their halves pair with no negotiation frame.

The `NetConn` is cloneable in every case (from the generic-stream refactor): a quic dialer
clones its `Connection`; the quic and tcp acceptors clone a request-sender to their demux;
a tcp dialer clones its dial handle. So handing a clone to both `reader_task` and
`writer_task` costs nothing and needs no lifecycle bookkeeping.

## 5. Striping, ordering, and pipelined assembly

**Order is defined by the control stream.** Every message appears there in order: a small
message inline (payload and all), a large message as a `LargeMsg { seq, total_len }`
descriptor (the split is an even partition across that direction's data streams, so
per-stream offsets/lengths are derivable — extensible to explicit per-stream lengths).

**Send path** (a side's writer, once its outbound data streams are open):

- Small message → inline frame on the control stream, as today.
- Large message → write `LargeMsg { seq, total_len }` on the control stream, then write
  shard *i* onto outbound data stream *i*. Per stream, shards go out in `seq` order, so
  each data stream is a FIFO of shards ordered by message.

**Receive path — the control reader becomes the in-order delivery gate.** The `LargeMsg`
descriptor is the only trigger: seeing the first one, the reader opens its data streams
(its fixed `td`/`ta` set, per §4) and starts consuming shards. No separate "striping
started" frame is needed.

- The control reader allocates the destination for each `LargeMsg` (the same slab block
  the non-striped shm path would use) and tells each data-stream reader *i*: "fill this
  (block, offset, len) next." Instructions queue in `seq` order.
- Each data-stream reader *i* pulls its next instruction, reads that many bytes straight
  into the block at its offset (zero-copy into shm, fully parallel across the readers), and
  reports completion. It immediately proceeds to the next message's shard — it does **not**
  wait for the other streams. This is the pipelining: message *B*'s shards assemble on the
  fast streams while message *A* waits on a slow one.
- The control reader counts a completion per shard; a message is *ready* when all its
  shards land (a small message is ready immediately). It delivers ready messages to the
  command loop **in `seq` order** — holding back everything behind the oldest
  not-yet-ready message. When a stalled large message finally completes, it and all the
  already-assembled messages queued behind it flush at once.

This gate is not an extra coroutine — it is the same control-reader task, doing the same
single forward to the command loop it does today. With nothing striped (the head of the
order is always ready), it forwards each message immediately, so small-message latency is
byte-for-byte today's path (see §2). The only added wait is the semantically required one:
a small message that *followed* an unfinished large message on the control stream, which
in-order delivery must hold regardless.

This keeps the control stream readable while payloads are still in flight (the reader
never blocks on shard bytes — only the *delivery* of a specific message waits), gives
correct ordering, and pipelines assembly so a straggler only delays messages that truly
follow it in order.

The data streams carry pure bytes with no per-shard framing — the control descriptors and
the per-reader instruction queue fully determine the layout — keeping the hot data path a
plain `read_exact`/`write_all` into slab memory.

## 6. Failure handling

- **A data stream dies.** A striped message in flight can no longer complete, and order
  can't skip it, so a dead data stream severs the connection — the same outcome as an
  index-0 data-stream failure today. No separate lifecycle to reason about, since the data
  streams belong to the connection.
- **Heartbeat unchanged.** Liveness is still index 1's job; the data streams carry no
  heartbeat and need none.
- **Shutdown.** The writer's data-stream send halves flush/`shutdown` alongside index 0.
  For tcp the extra sockets are just more of the same connection's sockets; they close with
  it.

## 7. Env knobs

- `MM_NET_LARGE_MSG_BYTES` — size at/above which a message is considered large: it is
  striped (once that direction's data streams exist) and trips striping initiation.
- `MM_NET_N_DATA_STREAMS` — data streams per striping direction (so up to that many extra
  sockets per direction for tcp). One-shot to this width, our best guess at what saturates
  the link.

No incremental-growth or cooldown knobs: initiation is one-shot to the target.

## 8. Summary of changes

Transport support already landed with the generic-stream refactor; the remaining work is
`net_transport` + `framing`.

| Area | Change | Status |
|------|--------|--------|
| `net` / `NetConn` | Generic `stream(index, priority)`; direction is a property of the connection. | **Done** |
| `tcp_net` / `quic_net` | Per-connection acceptor demux coroutine pairing accepted streams to requests via a per-index `Matcher`; cloneable request-sender handle; demux owns deregistration. | **Done** |
| `net_transport` | Inline trip check in the existing writer. On trip it writes a `LargeMsg` descriptor on the control stream and opens its own fixed-index data streams (`stream(k, PRIORITY_DATA)`, keeping send halves) to write shards; the reader opens its own fixed-index data streams (keeping recv halves) on seeing the first `LargeMsg`. Both `reader_task` and `writer_task` hold a clone of the `NetConn` and call `stream().await` themselves — no shared handle, no half hand-off. Control reader is the in-order delivery gate (allocate destination, instruct data readers, count completions, deliver in `seq` order). | To do |
| `framing` | `LargeMsg { seq, total_len }` on the control stream (the sole striping trigger — no separate signal, since indices are fixed). Data streams carry unframed payload bytes. | To do |
| ctx / actors | None. | — |

## 9. Open questions

- **N default.** What width actually saturates a cross-region link — is a single fixed
  `MM_NET_N_DATA_STREAMS` enough, or should it key off observed RTT/bandwidth? Start
  fixed; measure.
- **Even split vs. explicit layout.** Even split needs only `total_len` on the wire;
  explicit per-stream lengths cost a little header but allow weighting a faster stream.
  Start even.
- **Latency floor.** A large message that is *latency* sensitive rather than throughput
  sensitive doesn't want to wait on the slowest stream. The size gate is the crude guard;
  revisit if a workload cares.
