# Unordered Channels

`channel::unordered::dial(addr, num_streams)` returns a `ChannelTx<M>` with
weaker guarantees than [`channel::dial`](tx_rx.md). It promises only that
every message is either delivered or reported as failed. It does not order
messages, and it does not tie a sender to one receiving process. Ordering and
duplicate suppression belong to the layer above it; for actor messaging, that
layer is the destination actor's reorder buffer.

Serve with `channel::unordered::serve`, or with `channel::serve`: both use the
same listener, which routes unordered sessions by their stream ids.

This chapter is the contract for the unordered channel. The rest of the
channel chapters describe `channel::dial`, whose guarantees differ (see
[Comparison with `channel::dial`](#comparison-with-channeldial)).

## Terms

- **Posted:** handed to the `Tx` through `post`, `try_post`, `do_post`, or
  `send`.
- **Delivered:** handed by a server to its `ChannelRx` queue. Delivery does not
  mean that the application has received or processed the message.
- **Accepted:** the message's completion reports success; `send` returns
  `Ok(())`.
- **Rejected:** the message's completion reports a `SendError`, which carries
  the message back to the sender.
- **Settled:** accepted or rejected.
- **Server:** a `ChannelRx` serving the destination address. Over the lifetime
  of a `Tx`, several servers, in several processes, may serve the same
  address one after another.

## Guarantees

### Every message settles exactly once

Each posted message is settled exactly once. A message is never dropped
without settling, and it is never accepted unless a server acknowledged it.

A message settles no later than `MESSAGE_DELIVERY_TIMEOUT` after it was
posted, give or take scheduling delay. When the timeout passes, the `Tx`
rejects the message with `SendErrorReason::DeliveryTimeout`.

### Acceptance means delivery

A message is accepted only after a server acknowledges it, and a server
acknowledges a message only after it has delivered the message. If the `Tx`
reconnects to a new server, an ack from the new server covers only what the
new server delivered.

### Rejection does not mean the message was not delivered

The outcome of a rejected message depends on the reason:

| Reason | Delivered? |
|--------|------------|
| `OversizedFrame` | Never. The `Tx` rejects the message before writing it. |
| `Other` (serialization failure) | Never. |
| `DeliveryTimeout` | Unknown. A server may have delivered the message and lost the ack. |

### No ordering

Messages may be delivered in any order. With `num_streams > 1`, two messages
posted back to back may travel on different connections and arrive out of
order. With `num_streams == 1`, the current implementation delivers messages
in posting order, but callers must not rely on this.

### At most once per server

A server delivers each message at most once, even when the `Tx` retransmits
it. Across servers there is no such guarantee: a message delivered by one
server whose ack was lost is retransmitted to the next server at the same
address, which delivers it again.

The layer above must make that harmless. For actor messaging, a process that
restarts at the same address must use a new `ProcId`, so a retransmitted
message addressed to an actor of the old process is returned as undeliverable
rather than delivered. Restarting a proc with the same `ProcId` is undefined
behavior.

### No session fence

A server accepts any session, starting at any sequence number. It never
rejects a session because it has no record of it, and the `Tx` never stops
because a server went away. When the peer restarts at the same address, the
`Tx` reconnects to the new process and retransmits every unacknowledged
message.

### The `Tx` does not close while it is alive

`status()` stays `TxStatus::Active` for as long as the `Tx` exists. No peer
behavior closes it: not a refused connection, not a server shutdown, and not
a delivery timeout. A failure affects only the messages it affects, and the
next message posted is delivered as soon as a server is reachable.

When the `Tx` is dropped, its outstanding messages are not discarded: they
are still delivered or rejected, and the status changes to `Closed` once all of
them have settled.

### Connections are made on demand

The `Tx` connects only while it has unsettled messages, and it reconnects
with backoff for as long as it has any. A `Tx` to a peer that is permanently
gone costs nothing once its messages have timed out.

## What the layer above provides

For actor messaging, the guarantees that the unordered channel lacks come from
the actor layer:

- **Ordering:** every message from an actor carries `SEQ_INFO`, and the
  destination actor's reorder buffer releases messages in sequence order.
- **Duplicate suppression:** the reorder buffer drops a sequenced message whose
  sequence number it has already released. Messages without a sequence number
  (`SeqInfo::Direct`) rely on the channel's at-most-once-per-server guarantee.
- **Protection from stale receivers:** a `ProcId` names one process, so a
  message for a process that no longer exists cannot reach a different one.
- **Failure handling:** a rejected message becomes an `Undeliverable`, which
  the sender handles through supervision.

## Comparison with `channel::dial`

| Property | `channel::dial` | `channel::unordered::dial` |
|----------|-----------------|----------------------------|
| Ordering | Posting order | None |
| Duplicates | None within a session | None per server; possible across servers |
| Unknown session at the server | Rejected; the `Tx` closes | Accepted |
| Server shutdown | The `Tx` closes | The `Tx` reconnects |
| Delivery timeout | Every outstanding message is rejected; the `Tx` closes | Only the expired message is rejected |
| Oversized frame | The message is rejected; the `Tx` closes | Only the message is rejected |
| `TxStatus::Closed` | When the session fails | Only after the `Tx` is dropped and every message has settled |

## Protocol

### Connections

The `Tx` opens `num_streams` links that share one random `SessionId`. Each
link writes a `LinkInit` header with a distinct `stream_id` in
`1..=num_streams`; the nonzero stream id routes the connection to the
unordered receive path on the server. A stream outlives its connections: when
a connection ends, the stream reconnects with the same session and stream id.
The server keys its state by (session, stream), and the streams of a session
share nothing on the server.

A duplex server on the `Local` transport also accepts unordered sessions,
since gateways dial in-process hosts' duplex frontends with simplex
channels. It hands all of them to its application as one inbound
connection, whose `DuplexTx` carries nothing.

### Frames

The client writes `Frame::Message(seq, message)`: a message and its sequence
number. Each stream numbers the messages it writes from 0, in the order it
first writes them. A retransmitted message keeps its stream and its number.

The server answers with:

- `NetRxResponse::Ack(seq)`: every message of this stream numbered `seq` or
  below has been delivered. Acks are cumulative and per stream.
- `NetRxResponse::Closed`: the server is shutting down.

### Client state

Posted messages wait in a queue shared by the streams, oldest first. A stream
takes the oldest queued message, numbers it, and holds it until it settles.
The messages a stream holds are in number order, which is also posting order.

- **Ack:** the stream accepts every message it holds numbered `seq` or below.
- **Connection lost** (I/O error, EOF, or `Closed`): the stream keeps its
  messages and reconnects with backoff. On the new connection, it rewrites the
  messages it holds, in order and with the same numbers, before it takes new
  ones.
- **Delivery timeout:** a message posted more than `MESSAGE_DELIVERY_TIMEOUT`
  ago is rejected, whether it is queued or held by a stream, and whether that
  stream is connected, connecting, or backing off. A late ack for it is
  ignored.
- **Oversized frame:** a message whose frame exceeds `CODEC_MAX_FRAME_LENGTH`
  is rejected without being written, and consumes no number.

A message stays with the stream that first wrote it. The server identifies a
message only by its stream and number. A message written on a connection that
then failed may already have been delivered; rewritten on another stream with
a new number, it would be delivered again. Moving messages between streams
would need a session-wide identity and duplicate state shared by the streams.

### Server state

For each stream, the server keeps the next expected number: one past the
highest number it has delivered. A message numbered below it is a duplicate,
and the server drops it. A message numbered at or above it is delivered, and
the next expected number moves past it. A gap is allowed for two reasons: a
stream does not rewrite a message that expired, so the server may never
receive its number, and a server that restarts picks up a stream at whatever
number arrives first.

The server acks a stream's highest delivered number at the start of each of
its connections, after `MESSAGE_ACK_EVERY_N_MESSAGES` messages or
`MESSAGE_ACK_TIME_INTERVAL`, whichever comes first, and once more when the
connection ends. The ack at the start replaces any ack lost with the previous
connection, and lets the client accept what was already delivered without
waiting for its retransmissions.

The server processes a stream's connections one at a time: a new connection
waits until the previous one ends.

### Why acks are correct

- On every connection, a stream writes frames in increasing number order:
  first the messages it holds, then new ones. A connection preserves the
  order of its frames, and the server processes a stream's connections one at
  a time.
- So when the server delivers number `n` from a connection, it has already
  received, on that connection, every message numbered below `n` that the
  stream held when it wrote `n`. It delivered each of them then, or had
  delivered it earlier and dropped the repeat. An ack for `n` therefore lets
  the client accept every message it holds numbered `n` or below: acceptance
  means delivery.
- The next expected number only rises within a server process, and a
  retransmission keeps its stream and number, so a server never delivers a
  message twice. A restarted server starts over and may deliver a message
  again, as the contract allows.
- Gaps come only from messages the client has already rejected, so an ack
  that covers a gap affects nothing the client still holds.
