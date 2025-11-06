# Transmits and Receives

Transmit (`Tx`) and receive (`Rx`) ends define the basic channel interface. `Tx` can post or send messages, `Rx` can asynchronously receive them.

## Overview

Channels provide one-way, typed communication between processes.

The public API is:

- `Tx<M>`: enqueue (`try_post`), fire-and-forget (`post`), or synchronous (`send`) sends; monitor health with `status()`.
- `Rx<M>`: `recv()` the next message.

Under the hood, network transports use a length-prefixed, multipart frame with cancel-safe I/O. Delivery preserves **per-sender order** and deduplicates retransmits; across different senders there is no global ordering guarantee.

> Request/response is done at the actor/mailbox layer by carrying a **reply port reference** (e.g., `PortRef<Reply>`) inside the message. Channels only move bytes.

## Semantics

### `Tx<M: RemoteMessage>` (transmit end)

```rust
#[async_trait]
pub trait Tx<M: RemoteMessage>: std::fmt::Debug {
    fn try_post(&self, message: M, return_channel: oneshot::Sender<SendError<M>>);
    fn post(&self, message: M);
    async fn send(&self, message: M) -> Result<(), SendError<M>>;
    fn addr(&self) -> ChannelAddr;
    fn status(&self) -> &watch::Receiver<TxStatus>;
}
```

- **`try_post(message, return_channel)`**
  Enqueues locally.
  - If delivery later fails, the original message is sent back on `return_channel` as SendError.

- **`post(message)`**
  Fire-and-forget wrapper around `try_post`. The caller should monitor `status()` for health instead of relying on return values.

- **`send(message)`**
  Convenience over `try_post`.
  - `Ok(())` when the message is enqueued on the remote end.
  - `Err(SendError(ChannelError::Closed, message))` if the channel closes and the message is returned.

- **`addr()`**
  The destination `ChannelAddr`.

- **`status()`**
  A `watch::Receiver<TxStatus>` that flips to `Closed` when the receive side is gone.

**Note:** `SendError<M>` carries both the error and the original `M` for retry or inspection.

### Rx<M: RemoteMessage> (receive end)
```rust
#[async_trait]
pub trait Rx<M: RemoteMessage>: std::fmt::Debug {
    async fn recv(&mut self) -> Result<M, ChannelError>;
    fn addr(&self) -> ChannelAddr;
}
```
- **`recv()`**
  Asynchronously yields the next message.
  - `Ok(message)` when a message is available.
  - `Err(ChannelError::Closed)` when the channel is closed and no more messages will arrive.

- **`addr()`**
  The source `ChannelAddr` from which this `Rx` is receiving.

## Guarantees and Limits

### Delivery & ordering
- **Per-sender FIFO.** Messages posted from a single `Tx` are delivered in order.
  (Network transports tag each message with a monotonically increasing `seq`; the server enforces in-order delivery and ignores retransmits with lower `seq`.)
- **No global ordering across different `Tx`es.** When multiple transmitters send to one receiver, relative order between different senders is unspecified.

### Acknowledgement & exactly-once at the channel boundary
- **Network transports (TCP/Unix/MetaTLS):** delivery is confirmed by explicit acks from the server (`NetRxResponse::Ack`). The server enforces *no duplicates and no reordering* per session (`seq`/`ack`) **for delivery into the server's bounded queue**.
  - `Tx::send` resolves **`Ok(())` when the ack is observed** (which implies the message was enqueued on the server's `mpsc`), not when the remote handler processes it.
- **Local transport:** in-process MPSC handoff; **no network acks** involved.
  - `Tx::send` returns **immediately** after `try_post` succeeds (the oneshot sender is dropped right away).

### Backpressure & buffering
- **Server-side buffer (network):** incoming messages are relayed into a bounded `mpsc` (default capacity **1024**). If full, the server's forwarding task uses `reserve()` in a retry loop, logging and incrementing a metric until space frees. This can delay acks.
- **Client outbox (network):** each `Tx` maintains an outbox + unacked queue. On reconnect, all unacked messages are requeued in order and retransmitted; the server deduplicates them by `seq`.

### Cancellation safety
- Cancelling a task awaiting `recv()` or a network frame read/write does **not** corrupt channel state; progress resumes on the next poll.
- Cancelling `Tx::send(message)` only cancels your await; it doesn't "unsend" the message. The message may still be in-flight or already delivered. Use `status()` to monitor liveness.

### Failure semantics
- **Closed receiver:** `recv()` returns `Err(ChannelError::Closed)`.
- **Network transports:** disconnects trigger exponential backoff reconnects; unacked messages are retried. If recovery ultimately fails (e.g., connection cannot be re-established within the delivery timeout window), the client closes and returns all undelivered/unacked messages via their `return_channel`. `status()` flips to `Closed`.
- **Local transport:** no delayed return path.
- **Network disconnects (EOF/I/O error/temporary break):** the client reconnects with exponential backoff and resends any unacked messages; the server deduplicates by `seq`.
- **Delivery timeout:** see [Size & time limits](#size--time-limits).

### Size & time limits
- **Max frame size:** frames larger than `config::CODEC_MAX_FRAME_LENGTH` are rejected.
- **Ack policy:** acks are sent every `config::MESSAGE_ACK_EVERY_N_MESSAGES` or after `config::MESSAGE_ACK_TIME_INTERVAL` (whichever comes first).
- **Delivery timeout (network):** if the oldest pending message (unsent or unacked) exceeds `config::MESSAGE_DELIVERY_TIMEOUT` without being acked, the link is deemed irrecoverable.  The client closes the channel, returns all queued and unacked messages via their `return_channel`, and `Tx` transitions to `Closed`.

## Implementations

Concrete channel implementations that satisfy `Tx<M>` / `Rx<M>`:

- **Local** — in-process only; uses `tokio::sync::mpsc`. No network framing/acks.
  _Dial/serve:_ `serve_local::<M>()`, `ChannelAddr::Local(_)`.

- **TCP** — `tokio::net::TcpStream` with 8-byte BE length-prefixed frames; `seq`/`ack` for exactly-once into the server queue; reconnects with backoff.
  _Dial/serve:_ `tcp:HOST:PORT`.

- **Unix** — Unix domain sockets; same framing/ack semantics as TCP.
  _Dial/serve:_ `unix:/path` or abstract names (Linux) `unix:@name`.

- **MetaTLS** — TCP wrapped in TLS via `tokio-rustls`; same framing/ack semantics; Meta cert plumbing.
  _Dial/serve:_ `metatls:HOST:PORT`.

- **Sim** — simulation transport for tests; exercises the same channel semantics without real sockets.
  _Dial/serve:_ `sim:<inner-addr>`.

<!-- ## Integration with Transports -->
