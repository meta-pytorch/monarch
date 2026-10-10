# Frames

Frames define the wire format for channel messages: a single-stream, length-prefixed protocol that carries a serialized, multipart message per frame. Each frame is:
```text
[length: u64 big-endian][payload bytes …]
```
where the 8-byte prefix declares exactly how many payload bytes follow.

This section describes:
- The **frame format** (u64 length + payload) used by channels
- **`FrameReader`**: a cancel-safe reader that yields whole frames as `Bytes`
- **`FrameWrite`**: a cancel-safe writer that emits a frame from any `Buf`, using vectored I/O
- **Safety and limits**: `EOF` behavior and a configurable max frame size

**Multipart by default**. Channel messages are serialized as `serde_multipart::Message`. On the send path, we frame the message's bytes (body + parts) and write them without coalescing: `FrameWrite` accepts a multipart `Buf` (via `Message::framed()`), then uses `write_vectored` to hand the kernel multiple `IoSlice`s at once. On the receive path, `FrameReader` reads the length, then reads exactly that many bytes and returns a contiguous `Bytes` payload; higher layers split that payload back into body/parts.

Key properties:
- **Zero-copy friendly (user space)**: large `Bytes` parts are never copied for framing; vectored writes avoid gather buffers.
- **Cancellation-safe**: both read and write can be used in `tokio::select!` without risking dropped or partial frames. Progress is preserved across cancellations.
- **Bounded**: frames exceeding `max_frame_length` are rejected early.
- Clear `EOF` semantics: `Ok(None)` only when `EOF` happens on a frame boundary; mid-frame `EOF` is `UnexpectedEof`.

That's the overview. Next we'll spell out the exact format and walk through `FrameReader` and `FrameWrite` in detail.

## Frame format

Frames carry a single serialized **multipart** message per frame.
```text
[length: u64 big-endian][payload bytes …]
```
- **Length prefix (8 bytes)**: total number of bytes in the payload that follows.
- **Payload**: the bytes of a `serde_multipart::Message::framed()` value. This is logically multipart (body + parts), but appears on the wire as a single contiguous length-sized region.

### Invariants

- The length is the exact payload size (`payload.len() == length`).
- Zero-length frames are valid.
- Receivers **must** reject frames whose length exceeds a configured maximum.
- `EOF` semantics:
  - `EOF` on a frame boundary => end of stream (no error).
  - `EOF` mid-prefix or mid-payload => `UnexpectedEof` (error).

### Why multipart matters (even on one stream)

`Message::framed()` implements `Buf` with vectored slices (`chunks_vectored`). `FrameWrite` uses `write_vectored` to hand the OS multiple `IoSlice`s (prefix, body, parts) without coalescing in user-space. This preserves zero-copy characteristics for large parts while still producing a single length-delimited frame on the wire.

## `FrameReader`

`FrameReader<R: AsyncRead + Unpin>` yields whole frames as `Bytes` and is cancellation-safe (safe to use in `tokio::select!` without losing frames).

```rust
/// A FrameReader reads frames from an underlying [`AsyncRead`].
pub struct FrameReader<R> {
    reader: R,
    max_frame_length: usize,
    state: FrameReaderState,
}

enum FrameReaderState {
    /// Accumulating 8-byte length prefix.
    ReadLen { buf: [u8; 8], off: usize },
    /// Accumulating body of exactly `len` bytes.
    ReadBody { buf: Vec<u8>, len: usize }, // buf.len() <= len
}
```
- `new(reader, max_frame_length)` initializes `state = ReadLen { buf: [0;8], off: 0 }`.
- `next()` drives a small state machine:
  - `ReadLen`: fill 8 bytes; on `EOF` with `off == 0` → `Ok(None)`; with `off > 0` → `UnexpectedEof`.
  - Parse `u64` length (big-endian); `if len > max_frame_length` → `InvalidData` (fatal).
  - `ReadBody`: read exactly `len` bytes; `EOF` mid-body → `UnexpectedEof`.
  - On completion, `take(buf).into()` returns a `Bytes` payload and state resets to `ReadLen`.

**Cancellation-safe**: If polled inside `tokio::select!` and cancelled at any `Pending`, progress is not lost and no partial frame is surfaced; the next `next()` call resumes from state.

## `FrameWrite`

```rust

/// A Writer for message frames. `FrameWrite` requires the user to
/// drive the underlying state machines through (possibly) successive
/// calls to `send`, retaining cancellation safety. The `FrameWrite`
/// owns the underlying writer until the frame has been written to
/// completion.
pub struct FrameWrite<W, B> {
    writer: W,
    len_buf: Bytes,
    body: B,
}

impl<W: AsyncWrite + Unpin, B: Buf> FrameWrite<W, B> {
    /// Create a new frame writer, writing `body` to `writer`.
    pub fn new(writer: W, body: B, max_len: usize) -> Result<Self, (W, io::Error)> { /* builds 8-byte BE prefix */ }

    /// Drive the underlying state machine. The frame is written when this
    /// returns `Ok(())`.
    pub async fn send(&mut self) -> io::Result<()> { /* length → body → flush */ }

    /// Return ownership of the underlying writer (call after success).
    pub fn complete(self) -> W { /* … */ }

    /// Convenience: write a single frame and return the writer.
    pub async fn write_frame(writer: W, buf: B, max: usize) -> Result<W, (W, io::Error)> { /* … */ }
}
```

### What it does

- Length-prefix first. `new()` precomputes an 8-byte big-endian prefix with `body.remaining()` and freezes it (`BytesMut::put_u64(..).freeze()`). Returns an error if `body.remaining()` exceeds `max_len`.
- Then the body. `send()` writes the prefix, then the body:
  - Uses **vectored I/O** when possible: builds up to 4 `IoSlice`s from `B: Buf` via `chunks_vectored`, then calls `write_vectored`.
  - Falls back to scalar writes if the underlying writer isn't vectored.
  - Flush at the end. Not all transports auto-flush (e.g., rustls); `send()` calls `flush()` before returning `Ok(())`.
  - Hand back the writer. `complete(self)` returns the inner `AsyncWrite` so you can reuse it.

### Cancellation safety

`send()` is designed to be used inside `tokio::select!` and safely cancelled:
- If cancelled while writing the length prefix, no body bytes are written yet; a subsequent `send()` call resumes and finishes the prefix before writing body bytes.
- If cancelled during the body, progress is preserved. On each poll, `write_vectored` (per Tokio docs) guarantees that if a different `select!` branch wins, no data was written in that poll. Short writes are handled by advancing the `Buf` (monotonic progress).
- Because the reader consumes exactly one `[len][payload]` pair, the receiver either sees a complete frame or nothing—not a "split" frame.

## Atomicity (frame granularity)

Guarantee: A receiver observes either the entire frame body exactly once, or it observes nothing for that frame.
This flows from:

- length-prefix protocol + `FrameReader`'s "read exactly len bytes" contract, and
- writer's monotonic progress (no user-space reordering) + vectored writes.

## Errors & state

- `send()` returns an `io::Result<()>`. Any error means the underlying stream is in an unknown state; higher layers typically reconnect.
- Call `complete()` only after a successful `send()`. Calling it early yields an undefined stream state (by design).

## Multipart friendly

Pass a multipart buffer to `new()`:
```rust
use serde_multipart::Message;
let msg: Message = // serialized typed message
let body = msg.framed(); // impl Buf with chunks_vectored()
let mut fw = FrameWrite::new(writer, body, max_len).map_err(|(_, e)| e)?;
fw.send().await?; // writes [len][body][part0]...[partN] via writev
let writer = fw.complete(); // reuse for the next frame
```

## Oneshot convenience

```rust
// Send exactly one frame, then get the writer back.
let writer = FrameWrite::write_frame(writer, msg.framed(), max_len).await.map_err(|(_, e)| e)?;
```

## TL;DR

- **Send**:
`serde_multipart::serialize_bincode(M) -> Message → Message::framed() -> impl Buf`
- **Recv**:
- `FrameReader::next() -> Bytes → Message::from_framed(Bytes) -> Message → serde_multipart::deserialize_bincode::<M>(Message) -> M`

See [Typed Message Lifecycle](../appendix/lifecycle.md) for the full end-to-end walkthrough, including serialization, framing, cancel-safe I/O, and deserialization.
