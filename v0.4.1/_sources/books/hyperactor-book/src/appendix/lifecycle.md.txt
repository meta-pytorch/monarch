# Typed Message Lifecycle: Zero-Copy Serialization, Framing, and Cancel-Safe I/O

## Serialization (Typed → Logical Wire Format)
- Input: A typed message `M` (struct, enum, etc.) which may include `Part` fields for large binary blobs.
- Process:
  - Walk `M` with a custom `Serialize` impl.
    - Run `serde_multipart::serialize_bincode(&M)`, using a custom Serde+bincode serializer.
  - Emit the "body" as compact bincode into a contiguous `BytesMut` buffer.
  - For each `Part`, don’t write anything into the body; just push its backing `Bytes` into parts in visitation order. The body remains compact bincode without placeholders.
- Output: A `Message { body: Bytes, parts: Vec<Bytes> }`.

This is the heart of our zero-copy story: large payloads (`Part`) stay in their original `Bytes` allocations, and the body just carries lightweight references.

## Framing (Logical Message → Transport Frame)

- Input: Message from serialization.
- Process:
  - Wrap with length prefix (`u64`, big-endian): total byte length of serialized body + all parts.
  - Build a `Buf` that is multipart:
    - Slice #0: 8-byte length prefix.
    - Slice #1: body (compact metadata + placeholders).
    - Slice #2+: each part as-is.
- Key Property: Zero-copy – `Bytes` is reference-counted, no data copying when building the frame
  -  Representation: The framed value is a multipart `Buf`, not a single contiguous buffer. Internally it's a small deque of `Bytes` slices — `[len-prefix][body][part₀]…[partₙ]` — which implements `chunks_vectored()`. That means `FrameWrite` can hand the OS a vector of slices (`writev`) without coalescing, preserving zero-copy semantics end-to-end.

*Note: "Zero-copy" here refers to user-space: we still incur one unavoidable kernel copy on send (`writev`) and one on receive (`read` into `BytesMut`), but avoid any additional user-space coalescing/copying.*

## I/O (FrameWrite / FrameReader)

- Send (`FrameWrite`):
  - Accepts any `Buf` implementation.
    - Uses `poll_write_vectored` under the hood:
    - The OS sees multiple `IoSlice`S: `prefix`, `body`, `parts`.
    - Calls `writev` to push them in one syscall.
    - Benefit: No gather-buffering in userland; fully vectored writes.
- Receive (`FrameReader`):
    - Reads the length prefix.
    - Reads exactly that many bytes.
    - Returns a contiguous `Bytes` slice of the frame payload to the caller.

## Deframing & Deserialization (Transport → Typed)

- Input: Contiguous frame payload.
  - Process:
    - Split payload into body bytes + any parts (zero-copy): this is just slicing a shared buffer into smaller pieces (creating lightweight metadata views, not copying data).
    - Run custom `Deserialize`:
    - Reconstruct `M` by deserializing the compact bincode body and, whenever a `Part` field is visited, take the next entry from parts (same visitation order).
- Output: A fully-typed message `M`.

## Highlights

- **Zero-copy**: `Part`s (`Bytes`) are never copied—only referenced.
- **Vectorized I/O:** OS-level `writev` (send) and read (`recv`) avoid extra user-space copies
- **Extensible**: Supports unipart and multipart seamlessly.
- **Cancel-safe**: `FrameRead`/`FrameWrite` can be canceled mid-poll and resumed without corrupting state.


## TL;DR

- **Send:** `serde_multipart::serialize_bincode(M) -> Message → Message::framed() -> impl Buf`
- **Recv:** `FrameReader::next() -> Bytes → Message::from_framed(Bytes) -> Message → serde_multipart::deserialize_bincode::<M>(Message) -> M`
