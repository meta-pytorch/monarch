# Chunked request endpoints

Status: implementation specification for an experimental Python API.

## Problem and scope

A routing actor sometimes needs to observe or transform a request as it passes
through, while retaining application state. Ordinary endpoint calls materialize
the request's Python object at every hop. Applications can instead serialize the
object once and forward chunks of its serialized representation. This proposal
packages that pattern as an endpoint decorator and messaging adverbs using
existing Monarch channels and actor lifecycle support.

The first implementation is Python-only. It does not change Rust transports,
delivery guarantees, sequence allocation, or CastActor. It does not promise
zero-copy transfer: ordinary Monarch messages still encode/decode their bytes
and control envelopes. It avoids reconstruction of the original application
object at intermediate actors and avoids whole-request accumulation there.

If no transformation or observation is needed, returning a destination Port or
actor mesh reference is still preferable because it removes the payload hop.

## Public API

Export `stream_endpoint` and `ByteStream` from `monarch.actor`. A stream endpoint
is an async method with exactly one input, a `ByteStream`, and one normal return
value. Reject synchronous methods and async-generator methods at decoration
time; async generators are supported as input producers, not endpoint bodies.

```python
from monarch.actor import Actor, ByteStream, stream_endpoint


class Consumer(Actor):
    @stream_endpoint
    async def ingest(self, chunks: ByteStream) -> int:
        request = await chunks.load()
        return len(request)


class Router(Actor):
    def __init__(self, destination):
        self.destination = destination
        self.bytes_forwarded = 0

    @stream_endpoint
    async def ingest(self, chunks: ByteStream) -> int:
        async def observe():
            async for chunk in chunks:
                self.bytes_forwarded += len(chunk)
                yield chunk

        return await self.destination.ingest.stream_chunks(observe())


# Both router and destination are singleton actor mesh selections.
result = await router.ingest.stream(large_request, chunk_size=64 * 1024)
```

A pass-through endpoint can simply return
`await self.destination.ingest.stream(chunks)`. Passing an incoming `ByteStream`
to `stream` forwards its bytes without serializing the iterator or decoding the
application value.

### Client handle

The decorator binds a distinct client handle with:

- `stream(message, *, chunk_size=64 * 1024) -> Future[R]`: serialize an ordinary
  object once using cloudpickle, then send its bytes incrementally. An incoming
  `ByteStream` is forwarded instead of serialized.
- `stream_chunks(chunks: AsyncIterable[bytes], *, chunk_size=64 * 1024)
  -> Future[R]`: send an existing byte producer without object serialization.
  Split oversized elements into messages no larger than `chunk_size`.

These calls select exactly one actor, using the existing `call_one` validation.
Reject invalid chunk sizes before dispatch. Use Monarch `Future` and
`returns_future` conventions so callers can await or use `.get()` from synchronous
code. Document when serialization starts; the preferred behavior is lazy work on
first observation, on the observing caller's loop.

Do not change ordinary `@endpoint` adverbs. In particular, its existing
`.stream()` continues to broadcast and yield per-actor replies. The experimental
stream endpoint's `.stream()` has a different, explicitly documented meaning:
a streamed request and one final response. Do not expose ordinary broadcast or
fanout adverbs on this new handle accidentally through a generic proxy.

### ByteStream

`ByteStream` is a single-consumer async iterator yielding `bytes`. It additionally
supports:

- `await chunks.read() -> bytes`: collect the remaining bytes, explicitly.
- `await chunks.load() -> object`: collect and deserialize the remaining bytes.

Neither operation is called implicitly by routing code. `load()` requires an
intact serialized object; applying a byte transformation may require a matching
inverse operation at the consumer. It is only appropriate for trusted pickle
payloads, as with ordinary Python object serialization.

Only one read may be outstanding. Reject overlapping reads rather than silently
misassociating responses. EOF is stable: subsequent reads terminate without
sending more requests. Distinguish EOF from an empty byte chunk.

This API uses explicit cloudpickle semantics. Do not claim transparent support
for Monarch tensor-engine references or its pending mesh-reference machinery.
Those would require a subsequent design to preserve out-of-band reference
tables. The source may hold the full serialized object; streaming serialization
itself is outside the initial scope.

## Protocol and bounded buffering

Use an existing endpoint invocation to open one logical stream session and carry
its final response. Use existing `Channel`/`Port` pairs for chunks, so reading
chunks does not enqueue another actor endpoint behind the currently running
handler. Each session has isolated channels and an opaque unique session ID.

A minimal pull protocol is sufficient:

1. The caller opens a channel for demand and starts the destination's session
   invocation, passing the demand Port and session ID.
2. The receiver constructs a ByteStream and opens a data-response channel.
3. Each iteration sends a pull containing the response Port. The caller's pump
   reads at most one bounded chunk from its producer and replies through it.
4. A dedicated EOF value terminates iteration. Source exceptions are sent through
   the data-response channel as errors and raised by the iterator.
5. The endpoint's normal return or exception travels through the original
   endpoint response mechanism. It is not chunked.

Reuse the data-response channel within the session. The initial implementation
can permit one outstanding pull per hop. It must not eagerly enqueue the whole
request, create a list of all chunks, or accumulate all chunks in a relay.
Creating each bytes slice may copy that chunk; document this limitation rather
than adding Rust buffer machinery to this change.

For a chain, downstream demand drives the relay's producer, which in turn pulls
from upstream. This bounds intermediary buffering to a small number of chunks
per hop, aside from application-owned state. One-pull-at-a-time has a per-chunk
round-trip cost. A bounded credit window is a future performance improvement,
not a claim of this version. Chunk delivery retains existing ordering.

## Scheduling, termination, and failure

Use existing concurrent-endpoint lifecycle support so a blocked stream does not
prevent another session or its cancellation control message from being handled.
Document that stream endpoint invocations may overlap. Actor state updates are
ordinary asyncio updates; users must account for interleaving at awaits.

Every invocation must settle once with a normal result or error. A normal early
return is allowed and stops the upstream producer. An exception at a downstream
hop propagates through awaiting relay calls. No automatic retry is introduced.

Cancellation needs an explicit protocol; cancelling a local wait does not by
itself cancel a remote actor invocation. A workable implementation uses a cancel
control message on the same underlying concurrent endpoint, keyed by session ID:

- Keep the final-response task alive while cancelling, send the cancellation
  request, and observe terminal settlement before releasing its receiver where
  practical. Avoid sending terminal replies to an intentionally discarded port.
- The receiver cancels the matching handler task, unwinds async generators, and
  reports a normal stream-cancellation error without failing the actor.
- Forwarding calls propagate cancellation to their downstream sessions.
- Cancellation of an already completed session is harmless and must not create
  retained tombstones.
- Actor shutdown uses existing concurrent-endpoint task cleanup. Distinguish
  user-requested stream cancellation from actor-shutdown cancellation.

The implementation agent should refine this protocol if tests expose races.
Do not leave blocked pumps or background tasks after early return, source error,
remote error, cancellation, or actor shutdown. Close owned async generators when
the session ends. A caller must not reuse a consumed/closed incoming ByteStream.
Underlying process and network failures follow existing Monarch supervision and
delivery behavior; do not claim durable/exactly-once sessions or new failure
detection guarantees. Cancellation is cooperative, not a hard deadline for an
unresponsive process.

## Implementation layout

1. Add `python/monarch/_src/actor/stream.py` for the stream protocol, ByteStream,
   client handle, decorator, and private endpoint-property subclass.
2. Add a narrow client-binding hook on `EndpointProperty`, used when ActorMesh
   constructs endpoint handles. The default returns the existing ActorEndpoint
   unchanged. The stream subclass wraps it with the stream-specific client.
   Two preliminary edits implementing this hook are already in the worktree;
   they are uncommitted and may be revised.
3. Reuse `concurrent_endpoint` rather than inventing separate actor task cleanup.
   Avoid circular imports; follow the existing decorator modules' structure.
4. Export the public decorator and iterator. Add user-facing documentation with
   a runnable chain example and the ordering, concurrency, memory, serialization,
   response, and performance limitations above.
5. Add focused unit and multi-process integration tests. No Rust rebuild should
   be needed.

## Validation and acceptance criteria

Use `.venv/bin/python` and the repository's pytest configuration. Monarch imports
successfully from this checkout in that environment. Inspect applicable
repository instructions and use existing isolation/test fixtures.

Required behavior tests:

- Object round trip through one endpoint, including `.get()` where supported.
- Three-process sender/router/consumer chain, with the downstream result returned
  to the original caller.
- A pickle reconstruction probe proving the application object is decoded only
  by the final consumer, not by the router.
- Raw async-generator input, empty input/chunks, exact and non-exact chunk sizes,
  and an oversized producer element.
- Incremental forwarding: the final consumer receives a first chunk before the
  producer is allowed to finish; use gates, not timing assertions.
- Bounded pulling: a gated consumer prevents the source from exhausting its
  producer, including through a relay.
- Overlapping invocations stay isolated and reject simultaneous reads on one
  ByteStream.
- Source exceptions, destination exceptions, and early return stop production
  without failing otherwise healthy actors or leaking tasks.
- Cancellation before the first chunk and during forwarding, plus shutdown with
  an active stream. A healthy endpoint remains callable after stream errors and
  user cancellation.
- Multi-actor selections and malformed configuration fail clearly.
- Existing ordinary `.stream()` and endpoint/concurrent endpoint behavior remain
  unchanged.

Run focused new tests plus relevant existing endpoint, port, and concurrent
endpoint tests. Run available formatter/lint and focused type checks. Report
environment limitations accurately and do not replace real transport tests with
only mocks. No throughput claims without measurements.

## Delivery plan

Commit this specification separately. Delegate implementation to a
`gpt-6.1-sol` subagent, which should implement, test, document, and report changed
files and validation results. The primary agent reviews the result and handles
any necessary follow-up. Keep this specification aligned with the final API if
implementation evidence requires changes.
