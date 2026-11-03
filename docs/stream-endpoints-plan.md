# Chunked request endpoints

Status: superseded by [Raw message actors and protocol libraries](raw-message-actors-plan.md).
This records the first prototype and its validation; its experimental endpoint
API is being replaced by a library on the raw message-handler foundation.

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
is an async method with exactly one parameter annotated `ByteStream`, any number
of ordinary parameters, and one normal return value. No invocation has multiple
streamed arguments. Reject synchronous methods and async-generator methods at decoration
time; async generators are supported as input producers, not endpoint bodies.

```python
from monarch.actor import Actor, ByteStream, stream_endpoint


class Consumer(Actor):
    @stream_endpoint
    async def ingest(self, chunks: ByteStream, *, route_key: str) -> int:
        total = 0
        async for chunk in chunks:
            total += len(chunk)
        return total


class Router(Actor):
    def __init__(self, destination):
        self.destination = destination
        self.bytes_forwarded = 0

    @stream_endpoint
    async def ingest(self, chunks: ByteStream, *, route_key: str) -> int:
        async def observe():
            async for chunk in chunks:
                self.bytes_forwarded += len(chunk)
                yield chunk

        return await self.destination.ingest.stream_chunks(
            observe(), route_key=route_key
        )


# Both router and destination are singleton actor mesh selections.
result = await router.ingest.stream(
    large_request, route_key="training", chunk_size=64 * 1024
)
```

A pass-through endpoint can simply return
`await self.destination.ingest.stream(chunks, route_key=route_key)`.
Passing an incoming `ByteStream`
to `stream` forwards its bytes without serializing the iterator or decoding the
application value.

### Client handle

The decorator binds a distinct client handle with:

- `stream(*args, chunk_size=64 * 1024, **kwargs) -> Future[R]`: bind the endpoint's
  Python signature and serialize the argument corresponding to `ByteStream`
  once using cloudpickle, then send its bytes incrementally. An incoming
  `ByteStream` is forwarded instead of serialized. Other arguments are ordinary
  Monarch values in the opening invocation, available before reading any chunks.
- `stream_chunks(*args, chunk_size=64 * 1024, **kwargs) -> Future[R]`: the same
  binding, but the streamed argument must be an `AsyncIterable[bytes]`. Send its
  bytes without object serialization. Split oversized elements into messages
  no larger than `chunk_size`.

The streamed argument may be positional or keyword-only; ordinary positional,
keyword, and default argument behavior follows Python signature binding. Require
one explicitly annotated, non-variadic ByteStream parameter without a default.
Reject missing or multiple stream annotations. Reserve `chunk_size` for the
adverb and reject a declared endpoint parameter with that name. Annotation
inspection must identify the stream without imposing annotation requirements on
ordinary metadata. A client may supply an object where the handler sees a
ByteStream, so runtime binding does not imply that a standard Python type checker
can automatically transform arbitrary client signatures.

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

`ByteStream` is a single-consumer async iterator yielding `bytes`. Do not expose
`read()` or `load()` or another whole-stream materialization helper. The receiver
processes chunks incrementally. `aclose()` is a lifecycle operation, not a read
operation. The public examples must demonstrate incremental processing.

Metadata needed by a router is an ordinary argument sent up front. When metadata
is needed only at the final consumer, applications can pair it with payload
records and serialize that combined stream themselves. This API does not zip
separate streams or define a record format. Transport chunks need not align with
application records; applications using record streams supply framing and an
incremental decoder. A monolithic cloudpickle from `.stream(object)` is a byte
stream, not automatically an incrementally decoded stream of Python objects.

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
   invocation, passing the demand Port, session ID, and bound ordinary arguments.
   The streamed application value is not also included in that envelope.
2. The receiver constructs a ByteStream, opens a data-response channel, and
   inserts the ByteStream into the declared parameter when invoking the handler.
   Metadata is immediately available even when the source has produced no chunks.
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
  Test-only application code may explicitly collect and decode to verify pickle
  behavior; the public iterator must have no collection/deserialization helpers.
- Ordinary positional and keyword metadata, defaults, a keyword-only streamed
  argument, and forwarding with metadata. Prove metadata can be used before the
  first source chunk; reject ambiguous annotations and invalid bindings before
  remote dispatch.
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

## Argument semantics and scope

Which argument streams is explicitly declared, not selected dynamically by size.
Even a tiny value supplied for that parameter appears as a ByteStream at the
receiver. All other values are up-front metadata; unexpectedly large metadata is
not silently promoted to a stream. Serialization of the payload separately from
the metadata does not preserve object identity or aliases across that boundary.
Multiple stream parameters, automatic zipping, typed object-loading wrappers,
and size-based inline optimizations are outside this implementation.

## Implementation refinements

The client keeps returns_future's lazy execution and synchronous get behavior.
Its stream-specific asyncio observer requests invocation cancellation, then waits
for cleanup rather than cancelling its own observer task immediately. This also
covers cancellation before the observer's first step. Ordinary endpoint Futures
retain their existing cancellation behavior.

Channel receives run on Tokio and survive cancellation of an asyncio observer.
A cancelled stream read wakes its pending receive through its own private port
and observes settlement before releasing the channel. Private demand/data ports
discard late packets after terminal settlement to prevent undeliverable replies
from races with in-flight pulls. The original endpoint response stays observed
while the receiver handles the explicit cancel control. Unknown or completed
session IDs allocate no retained cancellation state. The invocation owns producer
closure and invokes an available aclose exactly once.

## Validation record (2026-10-19)

- Final combined transport run: `.venv/bin/python -m pytest
  python/tests/test_stream_endpoints.py python/tests/test_concurrent_endpoints.py
  python/tests/test_port_forward.py python/tests/test_port_reconstruction.py -q
  -m 'not oss_skip'`: **59 passed** in 34.40 seconds, including 27 new stream tests.
  Reported warnings came from pytest assertion rewriting's deprecated AST APIs.
- Supported ordinary endpoint/adverb selection in `test_python_actors.py`:
  **15 passed**, 49 deselected, including the existing ordinary `.stream()` test.
- The complete incremental three-process example in `docs/source/actors.md`
  was extracted verbatim and ran successfully with `.venv/bin/python`.
- Focused Pyrefly checks passed for the new stream module/tests, endpoint binding
  hook, and public exports. Available Ruff formatting and E/F/I lint checks
  passed, as did `git diff --check`. This checkout has no `./check` script.

The tested local environment initially loaded an obsolete ignored Python Rust
extension. Validation used the existing compatible
`target/release/lib_rust_bindings.so`, copied into the ignored
`python/monarch/_rust_bindings.cpython-312-x86_64-linux-gnu.so` location. The old
extension was backed up at `/tmp/monarch-stream-old-bindings.so`. This was a local
runtime adjustment, not a committed source change or a Rust rebuild.

An exploratory broader actor run included OSS-unsupported tests and was stopped
after 101 passes and three environment-only failures (missing IPython in two
logging tests and unavailable packaged test-bin context in another). The final
focused runs above exclude `oss_skip` tests. Full repository typechecking was not
claimed: broader actor_mesh Pyrefly checking reports existing unrelated errors,
and the local Pyright launcher/interpreter discovery is stale.
