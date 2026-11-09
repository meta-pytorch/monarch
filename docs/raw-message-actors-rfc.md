# RFC: A minimal raw message handler for Python actors

## Summary

Introduce an experimental `RawMessageActor` with one `handle(message)` method.
Messages contain decoded control headers, an opaque byte payload, and an optional
reply Port. Implement ordinary `Actor` on that foundation, and implement streaming
as a separate Python protocol library.

The goal is a small extension boundary for routing, codecs, and concurrency,
without adding a new endpoint decorator or adverb for every communication pattern.

## Motivation

A router that only selects a destination can return a Port or actor mesh and let
the caller send directly. That remains the simplest solution when the router
does not need to stay in the data path.

Other intermediaries must observe successive inputs, update actor state, and
change what they forward: batching records, merging inputs, compressing bytes, or
deduplicating messages. They should not need to reconstruct application objects
just to inspect routing metadata or forward an opaque body.

A specialized streaming endpoint could solve one case, but each additional
pattern risks another non-composable endpoint API. Instead, expose the common
message boundary and let libraries define their own protocols.

## Proposed foundation

The public surface is:

- `RawMessageActor.handle(message)`: one synchronous or asynchronous dispatch
  hook. Headers determine the protocol or operation; Python annotations do not
  generate handlers. Return values have no implicit reply meaning.
- `MessageEnvelope(headers, payload=b"", reply_port=None)`: control metadata,
  read-only bytes, and an optional existing Monarch Port. Mutable outgoing bytes
  are snapshotted; headers are shallow read-only snapshots.
- `send_message(target, message, *, selection=None)`: send or forward without
  endpoint argument binding or application serialization. The default requires
  a singleton target; broadcast or choose-one selection is explicit.
- `spawn_actor_task(actor, coroutine, *, name=None)`: actor-owned asynchronous
  work with shared failure and shutdown handling.

For example, a router can update its own sequence counter and the forwarded
headers without inspecting the application payload:

```python
class Router(RawMessageActor):
    def __init__(self, destination):
        self.destination, self.sequence = destination, 0

    def handle(self, message):
        self.sequence += 1
        send_message(self.destination, message.replace(headers={
            **message.headers, "sequence": self.sequence,
        }))
```

`replace` preserves the payload, reply capability, and private reference
attachments. The eventual receiver explicitly uses `message.reply_port.send(...)`
if the protocol calls for a reply. Sending alone does not acknowledge processing.

## Separate controls from application decoding

Control metadata and body bytes are independently framed. Receiving a raw
message decodes the controls only. Forwarding may re-encode changed headers, but
does not decode or repickle the application body.

The initial implementation uses Monarch's trusted Python serialization for
control capabilities. Application bytes can contain pickle, JSON, FlatBuffers, or
another encoding chosen by the receiver. For example, a JSON handler explicitly
calls `json.loads(bytes(message.payload))`; the raw runtime needs no codec change.
This does not establish a cross-language transport or a new trust boundary.

Reference tables and buffer ownership stay private. Exposing serialized bytes
must not silently discard the references needed to interpret them later.

## Ordinary actors and concurrency are extensions

`Actor` inherits `RawMessageActor`. The runtime actually invokes the instance's
`handle`; `Actor.handle` owns endpoint argument decoding, method dispatch, and
return-value/error replies. Initialization, actor context, supervision, and
lifecycle remain in the runtime.

An existing actor can inspect controls before delegating:

```python
class InspectingActor(Actor):
    def handle(self, message):
        self.last_method = message.headers["method"]
        return super().handle(message)

    @endpoint
    async def ping(self):
        return "pong"
```

This also composes with `concurrent_endpoint`. Both concurrent endpoints and raw
protocols use the same owned-task registry. Tasks are cancelled and awaited before
user cleanup; uncaught failures follow supervision. Handlers remain sequential
unless they explicitly start tasks, whose state updates may interleave at awaits.

## Streaming as a library

`monarch.actor.streams` supplies a client `request` helper and a server `handle`
helper. Its callback consumes a `ByteStream` solely through asynchronous iteration;
there is no `load()` operation. This shortened sink illustrates the boundary:

```python
class Sink(RawMessageActor):
    async def handle(self, message):
        await streams.handle(self, message, self.consume)

    async def consume(self, chunks, metadata):
        size = 0
        async for chunk in chunks:
            size += len(chunk)
        return size

size = await streams.request(sink, byte_source(), metadata={"batch": 7})
```

The snippets omit imports and mesh setup; `byte_source()` is an async iterable of
bytes. A router callback can pass its incoming iterator to another
`streams.request`, or wrap it in an async generator that transforms chunks and
updates state. The downstream result becomes the callback's result.

Open/cancel controls enter the single raw handler. The helper starts an owned
session task and returns promptly so further controls can be processed. Pull-based
flow control bounds production; the library owns response ports, cancellation,
error propagation, early termination, and iterator cleanup. Automatic replies
belong to this library, not to the raw foundation.

Initially, each request has one byte stream and up-front metadata. Metadata needed
by an intermediary goes up front; other metadata can be encoded with the payload.
Multiple streams and record framing are future library choices. Ordinary
`@endpoint.stream()` retains its existing meaning.

## Limits and compatibility

- This is not CastActor's native streaming fast path. Framing and forwarding can
  copy and buffer whole individual messages; byte-channel envelopes still use
  existing serialization. Chunking bounds each message, not the size of an
  application's own source allocations. Message ordering is unchanged.
- The prototype supports resolved mesh references and Ports in controls, but
  explicitly rejects pending header references and tensor-engine header
  references. Opaque forwarding of payloads with tensor-engine local state is
  also rejected. Ordinary Actor delivery retains its reference handling.
- Ordinary endpoint dispatch retains the original native payload buffer; merely
  inheriting from RawMessageActor does not add an unconditional byte snapshot.
  Reading its public payload view creates a lazy snapshot.
- An endpoint named `handle` conflicts with the new dispatch hook and is rejected
  explicitly. This compatibility decision needs review before adoption.

## Prototype and questions for review

A Python prototype is committed on `codex/stream-endpoints` at `0afa6ec20`.
It requires no Rust source changes. Validation passed 102 focused real-transport
tests, a subsequent 22-test task/concurrency run, both complete documentation
examples, and focused lint/type checks. This is not a full-repository validation
claim. The branch is a local prototype, not a merged or released API.

Questions for review:

1. Is this single-handler boundary sufficient for protocol libraries, or are any
   essential capabilities missing?
2. Should these names and the `handle` collision policy be the public contract?
3. Should deferred control references be supported before exposing the API?
4. Which performance needs justify native buffer forwarding or credit windows
   later, without expanding the initial public surface?
