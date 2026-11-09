# Raw message actors and protocol libraries

Status: implemented experimental Python foundation and protocol library.
Supersedes the public stream-endpoint design in `stream-endpoints-plan.md`.
Concrete API and focused validation are recorded below.

## Objective

Make one message handler the foundation of Python actors. A raw actor receives
decoded control headers and an opaque byte payload. Receiving, inspecting headers,
and forwarding must not deserialize or repickle the application payload. Endpoint
dispatch, application codecs, concurrency, and streaming are composable layers.
Implement existing `Actor` on this foundation, preserving its existing public
endpoint behavior. Refactor the prototype into a streaming library rather than
adding another EndpointProperty or endpoint adverb.

This changes the public experimental design on this branch; it need not preserve
the newly introduced `stream_endpoint` API. Preserve established Monarch APIs.
Keep all commits on `codex/stream-endpoints`. Implementation is delegated to
gpt-6.1-sol; the primary agent reviews it.

## Minimal public foundation

Export `RawMessageActor`, `MessageEnvelope`, and `send_message` from
`monarch.actor`. Names may be refined if existing APIs collide, but keep this
surface small and document the final names.

`RawMessageActor` provides actor lifecycle, supervision, and exactly one user
dispatch hook: `handle(self, message: MessageEnvelope)`. A raw subclass performs
dynamic dispatch on headers. Do not generate typed handlers or route messages
using Python parameter annotations. A synchronous handler is allowed; an async
handler is allowed on an async actor. Its return value is not a response.

The envelope has three conceptual parts:

- `headers`: decoded control metadata (protocol/version/operation and optional
  application metadata). Treat it as immutable or snapshot it when sending.
- `payload`: read-only bytes or a read-only buffer view; the runtime never invokes
  an application decoder simply to deliver it. Mutable outgoing buffers must be
  snapshotted or rejected explicitly.
- `reply_port`: optional transferable `Port` for the application or protocol
  library to use explicitly. Forwarding can retain the original port. A raw
  handler has no implicit return-value reply or automatic request aggregation.

Use ordinary supported Port capabilities rather than exposing bare Rust port
IDs, mailbox internals, unsafe buffer lifetimes, or method-specifier variants.
If implementation stores reply_port in encoded control headers, that is private.
The runtime does not infer cancellation, response count, or completion from it.

`send_message(target, envelope, ...)` sends to a raw actor/actor mesh without an
EndpointProperty, argument binding, or application serialization. Reuse existing
mesh selection/delivery semantics, specifying selection explicitly for fanout;
prefer singleton validation by default. Sending is not an acknowledgement of
processing. The same operation can forward a received envelope unchanged or with
explicitly replaced headers. Do not add a family of raw messaging adverbs.

Illustrative application (final naming may differ):

```python
class JsonCounter(RawMessageActor):
    def handle(self, message: MessageEnvelope) -> None:
        if message.headers["operation"] != "count":
            raise ValueError("unknown operation")
        value = json.loads(bytes(message.payload))  # deliberate codec choice
        message.reply_port.send(len(value))


reply, receiver = Channel[int].open(once=True)
send_message(counter, MessageEnvelope(
    headers={"operation": "count", "content_type": "application/json"},
    payload=json.dumps([1, 2, 3]).encode(),
    reply_port=reply,
))
result = await receiver.recv()
```

## Separate header decoding from the opaque body

Do not pickle an envelope containing the application object and then unpickle it
in the raw receive path. Headers and payload must be independently framed. It is
acceptable initially to use Monarch pickling for Python control capabilities in
headers, with a length-delimited opaque payload after them. Validate framing and
header lengths before constructing views. State the control serialization policy;
support for JSON application payloads is not a claim of a cross-language transport.

The existing Rust-to-Python dispatch already provides FrozenBuffer, method and
reply metadata, local state, and mesh-reference tables before `_Actor._prepare`
unpickles. Split delivery there. Existing Buffer/PythonMessage/mesh casting may
support the implementation without native changes. A private discriminator/frame
is acceptable; do not implement raw messages by calling an ordinary endpoint and
unpickling a tuple containing the payload. If a small native change is necessary,
document it and validate the actual resulting extension, not only Python mocks.

Opaque means no application reconstruction or encoding on intermediate hops.
Copies caused by framing or buffers may remain; do not claim zero-copy or changed
message ordering. A whole received wire message may still be buffered. Incremental
transfer of a larger logical payload belongs to the streaming library.

Preserve reference attachments for existing Monarch endpoint payloads. Never
detach pickle bytes from their local/tensor/mesh-reference state and then decode
or forward them as if those attachments did not exist. Keep attachments owned
privately by an envelope/payload carrier. If a particular opaque forwarding case
cannot preserve them, reject it clearly and document the boundary rather than
silently dropping references. Public callers must not construct attachment tables.

## Existing Actor is an actual extension

Make `Actor` inherit `RawMessageActor`. Move ordinary method dispatch and payload
unpickling behind `Actor.handle` (possibly through a private dispatcher helper).
The runtime must genuinely invoke that handler for ordinary endpoint messages;
merely changing inheritance while keeping an independent endpoint dispatch path
does not meet this requirement.

The raw runtime keeps initialization, actor context, supervision, queue/lifecycle
bookkeeping, and delivery-error handling. Administrative initialization may retain
the existing serialization. The Actor layer owns endpoint argument decoding,
method lookup, explicit-response-port behavior, return-value replies, and normal
endpoint error conversion. Preserve tracing, debugger behavior, sync/async actor
kind validation, pending references, endpoint adverbs, and exception semantics.

Existing `Actor` subclasses should not have to call a new base constructor.
Support existing synchronous endpoints: do not force every actor onto an async
loop simply because the base now has a handle. A synchronous Actor.handle may
return an awaitable for asynchronous endpoint dispatch, if needed. The runtime
adapter must distinguish this from plain sync raw handlers correctly.

Prove composition with a subclass that inspects the envelope headers and delegates
ordinary endpoint messages to `super().handle(message)`. Include cases with
`@endpoint` and `@concurrent_endpoint`. Identify any handle-name collisions with
legacy endpoint names and preserve them where practical or fail explicitly.

The foundation does not choose a global application codec. JSON and future
FlatBuffers handlers interpret payload bytes themselves. Demonstrate JSON on the
same raw transport with no core dispatch changes. A full codec registry, generated
schemas, or FlatBuffers dependency is unnecessary. Standard Actor keeps today's
Monarch pickle/reference semantics; codec-specific endpoint layers can be added
without changing RawMessageActor.

## Generic actor-owned tasks and concurrent_endpoint

Factor actor-owned background task tracking/cleanup out of the endpoint-specific
concurrency implementation. Provide one small reusable operation (for example
`spawn_actor_task(actor, coroutine, ...)`) usable by raw protocol libraries and
existing concurrent endpoints. Reuse a common task registry and shutdown behavior.

The operation must track failures and cancel/await owned tasks before user cleanup.
Uncaught failures must not disappear. A protocol that handles exceptions and sends
them through a reply port can do so; otherwise failure follows actor supervision.
Preserve concurrent_endpoint's existing start-order, exception, and shutdown
contracts. Per-method endpoint tracing/reply behavior can remain in its wrapper.

A raw handler normally processes messages sequentially. A long-lived protocol
starts owned tasks and returns so cancellation/control messages can be dispatched.
Do not implicitly make every handler concurrent. Document that explicit tasks may
interleave actor state updates at awaits.

## Streaming is a library above raw delivery

Remove the prototype's custom EndpointProperty, annotation-based argument binding,
and new stream endpoint export. Revert the prototype-only EndpointProperty._bind
hook if no longer needed. Retain the demand-driven chunk protocol, iterator-only
ByteStream, cancellation races, and closure fixes where useful.

Expose an ordinary library interface, preferably a small module such as
`monarch.actor.streams`, with a client request helper and a server dispatch helper.
For example (implementation may refine names):

```python
class Router(RawMessageActor):
    async def handle(self, message: MessageEnvelope) -> None:
        if message.headers["protocol"] == streams.PROTOCOL:
            await streams.handle(self, message, self.forward)
        else:
            raise ValueError("unknown protocol")

    async def forward(self, chunks: ByteStream, metadata):
        destination = self.choose_destination(metadata["route_key"])

        async def transform():
            async for chunk in chunks:
                self.bytes_forwarded += len(chunk)
                yield chunk

        return await streams.request(destination, transform(), metadata=metadata)


result = await streams.request(router, producer(), metadata={"route_key": "training"})
```

Here `streams.handle` handles open/cancel messages and promptly schedules a session
through the generic owned-task facility. The callback's result/error convention
belongs to this request library, not to RawMessageActor. The caller helper owns
its response port, source pump, cancellation, and settlement. ByteStream itself
has async iteration and lifecycle operations only; no read/load helper.

One stream per request and ordinary up-front metadata remain the initial library
contract. The raw foundation must not enforce that restriction: other libraries
may design different protocols. Control headers carry capabilities/session IDs;
bulk chunks remain bytes on existing channels or raw messages. Existing byte
channel envelope encoding is acceptable and must be documented honestly.

Prefer an explicit async byte producer as the library input. Optional object
serialization is a separate source helper or application code, not a payload
codec built into raw delivery. Preserve bounded demand, empty-chunk vs EOF,
overlapping-read rejection, downstream error propagation, early return, lazy
client Future/get support where retained, and cancellation/actor-shutdown cleanup.
Ordinary @endpoint.stream must remain unchanged.

## Validation

Keep real multi-process tests and the existing environment's usable Rust library.
Required coverage:

- Raw headers available without payload decoding: pass arbitrary non-pickle bytes
  and a serialized reconstruction probe through a router. Only the intended final
  consumer explicitly invokes a decoder. Forwarding retains payload bytes/reply.
- Framing errors and immutable/snapshotted payload behavior; no payload object is
  secretly pickled into control headers.
- JSON and a byte-oriented non-JSON handler on the same foundation.
- Genuine Actor subclass delegation through handle, sync and async endpoints,
  explicit replies/errors, references, ordinary stream adverb, and concurrent
  endpoints. Run relevant existing actor/endpoint/port/lifecycle tests.
- Raw generic tasks and concurrent_endpoint share correct cleanup/failure policy.
- Streaming library works on a RawMessageActor with no endpoint definitions:
  sender/router/consumer, incremental/bounded production, metadata before chunks,
  independent requests, early return, source/remote errors, cancellation before
  and after the first chunk, timeout, local actor definitions, and shutdown.
- Avoid replacing functioning test scenarios with superficial mocks. Adapt the
  prototype tests to library API; remove obsolete annotation tests.
- Execute documented examples verbatim. Run focused lint/type checks and diff
  checks. Record exact results, environmental limitations, and copy/ordering
  limitations. Do not claim full-suite success if only targeted suites passed.

## Delivery

Commit this plan before implementation. Delegate the refactor to gpt-6.1-sol.
Keep implementation changes uncommitted until primary review, then commit them
on the existing feature branch. Update this design with concrete final API names
and validation results. Mark the prior prototype plan as superseded and replace
its public guide/API documentation with the raw foundation plus library examples.

## Concrete implementation

Public foundation: `RawMessageActor`, `MessageEnvelope`, `send_message`, and
`spawn_actor_task` are exported by `monarch.actor`. `MessageEnvelope(headers,
payload=b"", reply_port=None)` snapshots outgoing mutable buffers and exposes
read-only headers/payload. `replace(headers=...)` retains payload/reply/attachments.
`send_message(target, envelope, selection=None)` requires one actor by default;
explicit `selection="all"` or `"choose"` uses existing selection semantics. Raw
handler return values are ignored. `spawn_actor_task(actor, coroutine, *,
name=None)` is the small public task operation; endpoint-specific tracking flags
remain private.

`Actor(RawMessageActor).handle` genuinely owns normal endpoint decode/dispatch
through a private dispatcher. The runtime invokes the instance's handler for
ordinary and raw messages. It still owns Init, context, queues, supervision, and
cleanup. Ordinary endpoint delivery retains the original FrozenBuffer and both
reference tables. A user inspection of its payload makes a lazy snapshot using
an independent buffer cursor; it does not consume the Actor decoder's state.
The generic registry cancels/awaits owned tasks before user cleanup and is shared
by concurrent_endpoint and the stream library. Raw handlers/owned tasks receive
execution bookkeeping, while ordinary endpoint tokens are not double counted.
Endpoint name `handle` is rejected explicitly as a dispatch-hook collision.

Raw frames have a private magic/version, control byte length, and control mesh-ref
count, followed by trusted Monarch-pickled headers/reply capability and opaque
body bytes. The wire reference table contains header refs followed by body refs;
receiving splits them before decoding controls. Framing validates lengths and
reference-table bounds before slicing. No application payload decoder runs in
raw delivery or forwarding. Control capabilities/Ports and byte-channel envelopes
still use Monarch pickle; application JSON in the guide is a distinct codec,
not a cross-language transport guarantee.

The prototype preserves resolved header mesh references. Pending header refs are
collected safely with allow_mesh_references=True, detected by try_resolve_now,
and rejected with initialization guidance rather than invoking blocking fallback
reducers. Tensor-engine header references and opaque forwarding of nonempty
received tensor local_state are explicitly rejected. Standard Actor reference
semantics remain supported. Fully deferred control headers would require an
additional private state-composition helper and are future work.

The send path uses the existing queued cast_unresolved(PendingMessage) primitive
with an already-composed state; AsyncActorMesh.cast itself is unimplemented. No
native source changes or rebuild were needed. Raw framing snapshots FrozenBuffer
contents into immutable Python bytes before making public views. This avoids an
existing native buffer-export format hazard observed when passing a
memoryview(FrozenBuffer) into Buffer.write. Ordinary Actor delivery does not make
that unconditional copy. Framing/forwarding and requested payload inspection may
copy whole messages; zero-copy and throughput claims are outside this change.

Streaming is `monarch.actor.streams`: `PROTOCOL`, `ByteStream`,
`request(target, async_byte_source, *, metadata=None, chunk_size=65536)`, and
`await handle(actor, envelope, callback)`. The callback receives `(chunks,
metadata)` and runs as an explicitly owned session task. Open/cancel handling
returns promptly. Dynamic application operation lookup belongs inside the
callback, so cancel controls do not require application metadata. There is no
stream EndpointProperty, annotation binding, new endpoint adverb, or implicit
application serialization. The prototype-only binding hook/export were removed.

The library retains one-pull-per-hop flow control, stable EOF, empty chunks,
oversized-element splitting, read overlap rejection, explicit remote cancellation,
settlement before releasing reply receivers, native receive self-wakeup, and
exactly-once owned iterator closure. Generic task completion also settles sessions
cancelled before their first step. Source/callback errors and early returns close
sources; actor shutdown settles active sessions and cleans tasks before hooks.
ByteStream remains iterator-only, with aclose for lifecycle and no read/load API.

## Validation record (2026-10-19)

A combined real-transport run passed **102 tests** in **78.84 seconds**:
`test_streams.py`, both raw acceptance files, `test_concurrent_endpoints.py`,
`test_port_forward.py`, `test_port_reconstruction.py`, `test_sync_actor_driver.py`,
and selected established `test_python_actors.py` endpoint/mesh/cleanup tests,
excluding `oss_skip`. It includes 25 stream tests and 16 complementary raw tests.
Coverage includes JSON/non-pickle bytes, reconstruction probes, header/body mesh
and Port attachments, pending control rejection, mutable payload snapshots,
framing errors, real Actor delegation, shared task cleanup/failure supervision,
sync driver behavior, existing stream adverb, cancellation, and active shutdown.
Both complete examples in `docs/source/actors.md` were extracted verbatim and
executed successfully with `.venv/bin/python`. After isolating internal task
tracking arguments behind the private helper, the concurrent/raw-owned-task
selection passed another **22 tests** in **9.97 seconds**.

Focused Ruff and Pyrefly checks cover the new modules, concurrency wrapper,
public exports, and acceptance tests. Broader Pyrefly checking of existing
actor_mesh/proc_mesh still reports six pre-existing issues (Meta __manifest__
imports, native stub typing, optional setup callback typing, and dynamic marker
attributes). This is not a full-repository typecheck claim. The checkout has no
./check script. Test warnings are existing pytest AST deprecations.

Tests use the existing compatible `target/release/lib_rust_bindings.so` copied
into ignored `python/monarch/_rust_bindings.cpython-312-x86_64-linux-gnu.so`.
The prior ignored extension remains backed up at
`/tmp/monarch-stream-old-bindings.so`. This is a local environment adjustment,
separate from committed source changes. No Rust rebuild was performed.
