# Host identity discovery over duplex

A Host frontend address identifies an endpoint. Its concrete service `ProcId`,
such as `service<gVQHWd9h6Km>`, identifies one Host incarnation. A default Host
keeps that ID throughout its lifetime and allocates a fresh ID when restarted
at the same endpoint. Retained concrete references continue to name the old
incarnation; a proc or actor label cannot redirect them to its replacement.

Clients that know only the frontend discover its service proc on a fresh duplex
session:

```rust,ignore
use hyperactor_mesh::host::query_service_proc;

let service = query_service_proc(frontend).await?;
```

The resulting `ProcAddr` contains the learned instance ID and the normalized dial
location. Discovery is an identity snapshot, with actor readiness managed by the
caller. It registers no Gateway peer and changes no caller route or advertised
location.

## Why discover the instance

Reusing a service ID across restarts would also reuse its actor identities and
per-destination sequencing state. A fresh instance ID gives the replacement new
concrete destinations, while stale references retain their original meaning.
Label lookup and a shared `service` singleton cannot uniquely identify several
Hosts or distinguish successive incarnations at one frontend.

The Host knows its immutable service ID before binding the frontend. It supplies
that ID to the listener directly. Each frontend owns an optional advertisement;
several frontends sharing a Gateway can advertise different concrete IDs. A
local proc with the same label has no role in answering the request. A generic
Gateway frontend can omit the advertisement and return an explicit unavailable
reply.

The protocol uses existing duplex sessions and TLS configuration. It adds no
transport kind or framing format. This also permits discovery and Gateway
attachment on the same connection without a separate query transport.

## Session protocol

Every accepted duplex session starts in setup. Existing sessions can carry
ordinary traffic while a new session is still identifying or attaching.

| Setup input | Behavior |
| --- | --- |
| `IdentifyService` | Reply with this frontend's instance ID, or an explicit unavailable error; remain in setup. |
| First `AttachRequest { uid }` | Register its peer route and reply with the assigned via location; remain in setup. |
| Further `IdentifyService` | Return the same immutable ID, including after attachment. |
| Second `AttachRequest` | Reject the duplicate and close the session without replacing another registration. |
| First ordinary actor envelope | Deliver it exactly once and hand the remaining receiver to ordinary routing. |
| EOF, cancellation, attachment rejection, or malformed control | Release the session and its peer guard. |

Control envelopes use their stored type hash for classification. Only a matching
control type is deserialized; an ordinary payload stays opaque. Placeholder
control addressing is consumed during setup and never becomes a routing alias.
After the first ordinary envelope, the server uses the normal receiver path and
performs no continuing payload inspection for late controls. Late controls
cannot reopen setup or mutate the peer registration.

The existing `AttachRequest`, `AttachAck`, and ordinary envelope representations
are preserved. The identity response is appended after the existing `Ack` and
`Envelope` variants of `AttachWire`, preserving their variant ordering. A client
contacting an older/nonresponding endpoint has a bounded setup deadline and
cleans up its private session.

## Client ownership and interleaved traffic

The client fixes its control plan before sending ordinary outbound traffic on
the new session. It matches replies in request order and fails on the wrong
reply kind, rejection, malformed recognized control, or non-instance identity.

The peer route is registered before `AttachAck`, so server-to-client actor
envelopes can arrive before or between replies. They do not satisfy pending
control operations. The setup owner buffers an ordered prefix with a finite
limit of 1024 envelopes. After successful setup, it serves that prefix before
reading further ordinary data. Failure or caller cancellation routes the
already consumed prefix through the Gateway rather than silently discarding it.

`Gateway::identify_service` performs identity-only setup. Its successful return
cancels and joins that private duplex session. `Gateway::serve_via_and_identify`
combines identification and attachment on one session; `Gateway::serve_via`
continues to work for callers that already know the identity. The outgoing route
is published after every operation in the plan finishes, so unrelated outgoing
actor traffic cannot prematurely end server setup.

A client setup deadline covers connection and control exchange. Scoped setup
uses explicit duplex cancellation because the general `DuplexClient` retains
its established detach-on-drop behavior for independently handed-off halves.
Successful attachment transfers ownership to its serve handle; stopping or
dropping that handle stops the owned session and removes its route metadata.
The peer removes its registration when it observes closure.

Connected, accepted attachments can remain idle indefinitely and receive inbound
traffic while still in setup. Only disconnected reconnect waits use the existing
message-delivery timeout to bound abandoned session resources. Listener shutdown
cancels and joins active setup sessions, including sessions that have not yet
sent their first envelope. A full accept queue also respects cancellation.

## Python bootstrap and supplied IDs

A default worker can start from a bare channel URL and allocate its own ID:

```python
from monarch.actor import run_worker_loop_forever

run_worker_loop_forever(
    address="tcp://127.0.0.1:4444", ca="trust_all_connections"
)
```

Clients can pass that URL to `attach_to_workers`, which discovers the current
service incarnation before contacting the HostAgent:

```python
from monarch.actor import attach_to_workers

hosts = attach_to_workers(
    workers=["tcp://127.0.0.1:4444"], ca="trust_all_connections"
)
```

A supplied concrete `service<uid>@channel-url` remains supported and bypasses
discovery. This supports callers with known identities and non-duplex test
transports. Explicitly reused launcher IDs remain the launcher's responsibility;
this change does not migrate Slurm or Kubernetes identity allocation.

## Scope and validation

This discovery change keeps the existing Gateway outbound route publication
behavior. Draining old traffic, migrating retained forwarders, and fencing
concurrent outbound sends belong to the independent generic attachment fix.
The discovery protocol works directly on the original base without that fix.

Focused tests cover ordinary-first delivery and late controls, both control
orders, repeated and concurrent identities, frontend-specific advertisements,
interleaved actor envelopes, malformed/wrong/duplicate controls, unsupported and
nonresponding endpoints, cancellation, successful query session cleanup, idle
attachments, and listener shutdown during setup. Host restart tests reuse one
caller, assert a terminal failure for a stale concrete reference, and reach a
replacement actor with the same label under its new proc ID. Python integration
covers two attachments from one caller, restart at the same bare frontend,
explicit stale delivery failure through supervision, and replacement RPCs.
