# Endpoint-owned proc resolution specification

## Goal and identity

A frontend address names an endpoint. A concrete service `ProcId`, such as
`service<gVQHWd9h6Km>`, names one Host process incarnation. A default Host generates
a fresh ID on every startup, including a container restart with an unchanged
Kubernetes Pod UID. Retained actor references keep naming their original
incarnation. Recreating actors with the same labels never rebinds those references.
Explicit concrete IDs remain supported; callers supplying them control their reuse.

An external client C may reach only a gateway endpoint A, while A can reach worker
B inside a cluster. C must discover B's current service ID from B's frontend
address through C's existing attachment to A. No launcher identity lists, identity
cache, or side-channel update is needed after B restarts at the same endpoint.

## Ownership and routing invariants

- Each supported listener has an explicitly registered concrete proc owner.
  Listeners sharing a Gateway may have different owners, even with identical
  labels. Registration requires a proc attached to that Gateway and rejects
  conflicting live owners. Expired owners may be replaced. Weak registration
  retains neither the proc nor its Gateway/server; no live owner means unavailable.
- Registration and teardown belong to that listener's slot. Stopping an old
  listener cannot erase another listener's owner. Stopped handles reject new
  registration; replacement listeners do not inherit ownership.
- `Gateway.procs` contains actual proc registrations only. Discovery never adds
  a singleton alias, changes `ProcId` equality, searches by label, or changes
  ordinary local delivery. Same-label procs at C, A, or B cannot shadow B.
  Ordinary actor messages cannot use the discovery selector to reach an owner.
- Concrete IDs never fall back to resolution. A stale B1 actor reference is
  never rewritten to B2, including after rediscovery returns B2's new ID.
- Owner resolution runs in runtime control before actor lookup or work queues.
  It works before actor startup, after ordinary traffic, concurrently, and while
  handler sequencing waits on a gap. Requests consume no handler sequence numbers.
  Each request owns a normal `OncePortRef` reply endpoint.

## Runtime control and Via reuse

An ordinary `MessageEnvelope` addresses `ControlPort::ResolveProc` at a fixed
channel-local protocol destination. Its `ResolveProc` payload distinguishes:

| Target | Meaning at the receiving listener |
| --- | --- |
| `OwnEndpoint` | Return only this listener's registered live owner. |
| `Endpoint(ChannelAddr)` | Contact that explicit endpoint and request its own owner. Never answer from the relay's owner or a label lookup. |

The listener ingress validates the exact protocol destination, rejects next-hop
overrides and Via destinations, and validates the payload. Malformed requests
fail without panic or route mutation. Ordinary Gateway submission rejects this
control port before local proc lookup or Via forwarding. Only direct submission
on the selected channel invokes discovery; it is not a routable singleton alias.

Existing `Location::Via` semantics remain unchanged: Via hops name known
Gateway/peer UIDs, which are peeled during forwarding. They supply the query's
return route and identify the existing attachment used for its forward leg.
An independent target B need not already be A's registered peer, so the relay
request also carries B's explicit `ChannelAddr`. No parallel source-route syntax
or assumption equating Gateway UIDs with service ProcIds is introduced.

A relay always sends `OwnEndpoint` to B on a fresh private direct attachment.
B alone answers for B's physically contacted listener. Terminal resolution never
recursively relays or falls back to another endpoint. Bind/dial string equality
is never used; wildcard binds, hostname resolution, loopback dialing, and address
aliases retain existing transport behavior and normalization.

Replies use ordinary serialization and Gateway delivery. Results are a concrete
`Resolved(ProcId)`, `Unavailable`, or an explicit relay/target failure. Clients
reject singleton-valued resolved replies. No duplex wire variant or connection
phase is added: `AttachWire` remains `Ack` then `Envelope`, with existing tags.

## Context-aware query API

`Gateway::resolve_endpoint_proc(address)` uses the caller's newest active Via
attachment when one exists. It atomically snapshots that attachment's sender and
accepted return `Location`; a newer local listener's `default_location` does not
select the query route. The query posts directly on the selected attached sender.

A temporary query Proc/client binds an ordinary reply port on the caller Gateway.
The reply is advertised with the snapshot's accepted Via location, so a later
attachment or default-route change cannot redirect it. Discovery does not mutate
the caller's default location, forwarder, or existing attachments. Failure on the
selected route is returned; it never triggers a caller-side direct dial to B.

`query_service_proc_with_gateway` exposes this behavior to Host clients. Native
`attach_to_workers` passes its instance's Gateway when resolving bare frontend
URLs. Explicit concrete `ProcAddr` inputs bypass discovery, including existing
Via addresses and non-duplex transports.

When no attachment exists, the contextual API uses the direct helper:

```rust,ignore
use hyperactor_mesh::host::{query_service_proc, query_service_proc_with_gateway};

let direct = query_service_proc(frontend.clone()).await?;
let through_current_attachment = query_service_proc_with_gateway(&gateway, frontend).await?;
```

The explicitly direct `query_service_proc` / `Gateway::query_endpoint_proc` helper
creates a fresh private Gateway, establishes the existing attach handshake, then
creates its query Proc/client and reply ports. Only after the acknowledgement can
legitimate query replies exist. It sends `OwnEndpoint`, receives the normal reply,
and stops and joins its private attachment. `GatewayServeHandle::resolve_proc`
also supports `OwnEndpoint` on a suitable existing direct attachment.

Private discovery temporarily registers a peer at the contacted endpoint. Direct
resolution costs connection setup, an attach round trip, and a resolution round
trip. Relayed discovery reuses C–A, adds its request/reply, and performs that
private A–B exchange. Caller routes remain unchanged. Fresh private Gateways have
no old outbound traffic or pre-ack query replies to drain; discovery does not
change generic attachment cutover behavior.

## Deadlines, cancellation, and admission

One 30-second caller deadline bounds resolution. Each relay's terminal query is
also bounded by 30 seconds. A listener admits at most 128 concurrent relay tasks,
using non-waiting permit acquisition; overload receives an explicit error. No
unbounded queue of tasks waits for a permit. No owner/routing lock spans an await.

Relay admission and task registration synchronize with listener shutdown, so no
work can be spawned after shutdown has observed all tasks complete. Tracked tasks
observe listener cancellation; listener teardown joins them. Target query errors
are returned normally and asynchronous panics are logged. Duplex source-connection
closure also cancels its in-flight relay work. Canceling an individual request on
a shared still-live connection has no wire cancellation protocol: remote work
remains bounded by its terminal deadline. Simplex receiver lifetime can span
multiple sources, so individual source loss there has the same bounded behavior.

Private-session drop guards close in-progress attachments on failure, timeout,
or cancellation. Closing them removes temporary peers and sessions. Healthy
accepted connections waiting for their first message gain no timeout. Ordinary
attachments, idle messaging, and source-route hop semantics remain intact;
connection task join failures are logged during serving and shutdown.

## Validation requirements

Tests cover listener ownership, live/dead/conflicting owners, malformed control,
explicit target isolation among same-label procs, concurrent B/D queries, and
fresh IDs after same-endpoint restart. They verify an overridden local default and
a second attachment started during a pending query do not change its reply route.
Failure coverage includes unavailable owners, invalid singleton replies, rejected
attachment, target failure, overload, timeout, cancellation, source disconnect,
listener shutdown, and temporary peer/session cleanup. Existing ordinary Gateway,
Via, duplex idle/disconnected, bootstrap parsing, and explicit-ID tests remain valid.

A Linux multi-process Python integration establishes C–A, then denies all new
`connect(2)` calls on C's existing native threads using a test-only seccomp TSYNC
filter. It verifies C's direct B connection receives `EACCES`, while bare-address
`attach_to_workers(B)` succeeds through A. B restarts with the same endpoint and
Pod UID environment; rediscovery succeeds without new metadata, new actors work,
and B1 references fail. Worker/supervisor processes remain outside C's filter.
No live Kubernetes cluster is required or contacted by these tests.
