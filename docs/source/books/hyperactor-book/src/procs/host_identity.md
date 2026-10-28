# Endpoint-owned proc resolution specification

## Goal and identity

A frontend address names an endpoint. A concrete service `ProcId`, such as
`service<gVQHWd9h6Km>`, names one Host incarnation. A default Host generates a
fresh ID at startup, keeps it throughout its lifetime, and generates another
when restarted at the same endpoint. Retained references continue to name the
old incarnation; recreating actors with the same labels never rebinds them.
Explicit concrete IDs remain supported unchanged. Reusing an explicit ID across
restarts remains the caller's responsibility.

This feature lets a client knowing only an endpoint obtain its current owner,
without distributing concrete IDs through launch configuration. Ownership is
an explicit property of the served endpoint, rather than a singleton alias,
a Gateway default location, or a search for a proc with a matching label.

## Required behavior

- Each supported listener has one independently registered proc owner. Two
  listeners on one Gateway may have different owners, even with the same label.
  Registration requires a concrete proc attached to that Gateway. A conflicting
  live owner is rejected; an expired owner may be replaced. Ownership is weak:
  it retains neither the proc nor its Gateway. No live owner returns unavailable.
- Registration and cleanup belong to that listener's slot. Stopping an old
  listener cannot erase a replacement listener's registration. Stopped handles
  reject registration. A new listener does not inherit a previous listener's owner.
- `Gateway.procs` contains actual proc registrations only. Ownership never adds
  a singleton alias, changes `ProcId` equality, or changes ordinary local delivery.
  Ordinary actor messages to a singleton selector cannot reach a concrete owner.
  Concrete IDs never fall back to owner resolution, including after restart.
- Resolution runs in the proc runtime control path, before actor lookup and work
  queues. It works before any actor starts, after ordinary traffic, concurrently,
  and while handler sequencing waits on a gap. It consumes no handler sequence
  numbers. Every request has its own ordinary `OncePortRef` reply endpoint.
- Malformed control requests fail without panic or route mutation. Replies are
  an explicit `Resolved(concrete ProcId)` or `Unavailable`; a singleton-valued
  resolved reply is rejected by the client.

## Direct endpoint scope

Resolution is deliberately **direct and channel-local**. It is not a general
routable `service@location` reference. The public low-level operation is
`GatewayServeHandle::resolve_proc(&client)` on an existing direct attachment.
The client's reply ports must already be reachable from that endpoint.

`ResolveProc` uses the dedicated runtime `ControlPort::ResolveProc` and a fixed
protocol destination, containing no remote location. Choosing the connected
channel chooses the listener. The listener's ingress adapter validates the exact
destination, rejects next-hop overrides and Via destinations, deserializes the
request, and reads only its own owner slot. Bind/dial string equality is never
used: wildcard binds, loopback dialing, DNS names, and existing address aliases
therefore preserve their normal transport semantics.

Ordinary Gateway submission rejects this control port **before local proc
lookup or Via forwarding**, on both originating and intermediate Gateways.
Forwarded resolution is unsupported. A caller wanting endpoint B must connect
directly to B; an attachment through endpoint A does not provide a locator for
B. Same-label procs at the caller or at transit Gateways cannot intercept a
query. Ordinary source-route hop semantics remain unchanged.

The runtime request is an ordinary `MessageEnvelope` containing `ResolveProc`
and an existing `OncePortRef<ResolveProcResult>`. The runtime replies through
normal reply serialization and Gateway delivery. The request is constructed
with `SeqInfo::Direct` and submitted directly to the channel, bypassing handler
sequencer allocation. No duplex request/reply variant, connection phase, or
identity-only first-frame dispatch is added. `AttachWire` remains `Ack` followed
by `Envelope`, retaining their existing wire tags.

## Address-only helper and lifetime

```rust,ignore
use hyperactor_mesh::host::query_service_proc;

let service = query_service_proc(frontend).await?;
```

`query_service_proc(ChannelAddr)` normalizes the dial address and calls
`Gateway::query_endpoint_proc`. That helper:

1. Creates a private, fresh Gateway and establishes an existing
   `AttachRequest` / `AttachAck` session to the endpoint.
2. Creates its query Proc/client **after** attachment, so the reply port carries
   the accepted Via return route.
3. Sends `ResolveProc` directly over that attachment and receives its normal reply.
4. Stops and joins the private attachment.

Discovery temporarily registers a peer at the endpoint. It costs connection
setup, an attach round trip, and a resolution/reply round trip; it is not
peer-free. It changes no routes, forwarders, or default locations on a caller's
Gateway. The result contains the concrete ID and normalized direct dial location.

One 30-second deadline covers attachment, resolution, and teardown. A private
session cancellation guard covers pending attachment, errors, timeouts, and
caller cancellation. Closing that session removes its temporary remote peer
registration. Successful queries stop and join the private attachment before
returning. A healthy accepted connection waiting for its first message has no
new timeout. Ordinary attachments and connected idle messaging retain their
existing protocol. Connection task join failures remain logged during both
normal operation and shutdown.

The result is a snapshot. A restart between discovery and subsequent messaging
leaves the discovered ID stale; it never redirects to the replacement. Clients
needing a lasting attachment establish it separately. This design is independent
of the generic attachment draining/cutover fix in #5134: a private fresh Gateway
has no existing outbound traffic to migrate. Its query Proc and reply ports
exist only after the attach acknowledgement, so legitimate query replies cannot
precede that acknowledgement. Discovery therefore needs neither a handshake
inbox nor generic attach draining; existing `serve_via` cutover behavior is not
changed here.

## Compatibility and validation

Python bootstrap accepts both a bare frontend URL and an explicit concrete
`ProcAddr`. Bare URLs discover the current incarnation before contacting its
HostAgent. Explicit `service<uid>@channel-url` addresses bypass discovery,
including on non-duplex transports. Slurm and Kubernetes identity allocation
remain unchanged.

Required regressions cover endpoint ownership with same-label caller/transit
procs and multiple listeners, ordinary Via routing, explicit rejection of routed
control, wildcard bind/loopback dialing, no alias delivery or stale-ID rebinding,
restart identity, and explicit IDs. Runtime tests exercise resolution before
actor startup, after ordinary messages, concurrently, and across a handler
sequence gap. Failure tests cover absent/dead/conflicting owners, malformed
control, invalid singleton replies, rejected attachment, cancellation and timeout
during both attachment and pending resolution, and temporary peer/session cleanup.
Host/bootstrap Rust tests and Python address/restart integration tests validate
the public entry points against a freshly rebuilt native extension.
