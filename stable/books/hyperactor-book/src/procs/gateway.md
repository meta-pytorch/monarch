# Gateway

A `Gateway` is the connectivity layer for procs. A [`Proc`](proc.html) by
itself is an isolated actor runtime: it owns local actor lifecycle and
mailboxes, but it has no way to reach--or be reached by--the outside world.
It communicates by attaching to a gateway. The gateway gives each attached
proc an advertised location, accepts inbound traffic for that location, and
forwards outbound traffic to destinations outside the proc.

This separation lets us compose topologies without changing proc identity.
A [`Host`](host.html) attaches its in-process procs to one gateway and registers
spawned child proc gateways as peers, so the gateway multiplexes ingress and
routes egress for the host. A gateway can also attach to another gateway over a
duplex link and advertise its local procs through that peer's location.

From the connectivity perspective, each location has exactly one gateway.
Operationally, a gateway is both a *proc multiplexer* for ingress and a
*router* for egress.

## What lives inside a gateway

A gateway is two routing tables plus synchronized advertised-location state:

| Field | Type | Role |
| --- | --- | --- |
| `uid` | `Uid` | A stable routing key. Other gateways address this gateway with `Via(uid, ...)`. |
| `procs` | `HashMap<ProcId, WeakProc>` | In-process procs attached to this gateway. The weak reference lets the proc's lifetime detach the route. |
| `peers` | `HashMap<Uid, BoxedMailboxSender>` | Gateway peers keyed by uid. This includes duplex-attached clients and child proc gateways registered by a host. |
| `forwarder` | `BoxedMailboxSender` | Catch-all egress for destinations not owned by this gateway. Defaults to `DialMailboxRouter`; `serve_via` temporarily replaces it with the duplex sender. |
| `default_location` | `Location` | The location stamped onto newly bound refs. A `serve_via` session advertises refs as `Via(self_uid, peer_location)`. |

`forwarder`, `default_location`, and the active local-delivery locations live
behind one lock. Starting or stopping a serve updates reachability and egress
together.

## The single routing decision

Every message that reaches a gateway runs through `Gateway::post_unchecked`.
The gateway routes by repeatedly inspecting the next hop's outermost
[`Via`](../references/syntax.html) hop:

```
envelope arrives
 |
 v
next_hop.location().pop_via()
 |
 +-- uid in peers -> peel hop, forward to that peer
 +-- uid == self -> peel hop, continue routing the inner location
 +-- foreign uid -> hand unchanged envelope to the forwarder
 +-- no via -> try local proc delivery; otherwise forward or fail
```

Local delivery uses `procs[dest.proc_id]` only after all owned via hops have
been peeled. This prevents a foreign via from being delivered locally merely
because its inner proc id collides with a local proc id. If the envelope still
has an explicit next hop after peeling and no local proc matches, the gateway
returns `NoRoute`; otherwise it hands the envelope to the forwarder.

## Constructing a gateway

| Constructor | Forwarder | Use |
| --- | --- | --- |
| `Gateway::new()` | `DialMailboxRouter` | A fresh gateway that dials addresses for egress. |
| `Gateway::isolated()` | `UnroutableMailboxSender` | Local-only; outbound to unknown destinations is undeliverable. Handy in tests. |
| `Gateway::global()` | `DialMailboxRouter` | The process-wide singleton. `Host::new` uses this by default. |
| `Gateway::configured(loc, fwd)` | caller-supplied | Explicit advertised location and forwarder. |

Procs attach during construction: `Proc::builder().shared_gateway(gw).build()`
calls `Gateway::attach_proc`, which registers the proc as a local entry in
`procs` and returns a guard that detaches it on drop. A proc's outbound path runs through
`Proc::gateway()`, so procs sharing a gateway reach each other by an in-gateway
lookup rather than dialing.

## Serving a gateway

A gateway must be served before it can accept inbound traffic:

- `serve(addr)` serves a simplex mailbox endpoint.
- `serve_with_listener(addr, listener)` uses a caller-provided listener when
present and chooses simplex or duplex based on the transport.
- `serve_duplex(addr)` opens a duplex endpoint that accepts both ordinary
envelopes and gateway-attach handshakes.

Serving records an active location. The newest active normal serve or
`serve_via` session becomes `default_location`; older active serve locations
remain valid for local delivery.

### `serve_via`: client-side attach

`serve_via(addr)` connects *this* gateway to a remote gateway over a duplex
channel, so a client outside a cluster can reach an in-mesh host through one
exposed address. The handshake:

1. Dial the peer's duplex endpoint and post `AttachRequest { uid: self.uid }`.
2. The peer records `peers[self.uid] = sender-over-the-duplex` and replies
`AttachAck::Accepted { location: Via(self.uid, peer_default) }`.
3. The client records that location as an active serve, so it becomes
`default_location` until a newer active serve replaces it, and uses the
duplex sender as its forwarder.

From then on both directions carry plain `MessageEnvelope`s over the one
socket, and each end runs the same routing decision. Remote clients and spawned
children are both `peers` entries matched by uid. They differ only in the
sender behind the peer entry: a duplex link for `serve_via`, and usually a
one-way dialed `MailboxClient` for a spawned child.

## `GatewayServeHandle`

`serve`, `serve_duplex`, and `serve_via` all return a `GatewayServeHandle`.
Shutdown is two explicit steps:

- `stop(reason)` signals the server and removes that handle's active serve
location. For duplex serving it also cancels the accept loop.
- `join()` awaits teardown. It does not stop--call `stop` first.

The handle carries one `HandleKind` per flavor (`Serve`, `ServeDuplex`,
`ServeVia`), each owning its own teardown state. The `ServeVia` variant removes
its active serve entry even if the handle is dropped without `stop`, so a
dropped attach never leaves the gateway holding a dead duplex sender.

## See Also

- [Proc](proc.html) -- the runtime that attaches to a gateway
- [Host](host.html) -- owns one gateway and wires its procs into it
- [Syntax](../references/syntax.html) -- `Location` and the `Via` source-route form
- [Routers](../mailboxes/routers.html) -- `DialMailboxRouter`, the default forwarder