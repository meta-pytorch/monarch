# Gateway

A `Gateway` provides connectivity for one or more [`Proc`](proc.md)s. It
advertises their locations, accepts inbound messages, and routes outbound
traffic. A [`Host`](host.md) shares one gateway among its in-process procs and
registers child proc gateways as attachments.

Locations name one core address and a top-down path of attachment keys.
Core gateways can all dial one another; each leaf has one upstream. See
[Stable Locations](stable_locations.md) for the topology, slot/identity split,
cursor protocol, stability limits, and failure semantics.

## State

- `procs` maps `ProcId` to weak in-process proc registrations.
- `peers` maps an attachment key to a sender, registration id, and replacement
  cancellation token. Old guards cannot remove a newer registration.
- Synchronized routing state holds listeners, the optional upstream, the base
  forwarder, and the default advertised location.
- `uid` supplies the default instance attachment key. Deployment-chosen
  singleton keys are independent of the gateway's incarnation.

## Routing

Every envelope retains its canonical destination and sender. Its optional
`route_cursor` records only how many destination attachment keys have been
consumed:

```text
None: go up the existing upstream, or dial the core address
Some(n), n < path_len: select peers[path_key(n)], advance, and forward
Some(path_len): deliver by concrete ProcId, or fail at this endpoint
```

An ordinary message may start descent at a recognized local ancestor, without
going to the core and back. A prepared cursor is never reset at subsequent
gateways. Endpoint-scoped discovery must reach the selected ingress rather than
using that shortcut. Terminal misses never go back through the forwarder.

## Construction and serving

`Gateway::new()` uses a `DialMailboxRouter` as its base forwarder.
`Gateway::isolated()` uses an `UnroutableMailboxSender`; attaching an upstream
still lets it reach destinations through that existing connection.
`Gateway::global()` is process-wide. `configured` supplies an explicit fallback
location and base forwarder.

Procs register during `Proc::builder().shared_gateway(gateway).build()`.
Their guards remove the local registration when the proc dies.

- `serve(addr)` starts a simplex listener.
- `serve_duplex(addr)` accepts actor traffic and attachment handshakes.
- `serve_with_listener(addr, listener)` can reuse a pre-bound listener.
- `serve_via(addr)` attaches to one upstream under this gateway's uid.
- `serve_via_with_key(addr, Some(key))` selects a deployment-chosen slot.

An active upstream supplies both the default advertised location and outbound
forwarder. A later private listener does not replace that location. With no
upstream, the newest listener becomes the default. Other active listener
locations remain valid aliases.

The attach handshake appends the client's key to the upstream's advertised
path and installs its duplex sender. The newest attach for a key wins and closes
the previous session. A gateway rejects a second active or connecting upstream.

## Lifetime and flushing

Serving returns a `GatewayServeHandle`. `stop(reason)` signals shutdown and
removes the active location; `join()` waits for teardown. Call both before
changing upstreams. Dropping an outbound attachment handle also removes its
routing registration.

`Gateway::flush` snapshots local procs, the base forwarder, and the upstream.
It drains each without holding routing locks across asynchronous waits and
does not recursively flush peer gateways.

## See Also

- [Stable Locations](stable_locations.md)
- [Host](host.md)
- [Proc](proc.md)
- [Routers](../mailboxes/routers.md)

## Attachment delivery guarantees

See [Gateway attachment and outbound delivery](gateway_connection.md) for the
route cutover contract, cancellation behavior, and regression scenarios.
