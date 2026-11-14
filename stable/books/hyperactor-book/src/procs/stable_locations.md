# Stable locations and attachment paths

A `Location` names a slot; a `ProcId` names one process incarnation.
The core is a set of gateways that can all dial one another. Each leaf has
one upstream and can receive traffic over its outbound attachment.

## Normal form

Locations contain one core `ChannelAddr` and a top-down attachment path.
We retain the `Addr`/`Via` encoding:

```
tcp://core:4444 core gateway
rack.tcp://core:4444 leaf under slot rack
rack.worker.tcp://core:4444 nested leaf under rack, then worker
```

`with_via` prepends a key; `append_via` extends the path. `path_key(0)` is
the first key below the core, not the most recently attached leaf.
There are no embedded address hops or sender-relative multi-address routes.

## Upstream and keys

`Gateway::serve_via(addr)` uses the attaching gateway's uid as its key.
`serve_via_with_key(addr, Some(Uid::singleton(label)))` uses a stable deployment
slot. Keys must be unique among one upstream's attachments. No key is inferred
from a process identity or launcher metadata.

The attachment handshake appends the key to the upstream's advertised location.
An active upstream supplies both the gateway's default advertised location and
its outbound forwarder. Starting a private listener does not replace that
advertised location. A second upstream is rejected, including during an
in-progress handshake; cancellation releases the handshake reservation.

The newest registration for a key wins. Replacement logs a warning, increments
`gateway.attachment_replacements`, and cancels the old session. Registration
ids fence cleanup: an old guard cannot remove a newer registration.

## Routing

Traffic goes **up**, **across**, and **down**:

1. A leaf forwards outbound traffic up its existing attachment.
2. A core gateway dials the destination's core address.
3. The core and subsequent gateways select attachment keys top-down.
4. At the end of the path, delivery looks up the concrete `ProcId`.

The envelope retains its canonical destination, sender, and sequencing headers.
Its only route state is `route_cursor: Option<usize>`:

- `None`: going up or across, before reaching the destination core.
- `Some(n)`: going down, with `n` keys consumed.
- `Some(path_len)`: terminal delivery at the selected endpoint.

The dialer enters the downward phase at zero. Each attachment sender advances
the cursor once. Prepared routes are not reinitialized at a subsequent gateway;
transport retries keep both the selected endpoint and the cursor. No gateway
redials a terminal miss. A missing concrete incarnation reports `ProcNotExist`,
and a missing final attachment reports `RouteNotExist`; a broken intermediate
path reports `NoRoute`. `ProcNotExist` and `RouteNotExist` are invalid-reference
failures: the default actor handler fails the sending actor and propagates the
fault through supervision. An actor that needs to retain stale references must
override its invalid-reference policy. Endpoint discovery itself uses a bounded
query and does not silently redirect retained references.

Ordinary traffic can take a shortcut at a shared ancestor. The gateway recognizes
its advertised locations as prefixes and begins descent at the prefix length.
Endpoint-scoped discovery must instead reach the selected endpoint ingress,
including when the caller and endpoint share a gateway.

`Gateway::flush` snapshots local procs, the base forwarder, and the upstream;
it holds no routing lock across an asynchronous wait and does not recursively
flush attached peers.

## Stability and scope

A location is as stable as its core address and keys. Reattaching under the same
key preserves the slot, but not the process incarnation. Existing references
must retain their original ids; resolution is an explicit operation that mints
fresh references rather than silently retargeting old ones.

If a core gateway restarts at the same address and its leaves reattach under
the same keys, their locations remain valid. Moving an upstream to another
address or reparenting a leaf changes its location. Advertised locations are
snapshots; updates are not pushed down the tree.

Topologies without a mutually reachable core, attachment traffic migration,
epoch/lease fencing, and automatic deployment key assignment are out of scope.
The envelope wire format changes, so participants must run matching builds.