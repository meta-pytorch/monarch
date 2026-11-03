# Endpoint-owner discovery over address and peer routes

## Goals

A frontend location identifies a reachable endpoint. A concrete service ProcId
identifies one Host process incarnation. The default Host creates a fresh concrete
ID on every process start, even when its address and Kubernetes Pod UID do not
change. Existing actor references retain their original identities. Explicitly
supplied concrete ProcIds remain supported and bypass discovery.

An external client C can reach a gateway A while only A can reach worker B. C must
be able to discover B's service ProcId using endpoint addresses and its existing
runtime context, then send ordinary actor messages and receive replies through
that topology. B does not need to register a symbolic name at A or supply its
incarnation identity through a launcher side channel.

The runtime implements the routing and discovery behavior described here.
Locations retain concrete process identity independently of route progress.

## Locations and identities

A location can describe an ordered path containing address steps and peer steps:

- Address(A): reach the endpoint at channel address A.
- Peer(k): use the connection registered under key k at the current gateway.

Examples in traversal order:

- [Address(B)]: directly reach B.
- [Address(A), Address(B)]: reach A, then let A reach B.
- [Address(A), Peer(k)]: reach A, then use A's registered peer k.
- [Address(A), Address(B), Peer(child)]: reach B through A, then use B's child route.

Native Python worker addresses accept these strings (`route:` is followed by a
JSON array containing at least two strings):

```text
tcp://worker:4444
route:["tcp://gateway:4444","tcp://worker:4444"]
route:["tcp://gateway:4444","child.tcp://worker:4444"]
child.route:["tcp://gateway:4444","tcp://worker:4444"]
```

The third example means gateway, worker, then worker's peer `child`; the fourth
means gateway, gateway's peer `child`, then worker. A legacy `Via` prefix applies
after the address immediately beneath it. Nested instance UIDs remain accepted.
Consecutive address steps display as a flat JSON array. JSON quoting escapes
embedded quotes and backslashes; ordinary URLs stay opaque, so an existing
`ipc://` socket path containing `>` retains its meaning. Concrete identities can
prefix any location with `service<UID>@` and bypass discovery.

Peer keys select connections; they need not equal the final destination ProcId.
Existing Via addresses remain accepted with their existing meaning. A new address
hop must not make an old Via change meaning. Concrete actor/port identity remains
separate from the location and from progress along the route.

Discovery takes a context and an unresolved location and returns a concrete
ProcAddr. A location without a ProcId is a discovery input, not an ordinary actor
reference that automatically re-resolves on every send. Unknown concrete IDs
never fall back to owner resolution. Intermediate hop identities are not implicitly
queried. A peer-only endpoint still requires a known registered key.

## Sender-side peeling

For an explicit route [Address(A), Address(B)]:

1. C selects the connection to A, removes only Address(A), and sends [Address(B)].
2. A selects the connection to B, removes only Address(B), and sends a terminal route.
3. B receives terminal arrival and handles the message locally or reports failure.

A sender removes a hop only when dispatching onto its selected connection. It must
not repost the shortened route to its own general routing entry point. Multiple
MailboxSender adapters on a path do not represent multiple address hops.

Explicit terminal arrival differs from having no explicit route state. Terminal
failure never consults an ambient forwarder, redials the endpoint, or restarts the
canonical destination's route. A missing peer or failed address step is a failure,
not a reason to try another gateway. The canonical destination, sender, and actor
sequence information remain unchanged as route progress advances.

Transport retry resends the prepared envelope to the same selected endpoint; it
does not interpret the remaining route at the original sender. TTL and delivery
failure handling remain bounded. Address aliases, DNS names, wildcard binds, and
normal transport normalization work without comparing a receive-side bind string
to a dial string to decide whether a hop was consumed.

The envelope stores one progress state: unset, a legacy next-hop reference, or
an explicit deque of address/peer actions. An empty explicit deque means terminal
arrival. Normalization preserves legacy Via traversal order before sending.
`MailboxSender` adapters never consume actions. Actor and ephemeral-port sequence
keys use ActorId and PortId respectively, so reaching the same identity through
different locations cannot create duplicate sequence streams.

The `Addr` and `Via` Location enum variants retain their serialized names and
order; `Address` is appended. Envelope progress and ResolveProc wire payloads
changed, so this change does **not** promise rolling interoperability with the
previous envelope/control format. All participants must run matching builds.

## Existing attachments and return paths

Explicit routes can reuse suitable established attachments. A C-to-A session must
remain usable after C loses permission to open new network connections. Selecting
that session consumes the A step, never an unvisited B step.

Native attachment accepts a bare B frontend location in an already attached
runtime by constructing a route through the selected attachment. It uses ordinary
routing, not an A-side task that performs a second discovery exchange. Explicit
multi-address inputs are also accepted. The resolved ProcAddr retains a usable
location for subsequent actor messages. Python's public attachment signature does
not change; it already supplies the runtime context.

The query's one-shot reply port belongs to the existing actor context and has a
reachable location. The Rust caller must already have a listener or attachment
that supplies that return path; discovery does not establish one. Route selection
and return-route binding must not silently change because another attachment or
local listener starts during a query.
Discovery does not change the caller's default routing or create a temporary Proc,
Gateway, or private attach session. Existing Via replies and child-proc routes are
preserved; successful discovery alone is not sufficient if actor traffic fails.

## Endpoint ownership and control

ResolveProc is an ordinary unsequenced runtime control message. It traverses the
same routing mechanism as actor messages and is consumed only on terminal arrival
at the selected endpoint. Transit gateways never answer using their own owners,
and same-label local procs never intercept the request. The discovery selector is
not a registered singleton alias and cannot address ordinary actor messages.

Each supported listener or outbound duplex attachment receive endpoint has an
explicitly registered weak concrete owner. Live conflicting registration is
rejected. Expired owners can be explicitly replaced; old listener teardown cannot
clear a new listener's registration. A terminal endpoint with
no live owner reports unavailable. Owner lookup does not wait for actor handler
startup, sequence gaps, or handler work queues.

Each request has an independent one-shot reply port and a bounded deadline.
Concurrent and repeated queries are valid before or after ordinary traffic.
Dropping a request releases its local reply port. Forwarding does not spawn
per-query relay tasks, allocate temporary runtimes, or introduce duplex setup
messages. Ordinary transport queues and connection lifetimes carry the messages.

## Flush, attachment, and failure behavior

Address steps use the configured base DialMailboxRouter's existing cache and
stale-session replay. Gateway flush drains that base router and all active
outbound attachments, while excluding peer gateways to avoid recursive flush.
An isolated Gateway or a custom non-dial forwarder cannot gain a direct network
escape through explicit routing; a matching existing attachment can still be
selected. No routing locks span asynchronous waits. Attachment cutover and
migration are outside this contract; explicit routes must still participate in
the Gateway's flush semantics so their outbound queues can be drained.

A restarted B can be rediscovered at the same location, returning B2 rather than
B1. Retained B1 actor references fail; successful resolution does not rewrite them.
A broken intermediate route returns a bounded error. A missing concrete ProcId
at terminal arrival and a missing final peer are invalid-reference failures,
which preserve ordinary actor fault reporting for stale references. Missing
intermediate routes remain transport NoRoute failures. Reply and undeliverable
traffic keep their own destinations and do not accidentally reuse a forward
request's remaining route.

## Required validation

Tests must cover direct, two-address, three-address, and mixed address/peer routes;
legacy Via behavior; parsing/display and wire round trips; correct terminal owner
selection among same-label procs; no redial/default fallback at terminal failure;
retry preserving the selected hop; concurrent queries; unavailable/conflicting/dead
owners; ordinary traffic and sequencing after resolution; attachment reuse and
return-route stability; and draining buffered explicit-route sends.

A Linux multi-process Python test establishes C-to-A and then blocks new connect
calls on C's native threads. C discovers B and exchanges actor messages through A.
B restarts at the same address and with unchanged Pod UID metadata; rediscovery
returns a fresh identity, new actors work, and old references fail. Exercise both
explicit routes and contextual bare worker inputs. Explicit supplied ProcIds and
future-valued worker addresses continue working. No live Kubernetes cluster is
required.
