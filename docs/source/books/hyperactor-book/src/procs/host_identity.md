# Endpoint-owned proc resolution

A location names a slot; a `ProcId` names one incarnation. Discovery returns
that slot's current `ProcId`. Combine it with the original location to mint
fresh references. Existing actor references never re-resolve: after a restart,
their old `ProcId` fails final delivery with `ProcNotExist`.

See [Stable locations and attachment paths](stable_locations.md) for the
connected-core topology, top-down `Addr`/`Via` normal form, stable deployment
keys, upstream precedence, cursor routing, replacement policy, and stability
limits. Discovery adds no address-to-address routes or automatic launcher keys.

## Endpoint ownership

Each served listener and outbound attachment receive endpoint has a weak owner.
Register its concrete proc with `GatewayServeHandle::register_proc_owner`.
Conflicting live registration is rejected. A dead owner may be explicitly
replaced; a stopped endpoint cannot be reused. Ownership never registers a
singleton alias in the gateway's proc table.

`Gateway::resolve_endpoint_proc(cx, location)` sends an ordinary, unsequenced
`ResolveProc` control with a one-shot reply port in the existing context.
`hyperactor_mesh::host::query_service_proc` provides the Host-facing API.
Queries have a bounded deadline; cancellation releases the local reply port
without tearing down shared routing or attachments. Immediate local failures and
routed failures return their structured delivery cause to the query, rather than
waiting for a generic timeout or faulting the caller's actor. Routed error replies
use the existing mailbox-server failure loop; there is no per-query relay task.
The deadline remains for peers that never answer or an unreachable return path.
Discovery does not require a duplex query protocol or a temporary runtime.

Only terminal endpoint ingress answers. Transit gateways do not answer using
their own owners, and local procs with the selector's label cannot intercept
the query. Self-resolution traverses the exact endpoint, not an own-prefix
shortcut: a core gateway dials its selected listener, and a leaf goes up and
back down its attachment path. This also selects the right owner when one
gateway serves multiple endpoints or bind and dial spellings differ.

## Host and Python APIs

Default Hosts and workers started with a bare channel URL mint fresh service
identities. `attach_to_workers(workers=[location])` discovers those identities;
strings and future-valued strings are accepted. A concrete
`service<uid>@location` skips discovery and must name the correct incarnation.
Monarch's launchers start workers from bare channel URLs and attach by discovery,
so a restarted worker has a fresh identity.
Dial/bind aliases are canonicalized for dialing. The context's actor must be
created after the listener or attachment that provides its return path.

A core restart at the same address, or a leaf restart under the same stable
keys, preserves the location but changes the discovered `ProcId`. New refs
work; stale refs fail. Locations are snapshots, not subscriptions. Existing
outbound traffic is not migrated to a new attachment.

The route cursor and control payloads change the wire format. All participants
must run matching builds; rolling interoperability is not promised.
