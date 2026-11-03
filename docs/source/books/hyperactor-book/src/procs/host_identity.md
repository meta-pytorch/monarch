# Host Identity Discovery

A host's service proc needs a `ProcId` that is unique to the *process*, not
just to the machine or the launch arguments. This page explains why, compares
the ways a client could learn that identity, and describes the mechanism we
chose: a `Query` link kind on the host's frontend address.

## Problem

Every host serves its built-in service proc at an instance `ProcId` such as
`service<gVQHWd9h6Km>`. Since the shared `service` singleton was retired,
clients address hosts by full `ProcAddr` (`service<uid>@tcp://host:26600`).

When a host dies and is restarted on the same machine, the new process must
get a **new** `ProcId`:

- Senders sequence messages per destination `ActorAddr`
  (`Sequencer::assign_seq` in `hyperactor/src/ordering.rs`). If the restarted
  host reused its `ProcId`, every peer would continue at sequence number `N`
  for the same `ActorAddr`s, and the new process, which expects `1`, would
  buffer those messages forever.
- A fresh `ProcId` yields fresh `ActorAddr`s, fresh sequencing state, and a
  clean failure for anyone still holding the old `ProcAddr` (the message is
  returned undeliverable).

The channel layer already copes with a restarted peer: a client that
reconnects with its old session receives `Reject` on the sequence mismatch,
and `DialMailboxRouter` evicts the stale cached sender.

A per-process identity needs entropy that only the process has. Anything the
launcher supplies (arguments, environment variables such as
`MONARCH_SERVICE_PROC_IDS`, or a Kubernetes pod UID) is identical when the
process is restarted with the same arguments. So the client has to learn the
identity *from the process*.

## Requirements

1. **Unique per process.** A restart with identical arguments and environment
   yields a different `ProcId`.
2. **Discoverable without a side channel.** A client that knows only the
   host's channel address can learn the current `ProcId`. No store, registry,
   or scheduler metadata is required.
3. **No cost on the normal messaging path.** Messages to a known `ProcId` pay
   nothing extra: no wrapper type, no additional branch in delivery.
4. **Many queriers.** Any number of clients can query at any time,
   concurrently, independent of existing connections.
5. **Clean version skew.** An old server answers a new client with a fast,
   clear error rather than a hang.

## Options

| | Mechanism |
|---|---|
| **A** | **`Query` link kind on the same port.** A third `LinkInit` magic (`QRY\0`). The muxed listener writes one reply frame on that connection and closes it. |
| **B** | **Enum frame type.** The frontend accepts `enum { Query, Envelope(MessageEnvelope) }` instead of `MessageEnvelope`. |
| **C** | **Second port.** For example, 26600 for messages and 26601 for identity queries. |
| **D** | **Channel-level hello.** On every new link, the server first sends its identity (or an incarnation id). |
| **E** | **Bounce with the new `ProcId`.** A message to an unknown `ProcId` returns undeliverable, as today, but the reason also names the `ProcId` the host now serves. A probe to a placeholder `ProcId` performs discovery. |
| **F** | **Launcher-derived identity.** `ProcId = f(job, role, replica, attempt)`, as with `MONARCH_SERVICE_PROC_IDS` or the Kubernetes pod UID today. |
| **G** | **External registry.** Workers publish their identity to a store that clients read. |
| **H** | **Branch in the gateway's duplex attach.** An `IdentifyRequest` alongside `AttachRequest` in the gateway's duplex accept path. |
| **I** | **Location-addressed mailbox query.** A message addressed to "whatever is at this location" rather than to a `ProcId`. |

### Comparison

| | Cost per message | Discovery cost | Old server, new client | New server, old client | Configuration | Transports | Client must be reachable | Detects later restarts |
|---|---|---|---|---|---|---|---|---|
| **A** Query link kind | none | 1 connection | fails fast (unknown magic) | unaffected | none | muxed (net) frontends | no | no (query again after a failure) |
| **B** Enum frame | 4-byte tag and a branch, on every link | 1 connection + routed reply | all traffic breaks | all traffic breaks | none | all | yes | no |
| **C** Second port | none | 1 connection | refused quickly, or a timeout if firewalled | unaffected | second port everywhere | awkward beyond plain TCP | no | no |
| **D** Channel hello | 1 frame per link setup, on every link | free on first connect | all links break | all links break | none | net | no | yes, on reconnect |
| **E** Bounce + new id | none (failure path only) | 1 routed round trip | clear failure, no id | unaffected | none | all | yes | yes, automatically |
| **F** Launcher-derived | none | none | n/a | n/a | per launcher; client queries the scheduler | all | no | only launcher-driven restarts |
| **G** Registry | none | 1 store lookup | n/a | n/a | a store per platform | all | no | if workers republish |
| **H** Gateway duplex branch | none | 1 duplex connection | timeout (request routed as traffic) | unaffected | none | duplex frontends | no | no |
| **I** Location-addressed query | branch in delivery | 1 routed round trip | undeliverable | unaffected | none | all | yes | no |

### Notes

**A, `Query` link kind.** Every connection to a net frontend already starts
with a 13-byte `LinkInit` header, and the mux already matches on its magic
once per connection (`channel/net/mux.rs`). A query adds one arm to that
existing match. Normal simplex traffic is byte-for-byte unchanged and takes
exactly the same code path. Each query is its own short-lived connection with
no session state, so it is independent of `DialMailboxRouter`'s cached links,
and any number of clients can query concurrently. Query connections go
through the same `prepare_accepted_stream`, so they get the same TLS and CA
checks as messages. An old server rejects the unknown magic and closes the
connection immediately. Limitation: only muxed frontends answer, which today
means net transports (`ChannelTransport::is_net`). In-process callers on the
`Local` transport can be handed the `ProcId` directly.

**B, enum frame.** The raw per-message cost is small. Frames are encoded with
`bincode` fixed-width integers (`serde_multipart::options()`), so the variant
tag is a 4-byte `u32`. Decoding adds one well-predicted branch, and
unwrapping is a move of the envelope struct (the payload is buffer-backed and
is not copied). The larger costs:

- The channel type is not chosen per host. `DialMailboxRouter` caches
  senders by `ChannelAddr` without knowing whether an address is a host
  frontend, so the enum becomes the wire type for every link, and every
  receiver needs an arm to reject a `Query`.
- Simplex links have no reply path besides acks. The answer must travel back
  as a routed `MessageEnvelope`, so the client must be reachable first.
- Changing the framing breaks mixed-version traffic for every message.

**C, second port.** It is simple and easy to debug (the second port could
speak plaintext or HTTP). The costs are addressing and configuration:

- A host address holds one `ChannelAddr`. Either worker strings carry two
  addresses, or the second is derived by convention (port + 1). The
  convention breaks for ephemeral `:0` ports, Unix and IPC paths,
  `dial@bind` aliases, and pre-bound `host:fdN` listeners.
- Every deployment manifest needs the extra port: MAST `port_map`,
  Kubernetes `containerPort`, Services and NetworkPolicies, and firewalls.
- The identity port must not answer before the frontend is serving. A gets
  this ordering for free because it shares the listener.

**D, channel hello.** It would let every client notice a peer's new
incarnation on reconnect, before the sequence-mismatch `Reject`. But it
changes the wire protocol for every link and pushes identity into the channel
layer, which is otherwise identity-agnostic. It also fails requirement 4: the
hello arrives once per link, and a second caller in the same process reuses
the cached link and never sees it.

**E, bounce with the new id.** This is the only option that tells
*already-attached* clients about a restart without them asking. Normal
operation costs nothing, because the "no such proc" branch already exists.
Its weaknesses are that the reply is routed, so the client must be reachable,
and that initial discovery would require deliberately sending to a
placeholder `ProcId`. It is a good complement to A, not a replacement.

**F, launcher-derived.** It costs nothing at runtime but cannot satisfy
requirement 1. The Kubernetes launcher derives the identity from
`MONARCH_POD_UID` (`service_proc_id_from_uuid`), and a pod's UID survives an
in-place container restart, so a restarted worker comes back with the same
`ProcId`.

**G, registry.** It works, but it is the side channel that requirement 2
rules out, and it adds per-platform configuration.

**H, gateway duplex branch.** The gateway's duplex accept path is only used
to attach an external proc (`Gateway::serve_via`). Normal clients reach a
host over simplex, and spawned child procs connect through peer routes on the
backend, so this path is the wrong layer for host identity.

**I, location-addressed query.** This reintroduces the location-qualified
`service` singleton that was just removed, and it adds a branch to delivery.

## Decision

We use **A**. It satisfies every requirement: it costs nothing per message,
needs no configuration, answers on the connection that asked, supports any
number of concurrent queriers, and fails fast against old servers. **E** is a
possible follow-up so that an attached client hitting a stale `ProcAddr`
learns the new identity in its supervision event.

## The mechanism

- **Wire.** A query connection writes a `LinkInit` with magic `QRY\0` (the
  session and stream fields are unused). The server writes one length-prefixed
  frame containing an opaque reply and closes the connection. If the listener
  has no reply configured, it closes the connection without writing.
- **Channel API.** `channel::serve_mux` takes an optional query reply, and
  `channel::query(addr)` dials, reads the reply, and returns its bytes. The
  channel layer never interprets the bytes.
- **Host.** `Host` configures its frontend's query reply as the service
  proc's `ProcId` in string form, which is known before the frontend binds.
- **Worker.** A worker started from a bare channel URL mints a fresh instance
  `ProcId` at startup. The identity is fixed for the life of the process; a
  query is a pure read.
- **Client.** `attach_to_workers` accepts either a full `ProcAddr` or a bare
  channel URL. For a bare URL it queries the address and attaches to the
  returned `ProcId` at the address it dialed.

With discovery available, launchers no longer need to allocate service
identities and pass them through the environment, although explicit
`ProcAddr`s continue to work.

## Attaching again after a restart

A client that attached to a host before it restarted still has a cached
channel to the host's address. Two router behaviors let the same client
process attach to the restarted host:

- **Stale sessions.** The restarted host has no record of the cached
  session and rejects it. Messages rejected this way were not delivered, so
  `DialMailboxRouter` re-posts those addressed directly to a proc at that
  address on a fresh session. Without this, the first message after the
  restart, typically the attach config push, is lost, and the attach waits
  out `MESH_ATTACH_CONFIG_TIMEOUT`. Messages for a host's child procs are
  not re-posted, because those procs died with the old host process.
- **Long outages.** If the host stays down past `MESSAGE_DELIVERY_TIMEOUT`,
  the cached channel closes with a delivery timeout. The router evicts it,
  so the next send dials a fresh session once the host is back.
