# Host

A `Host` is the mesh concept of a host: it manages the lifecycle of the procs
running on one machine. It is deliberately narrow—connectivity is *not* its
job. A host owns exactly one [`Gateway`](gateway.md), and delegates all
routing and multiplexing to it. The host serves its endpoints through
the gateway and never inspects transports or rewrites locations itself.

## How a host is put together

A host owns:

- **one `Gateway`** — the connectivity layer for every proc on the host (see
  [Gateway](gateway.md));
- **two built-in procs**, `service_proc` and `local_proc`, that run
  *in-process* and therefore sit in the gateway's `procs` table;
- a **frontend address** (`*`), the gateway's bound listening address that the
  rest of the mesh uses to reach this host's procs;
- a **backend address** (`#`), where spawned child procs dial back;
- a **`ProcManager`** that knows how to make a proc real (fork a process, run
  the bootstrap command, wire the backchannel) or spawn it in-process for
  tests.

```text
  ┌─────────────────────────────────────────────┐
  │ Host  (one machine)                          │
  │  frontend = * (gateway listens here)         │
  │  backend  = # (children dial back here)      │
  │                                              │
  │   ┌──────────────── Gateway ─────────────┐   │
  │   │ local:   service_proc, local_proc    │   │   ┌─────────────────┐
  │   │ peers:   C1 → dial(child1) ──────────┼───┼──▶│ child gateway 1 │
  │   │          C2 → dial(child2) ──────────┼───┼──▶│ child gateway 2 │
  │   │ forwarder: DialMailboxRouter         │   │   └─────────────────┘
  │   └──────────────────────────────────────┘   │
  └─────────────────────────────────────────────┘
```

Spawned children use ids derived from their names. Their locations extend
`gateway.default_location()` with a child attachment key, preserving any
upstream path. The gateway advances the envelope's path cursor and forwards
over the registered child sender. See [Stable Locations](stable_locations.md).

## The two built-in procs

`service_proc` and `local_proc` are in-process and share the host's single
gateway, so they reach each other—and the host reaches them—through the
gateway's local `procs` table, with zero dialing.

`local_proc` and `service_proc` have fresh instance ids labeled `"local"` and
`"service"`. Launchers pass the service proc's identity through bootstrap and
retain its `ProcAddr` for attaching to the host. Multiple hosts can share a
gateway because their service proc ids are distinct.

## Spawned children

Each spawned child has its own gateway endpoint. In production that usually
means a separate OS process; `LocalProcManager` uses the same protocol
in-process for tests.

`Host::spawn` advertises the child at `host_location.append_via(child_uid)`,
waits for its serving address, and registers the dialed sender with
`Gateway::attach_peer(child_uid, sender)`. Messages descend the top-down path
and advance the cursor at each attachment. The returned `PeerAttachGuard`
keeps the route alive; dropping it frees that registration without removing
a newer replacement.

## Binding and serving

Connectivity lives in the gateway. `Host::new_with_gateway` starts the backend
and frontend listeners, then constructs the built-in procs against the gateway's
active advertised location.

The bound backend address is passed to children as their fallback route back to
the host. Bound listening addresses and advertised locations are distinct:
without an upstream, the newest listener is advertised; with an upstream, its
core address and attachment path remain the default. Built-in and child refs
snapshot that location, including when private listeners are started later.

## Local proc invariant (LP-1)

The local proc always exists as an instance proc with the label `"local"` on
the host gateway and is routed through the gateway's local proc table, but it
starts with zero actors. A `ProcAgent` and root client actor are added only when
`HostMeshAgent::handle(GetLocalProc)` is first called.

## See Also

- [Gateway](gateway.md) — the connectivity layer a host owns and delegates to
- [Proc](proc.md) — the runtime managed by the host's `ProcManager`
- [ProcId](../references/proc_id.md) — proc identity
