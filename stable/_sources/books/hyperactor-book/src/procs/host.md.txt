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

Spawned children use ids derived from their names and locations of the form
`Via(child_uid, frontend_addr)`, so all children are reached through the one
frontend address. The gateway peels the child uid and forwards to the child's
registered peer sender.

## The two built-in procs

`service_proc` and `local_proc` are in-process and share the host's single
gateway, so they reach each other—and the host reaches them—through the
gateway's local `procs` table, with zero dialing.

`local_proc` always has a fresh instance id labeled `"local"`. Launchers can
also give `service_proc` a fresh instance id labeled `"service"` through
`Host::new_with_gateway`. The default remains the legacy `"service"`
pseudo-singleton for compatibility. A gateway can contain multiple hosts only
when each host has a distinct service proc id.

## Spawned children

Each spawned child has its own gateway endpoint. In production that usually
means a separate OS process; `LocalProcManager` uses the same protocol
in-process for tests.

`Host::spawn` advertises the child at `Via(child_uid, host_location)`, waits
for the child to report its serving address, dials that address, and registers
the sender with `Gateway::attach_peer(child_uid, sender)`. A message for that
child arrives at the host gateway, matches the child uid in `peers`, has that
via hop peeled, and is forwarded to the child gateway. The returned
`PeerAttachGuard` keeps the route alive; dropping it frees the slot.

## Binding and serving

Connectivity lives in the gateway. `Host::new_with_gateway` starts both host
servers during construction:

```rust
let mut backend_handle = gateway.serve(ChannelAddr::any(manager.transport()))?;
let backend_addr = gateway.default_location().addr().clone();

let frontend_handle = gateway.serve_with_listener(addr, listener)?;
let frontend_addr = gateway.default_location().addr().clone();

let service_proc = Proc::builder()
    .proc_id(service_proc_id)
    .shared_gateway(gateway.clone())
    .build()?;
let local_proc = Proc::builder()
    .proc_id(ProcId::instance(Label::strip("local")))
    .shared_gateway(gateway.clone())
    .build()?;
```

The backend address is passed to each child as its fallback route back to the
host. The frontend address is the host location used in public proc refs.

Because the gateway owns the advertised location, later gateway serves affect
new refs. Host construction serves the backend and frontend, so built-in procs
snapshot the frontend location. A `serve_via` session that already exists
remains active as an outbound route and as a valid local-delivery location, but
the frontend serve becomes the default location until a newer serve replaces it.

## Local proc invariant (LP-1)

The local proc always exists as an instance proc with the label `"local"` on
the host gateway and is routed through the gateway's local proc table, but it
starts with zero actors. A `ProcAgent` and root client actor are added only when
`HostMeshAgent::handle(GetLocalProc)` is first called.

## See Also

- [Gateway](gateway.md) — the connectivity layer a host owns and delegates to
- [Proc](proc.md) — the runtime managed by the host's `ProcManager`
- [ProcId](../references/proc_id.md) — proc identity
