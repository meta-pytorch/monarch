# Proc

A `Proc` is the fundamental container for actors in hyperactor. It provides
the runtime context for actor execution: it owns actor lifecycle, delivers
messages to its local actors, and hosts supervision state.

A proc is **not** a network endpoint by itself. It reaches—and is reached by—
the outside world only through a [`Gateway`](gateway.md), its connectivity
boundary. Every proc is attached to exactly one gateway; outbound messages flow
through that gateway, and inbound messages for the proc arrive via the gateway's
ingress path. Procs that share a gateway reach each other by an in-gateway
lookup rather than dialing.

## Construction

Build a proc with the builder, choosing how it attaches to a gateway:

```rust
// Default: attach to the process-wide `Gateway::global()`.
// (Legacy pseudo-singleton ids are reserved for host-scoped construction.)
let proc = Proc::builder().proc_id(id).build()?;

// Attach to a caller-provided gateway — e.g. a host wiring its procs
// into the one gateway it owns. Accepts any proc id.
let proc = Proc::builder().shared_gateway(gateway.clone()).build()?;
```

Convenience constructors cover common cases:

- `Proc::isolated()` — a random-id proc on a fresh local-only gateway
  (`Gateway::isolated()`); outbound to unknown destinations is undeliverable.
  Useful in tests.
- `Proc::configured(proc_id, forwarder)` — wraps `Gateway::configured(location,
  forwarder)` so the proc advertises `location` and uses `forwarder` for
  egress.
- `Proc::direct(addr, name)` — a direct-addressed proc served on its own
  channel.

The proc's gateway is reachable via `Proc::gateway()`.

## Proc identity and addressing

`ProcId` is pure identity. `ProcAddr` pairs that id with the `Location` where the proc is reachable:

```rust
let addr: ChannelAddr = "unix:@abc123".parse()?;
let proc_addr = ProcAddr::instance(addr, "service");
```

A proc is reached through its gateway's advertised location. On a host, spawned
children are advertised through the host frontend with a
`Via(child_uid, frontend_addr)` location, so the host gateway can peel the child
uid and forward to the child gateway peer. See [ProcId](../references/proc_id.md)
for identity details and [Addr](../references/reference.md) for addressable
references.

## Egress through the gateway

When an actor sends a message to an actor in another proc, the message leaves
the proc through its gateway, which applies the single
[routing decision](gateway.md#the-single-routing-decision): deliver to a local
proc, forward to a child proc or a known peer by peeling its `Via` hop, or hand
to the forwarder for everything else. Spawned children are just peers keyed by
their child uid. The default forwarder is a `DialMailboxRouter`, which looks up
the target's address and dials it.

### Local proc bypass

For procs that run in *separate processes* on the same host, same-host
proc-to-proc traffic can skip the host round-trip. At spawn time the proc
manager gives the child gateway a `LocalProcDialer` forwarder that dials a
sibling proc directly over its Unix socket when the destination is local, and
falls back to the host backend otherwise:

```rust
let backend_sender = MailboxClient::dial(backend_addr);
let proc_forwarder = LocalProcDialer::new(host_frontend_addr, socket_dir, backend_sender);
let proc = Proc::configured(proc_id, proc_forwarder.boxed());
```

This reduces message copies and keeps the host agent off the hot path for
intra-host traffic. See `hyperactor_mesh::bootstrap::mailbox::LocalProcDialer`.

## Spawning actors

Once you have a proc, spawn actors within it:

```rust
let actor_handle = proc.spawn("my_actor", MyActor::new())?;
```

The spawned actor runs within this proc, inherits the proc's `ProcId` as part
of its `ActorId`, and uses the proc's gateway for outbound messages. See
[RemoteSpawn](../actors/remote_spawn.md) for spawning details.

## See Also

- [Gateway](gateway.md) — the connectivity boundary every proc attaches to
- [Host](host.md) — how hosts create and manage procs
- [ProcId](../references/proc_id.md) — proc addressing
- [ActorId](../references/actor_id.md) — how actors inherit proc identity
