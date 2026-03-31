# Proc

A `Proc` is the fundamental container for actors in hyperactor. It provides the runtime context for actor execution and determines how messages are routed—both inbound messages arriving at the proc from the network and outbound messages leaving the proc.

## Construction

Create a proc using `Proc::new()`:

```rust
pub fn new(proc_id: ProcId, forwarder: BoxedMailboxSender) -> Self
```

**Parameters:**

- `proc_id`: The proc's unique identity (see [ProcId](../references/proc_id.md))
- `forwarder`: A `BoxedMailboxSender` that routes all outbound messages from this proc

The forwarder is the critical routing component for remote delivery - when an actor within this proc sends a message to an actor in another proc, it flows through this forwarder.

## Example: Service Proc in Host

From `Host::new()`:

```rust
let router = DialMailboxRouter::new();

let service_proc_id = ProcId::Direct(
    frontend_addr.clone(),
    "service".to_string()
);

let service_proc = Proc::new(service_proc_id, router.boxed());
```

Here:
- The proc is identified by `ProcId::Direct(frontend_addr, "service")`
- All outbound messages use the `DialMailboxRouter` as their forwarder
- The router will look up target procs and dial their backend addresses

## ProcId: Direct Addressing

Procs are identified using `ProcId::Direct`:

```rust
ProcId::Direct(ChannelAddr, String)
```

Example:

```rust
let addr: ChannelAddr = "unix:@abc123".parse()?;
let proc_id = ProcId::Direct(addr, "service".to_string());
```

The proc is addressed by:
- The host's channel address (where it can be reached)
- A name identifying the proc within that host

See [ProcId](../references/proc_id.md) for complete details.

## Forwarder Integration

The forwarder determines how messages leave the proc. Typically, a `DialMailboxRouter` is used to enable dynamic routing to multiple destinations.

### Using DialMailboxRouter

Routes messages by looking up target addresses and dialing connections:

```rust
let router = DialMailboxRouter::new();
let proc = Proc::new(proc_id, router.boxed());
```

When an actor sends a message to another proc, the router:
1. Looks up the target's address in its address book
2. Dials a connection (or reuses a cached one)
3. Sends the message over that connection

See [Routers](../mailboxes/routers.md#dialmailboxrouter-remote-and-serializable-routing).

### Local Proc Bypass

When a proc sends a message to an actor in another proc, the message normally flows through the proc's forwarder to a backend address, where a central routing layer forwards it to the destination proc (see [Host](host.md) for the complete architecture with frontend and backend receivers). For procs running on the same host, this can be optimized: procs can dial each other directly via Unix sockets, bypassing the central routing layer entirely.

**Injection point**: When a proc manager spawns a new proc, instead of providing a plain backend sender as the forwarder, it wraps it with a local-aware forwarder:

```rust
// At proc spawn time, the manager injects a local bypass forwarder:
let backend_sender = MailboxClient::dial(backend_addr);
let proc_forwarder = LocalProcDialer::new(
    host_frontend_addr,   // Identify local procs by this address
    socket_dir,           // Directory where procs place Unix sockets
    backend_sender,       // Fallback for remote procs
);
let proc = Proc::new(proc_id, proc_forwarder.boxed());
```

The forwarder checks each outbound message: if the destination is a local proc (matching the host's frontend address), it dials directly via the proc's Unix socket. Otherwise, it routes through the backend sender:

```rust
impl MailboxSender for LocalProcDialer {
    fn post_unchecked(&self, envelope: MessageEnvelope, ...) {
        if envelope.dest().proc_id() == local_proc_on_this_host {
            unix_sender.post_unchecked(envelope, ...)  // Direct
        } else {
            backend_sender.post_unchecked(envelope, ...)  // Via host backend
        }
    }
}
```

This improves throughput for intra-host communication by reducing message copies and preventing the host agent from becoming a sequencing bottleneck for all messages between local procs, while maintaining compatibility with remote routing.

See `hyperactor_mesh::bootstrap::mailbox::LocalProcDialer` for the implementation.

## Spawning Actors

Once you have a proc, spawn actors within it:

```rust
let actor_handle = proc.spawn("my_actor", MyActor::new())?;
```

The spawned actor:
- Runs within this proc
- Inherits the proc's `ProcId` as part of its `ActorId`
- Uses the proc's forwarder for outbound messages

See [RemoteSpawn](../actors/remote_spawn.md) for spawning details.

## See Also

- [Host](host.md) - How hosts create and manage procs
- [ProcId](../references/proc_id.md) - Proc addressing schemes
- [Routers](../mailboxes/routers.md) - Forwarder implementations
- [ActorId](../references/actor_id.md) - How actors inherit proc identity
