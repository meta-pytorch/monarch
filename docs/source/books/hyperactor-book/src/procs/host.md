# Host

A `Host` manages a collection of spawned procs and provides bidirectional routing between them. It serves as the entry point for external connections and coordinates message delivery across local and remote actors.

## Overview

The `Host` struct maintains two key channel endpoints:

- **Frontend address**: accepts connections from external clients
- **Backend address**: receives messages from spawned procs

Both endpoints feed into a unified routing layer that can deliver messages to either the service proc (running within the host) or to spawned procs.

```text
                      ┌────────────┐
                  ┌───▶  proc *,1  │
                  │ #1└────────────┘
                  │
  ┌──────────┐    │   ┌────────────┐
  │   Host   │◀───┼───▶  proc *,2  │
 *└──────────┘#   │ #2└────────────┘
                  │
                  │   ┌────────────┐
                  └───▶  proc *,3  │
                    #3└────────────┘
```

Where:
- `*` is the host's frontend address (`frontend_addr`)
- `#` is the host's backend address (`backend_addr`)
- `#1`, `#2`, `#3` are the per-proc backend channels
- Each proc is direct-addressed via the host - its id is "proc at `*` named `N`"

## Structure

```rust
pub struct Host<M> {
    procs: HashSet<String>,
    frontend_addr: ChannelAddr,
    backend_addr: ChannelAddr,
    router: DialMailboxRouter,
    manager: M,
    service_proc: Proc,
    frontend_rx: Option<ChannelRx<MessageEnvelope>>,
}
```

**Fields:**

- `procs`: Stores proc names to avoid creating duplicates
- `frontend_addr`: The address external clients connect to
- `backend_addr`: The address spawned procs use to send messages back to the host
- `router`: A [`DialMailboxRouter`](../mailboxes/routers.md#dialmailboxrouter-remote-and-serializable-routing) for prefix-based routing to spawned procs
- `manager`: A `ProcManager` implementation that handles proc lifecycle
- `service_proc`: The host's local proc for system-level actors
- `frontend_rx`: Channel receiver for external connections (consumed during startup)

## Creating a Host

The `Host::new` constructor takes a `ProcManager` and a channel address to serve:

```rust
impl<M: ProcManager> Host<M> {
    pub async fn new(manager: M, addr: ChannelAddr) -> Result<Self, HostError> {
        let (frontend_addr, frontend_rx) = channel::serve(addr)?;
        let (backend_addr, backend_rx) = channel::serve(
            ChannelAddr::any(manager.transport())
        )?;

        let router = DialMailboxRouter::new();

        let service_proc_id = ProcId::Direct(
            frontend_addr.clone(),
            "service".to_string()
        );
        let service_proc = Proc::new(service_proc_id.clone(), router.boxed());

        let host = Host {
            procs: HashSet::new(),
            frontend_addr,
            backend_addr,
            router,
            manager,
            service_proc,
            frontend_rx: Some(frontend_rx),
        };

        let _backend_handle = host.forwarder().serve(backend_rx);

        Ok(host)
    }
}
```

### Understanding `channel::serve()`

`channel::serve()` is the universal "bind and listen" operation across different transport types. It:

- Takes a `ChannelAddr` (which can be a wildcard like `ChannelAddr::any()`)
- Binds a server/listener on that address
- Returns a tuple of:
  - The **actual bound address** (resolved from wildcards)
  - A **receiver** (`ChannelRx<MessageEnvelope>`) for incoming messages

This is why both calls in `Host::new` capture the returned address:

```rust
let (frontend_addr, frontend_rx) = channel::serve(addr)?;
let (backend_addr, backend_rx) = channel::serve(ChannelAddr::any(manager.transport()))?;
```

The returned address is the **actual bound address** you can give to others to connect to. For example, when you pass `ChannelAddr::Tcp(127.0.0.1:0)`:

- **Input**: "bind to localhost on any available port"
- **Output**: `(ChannelAddr::Tcp(127.0.0.1:54321), rx)` - the OS-assigned port

See [Channel Addresses](../channels/addresses.md) and [Transmits and Receives](../channels/tx_rx.md) for more on channel semantics.

### The Service Proc

The host creates a **service proc** identified by a `ProcId::Direct`:

```rust
let service_proc_id = ProcId::Direct(
    frontend_addr.clone(),
    "service".to_string()
);
let service_proc = Proc::new(service_proc_id, router.boxed());
```

This proc:
- Lives within the host process
- Uses `ProcId::Direct(frontend_addr, "service")` as its identity
- Forwards outbound messages through the `DialMailboxRouter`
- Hosts system-level actors that manage proc lifecycle and coordination

See [`ProcId` variants](../references/proc_id.md) for the distinction between `Ranked` and `Direct` addressing.

## Routing Architecture

The host implements bidirectional routing using a specialized `ProcOrDial` router (see [ProcOrDial Router](proc_or_dial.md)). Both the frontend and backend receivers are served by this router:

**Backend receiver** (from spawned procs):
```rust
let _backend_handle = host.forwarder().serve(backend_rx);
```

**Frontend receiver** (from external clients):
```rust
Some(self.forwarder().serve(self.frontend_rx.take()?))
```

### Complete Routing Flow

```text
frontend_rx (external connections)    ──┐
                                        ├──> serve() ──> ProcOrDial   ──┬──> service proc
backend_rx (from spawned procs)       ──┘                               └──> DialMailboxRouter
                                                                             │
                                                                             └──> looks up proc by name
                                                                                  └──> dials backend addr
```

Both receivers feed into the same `ProcOrDial` router, creating bidirectional routing:

- **Inbound (frontend)**: External → ProcOrDial → either service proc or spawned proc
- **Inbound (backend)**: Spawned procs → ProcOrDial → either service proc or other spawned procs
- **Outbound (from service proc)**: `service_proc.forwarder` = DialMailboxRouter → spawned procs

See [`MailboxServer::serve()`](../mailboxes/mailbox_server.md) for how receivers are bridged to routers.

## Channel Receivers

The `ChannelRx<M>` receiver returned from `channel::serve()` implements the `Rx<M>` trait:

```rust
trait Rx<M: RemoteMessage> {
    async fn recv(&mut self) -> Result<M, ChannelError>;
    fn addr(&self) -> ChannelAddr;
}
```

It's a stream of incoming messages of type `M`. In the host context, `M = MessageEnvelope`, so it receives actor messages from the network.

**How the host uses receivers:**

- **Frontend**: Serves the user-provided `addr` → receives messages from external connections via `frontend_rx`
- **Backend**: Serves a wildcard backend address → receives messages from spawned procs via `backend_rx`

Both are consumed by calling `.serve()` on the `ProcOrDial` forwarder, which bridges the channel receivers to the mailbox routing system.

## Next Steps

- See [ProcOrDial Router](proc_or_dial.md) for the routing implementation
- See [Routers](../mailboxes/routers.md) for `DialMailboxRouter` details
- See [Proc](proc.md) for how procs integrate with routers, including the local proc bypass optimization
