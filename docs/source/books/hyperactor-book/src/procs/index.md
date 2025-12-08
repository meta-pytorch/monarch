# Procs

A `Proc` is a container for actors and provides the runtime context for actor execution, message routing, and lifecycle management. In hyperactor's architecture, procs serve as the fundamental unit of deployment and isolation.

## Overview

Procs bridge the gap between actors (local computation) and the distributed system (remote communication). Each proc:

- **Hosts actors**: Spawns and manages actor instances within its process
- **Routes messages**: Provides a forwarder that determines how outbound messages reach their destinations
- **Identifies actors**: All actors within a proc share the proc's identity as part of their `ActorId`
- **Manages lifecycle**: Coordinates actor startup, supervision, and shutdown

## Key Components

### Proc

The `Proc` struct represents a single runtime instance. It's created with:
- A `ProcId` that uniquely identifies it (see [ProcId](../references/proc_id.md))
- A forwarder (`BoxedMailboxSender`) for routing outbound messages

See [Proc](proc.md) for details on construction and usage.

### Host

The `Host` manages multiple spawned procs and provides the infrastructure for:
- Accepting external connections (frontend)
- Coordinating message routing between procs (backend)
- Managing a service proc for system-level coordination

See [Host](host.md) for the complete hosting architecture.

### ProcOrDial Router

A specialized router that enables bidirectional communication by distinguishing between:
- Messages destined for the service proc (delivered locally)
- Messages destined for spawned procs (dialed remotely)

See [ProcOrDial Router](proc_or_dial.md) for routing implementation details.

## See Also

- [ProcId](../references/proc_id.md) - Proc identity and addressing
- [ActorId](../references/actor_id.md) - How actors inherit proc identity
- [Channels](../channels/index.md) - Transport layer for inter-proc communication
- [Routers](../mailboxes/routers.md) - Message routing infrastructure
