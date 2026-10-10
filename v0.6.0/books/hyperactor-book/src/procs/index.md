# Procs

A `Proc` is a container for actors and provides the runtime context for actor execution, message routing, and lifecycle management. In hyperactor's architecture, procs serve as the fundamental unit of deployment and isolation.

## Overview

Procs bridge the gap between actors (local computation) and the distributed system (remote communication). The three types in this section fit together as follows:

- A **`Proc`** hosts actors and manages their lifecycle, but is not itself a network endpoint.
- A **`Gateway`** is the connectivity layer a proc attaches to: it multiplexes inbound traffic to the right local proc and routes outbound traffic on the proc's behalf.
- A **`Host`** manages the procs on one machine and owns the single gateway through which they are reached.

## Key Components

### Proc

The `Proc` struct represents a single actor runtime. It owns actor lifecycle and identity (all actors within a proc share its `ProcId` as part of their `ActorId`) and reaches the outside world only through its gateway.

See [Proc](proc.html) for details on construction and usage.

### Gateway

The `Gateway` is the connectivity layer for procs. It has one table for local
in-process procs, one table for peer gateways keyed by `Via(uid)` hops, and a
forwarder for everything else. The same routing decision handles local delivery,
spawned children, attached clients, and ordinary egress.

See [Gateway](gateway.html) for the type and its routing decision.

### Host

The `Host` manages the lifecycle of the procs on one machine. It owns exactly
one gateway and delegates all connectivity to it: `service` and `local` attach
as in-process procs, while spawned children attach as gateway peers.

See [Host](host.html) for the complete hosting architecture.

## See Also

- [Gateway](gateway.html) - The connectivity layer procs attach to
- [ProcId](../references/proc_id.html) - Proc identity and addressing
- [ActorId](../references/actor_id.html) - How actors inherit proc identity
- [Channels](../channels/index.html) - Transport layer for inter-proc communication
- [Routers](../mailboxes/routers.html) - Message routing infrastructure