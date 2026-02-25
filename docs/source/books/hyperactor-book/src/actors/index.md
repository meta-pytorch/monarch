# Actors

Hyperactor programs are structured around actors: isolated state machines that process messages asynchronously.

Each actor runs in isolation, and maintains private internal state. Actors interact with the outside world through typed message ports and follow strict lifecycle semantics managed by the runtime.

This chapter introduces the actor system in hyperactor. We'll cover:

- The [`Actor`](./actor.md) trait and its lifecycle hooks
- The [`Handler`](./handler.md) trait for defining message-handling behavior
- The [`RemoteSpawn`](./remote_spawn.md) trait for enabling remote spawning
- The [remote actor registry](./remote.md) for registering remotable-spawnable actors via `remote!` and spawning them by global type name
- The [`Checkpointable`](./checkpointable.md) trait for supporting actor persistence and recovery
- The [`Referable`](./referable.md) marker trait for remotely referencable types
- The [`Binds`](./binds.md) trait for wiring exported ports to reference types
- The [`RemoteHandles`](./remote_handles.md) trait for associating message types with a reference
- The [`ActorHandle`](./actor_handle.md) type for referencing and communicating with running actors
- [Actor Lifecycle](./actor_lifecycle.md), including `Signal` and `ActorStatus`

Actors are instantiated with parameters and bound to mailboxes, enabling reliable message-passing. The runtime builds upon this foundation to support supervision, checkpointing, and remote interaction via typed references.
