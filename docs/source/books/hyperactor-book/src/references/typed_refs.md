# Typed References

Typed references are strongly typed wrappers over addresses like `ActorAddr` and `PortAddr`. These types are used throughout hyperactor's APIs: as parameters in messages, return values from `bind()` methods, and elements in routing decisions. They make distributed communication safe, expressive, and statically checked.

## Overview

There are three main typed reference types:

- [`ActorRef<A>`](#actorrefa): A typed reference to an actor implementing the `Referable` trait.
- [`PortRef<M>`](#portrefm): A reference to a reusable mailbox port for messages of type `M` implementing the `RemoteMessage` trait.
- [`OncePortRef<M>`](#onceportrefm): A reference to a one-shot port for receiving a single response of type `M` implementing the `RemoteMessage` trait.

These types are used as parameters in messages, return values from bindings, and components of the routing system.

---

## `ActorRef<A>`

`ActorRef<A>` is a typed reference to an actor of type `A`. It provides a way to identify and address remote actors that implement `Referable`.

```rust
let actor_ref: ActorRef<MyActor> = ActorRef::attest(actor_addr);
```

> **Note**: While `ActorRef::attest` can construct a reference from an `ActorAddr`, prefer the `ActorRef` returned from `ActorHandle::bind()`, which guarantees that the actor is running and has bound ports. `attest` bypasses that guarantee.

> **Note**: The `Referable` trait only requires that `A` provides a static name via `Named`. It does not impose `Send` or `Sync` bounds—those are added at specific call sites that need them.

Unlike `ActorHandle<A>`, an `ActorRef` is just a reference — it doesn't guarantee that the actor is currently running. It's primarily used for routing and type-safe messaging across `Proc`s.

### Definition
```rust
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ActorRef<A: Referable> {
    actor_addr: ActorAddr,
    phantom: PhantomData<A>,
}
```
This type is a thin wrapper around an `ActorAddr`, with a phantom type `A` to track which actor interface it refers to. It ensures you can only send messages supported by the actor's declared `RemoteHandles`.

## `PortRef<M>`

`PortRef<M>` refers to a mailbox port for messages of type `M`.
```rust
let (port, mut receiver) = actor.open_port::<MyMessage>();
let port_ref: PortRef<MyMessage> = port.bind();
```

This allows the port to be sent across the network or passed into other messages. On the receiving end, `PortRef` can be used to deliver messages of the expected type.

### Definition

```rust
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash, Ord, Named)]
pub struct PortRef<M> {
    port_addr: PortAddr,
    reducer_spec: Option<ReducerSpec>,
    streaming_opts: StreamingReducerOpts,
    phantom: PhantomData<M>,
    return_undeliverable: bool,
    unsplit: bool,
}
```
As with `ActorRef`, this is a typed wrapper around an address (`PortAddr`), carrying a phantom type for safety. It ensures that only messages of type `M` can be sent through this reference.

## `OncePortRef<M>`

A `OncePortRef<M>` is like a `PortRef`, but designed for exactly one response. Once used, it cannot be reused or cloned.
```rust
let (once_port, receiver) = actor.open_once_port::<MyMessage>();
let once_ref = once_port.bind();
```
These are commonly used for request/response interactions, where a single reply is expected.

### Definition

```rust
#[derive(Debug, PartialEq)]
pub struct OncePortRef<M> {
    port_addr: PortAddr,
    reducer_spec: Option<ReducerSpec>,
    return_undeliverable: bool,
    unsplit: bool,
    phantom: PhantomData<M>,
}
```
This wraps a `PortAddr` with a phantom message type `M` for type safety. Internally, the system enforces one-time delivery semantics, ensuring the port is closed after receiving a single message.
