# `ActorHandle<A>`

An `ActorHandle<A>` is a reference to a **local, running actor** of type `A`. It provides access to the actor's messaging ports, lifecycle status, and control methods (such as stop signals).

Unlike remote references (e.g. `ActorRef<A>`), which may refer to actors on other `Proc`s, an `ActorHandle` only exists within the same `Proc` and can be sent messages without requiring serialization.

## Definition

```rust
pub struct ActorHandle<A: Actor> {
    cell: InstanceCell,
    ports: Arc<Ports<A>>,
}
```
An `ActorHandle` contains:
- `cell` is the actor’s internal runtime state, including identity and lifecycle metadata.
- `ports` is a shared dictionary of all typed message ports available to the actor.

This handle is cloneable, sendable across tasks, and allows interaction with the actor via messaging, status observation, and controlled shutdown.

## Methods

### `new` (internal)

Constructs a new `ActorHandle` from its backing `InstanceCell` and `Ports`. This is called by the runtime when spawning a new actor.
```rust
pub(crate) fn new(cell: InstanceCell, ports: Arc<Ports<A>>) -> Self {
    Self { cell, ports }
}
```

### `cell` (internal)

Returns the underlying `InstanceCell` backing the actor.
```rust
pub(crate) fn cell(&self) -> &InstanceCell {
    &self.cell
}
```

### `actor_id`

Returns the `ActorId` of the actor represented by this handle.
```rust
pub fn actor_id(&self) -> &ActorId {
    self.cell.actor_id()
}
```

### `drain_and_stop`

Signals the actor to drain any pending messages and then stop. This enables a graceful shutdown procedure.
```rust
pub fn drain_and_stop(&self) -> Result<(), ActorError> {
    self.cell.signal(Signal::DrainAndStop)
}
```

### `status`

Returns a watch channel that can be used to observe the actor's lifecycle status (e.g., running, stopped, crashed).
```rust
pub fn status(&self) -> watch::Receiver<ActorStatus> {
    self.cell.status().clone()
}
```

### `send`

Sends a message of type `M` to the actor. The actor must implement `Handler<M>` for this to compile.

Messages sent via an `ActorHandle` are always delivered in-process and do not require serialization.
```rust
pub fn send<M: Message>(&self, message: M) -> Result<(), MailboxSenderError>
where
    A: Handler<M>,
{
    self.ports.get().send(message)
}
```

### `port`

Returns a reusable port handle for the given message type.
```rust
pub fn port<M: Message>(&self) -> PortHandle<M>
where
    A: Handler<M>,
{
    self.ports.get()
}
```

### `bind`

Creates a remote reference (`ActorRef<R>`) by applying a `Binds<A>` implementation.
```rust
pub fn bind<R: Binds<A>>(&self) -> ActorRef<R> {
    self.cell.bind(self.ports.as_ref())
}
```

### Binding and ActorRefs

The `bind()` method on `ActorHandle` creates an `ActorRef<R>` for a given remote-facing reference type `R`. This is the bridge between a local actor instance and its externally visible interface.
```rust
pub fn bind<R: Binds<A>>(&self) -> ActorRef<R>
```
This method requires that `R` implements the `Binds<A>` trait. The `Binds` trait specifies how to associate a remote-facing reference type with the concrete ports handled by the actor:
```rust
pub trait Binds<A: Actor>: Referable {
    fn bind(ports: &Ports<A>);
}
```
In practice, `A` and `R` are usually the same type; this is the pattern produced by the `#[export]` macro. But `R` can also be a trait object or wrapper that abstracts over multiple implementations.

### Binding internals

Calling `bind()` on the `ActorHandle`:
1. Invokes the `Binds<A>::bind()` implementation for `R`, registering the actor's message handlers into the `Ports<A>` dictionary.
2. Always binds the `Signal` type (used for draining, stopping, and supervision).
3. Records the bound message types into `InstanceState::exported_named_ports`, enabling routing and diagnostics.
4. Constructs the final `ActorRef<R>` using `ActorRef::attest(...)`, which assumes the type-level correspondence between `R` and the bound ports.

The result is a typed, routable reference that can be shared across `Proc`s.

## `IntoFuture for ActorHandle`

### Overview

An `ActorHandle<A>` can be awaited directly thanks to its `IntoFuture` implementation. Awaiting the `handle` waits for the actor to shut down.

### Purpose

This allows you to write:
```rust
let status = actor_handle.await;
```
Instead of:
```rust
let mut status = actor_handle.status();
status.wait_for(ActorStatus::is_terminal).await;
```

### Behavior

When awaited, the handle:
- Subscribes to the actor’s status channel,
- Waits for a terminal status (`Stopped`, `Crashed`, etc.),
- Returns the final status,
- Returns `ActorStatus::Unknown` if the channel closes unexpectedly.

### Implementation
```rust
impl<A: Actor> IntoFuture for ActorHandle<A> {
    type Output = ActorStatus;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let future = async move {
            let mut status_receiver = self.cell.status().clone();
            let result = status_receiver.wait_for(ActorStatus::is_terminal).await;
            match result {
                Err(_) => ActorStatus::Unknown,
                Ok(status) => status.clone(),
            }
        };
        future.boxed()
    }
}
```
### Summary

This feature is primarily ergonomic. It provides a natural way to synchronize with the termination of an actor by simply awaiting its handle.
