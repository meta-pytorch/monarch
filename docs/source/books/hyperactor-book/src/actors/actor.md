# The `Actor` Trait

The `Actor` trait defines the core behavior of all actors in the hyperactor runtime.

Every actor type must implement this trait to participate in the system. It defines how an actor is constructed, initialized, and supervised.

```rust
#[async_trait]
pub trait Actor: Sized + Send + Debug + 'static {
    type Params: Send + 'static;

    async fn new(params: Self::Params) -> Result<Self, anyhow::Error>;

    async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn spawn(
        cx: &impl context::Actor,
        params: Self::Params,
    ) -> anyhow::Result<ActorHandle<Self>> {
        cx.instance().spawn(params).await
    }

    async fn spawn_detached(params: Self::Params) -> Result<ActorHandle<Self>, anyhow::Error> {
        Proc::local().spawn("anon", params).await
    }

    fn spawn_server_task<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(future)
    }

    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        _event: &ActorSupervisionEvent,
    ) -> Result<bool, anyhow::Error> {
        Ok(false)
    }

    async fn handle_undeliverable_message(
        &mut self,
        this: &Instance<Self>,
        Undeliverable(envelope): Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        assert_eq!(envelope.sender(), this.self_id());

        anyhow::bail!(UndeliverableMessageError::delivery_failure(&envelope));
    }
}
```

## Construction: `Params` and `new`

Each actor must define a `Params` type:

```rust
type Params: Send + 'static;
```

This associated type defines the data required to instantiate the actor.

The actor is constructed by the runtime using:
```rust
async fn new(params: Self::Params) -> Result<Self, anyhow::Error>;
```

This method returns the actor's internal state. At this point, the actor has not yet been connected to the runtime; it has no mailbox and cannot yet send or receive messages. `new` is typically used to construct the actor's fields from its input parameters.

## Initialization: `init`

```rust
async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error>
```

The `init` method is called after the actor has been constructed with `new` and registered with the runtime. It is passed a reference to the actor's `Instance`, allowing access to runtime services such as:
- The actor’s ID and status
- The mailbox and port system
- Capabilities for spawning or sending messages

The default implementation does nothing and returns `Ok(())`.

If `init` returns an error, the actor is considered failed and will not proceed to handle any messages.

Use `init` to perform startup logic that depends on the actor being fully integrated into the system.

## Spawning: `spawn`

The `spawn` method provides a default implementation for creating a new actor from an existing one:

```rust
async fn spawn(
    cx: &impl context::Actor,
    params: Self::Params,
) -> anyhow::Result<ActorHandle<Self>> {
    cx.instance().spawn(params).await
}
```

In practice, `context::Actor` is implemented for types such as `Instance<A>` and `Context<A>`, which represent running actors. As a result, `Actor::spawn(...)` always constructs a child actor: the new actor receives a child ID and is linked to its parent through the runtime.

## Detached Spawning: `spawn_detached`

```rust
async fn spawn_detached(params: Self::Params) -> Result<ActorHandle<Self>, anyhow::Error> {
    Proc::local().spawn("anon", params).await
}
```
This method creates a root actor on a fresh, isolated proc.
- The proc is local-only and cannot forward messages externally.
- The actor receives a unique root `ActorId` with no parent.
- No supervision or linkage is established.
- The actor is named `"anon"`.

## Background Tasks: `spawn_server_task`

```rust
fn spawn_server_task<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future)
}
```

This method provides a hook point for customizing how the runtime spawns background tasks.

By default, it simply calls `tokio::spawn(...)` to run the given future on the Tokio executor.

# Supervision Events: `handle_supervision_event`

```rust
async fn handle_supervision_event(
    &mut self,
    _this: &Instance<Self>,
    _event: &ActorSupervisionEvent,
) -> Result<bool, anyhow::Error> {
    Ok(false)
}
```
This method is invoked when the runtime delivers an `ActorSupervisionEvent` to the actor — for example, when a child crashes or exits.

By default, it returns `Ok(false)`, which indicates that the event was not handled by the actor. This allows the runtime to fall back on default behavior (e.g., escalation).

Actors may override this to implement custom supervision logic.

## Undeliverables: `handle_undeliverable_message`

```rust
async fn handle_undeliverable_message(
    &mut self,
    this: &Instance<Self>,
    Undeliverable(envelope): Undeliverable<MessageEnvelope>,
) -> Result<(), anyhow::Error> {
    assert_eq!(envelope.sender(), this.self_id());

    anyhow::bail!(UndeliverableMessageError::delivery_failure(&envelope));
}
```
This method is called when a message sent by this actor fails to be delivered.
- It asserts that the message was indeed sent by this actor.
- Then it returns an error: `Err(UndeliverableMessageError::DeliveryFailure(...))`

This signals that the actor considers this delivery failure to be a fatal error. You may override this method to suppress the failure or to implement custom fallback behavior.
