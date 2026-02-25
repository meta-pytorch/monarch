# The `Actor` Trait

The `Actor` trait defines the core behavior of all actors in the hyperactor runtime.

Every actor type must implement this trait to participate in the system. It defines how an actor is constructed, initialized, and supervised.

```rust
#[async_trait]
pub trait Actor: Sized + Send + 'static {
    async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn cleanup(
        &mut self,
        _this: &Instance<Self>,
        _err: Option<&ActorError>,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    fn spawn(self, cx: &impl context::Actor) -> anyhow::Result<ActorHandle<Self>> {
        cx.instance().spawn(self)
    }

    fn spawn_detached(self) -> Result<ActorHandle<Self>, anyhow::Error> {
        Proc::local().spawn("anon", self)
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
        cx: &Instance<Self>,
        envelope: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        handle_undeliverable_message(cx, envelope)
    }

    fn display_name(&self) -> Option<String> {
        None
    }
}
```

## Initialization: `init`

```rust
async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error>
```

The `init` method is called after the actor has been constructed and registered with the runtime. It is passed a reference to the actor's `Instance`, allowing access to runtime services such as:
- The actor’s ID and status
- The mailbox and port system
- Capabilities for spawning or sending messages

The default implementation does nothing and returns `Ok(())`.

If `init` returns an error, the actor is considered failed and will not proceed to handle any messages.

Use `init` to perform startup logic that depends on the actor being fully integrated into the system.

## Cleanup: `cleanup`

```rust
async fn cleanup(
    &mut self,
    _this: &Instance<Self>,
    _err: Option<&ActorError>,
) -> Result<(), anyhow::Error>
```

The `cleanup` method is called before the actor shuts down. It provides a hook for async cleanup operations. The method receives:
- A reference to the actor's `Instance`
- An optional `ActorError` indicating whether the actor is failing

If `err` is not `None`, it contains the error that caused the actor to fail. Any errors returned by `cleanup` will be logged and ignored. If `err` is `None`, errors returned by `cleanup` will be propagated as an `ActorError`.

The default implementation does nothing. This method is not called if there is a panic in the actor or if the process is killed.

## Spawning: `spawn`

The `spawn` method provides a default implementation for spawning an actor:

```rust
fn spawn(self, cx: &impl context::Actor) -> anyhow::Result<ActorHandle<Self>> {
    cx.instance().spawn(self)
}
```

This method takes ownership of `self` (the actor instance) and spawns it as a child. In practice, `context::Actor` is implemented for types such as `Instance<A>` and `Context<A>`, which represent running actors. As a result, `Actor::spawn(...)` always constructs a child actor: the new actor receives a child ID and is linked to its parent through the runtime.

## Detached Spawning: `spawn_detached`

```rust
fn spawn_detached(self) -> Result<ActorHandle<Self>, anyhow::Error> {
    Proc::local().spawn("anon", self)
}
```
This method takes ownership of `self` and creates a root actor on a fresh, isolated proc.
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

## Display Name: `display_name`

```rust
fn display_name(&self) -> Option<String> {
    None
}
```

This method allows an actor to provide a custom display name for use in supervision error messages and logging. By default, it returns `None`, causing the runtime to use the `ActorId` for display purposes.

## Undeliverables: `handle_undeliverable_message`

```rust
async fn handle_undeliverable_message(
    &mut self,
    cx: &Instance<Self>,
    envelope: Undeliverable<MessageEnvelope>,
) -> Result<(), anyhow::Error> {
    handle_undeliverable_message(cx, envelope)
}
```
This method is called when a message sent by this actor fails to be delivered. The default implementation calls the free function `handle_undeliverable_message`, which:
- Asserts that the message was indeed sent by this actor.
- Returns an error: `Err(UndeliverableMessageError::DeliveryFailure(...))`

This signals that the actor considers this delivery failure to be a fatal error. You may override this method to suppress the failure or to implement custom fallback behavior.
