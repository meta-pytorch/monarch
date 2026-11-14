# The `Actor` Trait

The `Actor` trait defines the core behavior of all actors in the hyperactor runtime.

Every actor type must implement this trait to participate in the system. It defines how an actor is constructed, initialized, and supervised.

```
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
 Proc::isolated().spawn("anon", self)
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
 event: &ActorSupervisionEvent,
 ) -> Result<bool, anyhow::Error> {
 Ok(!event.is_error())
 }

 async fn handle_delivery_failure_event(
 &mut self,
 cx: &Instance<Self>,
 envelope: Undeliverable<MessageEnvelope>,
 ) -> Result<(), anyhow::Error> {
 handle_delivery_failure_event(self, cx, envelope).await
 }

 async fn handle_invalid_reference(
 &mut self,
 cx: &Instance<Self>,
 invalid: InvalidReference,
 envelope: Undeliverable<MessageEnvelope>,
 ) -> Result<(), anyhow::Error> {
 handle_invalid_reference(cx, invalid, envelope)
 }

 async fn handle_expired_delivery(
 &mut self,
 cx: &Instance<Self>,
 expired: ExpiredDelivery,
 envelope: Undeliverable<MessageEnvelope>,
 ) -> Result<(), anyhow::Error> {
 handle_expired_delivery(cx, expired, envelope)
 }

 async fn handle_undeliverable_message(
 &mut self,
 cx: &Instance<Self>,
 reason: UndeliverableReason,
 envelope: Undeliverable<MessageEnvelope>,
 ) -> Result<(), anyhow::Error> {
 handle_undeliverable_message(cx, reason, envelope)
 }

 fn display_name(&self) -> Option<String> {
 None
 }
}
```

## Initialization: `init`

```
async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error>
```

The `init` method is called after the actor has been constructed and registered with the runtime. It is passed a reference to the actor's `Instance`, allowing access to runtime services such as:

- The actor's ID and status
- The mailbox and port system
- Capabilities for spawning or sending messages

The default implementation does nothing and returns `Ok(())`.

If `init` returns an error, the actor is considered failed and will not proceed to handle any messages.

Use `init` to perform startup logic that depends on the actor being fully integrated into the system.

## Cleanup: `cleanup`

```
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

```
fn spawn(self, cx: &impl context::Actor) -> anyhow::Result<ActorHandle<Self>> {
 cx.instance().spawn(self)
}
```

This method takes ownership of `self` (the actor instance) and spawns it as a child. In practice, `context::Actor` is implemented for types such as `Instance<A>` and `Context<A>`, which represent running actors. As a result, `Actor::spawn(...)` always constructs a child actor: the new actor receives a child ID and is linked to its parent through the runtime.

## Detached Spawning: `spawn_detached`

```
fn spawn_detached(self) -> Result<ActorHandle<Self>, anyhow::Error> {
 Proc::isolated().spawn("anon", self)
}
```

This method takes ownership of `self` and creates a root actor on a fresh, isolated proc.

- The proc is local-only and cannot forward messages externally.
- The actor receives a unique root `ActorId` with no parent.
- No supervision or linkage is established.
- The actor is named `"anon"`.

## Background Tasks: `spawn_server_task`

```
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

```
async fn handle_supervision_event(
 &mut self,
 _this: &Instance<Self>,
 event: &ActorSupervisionEvent,
) -> Result<bool, anyhow::Error> {
 Ok(!event.is_error())
}
```

This method is invoked when the runtime delivers an `ActorSupervisionEvent` to the actor -- for example, when a child crashes or exits.

The outermost frame of the event names the actor's own child. An actor that does not handle an event fails as itself, with the child's event as its cause, so each actor that declines a failure adds one frame to the chain. `ActorSupervisionEvent::caused_by` returns the root cause. An actor can only recover its own child: the child's descendants have stopped with it and are recreated by the child.

- `Ok(true)`: the actor takes responsibility for the event. It may keep running, stop, or fail later.
- `Ok(false)`: the actor fails with an `UnhandledSupervisionEvent` status, caused by the child's event.
- `Err(error)`: the supervision handler failed; the actor fails with an `ErrorDuringHandlingSupervision` status, caused by the child's event.

Returning an `ActorErrorKind` through `anyhow::Error` preserves the structured failure, including through `anyhow::Context`. This is the single supported escape hatch for message handlers. Errors returned by supervision callbacks always retain the original child event as their cause. `UnhandledSupervisionEvent` carries the cause, and the runtime adds the failing actor's frame. Proxy actors opt into transparent supervision; user actors remain visible regardless of language. All frames remain in the physical causal chain, and the Python supervision boundary skips transparent causal wrappers in its public view. `ActorSupervisionEvent::causal_chain` traverses failure and normal-stop causes. Liveness failures carry the affected actor's identity as an observation, without adding a failure for an actor that may still be running.

Monitors use `ActorErrorKind::SyntheticSupervision` through the same path to retain the observed subject, monitor failure, and local cancellation fence.

By default, supervision handlers return `Ok(true)` for normal lifecycle events and `Ok(false)` for errors.

Actors may override this to implement custom supervision logic.

## Display Name: `display_name`

```
fn display_name(&self) -> Option<String> {
 None
}
```

This method allows an actor to provide a custom display name for use in supervision error messages and logging. By default, it returns `None`, causing the runtime to use the `ActorId` for display purposes.

## Delivery failure hooks

```
async fn handle_delivery_failure_event(
 &mut self,
 cx: &Instance<Self>,
 envelope: Undeliverable<MessageEnvelope>,
) -> Result<(), anyhow::Error> {
 handle_delivery_failure_event(self, cx, envelope).await
}
```

The runtime invokes `handle_delivery_failure_event` when delivery of a message
sent by this actor fails. The notification contains either the returned envelope
or a `DeliveryFailureReport` if the original payload is unavailable. Use
`envelope.root_delivery_failure()` to inspect the original structured failure.
Failures on the return path do not replace that first failure.

The default generic handler dispatches on the root `DeliveryFailureKind`:

| Kind | Hook | Default result |
| --- | --- | --- |
| `InvalidReference` | `handle_invalid_reference(cx, invalid, envelope)` | `Err`: fails the sending actor |
| `Expired` | `handle_expired_delivery(cx, expired, envelope)` | `Err`: fails the sending actor |
| `Undeliverable` | `handle_undeliverable_message(cx, reason, envelope)` | `Ok(())` for `PortGone` and ordinary transport failures; `Err` for `TransportFailureReason::OversizedFrame` |

An unclassified failure returns `Err`.

Override a specific hook to change one category's policy. For example,
`handle_invalid_reference` can absorb `InvalidReferenceReason::ActorStopped` and
`ActorFailed` for replies to terminal recipients, while retaining errors for
unbound handlers or protocol mismatches. `handle_expired_delivery` is appropriate
when the application deliberately tolerates losing a message that exhausted its
hop TTL. `handle_undeliverable_message` can update bookkeeping for dropped reply
ports or make a transport failure fatal. `PortGone` is already absorbed by default.

Override `handle_delivery_failure_event` for a policy that spans categories. This
replaces its default dispatch; call the free
`hyperactor::actor::handle_delivery_failure_event(self, cx, envelope).await` to
delegate to the specific hooks. The free functions for each specific hook apply
that hook's Rust default.

Python actors expose the corresponding `_handle_delivery_failure_event`,
`_handle_invalid_reference`, `_handle_expired_delivery`, and
`_handle_undeliverable_message` methods. Each receives an
`UndeliverableMessageEnvelope`; `root_delivery_failure()` exposes enum mirrors
through `monarch.actor`. A generic Python override takes precedence over specific
Python overrides. Returning `True` absorbs the event, while `False` retains the
existing Python behavior directly, without calling another Python hook. Returned
messages use the corresponding Rust default; metadata-only reports remain fatal
unless explicitly handled, including `PortGone` reports.