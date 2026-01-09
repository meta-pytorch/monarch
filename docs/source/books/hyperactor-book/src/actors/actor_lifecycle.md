# Actor Lifecycle Types

This page documents auxiliary types used in actor startup, shutdown, and supervision logic.

## `ActorStatus`

`ActorStatus` describes the current runtime state of an actor. It is used to monitor progress, coordinate shutdown, and detect failure conditions.
```rust
pub enum ActorStatus {
    Unknown,
    Created,
    Initializing,
    Client,
    Idle,
    Processing(SystemTime, Option<(String, Option<String>)>),
    Stopping,
    Stopped,
    Failed(String),
}
```

### States
- `Unknown`: The status is unknown (e.g. not yet initialized).
- `Created`: The actor has been constructed but not yet started.
- `Initializing`: The actor is running its init lifecycle hook and is not yet receiving messages.
- `Client`: The actor is operating in “client” mode; its ports are being managed manually.
- `Idle`: The actor is ready to process messages but is currently idle.
- `Processing`: The actor is handling a message. Contains a timestamp and optionally the handler/arm label.
- `Stopping`: The actor is in shutdown mode and draining its mailbox.
- `Stopped`: The actor has exited and will no longer process messages.
- `Failed`: The actor terminated abnormally. Contains an error description.

### Methods
- `is_terminal(&self) -> bool`: Returns true if the actor has either stopped or failed.
- `is_failed(&self) -> bool`: Returns true if the actor is in the Failed state.
- `passthrough(&self) -> ActorStatus`: Returns a clone of the status. Used internally during joins.
- `span_string(&self) -> &'static str`: Returns the active handler/arm name if available. Used for tracing.

## `Signal`

`Signal` is used to control actor lifecycle transitions externally. These messages are sent internally by the runtime (or explicitly by users) to initiate operations like shutdown.
```rust
pub enum Signal {
    DrainAndStop,
    Stop,
    ChildStopped(Index),
}
```
Variants
- `DrainAndStop`: Gracefully stops the actor by first draining all queued messages.
- `Stop`: Immediately halts the actor, even if messages remain in its mailbox.
- `ChildStopped`: Indicates that a direct child actor with the given index was stopped.

These signals are routed like any other message, typically sent using `ActorHandle::send` or by the runtime during supervision procedures.

## `ActorError`

`ActorError` represents a failure encountered while serving an actor. It includes the actor's identity and the underlying cause.
```rust
pub struct ActorError {
    actor_id: ActorId,
    kind: ActorErrorKind,
}
```
This error type is returned in various actor lifecycle operations such as initialization, message handling, and shutdown. It is structured and extensible, allowing the runtime to distinguish between different classes of failure.

### Associated Methods
```rust
impl ActorError {
    /// Constructs a new `ActorError` with the given ID and kind.
    pub(crate) fn new(actor_id: ActorId, kind: ActorErrorKind) -> Self

    /// Returns a cloneable version of this error, discarding error structure
    /// and retaining only the formatted string.
    fn passthrough(&self) -> Self
}
```

## `ActorErrorKind`

```rust
pub enum ActorErrorKind {
    Generic(String),
    ErrorDuringHandlingSupervision(String, Box<ActorSupervisionEvent>),
    UnhandledSupervisionEvent(Box<ActorSupervisionEvent>),
}
```
### Variants

- `Generic`: Generic error with a formatted message.
- `ErrorDuringHandlingSupervision`: An error that occurred while trying to handle a supervision event.
- `UnhandledSupervisionEvent`: The actor did not handle the supervision event.
