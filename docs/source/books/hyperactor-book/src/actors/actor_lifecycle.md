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
    Saving(SystemTime),
    Loading(SystemTime),
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
- `Saving`: The actor is saving its state as part of a checkpoint. Includes the time the operation began.
- `Loading`: The actor is loading a previously saved state.
- `Stopping`: The actor is in shutdown mode and draining its mailbox.
- `Stopped`: The actor has exited and will no longer process messages.
- `Failed`: The actor terminated abnormally. Contains an `ActorErrorKind` describing the error.

### Methods
- `is_terminal(&self) -> bool`: Returns true if the actor has either stopped or failed.
- `is_failed(&self) -> bool`: Returns true if the actor is in the Failed state.
- `generic_failure(message: impl Into<String>) -> Self`: Creates a generic failure status with the provided error message.
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
- `ChildStopped`: Internal signal sent when a direct child with the given index was stopped.

These signals are routed like any other message, typically sent using `ActorHandle::send` or by the runtime during supervision and recovery procedures.

## `ActorError`

`ActorError` represents a failure encountered while serving an actor. It includes the actor's identity and the underlying cause.
```rust
pub struct ActorError {
    pub actor_id: Box<ActorId>,
    pub kind: Box<ActorErrorKind>,
}
```
This error type is returned in various actor lifecycle operations such as initialization, message handling, checkpointing, and shutdown. It is structured and extensible, allowing the runtime to distinguish between different classes of failure.

### Associated Methods
```rust
impl ActorError {
    /// Constructs a new `ActorError` with the given ID and kind.
    pub(crate) fn new(actor_id: &ActorId, kind: ActorErrorKind) -> Self
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

- `Generic`: A generic error with a formatted message.
- `ErrorDuringHandlingSupervision`: An error that occurred while trying to handle a supervision event.
- `UnhandledSupervisionEvent`: The actor did not attempt to handle a supervision event.

The `ActorErrorKind` also provides several constructor methods:
- `processing(err: anyhow::Error)`: Error while processing a message
- `panic(err: anyhow::Error)`: A panic occurred during message handling
- `init(err: anyhow::Error)`: Actor initialization failed
- `cleanup(err: anyhow::Error)`: Error during actor cleanup
- `mailbox(err: MailboxError)`: A lower-level mailbox error occurred
- `mailbox_sender(err: MailboxSenderError)`: A lower-level sender error occurred
- `checkpoint(err: CheckpointError)`: Error during save/load of actor state
- `message_log(err: MessageLogError)`: Failure in the underlying message log
- `indeterminate_state()`: The actor reached an invalid or unknown internal state
