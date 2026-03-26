# Delivery Semantics

This section defines the mechanics of message delivery and failure in the mailbox system.

Key components:

- `MessageEnvelope`: encapsulates a message, sender, and destination
- `DeliveryError`: enumerates failure modes (unroutable, broken link, etc.)
- Undeliverable handling: how messages are returned on failure
- Serialization and deserialization support

These types form the foundation for how messages are transmitted, routed, and failed in a structured way.

An envelope carries a message destined to a remote actor. The envelope contains a serialized message along with its destination and sender:
```rust
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Named)]
pub struct MessageEnvelope {
    /// The sender of this message.
    sender: ActorId,

    /// The destination of the message.
    dest: PortId,

    /// The serialized message.
    data: Serialized,

    /// Error contains a delivery error when message delivery failed.
    error: Option<DeliveryError>,

    /// Additional context for this message.
    headers: Attrs,
}
```

`MessageEnvelope::new` creates a message envelope:
```rust
impl MessageEnvelope {
  fn new(sender: ActorId, dest: PortId, data: Serialized, headers: Attrs) -> Self { ... }
}
```
`MessageEnvelope::new_unknown` creates a new envelope when we don't know who the sender is:
```rust
impl MessageEnvelope {
  fn new_unknown(dest: PortId, data: Serialized) -> Self {
    Self::new(id!(unknown[0].unknown), dest, data)
  }
}
```
If a type `T` implements `Serialize` and `Named`, an envelope can be constructed while serializing the message data:
```rust
impl MessageEnvelope {
  fn serialize<T: Serialize + Named>(
      source: ActorId, dest: PortId, value: &T, headers: Attrs) -> Result<Self, bincode::Error> {
    Ok(Self {
         data: Serialized::serialize(value)?,
         sender: source,
         dest,
         error: None,
        })
    }
}
```
We can use the fact that `T` implements `DeserializeOwned` to provide a function to deserialize the message data in an envelope:
```rust
impl MessageEnvelope {
  fn deserialized<T: DeserializedOwned>(&self) -> Result<T, anhyow::Error> {
    self.data.deserialized()
  }
}
```
This function stamps an envelope with a delivery error:
```rust
impl MessageEnvelope {
  fn error(&mut self, error: DeliveryError) {
    self.error = Some(error);
  }
}
```
The `undeliverable` function on a `MessageEnvelope` can be called when a message has been determined to be undeliverable due to the provided error. It marks the envelope with the error and attempts to return it to the sender.
```rust
impl MessageEnvelope {
    pub fn undeliverable(
        mut self,
        error: DeliveryError,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        self.try_set_error(error);
        undeliverable::return_undeliverable(return_handle, self);
    }
}
```

### Delivery errors

Delivery errors can occur during message posting:
```rust
#[derive(thiserror::Error, ...)]
pub enum DeliveryError {
    /// The destination address is not reachable.
    #[error("address not routable: {0}")]
    Unroutable(String),

    /// A broken link indicates that a link in the message
    /// delivery path has failed.
    #[error("broken link: {0}")]
    BrokenLink(String),

    /// A (local) mailbox delivery error.
    #[error("mailbox error: {0}")]
    Mailbox(String),
}
```

### Mailbox Errors

Errors can occur during mailbox operations. Each error is associated with the mailbox's actor ID:
```rust
pub struct MailboxError {
    actor_id: ActorId,
    kind: MailboxErrorKind,
}

#[non_exhaustive]
pub enum MailboxErrorKind {
    /// An operation was attempted on a closed mailbox.
    #[error("mailbox closed")]
    Closed,

    /// The port associated with an operation was invalid.
    #[error("invalid port: {0}")]
    InvalidPort(PortId),

    /// There was no sender associated with the port.
    #[error("no sender for port: {0}")]
    NoSenderForPort(PortId),

    /// There was no local sender associated with the port.
    /// Returned by operations that require a local port.
    #[error("no local sender for port: {0}")]
    NoLocalSenderForPort(PortId),

    /// The port was closed.
    #[error("{0}: port closed")]
    PortClosed(PortId),

    /// An error occured during a send operation.
    #[error("send {0}: {1}")]
    Send(PortId, #[source] anyhow::Error),

    /// An error occured during a receive operation.
    #[error("recv {0}: {1}")]
    Recv(PortId, #[source] anyhow::Error),

    /// There was a serialization failure.
    #[error("serialize: {0}")]
    Serialize(#[source] anyhow::Error),

    /// There was a deserialization failure.
    #[error("deserialize {0}: {1}")]
    Deserialize(&'static str, anyhow::Error),

    #[error(transparent)]
    Channel(#[from] ChannelError),
}
```

`PortLocation` describes the location of a port. It provides a uniform data type for ports that may or may not be bound.
```rust
#[derive(Debug, Clone)]
pub enum PortLocation {
    /// The port was bound: the location is its underlying bound ID.
    Bound(PortId),
    /// The port was not bound: we provide the actor ID and the message type.
    Unbound(ActorId, &'static str),
}
```

One place `PortLocation` is used is in the type `MailboxSenderError` which is specifically for errors that occur during mailbox send operations. Each error is associated with the port ID of the operation:
```rust
#[derive(Debug)]
pub struct MailboxSenderError {
    location: PortLocation,
    kind: MailboxSenderErrorKind,
}

/// The kind of mailbox sending errors.
#[derive(thiserror::Error, Debug)]
pub enum MailboxSenderErrorKind {
    /// Error during serialization.
    #[error("serialization error: {0}")]
    Serialize(anyhow::Error),

    /// Error during deserialization.
    #[error("deserialization error for type {0}: {1}")]
    Deserialize(&'static str, anyhow::Error),

    /// A send to an invalid port.
    #[error("invalid port")]
    Invalid,

    /// A send to a closed port.
    #[error("port closed")]
    Closed,

    // The following pass through underlying errors:
    /// An underlying mailbox error.
    #[error(transparent)]
    Mailbox(#[from] MailboxError),

    /// An underlying channel error.
    #[error(transparent)]
    Channel(#[from] ChannelError),

    /// An underlying message log error.
    #[error(transparent)]
    MessageLog(#[from] MessageLogError),

    /// An other, uncategorized error.
    #[error("send error: {0}")]
    Other(#[from] anyhow::Error),

    /// The destination was unreachable.
    #[error("unreachable: {0}")]
    Unreachable(anyhow::Error),
}
```
