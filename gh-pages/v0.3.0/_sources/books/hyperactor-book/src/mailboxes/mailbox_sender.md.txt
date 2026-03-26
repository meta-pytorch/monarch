# MailboxSender

`MailboxSender` is a trait that abstracts the ability to deliver `MessageEnvelope`s. Anything that implements `MailboxSender` can act as a message sink—whether it's a local `Mailbox`, a forwarding proxy, or a buffered client.

This section introduces:

- The `MailboxSender` trait
- The `PortSender` extension
- Standard implementations: `BoxedMailboxSender`, `PanickingMailboxSender`, `UndeliverableMailboxSender`

`MailboxSender`s can send messages through ports to mailboxes. The trait provides a unified interface for message delivery with TTL (time-to-live) handling:
```rust
pub trait MailboxSender: Send + Sync + Any {
    /// Apply hop semantics (TTL decrement; undeliverable on 0), then
    /// delegate to transport.
    fn post(
        &self,
        mut envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        if let Err(err) = envelope.dec_ttl_or_err() {
            envelope.undeliverable(err, return_handle);
            return;
        }
        self.post_unchecked(envelope, return_handle);
    }

    /// Raw transport: no TTL policy.
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    );
}
```

`PortSender` is an extension trait. The function `serialize_and_send` will serialize a message, install it in an envelope with an unknown sender and `post` it to the provided port. `serialize_and_send_once` `post`s to a one-shot port, consuming the provided port which is not resuable.
```rust
pub trait PortSender: MailboxSender {
    fn serialize_and_send<M: RemoteMessage>(
        &self,
        port: &PortRef<M>,
        message: M,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) -> Result<(), MailboxSenderError> {
        let serialized = Serialized::serialize(&message).map_err(|err| {
            MailboxSenderError::new_bound(
                port.port_id().clone(),
                MailboxSenderErrorKind::Serialize(err.into()),
            )
        })?;
        self.post(
            MessageEnvelope::new_unknown(port.port_id().clone(), serialized),
            return_handle,
        );
        Ok(())
    }

    fn serialize_and_send_once<M: RemoteMessage>(
        &self,
        once_port: OncePortRef<M>,
        message: M,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) -> Result<(), MailboxSenderError> {
        let serialized = Serialized::serialize(&message).map_err(|err| {
            MailboxSenderError::new_bound(
                once_port.port_id().clone(),
                MailboxSenderErrorKind::Serialize(err.into()),
            )
        })?;
        self.post(
            MessageEnvelope::new_unknown(once_port.port_id().clone(), serialized),
            return_handle,
        );
        Ok(())
    }
}
```
All `MailboxSender`s are `PortSender`s too:
```rust
impl<T: ?Sized + MailboxSender> PortSender for T {}
```
This is a perpetually closed mailbox sender. It panics if any messages are posted on it. Useful for tests or detached mailboxes.
```rust
#[derive(Debug, Clone)]
pub struct PanickingMailboxSender;

impl MailboxSender for PanickingMailboxSender {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        panic!("panic! in the mailbox! attempted post: {}", envelope)
    }
}
```
This is a mailbox sender of last resort for undeliverable messages that logs the failure:
```rust
#[derive(Debug)]
pub struct UndeliverableMailboxSender;

impl MailboxSender for UndeliverableMailboxSender {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        tracing::error!(
            name = "undelivered_message_abandoned",
            actor_id = envelope.sender.to_string(),
            dest = envelope.dest.to_string(),
            "message not delivered"
        );
    }
}
```

`BoxedMailboxSender`  is a a type-erased, thread-safe, reference-counted mailbox sender:
```rust
struct BoxedMailboxSender(Arc<dyn MailboxSender + Send + Sync + 'static>);

impl MailboxSender for BoxedMailboxSender {
    fn post(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        hyperactor_telemetry::declare_static_counter!(MAILBOX_POSTS, "mailbox.posts");
        MAILBOX_POSTS.add(
            1,
            hyperactor_telemetry::kv_pairs!(
                "actor_id" => envelope.sender.to_string(),
                "dest_actor_id" => envelope.dest.0.to_string(),
            ),
        );
        self.0.post(envelope, return_handle)
    }
}
```
hyperactor internally makes use of a global boxed panicking mailbox sender:
```rust
static BOXED_PANICKING_MAILBOX_SENDER: LazyLock<BoxedMailboxSender> =
    LazyLock::new(|| BoxedMailboxSender::new(PanickingMailboxSender));
```

`Mailbox` is a concrete type representing an actor’s local inbox. Internally, it holds a map of ports and routes incoming messages to their respective receivers.

Meanwhile, `MailboxSender` is an abstraction: a trait that represents anything capable of delivering a `MessageEnvelope` to a mailbox.

Every `Mailbox` implements `MailboxSender`. When you invoke post on a `Mailbox`, it performs local delivery by looking up the port and forwarding the message.

Other types - such as `MailboxServer`, `BoxedMailboxSender`, or adapters that forward to remote systems  also implement `MailboxSender`.
