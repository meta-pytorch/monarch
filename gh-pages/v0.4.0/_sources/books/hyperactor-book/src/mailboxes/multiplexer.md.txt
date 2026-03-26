# Multiplexers

**Muxers** (short for multiplexers) form the first level of indirection in the mailbox subsystem. While a `Mailbox` delivers messages to typed ports within a single actor, a `MailboxMuxer` delivers messages to the correct mailbox instance given an `ActorId`.

It acts as a dynamic registry, allowing multiple mailboxes to be addressed through a single posting interface.

This page introduces the `MailboxMuxer` and its role in:
- Aggregating multiple mailbox instances
- Dispatching incoming messages to the appropriate `MailboxSender`
- Supporting dynamic binding and unbinding of mailboxes

Let's begin by looking at the core structure of `MailboxMuxer`:
```rust
pub struct MailboxMuxer {
    mailboxes: Arc<DashMap<ActorId, Box<dyn MailboxSender + Send + Sync>>>,
}
```
The `MailboxMuxer` maintains a thread-safe, concurrent map from `ActorId` to `MailboxSender` trait objects. Each entry represents a live binding to a mailbox capable of receiving messages for a specific actor. This allows the muxer to act as a single dispatch point for delivering messages to any number of registered actors, abstracting over the details of how and where each mailbox is implemented.

To register a mailbox with the muxer, callers use the `bind` method:
```rust
impl MailboxMuxer {
    pub fn bind(&self, actor_id: ActorId, sender: impl MailboxSender + 'static) -> bool {
        match self.mailboxes.entry(actor_id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(Box::new(sender));
                true
            }
        }
    }

}
```
This function installs a new mapping from the given `ActorId` to a boxed `MailboxSender`. If the `ActorId` is already registered, the bind fails (returns `false`), and the existing sender is left unchanged. This ensures that actors cannot be accidentally rebound without first explicitly unbinding them—enforcing a clear handoff protocol. To rebind, the caller must invoke `unbind` first.

It's crucial to recall that `Mailbox` itself implements the `MailboxSender` trait. This is what allows it to be registered directly into a `MailboxMuxer`. The `post` method of a `Mailbox` inspects the incoming `MessageEnvelope` to determine whether it is the intended recipient. If the `ActorId` in the envelope matches the mailbox's own ID, the mailbox delivers the message locally: it looks up the appropriate port by index and invokes `send_serialized` on the matching channel. If the `ActorId` does *not* match, the mailbox delegates the message to its internal forwarder by calling `self.state.forwarder.post(envelope)`.

With this behavior in mind, we can now define a convenience method for registering a full `Mailbox`:

```rust
impl MailboxMuxer {
  fn bind_mailbox(&self, mailbox: Mailbox) -> bool {
    self.bind(mailbox.actor_id().clone(), mailbox)
  }
}
```
To support rebinding or teardown, the muxer also provides a symmetric `unbind` function, which removes the sender associated with a given `ActorId`:
```rust
    pub(crate) fn unbind(&self, actor_id: &ActorId) {
        self.mailboxes.remove(actor_id);
    }
```
And of course, we can implement `MailboxSender` for `MailboxMuxer` itself—allowing it to act as a unified dispatcher for all registered mailboxes:
```rust
impl MailboxSender for MailboxMuxer {
    fn post(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let dest_actor_id = envelope.dest().actor_id();
        match self.mailboxes.get(envelope.dest().actor_id()) {
            None => {
                let err = format!("no mailbox for actor {} registered in muxer", dest_actor_id);
                envelope.undeliverable(DeliveryError::Unroutable(err), return_handle)
            }
            Some(sender) => sender.post(envelope, return_handle),
        }
    }
}
```
This makes `MailboxMuxer` composable: it can be nested within other routers, shared across components, or substituted for a standalone mailbox in generic code. If the destination `ActorId` is found in the internal map, the message is forwarded to the corresponding sender. Otherwise, it is marked as undeliverable with an appropriate `DeliveryError`.
