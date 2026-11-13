# Multiplexers

**Muxers** (short for multiplexers) form the first level of indirection in the mailbox subsystem. While a `Mailbox` delivers messages to typed ports within a single actor, a `MailboxMuxer` delivers messages to the correct mailbox instance given an `ActorId`.

It acts as a dynamic registry, allowing multiple mailboxes to be addressed through a single posting interface.

This page introduces the `MailboxMuxer` and its role in:
- Aggregating multiple mailbox instances
- Dispatching incoming messages to the appropriate `MailboxSender`
- Supporting dynamic binding and unbinding of mailboxes
- Buffering messages for actors that have not bound yet

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
                let failure = DeliveryFailure::new(InvalidReference::new(
                    envelope.dest().actor_addr(),
                    InvalidReferenceReason::ActorNotExist,
                ));
                envelope.undeliverable(failure, return_handle)
            }
            Some(sender) => sender.post(envelope, return_handle),
        }
    }
}
```
This makes `MailboxMuxer` composable: it can be nested within other routers, shared across components, or substituted for a standalone mailbox in generic code. If the destination `ActorId` is found in the internal map, the message is forwarded to the corresponding sender. The snippet above shows the simplest behavior for a miss: return the message with an `InvalidReference` delivery failure.

## Messages for actors that have not bound yet

An `ActorRef` can exist before its actor does. For example, a remote spawn computes the new actor's address locally and returns it right away, while the target proc is still constructing the actor. A message sent through that ref reaches the right proc's muxer, but it can arrive before the actor binds its mailbox.

So on a miss, the muxer does not bounce the message right away. Instead, each entry in `mailboxes` records the state of an actor's mailbox:
```rust
enum ActorMailbox {
    Bound(Arc<dyn MailboxSender + Send + Sync>),
    Buffering(Vec<PendingMessage>),
}
```
A message for an unknown `ActorId` creates a `Buffering` entry that holds the envelope and its return handle. When `bind` later arrives for that id, it delivers the buffered messages to the sender and replaces the entry with `Bound`, all under that entry's lock. A message that arrives meanwhile waits for the lock, then goes straight to the sender. The actor thus sees messages in the order they reached the proc.

Buffering does not wait on user code: the runtime binds an actor's mailbox and handler ports before `Actor::init` runs. Messages then wait in the actor's own work queue until it starts handling them. The remote-spawn path binds the handler ports before it publishes the mailbox, so a buffered message never finds its port missing.

If no actor binds within `PENDING_ACTOR_DELIVERY_TIMEOUT` (30 seconds by default), the buffered messages are returned with `InvalidReferenceReason::ActorNotExist`, exactly as an immediate bounce would. Setting the timeout to zero restores the immediate bounce.
