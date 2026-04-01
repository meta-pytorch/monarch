# Reconfigurable Senders

Some actors are constructed before the full messaging graph is available.
For example, the `ReconfigurableMailboxSender` is used during `MeshAgent::bootstrap` to allow early creation of the `Proc` and agent before outbound routing is available.
The `.configure(...)` method installs the actual router later, once mesh wiring is complete.

## Motivation

Actors like `mesh_agent` are created before remote routing infrastructure is established. These actors need to send messages during setup, but the concrete `MailboxSender` they will use hasn't been determined yet.

To solve this, `ReconfigurableMailboxSender` implements [`MailboxSender`] and supports **deferred configuration**: it starts by queueing messages in memory, then later transitions to forwarding once a real sender is available.

## Internal Structure

The sender wraps a state machine:

```rust
struct ReconfigurableMailboxSender {
    state: Arc<RwLock<ReconfigurableMailboxSenderState>>,
}
```
There are two possible states:
```rust
type Post = (MessageEnvelope, PortHandle<Undeliverable<MessageEnvelope>>);

enum ReconfigurableMailboxSenderState {
    Queueing(Mutex<Vec<Post>>),
    Configured(BoxedMailboxSender),
}
```
- In the `Queueing` state, messages are buffered.
- When `.configure(...)` is called, the queue is flushed into the new sender, and the state is replaced with `Configured(...)`.

### Configuration

The `.configure(...)` method installs the actual sender. If called while in the `Queueing state`, it:
  - Drains all buffered messages to the given sender
  - Transitions to the `Configured` state
  - Returns `true` if this was the first successful configuration

Subsequent calls are ignored and return `false`.
```rust
fn configure(&self, sender: BoxedMailboxSender) -> bool {
    let mut state = self.state.write().unwrap();
    if state.is_configured() {
        return false;
    }

    let queued = std::mem::replace(
        &mut *state,
        ReconfigurableMailboxSenderState::Configured(sender.clone()),
    );

    for (envelope, return_handle) in queued.into_queueing().unwrap().into_inner().unwrap() {
        sender.post(envelope, return_handle);
    }

    *state = ReconfigurableMailboxSenderState::Configured(sender);
    true
}
```

This guarantees that messages posted before configuration are not dropped - they are delivered in-order once the sender becomes available.
