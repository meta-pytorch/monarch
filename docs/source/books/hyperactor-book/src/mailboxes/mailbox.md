# Mailbox

A `Mailbox` represents an actor's in-process inbox. It owns and manages all of the actor's ports, provides APIs to open and bind them, and routes messages based on their destination `PortId`.

A mailbox routes local messages directly to its ports. If a message is addressed to a different actor, the mailbox uses its configured forwarder to relay the message. If the message cannot be delivered-for instance, if the destination port is unbound-the mailbox wraps it as undeliverable and returns it via the supplied handle.

This section covers:

- Opening ports of various kinds
- Port binding and registration
- Internal mailbox state and delivery logic

The `State` holds all delivery infrastructure: active ports, the actor's ID, a port allocator, and a forwarding mechanism. Multiple clones of a `Mailbox` share access to the same state:
```rust
pub struct Mailbox {
    state: Arc<State>,
}
```
The `new` function creates a mailbox with the provided actor ID and forwarder for external destinations:
```rust
impl Mailbox {
    pub fn new(actor_id: ActorId, forwarder: BoxedMailboxSender) -> Self {
        Self {
            state: Arc::new(State::new(actor_id, forwarder)),
        }
    }
}
```
`new_detached` mailboxes are not connected to an external message forwarder and can only deliver to its own ports:
```rust
impl Mailbox {
    pub fn new_detached(actor_id: ActorId) -> Self {
        Self {
            state: Arc::new(State::new(actor_id, BOXED_PANICKING_MAILBOX_SENDER.clone())),
        }
    }
```

A mailbox can open ports, each identified by a unique `PortId` within the owning actor. The most common form is `open_port`, which creates a fresh, unbounded channel for message delivery:
```rust
impl Mailbox {
    pub fn open_port<M: Message>(&self) -> (PortHandle<M>, PortReceiver<M>) {
        let port_index = self.state.allocate_port();
        let (sender, receiver) = mpsc::unbounded_channel::<M>();
        let port_id = PortId(self.state.actor_id.clone(), port_index);
        (
            PortHandle::new(self.clone(), port_index, UnboundedPortSender::Mpsc(sender)),
            PortReceiver::new(
                receiver,
                port_id,
                /*coalesce=*/ false,
                self.state.clone(),
            ),
        )
    }
}
```
This allocates a new port index and sets up a pair of endpoints: a `PortHandle<M>` for sending messages into the port, and a `PortReceiver<M>` for asynchronously consuming them. Internally, these are two ends of an `mpsc::unbounded_channel`, meaning messages are buffered in memory and processed in order without backpressure.

In contrast to `open_port`, which uses a channel-backed buffer, `open_enqueue_port` constructs a port backed directly by a user-supplied enqueue function:
```rust
impl Mailbox {
    pub(crate) fn open_enqueue_port<M: Message>(
        &self,
        enqueue: impl Fn(M) -> Result<(), anyhow::Error> + Send + Sync + 'static,
    ) -> PortHandle<M> {
        PortHandle {
            mailbox: self.clone(),
            port_index: self.state.allocate_port(),
            sender: UnboundedPortSender::Func(Arc::new(enqueue)),
            bound: Arc::new(OnceLock::new()),
            reducer_typehash: None,
        }
    }
}
```
Unlike `open_port`, which yields both sender and receiver ends of an internal channel, `open_enqueue_port` returns only a `PortHandle`. Instead of buffering messages, this port invokes a user-supplied function directly on each message it receives.

Another variant `open_accum_port`, builds on the same principle as `open_enqueue_port`, but pairs the port with an accumulator that maintains state across messages. We'll return to this specialized port type later. We'll also encounter `open_once_port` analogous to `open_port` but sets up a one-shot message channel - useful for rendezvous-style communication - using the associated `OncePortHandle` and `OncePort` types.

## Port Binding

Binding is only required when a port must be referred to externally-for example, when sending it across the network or including it in a message. Binding a port produces a `PortRef`, which globally names the port and requires that the associated message type implements `RemoteMessage` (i.e., is serializable). All messages sent via a `PortRef` are serialized.

By contrast, `PortHandle` can be used locally to send any type implementing `Message`, including non-serializable types, and behaves like a typed in-memory queue.

Once a port is opened with `open_port`, it must be bound before it can receive messages routed through the mailbox. Binding installs the port into the mailbox's internal routing table and produces a `PortRef<M>`-a lightweight, serializable reference that remote actors can use to send messages to the port.

Port binding is performed by calling `.bind()` on a `PortHandle`:
```rust
impl<M: RemoteMessage> PortHandle<M> {
    pub fn bind(&self) -> PortRef<M> {
        PortRef::attest_reducible(
            self.bound
                .get_or_init(|| self.mailbox.bind(self).port_id().clone())
                .clone(),
            self.reducer_spec.clone(),
            self.reducer_opts.clone(),
        )
    }
}
```
This delegates to `Mailbox::bind(&self, handle)`, which performs the actual installation into the mailbox's internal `State`. If the port is already bound, this is a no-op.

The mailbox checks that the port handle belongs to it, computes the `PortId`, and then inserts the sender into the internal ports map if it hasn't been bound already:
```rust
impl Mailbox {
    fn bind<M: RemoteMessage>(&self, handle: &PortHandle<M>) -> PortRef<M> {
        assert_eq!(
            handle.mailbox.actor_id(),
            self.actor_id(),
            "port does not belong to mailbox"
        );
        let port_id = self.actor_id().port_id(handle.port_index);
        match self.state.ports.entry(handle.port_index) {
            Entry::Vacant(entry) => {
                entry.insert(Box::new(UnboundedSender::new(
                    handle.sender.clone(),
                    port_id.clone(),
                )));
            }
            Entry::Occupied(_entry) => {}
        }

        PortRef::attest(port_id)
    }
}
```
The result is a `PortRef<M>` that can be sent across the network to deliver messages to this bound port.

## Binding to a Specific Index

There is also a lower-level variant, bind_to, used internally by actor binding mechanisms (e.g., when installing well-known ports at known indices):
```rust
impl Mailbox {
  fn bind_to<M: RemoteMessage>(&self, handle: &PortHandle<M>, port_index: u64) {
      assert_eq!(
          handle.mailbox.actor_id(),
          self.actor_id(),
          "port does not belong to mailbox"
      );

      let port_id = self.actor_id().port_id(port_index);
      match self.state.ports.entry(port_index) {
          Entry::Vacant(entry) => {
              entry.insert(Box::new(UnboundedSender::new(
                  handle.sender.clone(),
                  port_id,
              )));
          }
          Entry::Occupied(_) => panic!("port {} already bound", port_id),
      }
  }
}
```

## Message Delivery via MailboxSender

The mailbox also handles message delivery. It does this by implementing the `MailboxSender` trait, which defines how messages-wrapped in `MessageEnvelope`-are routed, deserialized, and delivered to bound ports or forwarded to remote destinations.
```rust
impl MailboxSender for Mailbox {
    fn post(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        if envelope.dest().actor_id() != &self.state.actor_id {
            return self.state.forwarder.post(envelope, return_handle);
        }

        match self.state.ports.entry(envelope.dest().index()) {
            Entry::Vacant(_) => envelope.undeliverable(
                DeliveryError::Unroutable("port not bound in mailbox".to_string()),
                return_handle,
            ),
            Entry::Occupied(entry) => {
                let (metadata, data) = envelope.open();
                let MessageMetadata {headers, sender, dest, error: metadata_error } = metadata;
                match entry.get().send_serialized(headers, data) {
                    Ok(false) => {
                        entry.remove();
                    }
                    Ok(true) => (),
                    Err(SerializedSenderError {
                        data,
                        error,
                        headers,
                    }) => MessageEnvelope::seal(
                        MessageMetadata { headers, sender, dest, error: metadata_error },
                        data,
                    )
                    .undeliverable(DeliveryError::Mailbox(format!("{}", error)), return_handle),
                }
            }
        }
    }
}
```

### Breakdown of Delivery Logic

This implementation of `MailboxSender::post` defines how a mailbox handles message delivery:
1. Actor ID routing
```rust
if envelope.dest().actor_id() != &self.state.actor_id
```
If the message is not addressed to this actor, it's forwarded using the forwarder defined in the mailbox's state. This allows for transparent routing across process or network boundaries.

2. Port Lookup and Binding Check
```rust
match self.state.ports.entry(envelope.dest().index())
```
The mailbox uses the destination `PortId` to locate the bound port in its internal routing table. If the port hasn't been bound, the message is returned to the sender as undeliverable.

3. Deserialization and Delivery Attempt
```rust
match entry.get().send_serialized(headers, data)
```
If the port is found, the message is unsealed and passed to the corresponding `SerializedSender` (e.g., the `UnboundedSender` inserted during binding). This may succeed or fail:
  - `Ok(true)`: Message was delivered.
  - `Ok(false)`: Port is closed; remove it from the routing table.
  - `Err(...)`: Deserialization failed or other error; wrap the message and return it to the sender as undeliverable.

### Relationship to Bound Ports

Only ports that have been bound via `PortHandle::bind()` appear in the ports map and are eligible to receive messages via this `post` path. The entry in this map is a type-erased boxed `SerializedSender`, which, when invoked, attempts to deserialize the raw message payload into the expected concrete type and forward it to the associated `PortReceiver` or handler.

The mailbox's routing and delivery logic ultimately relies on the internal `State`, which stores port mappings, forwarding configuration, and allocation state.

## State
Each `Mailbox` instance wraps an internal `State` struct that contains all shared delivery infrastructure:
```rust
struct State {
    actor_id: ActorId,
    ports: DashMap<u64, Box<dyn SerializedSender>>,
    next_port: AtomicU64,
    forwarder: BoxedMailboxSender,
}
```
This structure is reference-counted via `Arc<State>` and is cloned across all components that need access to the mailbox's internal state. Each field plays a central role:
- **`actor_id`**: Identifies the actor that owns this mailbox. All ports in the mailbox are scoped under this actor ID and used to construct `PortId`s during binding and routing.
- **`ports`**: A concurrent map from port indices (`u64`) to type-erased `SerializedSenders`. Each entry corresponds to a bound port and provides the ability to deserialize and deliver raw messages to the correct `PortReceiver`. Only serializable ports are registered here.
- **`next_port`**: Tracks the next available user port index. Actor-assigned ports occupy indices 0..1024, and user-allocated ports begin from a constant offset (`USER_PORT_OFFSET`).
- **`forwarder`**: A boxed `MailboxSender` used for forwarding messages to other actors. If a message's destination is not owned by this mailbox, it will be passed to this sender.

### State: Internal Structure of a Mailbox

The `State` struct holds all the internal data needed for a functioning `Mailbox`. It's not exposed directly—rather, it's wrapped in `Arc<State>` and shared between `Mailbox`, `PortHandle`, and `PortReceiver`:
```rust
impl State {
    fn new(actor_id: ActorId, forwarder: BoxedMailboxSender) -> Self {
        Self {
            actor_id,
            ports: DashMap::new(),
            next_port: AtomicU64::new(USER_PORT_OFFSET),
            forwarder,
        }
    }

    fn allocate_port(&self) -> u64 {
        self.next_port.fetch_add(1, Ordering::SeqCst)
    }
}
```
**Notes**:
- The `actor_id` allows every `Mailbox` to know which actor it belongs to, which is essential for routing decisions (`post` checks this).
- The ports field holds the routing table: it maps each port index to a type-erased sink (`SerializedSender`) capable of deserializing and dispatching messages to the right receiver.
- `next_port` enables safe concurrent dynamic port allocation by atomically assigning unique port indices.
- The forwarder is used to send messages not destined for this actor-e.g., remote delivery.

## Sending and Receiving Messages

There are two distinct pathways by which a message can arrive at a `PortReceiver`. Both ultimately push a message into an `mpsc` channel (or functionally equivalent handler), but they differ in intent and routing mechanism.

### Local Sends via PortHandle

When you call `.send(msg)` on a `PortHandle<M>`, the message bypasses the `Mailbox` entirely and goes directly into the associated channel:
```text
PortHandle<M>::send(msg)
→ UnboundedPortSender<M>::send(Attrs::new(), msg)
→ underlying channel (mpsc::UnboundedSender<M>)
→ PortReceiver<M>::recv().await
```

### Routed Sends via Mailbox

When a message is wrapped in a `MessageEnvelope` and posted via `Mailbox::post`, routing logic takes over:
```text
Mailbox::post(envelope, return_handle)
→ lookup State::ports[port_index]
→ SerializedSender::send_serialized(headers, bytes)
→ UnboundedSender::send(headers, M) // after deserialization
→ mpsc channel
→ PortReceiver<M>::recv().await
```
This is the delivery path for remote messages or any message routed by a `PortRef`. A `PortHandle` must first be **bound** to participate in this.

## Capabilities

Capabilities are lightweight traits that control access to mailbox-related operations. They act as permissions: a type that implements a capability trait is allowed to perform the corresponding action, such as sending messages or opening ports.

These traits are sealed, meaning they can only be implemented inside the crate. This ensures that capability boundaries are enforced and cannot be circumvented by downstream code.

### Overview

| Capability     | Description                                         |
|----------------|-----------------------------------------------------|
| `CanSend`      | Allows sending messages to ports                    |
| `CanOpenPort`  | Allows creating new ports for receiving messages    |
| `CanSplitPort` | Allows splitting existing ports with reducers       |
| `CanSpawn`     | Allows spawning new child actors                    |

Each public trait (e.g., `CanSend`) is implemented for any type that implements the corresponding private `sealed::CanSend` trait. This gives the crate full control over capability delegation and encapsulation.

### Example: CanSend
```rust
pub trait CanSend: sealed::CanSend {}
impl<T: sealed::CanSend> CanSend for T {}
```

The sealed version defines the core method:
```rust
pub trait sealed::CanSend: Send + Sync {
  fn post(&self, dest: PortId, headers: Attrs, data: Serialized);
}
```
Only internal types (e.g., `Mailbox`) implement this sealed trait, meaning only trusted components can obtain `CanSend`:
```rust
impl cap::sealed::CanSend for Mailbox {
    fn post(&self, dest: PortId, headers: Attrs, data: Serialized) {
        let return_handle = self
            .lookup_sender::<Undeliverable<MessageEnvelope>>()
            .map_or_else(
                || {
                    let actor_id = self.actor_id();
                    if CAN_SEND_WARNED_MAILBOXES
                        .get_or_init(DashSet::new)
                        .insert(actor_id.clone()) {
                        let bt = std::backtrace::Backtrace::capture();
                        tracing::warn!(
                            actor_id = ?actor_id,
                            backtrace = ?bt,
                            "mailbox attempted to post a message without binding Undeliverable<MessageEnvelope>"
                        );
                    }
                    monitored_return_handle()
                },
                |sender| PortHandle::new(self.clone(), self.state.allocate_port(), sender),
            );
        let envelope = MessageEnvelope::new(self.actor_id().clone(), dest, data, headers);
        MailboxSender::post(self, envelope, return_handle);
    }
}
```
This implementation prefers that the mailbox has already bound a port capable of receiving undeliverable messages (of type `Undeliverable<MessageEnvelope>`). This port acts as a return address for failed message deliveries. If the port is not bound, message sending will warn with a backtrace indicating a logic error in system setup and fallback on a `monitored_return_handle` (ideally we'd `panic!` but backwards compatibility prevents this). This ensures that all messages have a well-defined failure path and avoids silent message loss.
