# Ports

Ports are the basic units of typed message delivery within a mailbox. Each port is a pair consisting of:

- a `PortHandle<M>`, used to send messages of type `M`, and
- a `PortReceiver<M>`, used to receive those messages asynchronously.

Variants like `OncePortHandle` and `OncePortReceiver` support one-shot communication. All ports are associated with a unique `PortId` within an actor, and may be wrapped in a [`PortRef<M>`](#portref) for safe external use.

This section defines the structure, behavior, and usage of ports.

## Delivery Backends

Each typed port uses an internal delivery mechanism to enqueue messages. This is abstracted by the `UnboundedSenderPort<M>` enum:
```rust
enum UnboundedPortSender<M: Message> {
    Mpsc(mpsc::UnboundedSender<M>),
    Func(Arc<dyn Fn(M) -> Result<(), anyhow::Error> + Send + Sync>),
}
```
- **`Mpsc`**: Sends messages into a tokio unbounded channel
- **`Func`**: Custom logic, often used to enqueue messages onto actor work queues.

Messages are sent via the `.send(headers, message)` method, which forwards to either the internal channel or the configured function.

## `PortHandle<M>`

A `PortHandle<M>` is used to send `M`-typed messages to a mailbox port. It represents the sending half of a typed port:
```rust
pub struct PortHandle<M: Message> {
    mailbox: Mailbox,
    port_index: u64,
    sender: UnboundedPortSender<M>,
    bound: Arc<OnceLock<PortId>>,
    reducer_typehash: Option<u64>,
}
```
### Fields

- **`mailbox`**: The `Mailbox` this port was created from. Stored so the handle can access the actor ID and bind itself into the mailbox’s internal port map.
- **`port_index`**: The local index of the port within the mailbox. Used as the key in the mailbox's port map.
- **`sender`**: The internal message delivery mechanism (e.g., MPSC channel). This determines how messages of type `M` are actually enqueued.
- **`bound`**: A lazily initialized `PortId` stored in a `OnceLock`. This is populated when the port is formally bound into the mailbox, enabling external references via `PortRef<M>`.
- **`reducer_typehash`**: An optional type hash representing a reducer function for accumulating messages. Used in specialized delivery modes (e.g., incremental updates).

### Construction and Use

```rust
impl<M: Message> PortHandle<M> {
    fn new(mailbox: Mailbox, port_index: u64, sender: UnboundedPortSender<M>) -> Self {
        Self {
            mailbox,
            port_index,
            sender,
            bound: Arc::new(OnceLock::new()),
            reducer_typehash: None,
        }
    }

    fn location(&self) -> PortLocation {
        match self.bound.get() {
            Some(port_id) => PortLocation::Bound(port_id.clone()),
            None => PortLocation::new_unbound::<M>(self.mailbox.actor_id().clone()),
        }
    }

    pub fn send(&self, message: M) -> Result<(), MailboxSenderError> {
        self.sender.send(message).map_err(|err| {
            MailboxSenderError::new_unbound::<M>(
                self.mailbox.actor_id().clone(),
                MailboxSenderErrorKind::Other(err),
            )
        })
    }
}
```
- `new` constructs a port handle with the mailbox, port index and delivery backend
- `location` reports whether the port is currently bound
- `send` enqueues a message via the internal sender, wrapping errors as needed.

### Binding

To make a port externally addressable (e.g. for use remote delivery), it must be **bound**:
```rust
impl<M: RemoteMessage> PortHandle<M> {
    pub fn bind(&self) -> PortRef<M>;
}
```
This registers the port in the owning `Mailbox` and returns a `PortRef<M>`. Binding is lazy and idempotent. For a detailed explanation of port binding, see [Mailbox](./mailbox.md#port-binding).

## PortLocation

`PortLocation` describes the logical address of a port. It is used in error messages and has two cases to represent whether or not a port is bound.
```rust
pub enum PortLocation {
    /// The port was bound: the location is its underlying bound ID.
    Bound(PortId),
    /// The port was not bound: we provide the actor ID and the message type.
    Unbound(ActorId, &'static str),
}
```

## `OncePortHandle<M>`

A `OncePortHandle<M>` is a one-shot sender for `M`-typed messages. Unlike `PortHandle<M>`, which supports unbounded delivery, this variant enqueues a single message using a one-time `oneshot::Sender`:
```rust
pub struct OncePortHandle<M: Message> {
    mailbox: Mailbox,
    port_index: u64,
    port_id: PortId,
    sender: oneshot::Sender<M>,
}
```
### Fields
- **`mailbox`**: The `Mailbox` this port was created from. Stored so the handle can access the actor ID and register itself in the mailbox’s port map.
- **`port_index`**: The local index of the port within the mailbox. Used as the key in the mailbox’s port map.
- **`port_id`**: The globally unique identifier for this port. Assigned eagerly, since one-shot ports are always bound at creation.
- **`sender`**: The one-shot message delivery channel. Used to transmit a single M-typed message.

Compared to [`PortHandle<M>`](#porthandlem), a `OncePortHandle<M>` is:

- **bound eagerly** at creation (it always has a `PortId`),
- **non-reusable** (it delivers at most one message),
- and uses a **one-shot channel** instead of an unbounded queue.

### Binding

Unlike `PortHandle<M>`, `OncePortHandle<M>` is already bound at creation. However, calling `bind()` produces a `OncePortRef<M>` that can be shared with remote actors:
```rust
impl<M: RemoteMessage> OncePortHandle<M> {
    pub fn bind(self) -> OncePortRef<M>;
}
```

## `PortRef` and `OncePortRef`

A `PortRef<M>` is a cloneable, sendable reference to a bound typed port. These are used to send messages to an actor from outside its mailbox, typically after calling `.bind()` on a `PortHandle<M>`:
```rust
pub struct PortRef<M: RemoteMessage> {
    port_id: PortId,
    reducer_typehash: Option<u64>,
    phantom: PhantomData<M>,
}
```
### Fields

- **`port_id`**: The globally unique identifier for this port. Used during message routing to locate the destination mailbox.
- **`reducer_typehash`**: Optional hash of the reducer type, used to validate compatibility when delivering messages to reducer-style ports.
- **`phantom`**: Phantom data to retain the `M` type parameter. This enforces compile-time type safety without storing a value of type `M`.

A `OncePortRef<M>` is a reference to a one-shot port. Unlike `PortRef`, it allows exactly one message to be sent. These are created by binding a `OncePortHandle<M>`.
```rust
pub struct OncePortRef<M: RemoteMessage> {
    port_id: PortId,
    phantom: PhantomData<M>,
}
```

### Fields

- **`port_id`**: The globally unique identifier for this port. Used during message routing to locate the destination mailbox.
- **`phantom`**: Phantom data to retain the `M` type parameter. This enforces compile-time type safety without storing a value of type `M`.

## `PortReceiver<M>`

A `PortReceiver<M>` is used to asynchronously receive `M`-typed messages from a port. It is the receiving half of a typed port pair:
```rust
pub struct PortReceiver<M> {
    receiver: mpsc::UnboundedReceiver<M>,
    port_id: PortId,
    coalesce: bool,
    state: Arc<State>,
}
```
### Fields

- **`receiver`**: The unbounded MPSC channel receiver used to retrieve messages sent to this port.
- **`port_id`**: The unique identifier for the port associated with this receiver. Used to deregister the port when the receiver is dropped.
- **`coalesce`**: If `true`, enables coalescing behavior — only the most recent message is retained when multiple are queued, and earlier ones are discarded.
- **`state`**: Shared internal mailbox state. Used to cleanly deregister the port from the mailbox when the receiver is dropped.

### Usage

A `PortReceiver<M>` is returned when calling `.open_port::<M>()` on a `Mailbox`. The actor can `await` messages on the receiver using `.recv().await`, which yields `Option<M>`:
```rust
let (port, mut receiver) = mailbox.open_port::<MyMsg>();
// ...
if let Some(msg) = receiver.recv().await {
    handle(msg);
}
```

### Construction and Use

A `PortReceiver` is created when calling `.open_port::<M>()` on a `Mailbox`. `new` just constructs a`PortReceiver` by wrapping the provided channel (`receiver`):
```rust
impl<M> PortReceiver<M> {
    fn new(
      receiver: mpsc::UnboundedReceiver<M>,
      port_id: PortId,
      coalesce: bool,
      state: Arc<State>
  ) -> Self {
        Self {
            receiver,
            port_id,
            coalesce,
            state,
        }
    }
}
```

Dropping the `PortReceiver<M>` automatically deregisters the associated port, preventing further message delivery.
```rust
impl<M> Drop for PortReceiver<M> {
    fn drop(&mut self) {
        self.state.ports.remove(&self.port());
    }
}
```

### `try_recv`

Attempts to receive a message from the port without blocking.

This method polls the underlying channel and returns immediately:

- `Ok(Some(msg))` if a message is available,
- `Ok(None)` if the queue is currently empty,
- `Err(MailboxError)` if the port is closed or disconnected.

If the port was created with `coalesce = true`, `try_recv()` drains the queue and returns only the most recent message, discarding earlier ones:
```rust
impl<M> PortReceiver<M> {
    pub fn try_recv(&mut self) -> Result<Option<M>, MailboxError> {
        let mut next = self.receiver.try_recv();
        // To coalesce, drain the mpsc queue and only keep the last one.
        if self.coalesce {
            if let Some(latest) = self.drain().pop() {
                next = Ok(latest);
            }
        }
        match next {
            Ok(msg) => Ok(Some(msg)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(MailboxError::new(
                self.actor_id().clone(),
                MailboxErrorKind::Closed,
            )),
        }
    }
}
```

### `recv`

Receives the next message from the port, waiting if necessary.

This is the asynchronous counterpart to `try_recv`. It awaits a message and returns it once available. If the port has been closed, it returns a `MailboxError`.

When `coalesce = true`, this method awaits one message, then drains the queue and returns only the most recent one.
```rust
impl<M> PortReceiver<M> {
    pub async fn recv(&mut self) -> Result<M, MailboxError> {
        let mut next = self.receiver.recv().await;
        // To coalesce, get the last message from the queue if there are
        // more on the mpsc queue.
        if self.coalesce {
            if let Some(latest) = self.drain().pop() {
                next = Some(latest);
            }
        }
        next.ok_or(MailboxError::new(
            self.actor_id().clone(),
            MailboxErrorKind::Closed,
        ))
    }
}
```

### `drain`

Drains all available messages from the port without blocking.

This method is used internally by `recv` and `try_recv` when `coalesce = true`, but can also be used directly to consume multiple messages in a batch.

If `coalesce` is enabled, all but the most recent message are discarded during the drain.
```rust
impl<M> PortReceiver<M> {
    pub fn drain(&mut self) -> Vec<M> {
        let mut drained: Vec<M> = Vec::new();
        while let Ok(msg) = self.receiver.try_recv() {
            // To coalesce, discard the old message if there is any.
            if self.coalesce {
                drained.pop();
            }
            drained.push(msg);
        }
        drained
    }
}
```

## `OncePortReceiver<M>`

A `OncePortReceiver<M>` is the receiving half of a one-shot port. It is returned when calling `.open_once_port::<M>()` on a `Mailbox`. Unlike `PortReceiver`, this variant:
- Receives exactly one message,
- Consumes itself on receive (i.e., recv takes self by value),
- Internally wraps a `oneshot::Receiver<M>` instead of an unbounded channel.
```rust
pub struct OncePortReceiver<M> {
    receiver: Option<oneshot::Receiver<M>>,
    port_id: PortId,
    state: Arc<State>,
}
```

### Receiving

`recv()` consumes the `OncePortReceiver` and awaits a single message. If the port is closed before the message is sent, it returns a `MailboxError`.
```rust
impl<M> OncePortReceiver<M> {
    pub async fn recv(mut self) -> Result<M, MailboxError> {
        std::mem::take(&mut self.receiver)
            .unwrap()
            .await
            .map_err(|err| {
                MailboxError::new(
                    self.actor_id().clone(),
                    MailboxErrorKind::Recv(self.port_id.clone(), err.into()),
                )
            })
    }
}
```
The `recv` method moves out the internal `oneshot::Receiver` using `std::mem::take(`) and awaits it. Any error (e.g., if the sender was dropped) is converted into a `MailboxError`.

### Lifecycle and Deregistration

Like `PortReceiver`, dropping a `OncePortReceiver` deregisters the port from the mailbox’s state:
```rust
impl<M> Drop for OncePortReceiver<M> {
    fn drop(&mut self) {
        self.state.ports.remove(&self.port());
    }
}
```
This ensures the port becomes unreachable and no further message delivery occurs once the receiver is dropped.

## Sending Messages

### `UnboundedSender` and `OnceSender`
Every open port is backed by a sender, responsible for delivering messages to the corresponding receiver. For unbounded ports, this sender is:
```rust
enum UnboundedPortSender<M: Message> {
    Mpsc(mpsc::UnboundedSender<M>),
    Func(Arc<dyn Fn(M) -> Result<(), anyhow::Error> + Send + Sync>),
}
```
These are wrapped in:
```rust
struct UnboundedSender<M: Message> {
    sender: UnboundedPortSender<M>,
    port_id: PortId,
}
```
The `send` method forwards messages and wraps errors in a `MailboxSenderError`:
```rust
impl<M: Message> UnboundedSender<M> {
    fn send(&self, message: M) -> Result<(), MailboxError> { ... }
}
```
`OnceSender<M>` is similar, but uses a `oneshot::Sender<M>` under the hood:
```rust
struct OnceSender<M: Message> {
    sender: Arc<Mutex<Option<oneshot::Sender<M>>>>,
    port_id: PortId,
}
```
Calling `.send_once(message)` on an `OnceSender` consumes the channel, and fails if the message has already been sent or the receiver is dropped.

### Type-Erased Delivery: `SerializedSender`

To enable uniform message routing, both `UnboundedSender` and `OnceSender` implement the `SerializedSender` trait:
```rust
trait SerializedSender: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn send_serialized(
        &self,
        headers: Attrs,
        serialized: Serialized,
    ) -> Result<bool, SerializedSenderError>;

```
This trait lets the mailbox deliver a `Serialized` message (a type-erased, encoded payload) by:
1. Deserializing the payload into a concrete `M` using `RemoteMessage` trait,
2. Sending it to the appropriate port, via its registered sender.

All active ports in a mailbox internally tracked in a type-erased form:
```
ports: DashMap<u64, Box<dyn SerializedSender>>,
```
This enables the mailbox to deliver messages to any known port regardless of its specific message type, provided deserialization succeeds.

If deserialization fails, or the underlying port is closed, an appropriate `MailboxSenderError` is returned via a `SerializedSenderError`.

See the (Mailbox) [`State`](./mailbox.md#state) section for details on how the mailbox owns and manages this ports map.

#### Example: `SerializedSender` for `UnboundedSender<M>`

Below is the canonical implementation of `SerializedSender` for `UnboundedSender<M>`:
```rust
impl<M: RemoteMessage> SerializedSender for UnboundedSender<M> {
    fn send_serialized(
        &self,
        headers: Attrs,
        serialized: Serialized,
    ) -> Result<bool, SerializedSenderError> {
        match serialized.deserialized() {
            Ok(message) => {
                self.sender.send(headers.clone(), message).map_err(|err| {
                    SerializedSenderError {
                        data: serialized,
                        error: MailboxSenderError::new_bound(
                            self.port_id.clone(),
                            MailboxSenderErrorKind::Other(err),
                        ),
                        headers,
                    }
                })?;

                Ok(true)
            }
            Err(err) => Err(SerializedSenderError {
                data: serialized,
                error: MailboxSenderError::new_bound(
                    self.port_id.clone(),
                    MailboxSenderErrorKind::Deserialize(M::typename(), err),
                ),
                headers,
            }),
        }
    }
}
```
This implementation:
- Attempts to decode the payload into a concrete `M`,
- Sends the decoded message via the associated port,
- Returns `Ok(true)` on success, or wraps any failure into a `SerializedSenderError`.

#### `OnceSender<M>`

`OnceSender<M>` implements `SerializedSender` similarly, deserializing the payload and forwarding it via a one-shot channel. It differs mainly in that the underlying port may only be used once and returns `Ok(false)` when consumed.
