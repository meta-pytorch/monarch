# Mailbox

A `Mailbox` is an actor's in-process inbox. It owns the actor's ports, allocates new port indices, binds serializable ports into a routing table, and delivers `MessageEnvelope`s to the correct typed receiver.

A mailbox is deliberately local. If an envelope is addressed to a different actor, `Mailbox::post_unchecked` returns it as an invalid reference (`WrongMailboxOwner`). Cross-actor and cross-proc routing happens before the mailbox, through procs, gateways, and routers.

## Opening ports

The most common API is `open_port`, which creates a reusable typed port:

```rust
let (handle, receiver) = mailbox.open_port::<MyMessage>();
```

The returned `PortHandle<M>` is the sending side, and `PortReceiver<M>` is the receiving side. Local sends through the handle do not require serialization:

```text
PortHandle<M>::post(cx, message)
-> UnboundedPortSender<M>
-> PortReceiver<M>::recv().await
```

Other variants cover common runtime needs:

- `open_once_port<M>()` creates a one-shot request/reply port.
- `open_accum_port<A>()` creates a reducer-backed port that accumulates updates into state.
- Internal enqueue ports wire actor handler ports directly to actor work queues.

## Binding ports

Binding is required only when a port must be addressable through routed delivery, such as when a port reference is sent to another actor:

```rust
let port_ref: PortRef<MyMessage> = handle.bind();
```

Binding installs the port's serialized sender in the mailbox's internal table and returns a `PortRef<M>` carrying a `PortAddr`. `M` must implement `RemoteMessage`, because routed delivery serializes the message payload.

By contrast, an unbound `PortHandle<M>` can still be used for local in-process sends and only requires `M: Message`.

## Routed delivery

Routed delivery enters through `MailboxSender`:

```text
Mailbox::post(envelope, return_handle)
-> decrement envelope TTL
-> Mailbox::post_unchecked(envelope, return_handle)
-> check destination actor matches this mailbox
-> lookup destination port index
-> SerializedSender::send_serialized(headers, data)
-> typed receiver or handler queue
```

Delivery can fail if the destination actor does not match, the port was never allocated, the handler port was not bound, the actor has stopped or failed, deserialization fails, or the receiving side is gone. Failures are returned as structured `DeliveryFailure`s; see [Delivery Semantics](delivery.md).

## State

All clones of a `Mailbox` share one state object:

```rust
pub struct Mailbox {
    inner: Arc<State>,
}

struct State {
    actor_id: ActorAddr,
    ports: DashMap<Port, Arc<dyn SerializedSender>>,
    next_ephemeral_port: AtomicU64,
    closed: RwLock<Option<ActorStatus>>,
}
```

The `ports` map is keyed by `Port` (an ephemeral index, a handler uid, or a control port). Each value is type-erased, but it knows the expected message type and attempts to deserialize the incoming `wirevalue::Any` before enqueueing it.

## Capabilities

Mailbox operations are gated by sealed *context* traits in `hyperactor::context`. An API takes the narrowest context it needs, and these traits cannot be implemented outside the crate.

| Context trait | Grants |
|---|---|
| `context::Mailbox` | Access to a `Mailbox`, and so opening ports (`open_port`, `open_once_port`). |
| `context::Actor` | Everything `Mailbox` grants, plus sending to ports, splitting ports, and spawning child actors. |

`context::Actor` extends `context::Mailbox`: only an actor context can send, because a send needs a return port for undeliverable replies. Actor implementations run with a `Context<A>`, backed by the actor's `Instance<A>`, so handler code already has the full set installed.
