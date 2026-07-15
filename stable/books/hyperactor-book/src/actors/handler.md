# The `Handler` Trait

The `Handler` trait defines how an actor receives and responds to messages of a specific type.

Each message type that an actor can handle must be declared by implementing this trait. The runtime invokes the `handle` method when such a message is delivered.

```
#[async_trait]
pub trait Handler<M>: Actor {
 async fn handle(&mut self, cx: &Context<Self>, message: M) -> Result<(), anyhow::Error>;
}
```

## Message Dispatch: `handle`

The `handle` method is invoked by the runtime whenever a message of type `M` arrives at a matching port on the actor.

- `message` is the received payload.
- `cx` gives access to the actor's runtime context, including its identity, mailbox, and any capabilities exposed by the `Context` type (such as spawning or reference resolution).
- The return value indicates whether the message was handled successfully.

An actor may implement `Handler<M>` multiple times -- once for each message type `M` it supports.

## Built-in Handlers

The runtime provides implementations of `Handler<M>` for a few internal message types:

### `Handler<Signal>`

This is a marker implementation indicating that all actors can receive `Signal`. The handler is not expected to be invoked directly -- its real behavior is implemented inside the runtime.

```
#[async_trait]
impl<A: Actor> Handler<Signal> for A {
 async fn handle(
 &mut self,
 _cx: &Context<Self>,
 _message: Signal,
 ) -> Result<(), anyhow::Error> {
 unimplemented!("signal handler should not be called directly")
 }
}
```

### Multipart routed messages

```
let mut message = wirevalue::Any::<wirevalue::encoding::Multipart>::serialize(&value)?;
message.visit_multipart_parts_mut::<PortRefRepr, anyhow::Error>(|port| {
 port.update_port_addr(rewritten_addr);
 Ok(())
})?;
```

Casting and accumulation routes serialized multipart messages through intermediate actors. Intermediate actors rewrite typed multipart parts directly, then forward the serialized message to the destination actor's ordinary typed handler port.

This construct is used in the implementation of **accumulation**, a communication pattern where a message is multicast to multiple recipients and their replies are gathered--possibly through intermediate actors--before being sent back to the original sender.

To enable this, selected fields such as `PortRef`s serialize as typed multipart parts. Intermediate nodes visit those part representations and rewrite the port addresses to point back to themselves. This ensures that replies from downstream actors are routed through the intermediate, enabling reply collection and reduction.

Once a message reaches its destination, the mailbox deserializes the multipart payload as the actor's typed message and dispatches it to the actor's existing `Handler<M>` implementation.

This allows actors to remain unaware of accumulation mechanics--they can just implement `Handler<M>` as usual.