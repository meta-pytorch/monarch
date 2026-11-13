# Delivery Semantics

This page describes how mailbox delivery succeeds, fails, and reports failure.

## Message envelopes

`MessageEnvelope` is the unit routed through `MailboxSender`s. It carries an addressed, serialized message:

```
pub struct MessageEnvelope {
 sender: ActorAddr,
 dest: PortAddr,
 next_hop: Option<PortAddr>,
 data: wirevalue::Any,
 delivery_failures: Vec<DeliveryFailure>,
 headers: Flattrs,
 ttl: u8,
 return_undeliverable: bool,
}
```

The sender and destination are addresses, not bare ids. `dest` is the canonical destination and does not change in flight. `next_hop` is where the next router should deliver; it defaults to `dest`, and a gateway rewrites it as it peels `Via` hops (see [Gateway](../procs/gateway.html)). A gateway routes on the next hop's `Location`; prefix routers match on `dest`'s actor address.

Constructors:

```
impl MessageEnvelope {
 pub fn new(
 sender: impl Into<ActorAddr>,
 dest: impl Into<PortAddr>,
 data: wirevalue::Any,
 headers: Flattrs,
 ) -> Self;

 pub fn serialize<T: Serialize + Named>(
 source: impl Into<ActorAddr>,
 dest: impl Into<PortAddr>,
 value: &T,
 headers: Flattrs,
 ) -> Result<Self, wirevalue::Error>;
}
```

Each `MailboxSender::post` decrements `ttl` before calling `post_unchecked`. When the TTL reaches zero, the envelope is returned as an expired delivery instead of being routed further.

## Structured failures

Delivery failures are structured and accumulate in the envelope's `delivery_failures` vector. Each entry is a `DeliveryFailure`: a `kind` plus attributes.

```
pub struct DeliveryFailure {
 pub kind: DeliveryFailureKind,
 pub attrs: Flattrs,
}

pub enum DeliveryFailureKind {
 InvalidReference(InvalidReference),
 Undeliverable(UndeliverableReason),
 Expired(ExpiredDelivery),
}
```

The first entry is the root failure. Later entries record failures encountered while the same envelope is forwarded onward.

Common failure classes:

- `InvalidReference`: the destination does not denote a valid live recipient, such as an unbound handler, a stopped actor, a port that was never allocated, or delivery to the wrong mailbox owner.
- `UndeliverableReason::Transport`: the message could not be carried to the destination, such as a closed channel, unavailable link, missing route, or acknowledgement timeout.
- `UndeliverableReason::PortGone`: the destination port's ordinary recipient is gone.
- `ExpiredDelivery`: the envelope exhausted its TTL.

## Returning undeliverables

When delivery fails, code calls:

```
envelope.undeliverable(failure, return_handle);
```

This records the `DeliveryFailure` on the envelope and, when `return_undeliverable` is true, sends an `Undeliverable<MessageEnvelope>` to the supplied return handle; when it is false, the envelope is dropped. The failure is recorded before the return is attempted, so the original cause survives even if the return path itself fails: a failed return is logged and the message abandoned, not retried.

Actors normally receive returned failures on their `Undeliverable<MessageEnvelope>` handler port. The default actor behavior treats invalid references and ordinary undeliverables as actor errors unless the actor overrides the corresponding handler.

## Mailbox delivery

`Mailbox::post_unchecked` performs local delivery only for its owning actor address:

1. Reject envelopes whose destination actor is not the mailbox owner with `InvalidReferenceReason::WrongMailboxOwner`.
2. Look up the destination port index in the mailbox's bound-port table.
3. Reject unallocated or unbound ports with an `InvalidReference`.
4. Deserialize the payload into the port's expected message type.
5. Enqueue the typed message, or remove exhausted one-shot ports.

Cross-actor and cross-proc routing happens before the envelope reaches a mailbox: procs, gateways, and routers choose the right mailbox; the mailbox only delivers to ports owned by its actor.