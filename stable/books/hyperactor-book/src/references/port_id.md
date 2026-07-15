# `PortId`

A `PortId` identifies a specific port on a particular actor. Ports are the entry points through which messages are delivered to an actor, and each `PortId` is globally unique.

```
#[derive(Clone, Serialize, Deserialize)]
pub struct PortId {
 actor_id: ActorId,
 port: Port,
}
```

- `actor_id` is the owning actor.
- `port` is a `Port` (an ephemeral index, a handler uid, or a control port), not a bare integer.

`PartialEq`, `Eq`, `Hash`, `Ord`, and `Display` are hand-implemented and key off `(actor_id, port)`.

## Construction

Fields are private; build one with `PortId::new`, passing the owning `ActorId` and a `Port`:

```
use hyperactor::PortId;
use hyperactor::port::Port;

# let actor_id = hyperactor::Proc::current().proc_addr().actor_addr("logger").id().clone();
let port_id = PortId::new(actor_id, Port::from(42u64));
```

For the addressable form, call `port_addr` on an `ActorAddr`. It pairs the `PortId` with the actor's `Location` and returns a `PortAddr`:

```
# use hyperactor::port::Port;
# let actor_addr = hyperactor::Proc::current().proc_addr().actor_addr("logger");
let port_addr = actor_addr.port_addr(Port::from(42u64));
```

## Methods

```
impl PortId {
 pub fn actor_id(&self) -> &ActorId;
 pub fn port(&self) -> Port;
 pub fn proc_id(&self) -> &ProcId;
}
```

- `.actor_id()` returns the owning actor.
- `.port()` returns the port number.
- `.proc_id()` returns the proc id that owns this port.

## Traits

`PortId` implements:

- `Display` -- formatted as `actor-id:port`, such as `logger.service:42`
- `FromStr` -- parses the same id-only syntax. Use `PortAddr` or `Addr` for strings that also include a location, such as `logger.service:42@tcp://[::1]:1234`.
- `Ord`, `Eq`, `Hash` -- usable as map keys or for dispatch
- `Named` -- supports reflection and typed messaging