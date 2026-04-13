# `PortId`

A `PortId` identifies a specific port on a particular actor. Ports are the entry points through which messages are delivered to an actor, and each `PortId` is globally unique.

```
#[derive(
 Debug,
 Serialize,
 Deserialize,
 Clone,
 PartialEq,
 Eq,
 PartialOrd,
 Hash,
 Ord,
 Named
)]
pub struct PortId(ActorId, u64);
```

- The first field is the owning `ActorId`.
- The second field is the port number (`u64`), typically derived from the message type's registered port.

## Construction

Fields are private; use a named constructor or derive from an `ActorId`:

```
use hyperactor::reference::{PortId, ActorId};

let port = PortId::new(actor.clone(), 42);
```

You can also construct a PortId from an `ActorId` using `.port_id(...)`:

```
let port = actor.port_id(42);
```

## Methods

```
impl PortId {
 pub fn actor_id(&self) -> &ActorId;
 pub fn index(&self) -> u64;
 pub fn into_actor_id(self) -> ActorId;
}
```

- `.actor_id()` returns the owning actor.
- `.index()` returns the port number.
- `.into_actor_id()` discards the port index and yields the owning actor ID.

## Traits

`PortId` implements:

- `Display` -- formatted as `addr,proc_name,actor_name[pid][port]`
- `FromStr` -- parses from strings like `"tcp:[::1]:1234,myproc,logger[1][42]"`
- `Ord`, `Eq`, `Hash` -- usable as map keys or for dispatch
- `Named` -- supports reflection and typed messaging