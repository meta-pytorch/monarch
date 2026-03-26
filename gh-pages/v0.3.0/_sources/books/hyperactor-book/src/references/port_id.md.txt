# `PortId`

A `PortId` identifies a specific port on a particular actor. Ports are the entry points through which messages are delivered to an actor, and each `PortId` is globally unique.

```rust
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
pub struct PortId(pub ActorId, pub u64);
```
- The first field is the owning `ActorId`.
- The second field is the port number (`u64`), typically derived from the message type’s registered port.

## Construction

```rust
use hyperactor::reference::{PortId, ActorId};

let port = PortId(actor, 42);
```
Or via the `id!` macro:
```rust
use hyperactor::id;

let port = id!(training[0].logger[1][42]);
// Equivalent to PortId(ActorId(...), 42)
```
You can also construct a PortId from an `ActorId` using `.port_id(...)`:
```rust
let port = actor.port_id(42);
```

## Methods

```rust
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
- `Display` — formatted as `world[rank].actor[pid][port]`
- `FromStr` — parses from strings like `"training[0].logger[1][42]"`
- `Ord`, `Eq`, `Hash` — usable as map keys or for dispatch
- `Named` — supports reflection and typed messaging
