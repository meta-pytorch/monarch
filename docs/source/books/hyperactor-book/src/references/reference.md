# `Reference`

The `Reference` enum is a type-erased, unified representation of all addressable entities in hyperactor. It provides a common format for parsing, logging, routing, and transport.

```rust
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, Named)]
pub enum Reference {
    World(WorldId),
    Proc(ProcId),
    Actor(ActorId),
    Port(PortId),
    Gang(GangId),
}
```
Each variant wraps one of the concrete identifier types:
- [`WorldId`](world_id.md)
- [`ProcId`](proc_id.md)
- [`ActorId`](actor_id.md)
- [`PortId`](port_id.md)
- [`GangId`](gang_id.md)

## Use Cases

- Used to represent references in a uniform way (e.g., CLI args, config, logs).
- Returned by `.parse::<Reference>()` when parsing from string.
- Enables prefix-based comparisons for routing or scoping.
- Can be converted `to`/`from` the concrete types via `From`.

## Construction

From concrete types:
```rust
use hyperactor::reference::{Reference, ActorId};

let actor_id = ...;
let reference: Reference = actor_id.into();
```
From a string:
```rust
let reference: Reference = "training[0].logger[1][42]".parse().unwrap();
```
You can match on the reference to access the underlying type:
```rust
match reference {
    Reference::Actor(actor_id) => { /* ... */ }
    Reference::Port(port_id) => { /* ... */ }
    _ => {}
}
```

## Methods

```rust
impl Reference {
    pub fn is_prefix_of(&self, other: &Reference) -> bool;
    pub fn world_id(&self) -> Option<&WorldId>;
    pub fn proc_id(&self) -> Option<&ProcId>;
    pub fn actor_id(&self) -> Option<&ActorId>;
}
```
- `.is_prefix_of(other)` checks whether one reference is a prefix of another (e.g., `WorldId` -> `ProcId` -> `ActorId`).
- `.world_id()` returns the reference's associated world, if any.
- `.proc_id()` and `.actor_id()` return their corresponding IDs if applicable.

## Ordering

Reference implements a total order across all variants. Ordering is defined lexicographically:
```rust
(world_id, rank, actor_name, pid, port)
```
This allows references to be used in sorted maps or for prefix-based routing schemes.

## Traits

Reference implements:
- `Display` — formats to the same syntax accepted by `FromStr`
- `FromStr` — parses strings like `"world[1].actor[2][port]"`
- `Ord`, `Eq`, `Hash` — useful in sorted/routed contexts
- `Named` — used for port assignment, reflection, and runtime dispatch
