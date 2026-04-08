# `Reference`

The `Reference` enum is a type-erased, unified representation of all addressable entities in hyperactor. It provides a common format for parsing, logging, routing, and transport.

```
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, Named)]
pub enum Reference {
 Proc(ProcId),
 Actor(ActorId),
 Port(PortId),
}
```

Each variant wraps one of the concrete identifier types:

- [`ProcId`](proc_id.html)
- [`ActorId`](actor_id.html)
- [`PortId`](port_id.html)

## Use Cases

- Used to represent references in a uniform way (e.g., CLI args, config, logs).
- Returned by `.parse::<Reference>()` when parsing from string.
- Enables prefix-based comparisons for routing or scoping.
- Can be converted `to`/`from` the concrete types via `From`.

## Construction

From concrete types:

```
use hyperactor::reference::{Reference, ActorId};

let actor_id = ...;
let reference: Reference = actor_id.into();
```

From a string:

```
let reference: Reference = "tcp:[::1]:1234,myproc,logger[1][42]".parse().unwrap();
```

You can match on the reference to access the underlying type:

```
match reference {
 Reference::Actor(actor_id) => { /* ... */ }
 Reference::Port(port_id) => { /* ... */ }
 _ => {}
}
```

## Methods

```
impl Reference {
 pub fn is_prefix_of(&self, other: &Reference) -> bool;
 pub fn proc_id(&self) -> Option<&ProcId>;
 pub fn actor_id(&self) -> Option<&ActorId>;
}
```

- `.is_prefix_of(other)` checks whether one reference is a prefix of another (e.g., `Proc` -> `Actor` -> `Port`).
- `.proc_id()` and `.actor_id()` return their corresponding IDs if applicable.

## Ordering

Reference implements a total order across all variants. Ordering is defined lexicographically:

```
(proc_id, actor_name, pid, port)
```

This allows references to be used in sorted maps or for prefix-based routing schemes.

## Traits

Reference implements:

- `Display` -- formats to the same syntax accepted by `FromStr`
- `FromStr` -- parses strings like `"tcp:[::1]:1234,myproc,actor[2][port]"`
- `Ord`, `Eq`, `Hash` -- useful in sorted/routed contexts
- `Named` -- used for port assignment, reflection, and runtime dispatch