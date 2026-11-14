# `Addr`

The `Addr` enum is the type-erased representation of addressable entities in hyperactor. It provides a common format for parsing, logging, routing, and transport.

```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Addr {
 Proc(ProcAddr),
 Actor(ActorAddr),
 Port(PortAddr),
}
```

Each variant wraps one of the concrete address types. Each address pairs an id with a `Location`:

- `ProcAddr`
- `ActorAddr`
- `PortAddr`

## Use Cases

- Used to represent references in a uniform way, such as CLI args, config, and logs.
- Returned by `.parse::<Addr>()` when parsing from string.
- Enables prefix-based comparisons for routing or scoping.
- Can be converted `to`/`from` the concrete types via `From`.

## Construction

From concrete types:

```
use hyperactor::{ActorAddr, Addr};

let actor_addr: ActorAddr = ...;
let reference: Addr = actor_addr.into();
```

From a string:

```
let reference: Addr = "logger.controller<2MuAHeDjLCEd>@tcp://[::1]:1234".parse().unwrap();
```

You can match on the reference to access the underlying type:

```
match reference {
 Addr::Actor(actor_addr) => { /* ... */ }
 Addr::Port(port_addr) => { /* ... */ }
 _ => {}
}
```

## Methods

```
impl Addr {
 pub fn is_prefix_of(&self, other: &Addr) -> bool;
 pub fn proc_addr(&self) -> ProcAddr;
}
```

- `.is_prefix_of(other)` checks whether one reference is a prefix of another (e.g., `Proc` -> `Actor` -> `Port`).
- `.proc_addr()` returns the proc address containing the target.

## Ordering

`Addr` implements a total order across all variants. Ordering is defined lexicographically:

```
(proc_addr, actor_uid, port)
```

This allows references to be used in sorted maps or for prefix-based routing schemes.

## Traits

`Addr` implements:

- `Display` -- formats to the same syntax accepted by `FromStr`
- `FromStr` -- parses strings like `"logger.controller<2MuAHeDjLCEd>@tcp://[::1]:1234"` and `"logger.controller<2MuAHeDjLCEd>:42@tcp://[::1]:1234"`
- `Ord`, `Eq`, `Hash` -- useful in sorted/routed contexts