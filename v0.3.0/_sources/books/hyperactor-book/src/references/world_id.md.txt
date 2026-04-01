# `WorldId`

A `WorldId` defines the top-level namespace for procs and actors. All procs, actors, ports, and gangs exist within a world.
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
pub struct WorldId(pub String);
```

## Construction

A `WorldId` wraps a string and can be created directly:
```rust
use hyperactor::reference::WorldId;

let world = WorldId("training".into());
```
Or statically using the `id!` macro:
```rust
use hyperactor::id;

let world = id!(training); // Equivalent to WorldId("training".into())
```

## Methods

```rust
impl WorldId {
    pub fn name(&self) -> &str;
    pub fn proc_id(&self, index: usize) -> ProcId;
    pub fn random_user_proc(&self) -> ProcId;
}
```
- `.name()` returns the world name string.
- `.proc_id(index)` constructs a `ProcId` rooted in this world.
- `.random_user_proc()` generates a `ProcId` with the high bit set, marking it as a user-space proc ID.

## Traits

`WorldId` implements:
- `Display` — string form is just the world name
- `FromStr` — parses from "training" into WorldId("training")
- `Ord`, `Eq`, `Hash` — suitable for use as map/set keys
- `Named` — used for type reflection and message dispatch
