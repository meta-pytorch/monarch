# `GangId`

A `GangId` identifies a logical group of actors with the same name across all procs in a world. It serves as a convenient shorthand for referring to all root instances of a given actor name.
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
pub struct GangId(pub WorldId, pub String);
```
- The first field is the WorldId.
- The second field is the shared actor name.

A `GangId` is conceptually like saying: “the actor named X on every proc in world W.”

## Construction

```rust
use hyperactor::reference::{GangId, WorldId};

let gang = GangId(WorldId("training".into()), "logger".into());
```

Or using the id! macro:
```rust
use hyperactor::id;

let gang = id!(training.logger);
// Equivalent to GangId(WorldId("training".into()), "logger".into())
```

## Methods

```rust
impl GangId {
    pub fn world_id(&self) -> &WorldId;
    pub fn name(&self) -> &str;
    pub fn actor_id(&self, rank: usize) -> ActorId;
    pub fn expand(&self, world_size: usize) -> impl Iterator<Item = ActorId> + '_;
}
```
- `.world_id()` returns the world this gang is defined in.
- `.name()` returns the shared actor name (e.g., "logger").
- `.actor_id(rank)` returns the root actor on that proc.
- `.expand(world_size)` yields all root ActorIds from rank `0..world_size`.

## Semantics

- Gangs are always composed of root actors (`pid = 0`) with a common name.
- Gang references are useful for broadcasting, coordination, or actor discovery.
- They are lightweight and purely name-based; no state is attached to a `GangId`.

## Traits

`GangId` implements:
- `Display` — formatted as world.actor
- `FromStr` — parses from strings like "training.logger"
- `Ord`, `Eq`, `Hash` — usable in maps, registries, and routing
- `Named` — enables type registration and metadata lookup
