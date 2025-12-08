# `ProcId`

A `ProcId` identifies a single runtime instance. All actors exist within a proc, and message routing between actors is scoped by the proc's identity.

Procs can be identified either by their rank within a world (Ranked) or by a direct channel address and name (Direct):

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
    Named,
    EnumAsInner
)]
pub enum ProcId {
    /// A ranked proc within a world
    Ranked(WorldId, Index),
    /// A proc reachable via a direct channel address, and local name.
    Direct(ChannelAddr, String),
}
```

## Construction

### Ranked Procs

You can construct a ranked `ProcId` directly:
```rust
use hyperactor::reference::{WorldId, ProcId};

let proc = ProcId::Ranked(WorldId("training".into()), 0);
```
Or statically using the `id!` macro:
```rust
use hyperactor::id;

let proc = id!(training[0]); // Equivalent to ProcId::Ranked(WorldId("training".into()), 0)
```

### Direct Procs

For direct addressing, construct with a channel address and proc name:
```rust
use hyperactor::reference::ProcId;

let addr = "tcp:127.0.0.1:8080".parse()?;
let proc = ProcId::Direct(addr, "service".to_string());
```

See [Host](../procs/host.md) for how direct procs are used in the Host architecture.

## Methods

```rust
impl ProcId {
    pub fn world_id(&self) -> Option<&WorldId>;
    pub fn world_name(&self) -> Option<&str>;
    pub fn rank(&self) -> Option<usize>;
    pub fn name(&self) -> Option<&String>;
    pub fn actor_id(&self, name: impl Into<String>, pid: usize) -> ActorId;
}
```
- `.world_id()` returns the `WorldId` for ranked procs, `None` for direct procs
- `.world_name()` returns the world name for ranked procs, `None` for direct procs
- `.rank()` returns the proc's rank for ranked procs, `None` for direct procs
- `.name()` returns the proc's name for direct procs, `None` for ranked procs
- `.actor_id(name, pid)` constructs an `ActorId` for an actor hosted on this proc (works for both variants)

# Notes

For ranked procs, ranks greater than or equal to `1 << (usize::BITS - 1)` are considered user-space procs. These are typically created with `WorldId::random_user_proc()` and are not assigned by the system.

## Traits

ProcId implements:
- `Display` — formatted as `world[rank]` for ranked procs, `channel_addr,name` for direct procs
- `FromStr` — parses from strings like `"training[0]"` or `"tcp:127.0.0.1:8080,service"`
- `Ord`, `Eq`, `Hash` — usable in maps and sorted structures
- `Named` — enables port lookup and type reflection
