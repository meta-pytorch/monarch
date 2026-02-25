# `ActorId`

An `ActorId` uniquely identifies an actor within a proc. It combines the proc the actor lives on, a string name, and a numeric pid (process-local instance index).

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
pub struct ActorId(pub ProcId, pub String, pub usize);
```
- The first field is the actor's `ProcId`.
- The second is the actor's name (used for grouping and logging).
- The third is the pid, which distinguishes multiple instances with the same name.

### Construction

Construct an actor ID directly:
```rust
use hyperactor::reference::{ActorId, ProcId, WorldId};

let proc = ProcId::Ranked(WorldId("training".into()), 0);
let actor = ActorId(proc, "worker".into(), 1);
```

Or with the `id!` macro:
```rust
use hyperactor::id;

let actor = id!(training[0].worker[1]);
// Equivalent to ActorId(ProcId::Ranked(WorldId("training".into()), 0), "worker".into(), 1)
```
To refer to the root actor (the canonical instance), use:
```rust
let root = ActorId::root(proc, "worker".into());
// Equivalent to ActorId(proc, "worker".into(), 0)
```

### Methods

```rust
impl ActorId {
    pub fn proc_id(&self) -> &ProcId;
    pub fn name(&self) -> &str;
    pub fn pid(&self) -> usize;
    pub fn world_name(&self) -> &str;
    pub fn rank(&self) -> usize;
    pub fn child_id(&self, pid: usize) -> Self;
    pub fn port_id(&self, port: u64) -> PortId;
    pub fn root(proc: ProcId, name: String) -> Self;
}
```

- `.proc_id()` returns the ProcId that owns this actor.
- `.name()` returns the logical name of the actor (e.g., "worker").
- `.pid()` returns the actor's instance ID.
- `.world_name()` returns the name of the actor's world. **Panics** if this is a direct proc.
- `.rank()` returns the proc rank (i.e., index) this actor runs on. **Panics** if this is a direct proc.
- `.child_id(pid)` creates a new `ActorId` with the same name and proc but a different pid.
- `.port_id(port)` returns a `PortId` representing a port on this actor.
- `.root(proc, name)` constructs a new root actor (`pid = 0`) in the given proc.

### Traits

`ActorId` implements:

- `Display` — formats as `world[rank].name[pid]`
- `FromStr` — parses strings like `"training[0].logger[1]"`
- `Clone`, `Eq`, `Ord`, `Hash` — useful in maps, sets, and registries
- `Named` — enables type-based routing, port lookup, and reflection

## Semantics

- The `name` groups actors logically within a proc (e.g., `"worker"`, `"trainer"`).
- The `pid` distinguishes physical instances:
  - `pid = 0` represents the **root** actor instance.
  - `pid > 0` typically corresponds to **child actors** spawned by the root.
- Most routing and API surfaces operate on root actors by default.
- Port creation is always rooted in an `ActorId`, via `.port_id(...)`.
