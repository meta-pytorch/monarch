# `ActorId`

An `ActorId` uniquely identifies an actor within a proc. It combines the proc the actor lives on and a string name.

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
pub struct ActorId(ProcId, String);
```
- The first field is the actor's `ProcId`.
- The second is the actor's name (used for grouping and logging).

### Construction

Fields are private; use named constructors:
```rust
use hyperactor::reference::{ActorId, ProcId};

let addr = "tcp:127.0.0.1:8080".parse()?;
let proc = ProcId::with_name(addr, "myproc");
let actor = ActorId::new(proc.clone(), "worker");
```

### Methods

```rust
impl ActorId {
    pub fn proc_id(&self) -> &ProcId;
    pub fn name(&self) -> &str;
    pub fn port_id(&self, port: u64) -> PortId;
}
```

- `.proc_id()` returns the ProcId that owns this actor.
- `.name()` returns the logical name of the actor (e.g., "worker").
- `.port_id(port)` returns a `PortId` representing a port on this actor.

### Traits

`ActorId` implements:

- `Display` — formats as `addr,proc_name,name`
- `FromStr` — parses strings like `"tcp:[::1]:1234,myproc,logger"`
- `Clone`, `Eq`, `Ord`, `Hash` — useful in maps, sets, and registries
- `Named` — enables type-based routing, port lookup, and reflection

## Semantics

- The `name` groups actors logically within a proc (e.g., `"worker"`, `"trainer"`).
- Most routing and API surfaces operate on actors by name.
- Port creation is always rooted in an `ActorId`, via `.port_id(...)`.
