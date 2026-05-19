# `ActorId`

An `ActorId` uniquely identifies an actor within a proc. It combines the owning `ProcId` with an actor uid.

```rust
#[derive(
    Serialize,
    Deserialize,
    Clone,
)]
pub struct ActorId {
    uid: Uid,
    proc_id: ProcId,
}
```

### Construction

Fields are private; use named constructors:

```rust
use hyperactor::id::{ActorId, Label, ProcId};

let proc = ProcId::instance(Label::new("myproc").unwrap());
let anonymous = ActorId::anonymous(proc.clone());
let worker = ActorId::instance(Label::new("worker").unwrap(), proc.clone());
let root = ActorId::singleton(Label::new("root").unwrap(), proc);
```

### Methods

```rust
impl ActorId {
    pub fn uid(&self) -> &Uid;
    pub fn proc_id(&self) -> &ProcId;
    pub fn label(&self) -> Option<&Label>;
}
```

- `.uid()` returns the actor identity uid.
- `.proc_id()` returns the `ProcId` that owns this actor.
- `.label()` returns display metadata for instance ids, or the singleton name for singleton ids.

### Traits

`ActorId` implements:

- `Display`
- `FromStr`
- `Clone`, `Eq`, `Ord`, `Hash` — useful in maps, sets, and registries

## Semantics

- `ActorId::anonymous(proc_id)` creates a fresh unlabeled actor identity.
- `ActorId::instance(label, proc_id)` creates a fresh actor identity with label metadata.
- `ActorId::singleton(label, proc_id)` creates an actor identity whose label is the uid identity.
