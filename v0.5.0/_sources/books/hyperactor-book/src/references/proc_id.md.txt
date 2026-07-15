# `ProcId`

A `ProcId` identifies a single runtime instance. All actors exist within a proc, and message routing between actors is scoped by the proc's identity.

Proc identity is separate from proc location. The addressable form is `ProcAddr`, which combines a `ProcId` with a `Location`.

```rust
#[derive(
    Serialize,
    Deserialize,
    Clone,
)]
pub struct ProcId {
    uid: Uid,
}
```

## Construction

Fields are private; use named constructors:

```rust
use hyperactor::id::{Label, ProcId};

let anonymous = ProcId::anonymous();
let worker = ProcId::instance(Label::new("worker").unwrap());
let service = ProcId::singleton(Label::new("service").unwrap());
```

See [Host](../procs/host.md) for how procs are used in the Host architecture.

## Methods

```rust
impl ProcId {
    pub fn uid(&self) -> &Uid;
    pub fn label(&self) -> Option<&Label>;
    pub fn pseudo_uid(&self) -> Uid;
}
```
- `.uid()` returns the identity uid.
- `.label()` returns display metadata for instance ids, or the singleton name for singleton ids.
- `.pseudo_uid()` returns a stable instance-shaped uid for contexts that need a short local path component.

## Traits

ProcId implements:
- `Display`
- `FromStr`
- `Ord`, `Eq`, `Hash` — usable in maps and sorted structures
