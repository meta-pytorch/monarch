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
use hyperactor::{Label, ProcId};

let anonymous = ProcId::anonymous();
let worker = ProcId::instance(Label::new("worker").unwrap());
let service = ProcId::instance(Label::new("service").unwrap());
```

Use `ProcAddr` when you need a reachable proc reference; it pairs a `ProcId` with a `Location`. See [Host](../procs/host.md) for how proc addresses are used in the host architecture.

## Methods

```rust
impl ProcId {
    pub fn uid(&self) -> &Uid;
    pub fn label(&self) -> Option<&Label>;
    pub fn to_path_elem(&self, base_dir: &Path) -> PathBuf;
}
```
- `.uid()` returns the identity uid.
- `.label()` returns display metadata for instance ids, or the singleton name for singleton ids.
- `.to_path_elem()` returns a stable, short socket path for local links.

## Traits

ProcId implements:
- `Display`
- `FromStr`
- `Ord`, `Eq`, `Hash` — usable in maps and sorted structures
