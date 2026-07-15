# `ProcId`

A `ProcId` identifies a single runtime instance. All actors exist within a proc, and message routing between actors is scoped by the proc's identity.

Proc identity is separate from proc location. The addressable form is `ProcAddr`, which combines a `ProcId` with a `Location`.

```
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

```
use hyperactor::{Label, ProcId};

let anonymous = ProcId::anonymous();
let worker = ProcId::instance(Label::new("worker").unwrap());
let service = ProcId::singleton(Label::new("service").unwrap());
```

Use `ProcAddr` when you need a reachable proc reference; it pairs a `ProcId` with a `Location`. See [Host](../procs/host.html) for how proc addresses are used in the host architecture.

## Methods

```
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
- `Ord`, `Eq`, `Hash` -- usable in maps and sorted structures