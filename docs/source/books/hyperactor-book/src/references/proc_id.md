# `ProcId`

A `ProcId` identifies a single runtime instance. All actors exist within a proc, and message routing between actors is scoped by the proc's identity.

Procs are identified by a direct channel address and name:

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
)]
pub struct ProcId(pub ChannelAddr, pub String);
```

## Construction

Construct a `ProcId` with a channel address and proc name:
```rust
use hyperactor::reference::ProcId;

let addr = "tcp:127.0.0.1:8080".parse()?;
let proc = ProcId(addr, "service".to_string());
```

See [Host](../procs/host.md) for how procs are used in the Host architecture.

## Methods

```rust
impl ProcId {
    pub fn name(&self) -> &str;
    pub fn actor_id(&self, name: impl Into<String>, pid: usize) -> ActorId;
}
```
- `.name()` returns the proc's name
- `.actor_id(name, pid)` constructs an `ActorId` for an actor hosted on this proc

## Traits

ProcId implements:
- `Display` — formatted as `channel_addr,name`
- `FromStr` — parses from strings like `"tcp:127.0.0.1:8080,service"`
- `Ord`, `Eq`, `Hash` — usable in maps and sorted structures
- `Named` — enables port lookup and type reflection
