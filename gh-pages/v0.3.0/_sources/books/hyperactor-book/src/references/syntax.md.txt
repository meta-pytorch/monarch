# Syntax

References in Hyperactor follow a uniform concrete syntax that can be written as strings, parsed at runtime, or constructed statically using the `id!` macro.

## String Form

The canonical string syntax supports hierarchical references, from worlds down to ports:
```text
world
world[rank]
world[rank].actor           // actor[0]
world[rank].actor[pid]
world[rank].actor[pid][port]
world.actor                 // gang reference
```

These forms can be used wherever a reference is accepted as a string, such as command-line arguments, config files, and logs.

Examples:

- `training` — world ID
- `training[0]` — proc 0 in world `training`
- `training[0].logger[1]` — actor named `logger`, pid 1
- `training[0].logger[1][42]` — port 42 of that actor
- `training.logger` — gang reference

The parser is robust and fails clearly on invalid syntax.

## Runtime Parsing

The `Reference` type implements `FromStr`, so you can parse strings into references:

```rust
use hyperactor::reference::Reference;

let r: Reference = "training[2].worker[0]".parse().unwrap();
```

It returns a strongly typed enum: `Reference::Actor`, `Reference::Port`, etc.

## Static Construction with `id!`

You can also construct references statically using the `id!` macro. This macro uses the same concrete syntax:
```rust
use hyperactor::id;
use hyperactor::reference::{WorldId, ProcId, ActorId, PortId, GangId};

let w: WorldId = id!(training);
let p: ProcId = id!(training[0]);
let a: ActorId = id!(training[0].logger[1]);
let port: PortId = id!(training[0].logger[1][42]);
let g: GangId = id!(training.logger);
```

The macro expands to correct type constructors and ensures compile-time validity. The `id!()` macro does not produce a `Reference` enum-it constructs the corresponding concrete type directly (e.g., `WorldId`, `ProcId`, `ActorId`). This contrasts with parsing, which always yields a `Reference`.
