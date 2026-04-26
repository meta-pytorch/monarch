# Syntax

References in Hyperactor follow a uniform concrete syntax that can be written as strings and parsed at runtime.

## String Form

The canonical string syntax supports hierarchical references, from procs down to ports:
```text
proc-id@location
actor-id@location
port-id@location

proc-id  := label | <uid> | label<uid>
actor-id := actor-part.proc-id
port-id  := actor-id:port
```

These forms can be used wherever a reference is accepted as a string, such as command-line arguments, config files, and logs.

Examples:

- `local@inproc://0` — proc reference for the singleton `local` proc
- `controller<2MuAHeDjLCEd>@tcp://[::1]:1234` — proc reference for a labeled proc instance
- `logger.controller<2MuAHeDjLCEd>@tcp://[::1]:1234` — actor reference
- `<2MuAHeDjLCEd>.<NRjEZGYjYibf>:42@tcp://[::1]:1234` — port 42 on an unlabeled actor instance

The parser is robust and fails clearly on invalid syntax.

## Runtime Parsing

The `Reference` type implements `FromStr`, so you can parse strings into references:

```rust
use hyperactor::reference::Reference;

let r: Reference = "worker.controller<2MuAHeDjLCEd>@tcp://[::1]:1234"
    .parse()
    .unwrap();
```

It returns a strongly typed enum: `Reference::Proc`, `Reference::Actor`, `Reference::Port`.
