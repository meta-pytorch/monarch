# Syntax

References in Hyperactor follow a uniform concrete syntax that can be written as strings and parsed at runtime.

## String Form

The canonical string syntax supports hierarchical references, from procs down to ports:
```text
location   := via* zmq-url
via        := uid "."
label      := lowercase letter, then lowercase letters, digits, `-`, or `_`,
              ending in a lowercase letter or digit
uid58      := base58(u64) using the Flickr alphabet:
              123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ
uid        := label | "<" uid58 ">" | label "<" uid58 ">"

proc-id    := uid
actor-part := uid
actor-id   := actor-part "." proc-id
port-id    := actor-id ":" decimal-port

proc-ref   := proc-id "@" location
actor-ref  := actor-id "@" location
port-ref   := port-id "@" location
```

Singletons are self-documenting and therefore use the bare `label` form. Instance ids use `<uid58>`, with an optional semantic label outside the brackets: `label<uid58>`. A `Location` can also carry one or more `Via` hops before the terminal ZMQ-style URL, such as `client.host<7PDmJtQJB5S>.tcp://[::1]:1234`.

These forms can be used wherever a reference is accepted as a string, such as
command-line arguments, config files, and logs.

Examples:

- `local@inproc://0` — proc reference for the singleton `local` proc
- `controller<2MuAHeDjLCEd>@tcp://[::1]:1234` — proc reference for a labeled proc instance
- `logger.controller<2MuAHeDjLCEd>@tcp://[::1]:1234` — actor reference
- `controller.some-proc-123<2MuAHeDjLCEd>@tcp://[::1]:1234` — actor with a singleton name in a labeled proc instance
- `<2MuAHeDjLCEd>.<NRjEZGYjYibf>:42@tcp://[::1]:1234` — port 42 on an unlabeled actor instance

The parser is robust and fails clearly on invalid syntax.

## Runtime Parsing

The `Addr` type implements `FromStr`, so you can parse strings into references:

```rust
use hyperactor::Addr;

let r: Addr = "worker.controller<2MuAHeDjLCEd>@tcp://[::1]:1234"
    .parse()
    .unwrap();
```

It returns a strongly typed enum: `Addr::Proc`, `Addr::Actor`, or `Addr::Port`.
