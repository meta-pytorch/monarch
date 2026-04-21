# Syntax

References in Hyperactor follow a uniform concrete syntax that can be written as strings and parsed at runtime.

## String Form

The canonical string syntax supports hierarchical references, from procs down to ports:
```text
addr,proc_name
addr,proc_name,actor_name
addr,proc_name,actor_name[pid]
addr,proc_name,actor_name[pid][port]
```

These forms can be used wherever a reference is accepted as a string, such as command-line arguments, config files, and logs.

Examples:

- `tcp:[::1]:1234,myproc` — proc reference
- `tcp:[::1]:1234,myproc,logger[1]` — actor named `logger`, pid 1
- `tcp:[::1]:1234,myproc,logger[1][42]` — port 42 of that actor

The parser is robust and fails clearly on invalid syntax.

## Runtime Parsing

The `Reference` type implements `FromStr`, so you can parse strings into references:

```rust
use hyperactor::reference::Reference;

let r: Reference = "tcp:[::1]:1234,myproc,worker[0]".parse().unwrap();
```

It returns a strongly typed enum: `Reference::Proc`, `Reference::Actor`, `Reference::Port`.
