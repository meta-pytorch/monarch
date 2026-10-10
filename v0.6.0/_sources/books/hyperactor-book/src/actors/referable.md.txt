# Referable

```rust
pub trait Referable: Named {}
```
This is a marker trait indicating that a type is eligible to serve as a reference to a remote actor (i.e., an actor that may reside on a different proc).

It requires:
- `Named`: the type must provide a static name.

Note that `Referable` itself does not impose `Send` or `Sync` bounds. Specific contexts that need these bounds (such as spawning across threads or storing in shared data structures) will add them explicitly at their call sites.
