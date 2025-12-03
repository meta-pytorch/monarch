# Referable

```rust
pub trait Referable: Named + Send + Sync {}
```
This is a marker trait indicating that a type is eligible to serve as a reference to a remote actor (i.e., an actor that may reside on a different proc).

It requires:
- `Named`: the type must provide a static name.
- `Send + Sync`: the type must be safely transferable and shareable across threads.
