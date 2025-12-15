# `#[export]`

The `#[hyperactor::export]` macro turns a regular `Actor` implementation into a remotely spawnable actor, registering its type information, `spawn` function, and supported message handlers for discovery and use across processes or runtimes.

## What It Adds

When applied to an actor type like this:

```rust
#[hyperactor::export(
    spawn = true,
    handlers = [ShoppingList],
)]
struct ShoppingListActor(HashSet<String>);
```
The macro expands to include:
 - A `Named` implementation for the actor
 - A `Binds<Self>` implementation that registers supported message types
 - Implementations of `RemoteHandles<T>` for each type in the `handlers = [...]` list
 - A `Referable` marker implementation
 - If `spawn = true`, the actor's `RemoteSpawn` implementation is registered in the remote actor inventory.

This enables the actor to be:
 - Spawned dynamically by name
 - Routed to via typed messages
 - Reflected on at runtime (for diagnostics, tools, and orchestration)

## Generated Implementations (simplified)
```rust
impl Referable for ShoppingListActor {}

impl RemoteHandles<ShoppingList> for ShoppingListActor {}
impl RemoteHandles<Signal> for ShoppingListActor {}

impl Binds<ShoppingListActor> for ShoppingListActor {
    fn bind(ports: &Ports<Self>) {
        ports.bind::<ShoppingList>();
    }
}

impl Named for ShoppingListActor {
    fn typename() -> &'static str {
        "my_crate::ShoppingListActor"
    }
}
```

> **Note:** The `Referable` trait itself only requires `Named`. It does not automatically provide `Send` or `Sync` bounds. If your actor needs to be passed across threads or stored in shared contexts, those bounds will be enforced at the specific call sites that require them.

If `spawn = true`, the macro also emits:
```rust
impl RemoteSpawn for ShoppingListActor {}
```
This enables remote spawning via the default `gspawn` provided by a blanket implementation.

It also registers the actor into inventory:
```
inventory::submit!(SpawnableActor {
    name: ...,
    gspawn: ...,
    get_type_id: ...,
});
```
This allows the actor to be discovered and spawned by name at runtime.

## Summary

The `#[export]` macro makes an actor remotely visible, spawnable, and routable by declaring:
 - What messages it handles
 - What messages it handles
 - How to bind those messages
 - What its globally unique name is
 - (Optionally) how to spawn it dynamically
