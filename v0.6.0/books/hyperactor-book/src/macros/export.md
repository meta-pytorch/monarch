# `#[export]`

The `#[hyperactor::export]` macro turns a regular `Actor` implementation into a remotely addressable actor by generating its type information and supported message handlers.

It also supports message casting (broadcasting to multiple actors) for exported remote message handlers.

## What It Adds

When applied to an actor type like this:

```
#[hyperactor::spawnable]
#[hyperactor::export(ShoppingList)]
struct ShoppingListActor(HashSet<String>);
```

The macro expands to include:

- A `Named` implementation for the actor
- A `Binds<Self>` implementation that registers supported message types
- Implementations of `RemoteHandles<T>` for each type in the `handlers = [...]` list
- A `Referable` marker implementation

This enables the actor to be:

- Routed to via typed messages
- Reflected on at runtime (for diagnostics, tools, and orchestration)

To make a concrete actor remotely spawnable, add `#[hyperactor::spawnable]`. For generic instantiations, use `hyperactor::register_spawnable!(MyActor<u64>);`.

## Casting Messages

Message types that need to be broadcast to multiple actors in a mesh must be exported remote message handlers:

```
#[hyperactor::export(
 handlers = [
 TestMessage,
 (),
 MyGeneric<()>,
 u64,
 ],
)]
struct TestActor {
 forward_port: PortRef<String>,
}
```

Cast routes send the message as multipart data to the destination's ordinary typed handler port, with reply-port mutation handled by visiting typed multipart part representations while the message is in transit.

## Generated Implementations (simplified)

```
impl Referable for ShoppingListActor {}

impl RemoteHandles<ShoppingList> for ShoppingListActor {}
impl RemoteHandles<Signal> for ShoppingListActor {}

impl Binds<ShoppingListActor> for ShoppingListActor {
 fn bind(ports: &HandlerPorts<Self>) {
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

If the actor is marked `#[hyperactor::spawnable]`, that attribute emits:

```
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

The `#[export]` macro makes an actor remotely visible and routable by declaring:

- What messages it handles
- How to bind those messages
- What its globally unique name is
- (Optionally) which messages support multicast (broadcasting)