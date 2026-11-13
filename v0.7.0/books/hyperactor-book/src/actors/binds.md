# Binds

The `Binds` trait defines how an actor's handler ports are associated with the message types it can receive remotely.

```
pub trait Binds<A: Actor>: Referable {
 fn bind(ports: &HandlerPorts<A>);
}
```

Implementing `Binds<A>` allows the system to determine which messages can be routed to an actor instance of type `A`.

## Code Generation

In most cases, you do not implement this trait manually. Instead, the `#[export]` macro generates the appropriate `Binds<A>` implementation by registering the actor's supported message types.

For example:

```
#[hyperactor::spawnable]
#[hyperactor::export(ShoppingList)]
struct ShoppingListActor;
```

Expands to:

```
impl Binds<ShoppingListActor> for ShoppingListActor {
 fn bind(ports: &HandlerPorts<Self>) {
 ports.bind::<ShoppingList>();
 }
}
```

This ensures that the actor is correctly wired to handle messages of type `ShoppingList` when used in a remote messaging context.