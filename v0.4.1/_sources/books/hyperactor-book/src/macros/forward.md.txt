# `#[handle]`

The `#[hyperactor::handle]` macro connects a user-defined handler trait implementation (like `ShoppingListHandler`) to the core `Handler<T>` trait required by the runtime.

In short, it generates the boilerplate needed to route incoming messages of type `T` to your high-level trait implementation.

## What it generates

The macro expands to:
```rust
#[async_trait]
impl Handler<ShoppingList> for ShoppingListActor {
    async fn handle(&mut self, ctx: &Context<Self>, message: ShoppingList) -> Result<(), Error> {
        <Self as ShoppingListHandler>::handle(self, ctx, message).await
    }
}
```
This avoids having to manually match on enum variants or duplicate message logic.

## When to use it

Use `#[handle(MessageType)]` when:

- You've defined a custom trait (e.g., `ShoppingListHandler`)
- You're handling a message enum (like `ShoppingList`)
- You want the runtime to route messages to your trait automatically.

This is most often used alongside `#[derive(Handler)]`, which generates the corresponding handler and client traits for a user-defined message enum.
