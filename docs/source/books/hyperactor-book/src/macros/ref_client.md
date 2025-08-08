# `#[derive(RefClient)]`

While `#[derive(HandleClient)]` enables calling the generated client trait on `ActorHandle<T>`, there are cases where you don’t have a handle, only a reference to an actor (`ActorRef<T>`). This is where `#[derive(RefClient)]` comes in.

## What It Adds

`#[derive(RefClient)]` generates the following implementation:
```rust
impl<T> ShoppingListClient for ActorRef<T>
where
  T: ShoppingListHandler + Send + Sync + 'static
```
This allows you to invoke methods like `.add(...)` or `.list(...)` directly on an `ActorRef<T>`.

In other words, `RefClient` connects the generated `ShoppingListClient` interface (from `Handler`) to the `ActorRef<T>` type, which refers to a remote actor.

## Generated Implementation (simplified)

```rust
use async_trait::async_trait;
use hyperactor::{
    ActorRef,
    anyhow::Error,
    cap::{CanSend, CanOpenPort},
    mailbox::open_once_port,
    metrics,
    Message,
};

#[async_trait]
impl<T> ShoppingListClient for ActorRef<T>
where
    T: ShoppingListHandler + Send + Sync + 'static,
{
    async fn add(&self, caps: &impl CanSend, item: String) -> Result<(), Error> {
        self.send(caps, ShoppingList::Add(item)).await
    }

    async fn remove(&self, caps: &impl CanSend, item: String) -> Result<(), Error> {
        self.send(caps, ShoppingList::Remove(item)).await
    }

    async fn exists(
        &self,
        caps: &impl CanSend + CanOpenPort,
        item: String,
    ) -> Result<bool, Error> {
        let (reply_to, recv) = open_once_port(caps)?;
        self.send(caps, ShoppingList::Exists(item, reply_to)).await?;
        Ok(recv.await?)
    }

    async fn list(
        &self,
        caps: &impl CanSend + CanOpenPort,
    ) -> Result<Vec<String>, Error> {
        let (reply_to, recv) = open_once_port(caps)?;
        self.send(caps, ShoppingList::List(reply_to)).await?;
        Ok(recv.await?)
    }
}
```
