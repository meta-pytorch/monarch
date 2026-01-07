# `#[derive(Handler)]`

The `#[derive(Handler)]` macro generates the infrastructure for sending and receiving typed messages in hyperactor. This can be applied to an enum or a struct. Structs can be generic and non-generic.

### For an Enum:
When applied to an enum like this
```rust
#[derive(Handler)]
enum ShoppingList {
    // Fire-and-forget messages
    Add(String),
    Remove(String),

    // Request-response messages
    Exists(String, #[reply] OncePortRef<bool>),
    List(#[reply] OncePortRef<Vec<String>>),
}
```
... it generates **two key things**:

#### 1. `ShoppingListHandler` trait
This trait defines a method for each variant, and a `handle` method to route incoming messages:
```rust
use async_trait::async_trait;
use hyperactor::anyhow::Error;

#[async_trait]
pub trait ShoppingListHandler: hyperactor::Actor + Send + Sync {
    async fn add(&mut self, ctx: &Context<Self>, item: String) -> Result<(), Error>;
    async fn remove(&mut self, ctx: &Context<Self>, item: String) -> Result<(), Error>;
    async fn exists(&mut self, ctx: &Context<Self>, item: String) -> Result<bool, Error>;
    async fn list(&mut self, ctx: &Context<Self>) -> Result<Vec<String>, Error>;

    async fn handle(&mut self, ctx: &Context<Self>, msg: ShoppingList) -> Result<(), Error> {
        match msg {
            ShoppingList::Add(item) => {
                self.add(ctx, item).await
            }
            ShoppingList::Remove(item) => {
                self.remove(ctx, item).await
            }
            ShoppingList::Exists(item, reply_to) => {
                let result = self.exists(ctx, item).await?;
                reply_to.send(ctx, result)?;
                Ok(())
            }
            ShoppingList::List(reply_to) => {
                let result = self.list(ctx).await?;
                reply_to.send(ctx, result)?;
                Ok(())
            }
        }
    }
}
```
Note:
  - `Add` and `Remove` are **oneway**: no reply port
  - `Exists` and `List` are **call-style**: they take a `#[reply] OncePortRef<T>` and expect a response to be sent back.

#### 2. `ShoppingListClient` trait

Alongside the handler, the `#[derive(Handler)]` macro also generates a client-side trait named `ShoppingListClient`. This trait provides a convenient and type-safe interface for sending messages to an actor.

Each method in the trait corresponds to a variant of the message enum. For example:
```rust
use async_trait::async_trait;
use hyperactor::anyhow::Error;
use hyperactor::cap::{CanSend, CanOpenPort};

#[async_trait]
pub trait ShoppingListClient: Send + Sync {
    async fn add(&self, caps: &impl CanSend, item: String) -> Result<(), Error>;
    async fn remove(&self, caps: &impl CanSend, item: String) -> Result<(), Error>;
    async fn exists(&self, caps: &impl CanSend + CanOpenPort, item: String) -> Result<bool, Error>;
    async fn list(&self, caps: &impl CanSend + CanOpenPort) -> Result<Vec<String>, Error>;
}
```

### For a struct
Supports both generic and non-generic structs. Generic structs must implement the trait bounds `Serialize`, `Deserialize`, `Send`, `Sync`, `Debug`, and `Named`. This is automatically enforced by the macro.
When applied to a struct like this
```rust
#[derive(Handler)]
struct GetItemCount<C> {
    category_filter: String,
    #[reply]
    reply: OncePortRef<C>,
}
```

... it generates:

#### 1. `GetItemCountHandler` trait
```rust
use async_trait::async_trait;
use hyperactor::anyhow::Error;

#[async_trait]
pub trait GetItemCountHandler<
    C: serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + Send
        + Sync
        + std::fmt::Debug
        + typeuri::Named,
>: hyperactor::Actor + Send + Sync  {
    async fn get_item_count(
        &mut self,
        _cx: &Context<Self>,
        category_filter: String,
    ) -> Result<(), anyhow::Error>;

    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        msg: GetItemCount,
    ) -> Result<(), anyhow::Error> {
        match msg {
            GetItemCount { category_filter, reply_to } => {
                let result = self.get_item_count(_cx, category_filter).await?;
                reply_to.send(_cx, result)?;
                Ok(())
            }
        }
    }
}
```

#### 2. `GetItemCountClient` trait
```rust
#[async_trait]
pub trait GetItemCountClient: Send + Sync {
    async fn get_item_count(
        &mut self,
        _cx: &Context<Self>,
        category_filter: String,
    ) -> Result<(), anyhow::Error>;
}
```


#### Capability Parameter
Each method takes a caps argument that provides the runtime capabilities required to send the message:
- All methods require `CanSend`.
- Methods with `#[reply]` arguments additionally require `CanOpenPort`.

In typical usage, `caps` is a `Mailbox`.

#### Example Usage
```rust
let mut proc = Proc::local();
let actor = proc.spawn::<ShoppingListActor>("shopping", ()).await?;
let client = proc.attach("client").unwrap();

// Fire-and-forget
actor.add(&client, "milk".into()).await?;

// With reply
let found = actor.exists(&client, "milk".into()).await?;
println!("got milk? {found}");
```
Here, actor is an `ActorHandle<ShoppingListActor>` that implements `ShoppingListClient`, and `client` is a `Mailbox` that provides the necessary capabilities.

#### `#[reply]`
`#[reply]` can take any of the four types:
- `OncePortRef<T>`
- `OncePortHandle<T>`
- `PortRef<T>`
- `PortHandle<T>`

Note that `OncePortRef<T>` and `OncePortHandle<T>` support one-shot communication, compared to `PortRef<T>` and `PortHandle<T>` which can be used for multiple requests and responses.
