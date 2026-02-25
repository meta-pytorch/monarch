# `behavior!`

The `behavior!` macro defines a type which represents an actor that handles a set of messages.
Behaviors allows you to define and hand out **stable or restricted APIs** without tying clients
to the full concrete actor type.

### Defining a behavior

A behavior groups together one or more message enums or structs:

```rust
#[derive(Handler)]
enum ShoppingList {
    Add(String),
    Remove(String),
    Exists(String, #[reply] OncePortRef<bool>),
    List(#[reply] OncePortRef<Vec<String>>),
}

#[derive(Handler)]
struct ClearList {
    reason: String,
}

#[derive(Handler)]
struct GetItemCount<C> {
    category_filter: String,
    #[reply]
    reply: OncePortRef<C>,
}

// Define a behavior `ShoppingApi` that exposes exactly these messages.
hyperactor::behavior!(
    ShoppingApi,
    ShoppingList,
    ClearList,
    GetItemCount<usize>,
);
```

The behavior can include any type of message:
- Enums (e.g. `ShoppingList`)
- Struct messages (e.g. `ClearList`, `GetItemCount<usize>`)
- Generic messages, with concrete type parameters bound at the alias site.

### Using a behavior

After spawning the real actor, re-type its id as the behavior:

```rust
let mut proc = Proc::local();
let shopping_list_actor: ActorHandle<ShoppingListActor> =
    proc.spawn("shopping", ()).await?;
let (client, _) = proc.instance("client").unwrap();

// Re-type the reference as ActorRef<ShoppingApi>.
// We use `attest` here for demonstration, because we know this id
// came from the actor we just spawned.
let shopping_api: ActorRef<ShoppingApi> =
    ActorRef::attest(shopping_list_actor.actor_id().clone());

// Use the curated API (method names come from the Handler derive)
shopping_api.add(&client, "milk".into()).await?;
let found = shopping_api.exists(&client, "milk".into()).await?;
println!("got milk? {found}");

let n = shopping_api.get_item_count(&client, "dairy".into()).await?;
println!("items containing 'dairy': {n}");

shopping_api.clear_list(&client, "end of session".into()).await?;
```

> **Note:** `behavior!` does *not* rename methods. It authorizes those calls on
> `ActorRef<ShoppingApi>` if and only if the message type was included.

> **Note:** `attest` is a low-level escape hatch. It asserts that a raw
> `ActorId` is valid for the behavior. This example uses it only because
> we just spawned the actor and know the id is safe.
> In general, prefer higher-level APIs (such as `Proc` utilities) for
> constructing behavior references, and use `attest` sparingly.

### Generated code (excerpt)

Expanding the example above yields a zero-sized struct with trait impls:

```rust
pub struct ShoppingApi {
    _phantom: std::marker::PhantomData<()>
}

impl hyperactor::actor::Referable for ShoppingApi {}

impl<A> hyperactor::actor::Binds<A> for ShoppingApi
where
    A: Actor
      + Handler<ShoppingList>
      + Handler<ClearList>
      + Handler<GetItemCount<usize>>,
{
    fn bind(ports: &hyperactor::proc::Ports<A>) {
        ports.bind::<ShoppingList>();
        ports.bind::<ClearList>();
        ports.bind::<GetItemCount<usize>>();
    }
}

impl hyperactor::actor::RemoteHandles<ShoppingList> for ShoppingApi {}
impl hyperactor::actor::RemoteHandles<ClearList> for ShoppingApi {}
impl hyperactor::actor::RemoteHandles<GetItemCount<usize>> for ShoppingApi {}
```

### Capability slicing

If a message type is not listed in the behavior, trying to call it will fail at compile time:

```rust
// If ClearList were omitted from the behavior:
shopping_api.clear_list(&client, "...").await?;
// ^ error: the trait bound `ShoppingApi: RemoteHandles<ClearList>` is not satisfied
```

This makes `behavior!` a useful tool for **compile-time capability control**.
