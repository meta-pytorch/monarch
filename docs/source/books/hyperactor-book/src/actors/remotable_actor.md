# The `RemoteableActor` Trait

```rust
pub trait RemotableActor: Actor
where
    Self::Params: RemoteMessage,
{
    fn gspawn(
        proc: &Proc,
        name: &str,
        serialized_params: Data,
    ) -> Pin<Box<dyn Future<Output = Result<ActorId, anyhow::Error>> + Send>>;

    fn get_type_id() -> TypeId {
        TypeId::of::<Self>()
    }
}
```
The `RemotableActor` trait marks an actor type as spawnable across process boundaries. It enables hyperactor's remote spawning and registration system, allowing actors to be created from serialized parameters in a different `Proc`.

## Requirements
- The actor type must also implement `Actor`.
- Its `Params` type (used in `Actor::new`) must implement `RemoteMessage`, so it can be serialized and transmitted over the network.

## `gspawn`
```rust
fn gspawn(
    proc: &Proc,
    name: &str,
    serialized_params: Data,
) -> Pin<Box<dyn Future<Output = Result<ActorId, anyhow::Error>> + Send>>;
```
This is the core entry point for remote actor spawning. It takes:
- a target `Proc` where the actor should be created,
- a string name to assign to the actor,
- and a `Data` payload representing serialized parameters.

The method deserializes the parameters, creates the actor, and returns its `ActorId`.

This is used internally by hyperactor's remote actor registry and `spawn` services. Ordinary users generally don't call this directly.

> **Note:** This is not an `async fn` because `RemotableActor` must be object-safe.

## `get_type_id`

Returns a stable `TypeId` for the actor type. Used to identify actor types at runtime—e.g., in registration tables or type-based routing logic.

## Blanket Implementation

The RemotableActor trait is automatically implemented for any actor type `A` that:
- implements `Actor` and `RemoteActor`,
- and whose `Params` type implements `RemoteMessage`.

This allows `A` to be remotely registered and instantiated from serialized data, typically via the runtime's registration mechanism.

```rust
impl<A> RemotableActor for A
where
    A: RemoteActor,
    A: Binds<A>,
    A::Params: RemoteMessage,
{
    fn gspawn(
        proc: &Proc,
        name: &str,
        serialized_params: Data,
    ) -> Pin<Box<dyn Future<Output = Result<ActorId, anyhow::Error>> + Send>> {
        let proc = proc.clone();
        let name = name.to_string();
        Box::pin(async move {
            let handle = proc
                .spawn::<A>(&name, bincode::deserialize(&serialized_params)?)
                .await?;
            Ok(handle.bind::<A>().actor_id)
        })
    }
}
```
Note the `Binds<A>` bound: this trait specifies how an actor's ports are wired determining which message types the actor can receive remotely. The resulting `ActorId` corresponds to a port-bound, remotely callable version of the actor.
