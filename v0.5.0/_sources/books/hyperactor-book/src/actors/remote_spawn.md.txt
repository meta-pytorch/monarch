# The `RemoteSpawn` Trait

```rust
pub trait RemoteSpawn: Actor + Referable + Binds<Self> {
    /// The type of parameters used to instantiate the actor remotely.
    type Params: RemoteMessage;

    /// Creates a new actor instance given its instantiation parameters.
    async fn new(params: Self::Params) -> anyhow::Result<Self>;

    fn gspawn(
        proc: &Proc,
        name: &str,
        serialized_params: Data,
    ) -> Pin<Box<dyn Future<Output = Result<ActorId, anyhow::Error>> + Send>> { /* default impl. */}

    fn get_type_id() -> TypeId {
        TypeId::of::<Self>()
    }
}
```
The `RemoteSpawn` trait marks an actor type as spawnable across process boundaries. It enables hyperactor's remote spawning and registration system, allowing actors to be created from serialized parameters in a different `Proc`.

## Requirements
- The actor type must also implement `Actor`.
- Its `Params` type (used in `RemoteSpawn::new`) must implement `RemoteMessage`, so it can be serialized and transmitted over the network.
- `new` creates a new instance of the actor given its parameters

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

> **Note:** This is not an `async fn` because `RemoteSpawn` must be object-safe.

## `get_type_id`

Returns a stable `TypeId` for the actor type. Used to identify actor types at runtime—e.g., in registration tables or type-based routing logic.
