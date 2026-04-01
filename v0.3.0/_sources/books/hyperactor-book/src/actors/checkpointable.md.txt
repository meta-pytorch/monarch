# Checkpointable

The `Checkpointable` trait enables an actor to define how its internal state can be saved and restored. This allows actors to participate in checkpointing and recovery mechanisms when supported by the surrounding system.

## Trait definition
```rust
#[async_trait]
pub trait Checkpointable: Send + Sync + Sized {
    type State: RemoteMessage;

    async fn save(&self) -> Result<Self::State, CheckpointError>;
    async fn load(state: Self::State) -> Result<Self, CheckpointError>;
}
```

## Associated Type

- `type State`: A serializable type representing the object's saved state. This must implement `RemoteMessage` so it can serialized and transmitted.

## `save`

Persists the current state of the component. Returns the Returns a `Self::State` value. If the operation fails, returns `CheckpointError::Save`.

## `load`

Reconstructs a new instance from a previously saved `Self::State`. If deserialization or reconstruction fails, returns `CheckpointError::Load`.

## `CheckpointError`

Errors returned by save and load operations:
```rust
pub enum CheckpointError {
    Save(anyhow::Error),
    Load(SeqId, anyhow::Error),
}
```

## Blanket Implementation

Any type `T` that implements `RemoteMessage` and `Clone` automatically satisfies `Checkpointable`:
```rust
#[async_trait]
impl<T> Checkpointable for T
where
    T: RemoteMessage + Clone,
{
    type State = T;

    async fn save(&self) -> Result<Self::State, CheckpointError> {
        Ok(self.clone())
    }

    async fn load(state: Self::State) -> Result<Self, CheckpointError> {
        Ok(state)
    }
}
```
This implementation uses `clone()` to produce a checkpoint and simply returns the cloned state in load.
