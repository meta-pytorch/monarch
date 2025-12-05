# Remote Registry

The `hyperactor::actor::remote` module provides the process-local registry for remote-spawnable actors. It is the counterpart to `RemoteSpawn`: given actor types that implement `RemoteSpawn` and are registered with `remote!`, this module discovers them at runtime and allows actors to be spawned by their global type name. The implementation uses the [`inventory`](https://docs.rs/inventory/0.3.21/inventory/index.html) crate to collect registrations contributed from any crate linked into the application.

## Registration model and the `remote!` macro

Remote-spawnable actors are registered using the `remote!` macro. Given an actor type that implements [`RemoteSpawn`](./remote_spawn.md) and `Named`, `remote!(MyActor)` arranges for a `SpawnableActor` record to be submitted to a global registry using the [`inventory`](https://docs.rs/inventory/0.3.21/inventory/index.html) crate.

In idiomatic use:
```rust
#[derive(Debug)]
#[hyperactor::export(handlers = [()])]
struct MyActor;

impl Actor for MyActor {}

#[async_trait::async_trait]
impl RemoteSpawn for MyActor {
    type Params = bool;

    async fn new(params: bool) -> anyhow::Result<Self> {
        if params {
            Ok(MyActor)
        } else {
            Err(anyhow::anyhow!("some failure"))
        }
    }
}

remote!(MyActor);
```

Conceptually, the `remote!` invocation expands to something like:
```rust
static MY_ACTOR_NAME: std::sync::LazyLock<&'static str> =
    std::sync::LazyLock::new(|| <MyActor as hyperactor::data::Named>::typename());

inventory::submit! {
    hyperactor::actor::remote::SpawnableActor {
        name: &MY_ACTOR_NAME,
        gspawn: <MyActor as hyperactor::actor::RemoteSpawn>::gspawn,
        get_type_id: <MyActor as hyperactor::actor::RemoteSpawn>::get_type_id,
    }
}
```
The real macro uses `paste!` to synthesize the `MY_ACTOR_NAME` identifier and the crate-local paths, but the effect is the same:
- compute a **global type name** for `MyActor` via `Named::typename()`,
- build a `SpawnableActor` record that points at `MyActor`s `RemoteSpawn` implementation, and
- submit that record into the inventory of `SpawnableActor` entries.

At runtime, the `Remote` registry (described below) discovers all such submissions via `inventory::iter::<SpawnableActor>` and makes them available for lookup and spawning by global type name.

## `SpawnableActor`: registration records

A `SpawnableActor` is the type-erased registration record produced by `remote!`. Each remotely spawnable actor type contributes exactly one of these records to the process. The registry discovers them at runtime and uses them to look up actors by global type name and to invoke their type-erased constructor.
```rust
#[derive(Debug)]
pub struct SpawnableActor {
    /// A URI that globally identifies an actor.
    pub name: &'static LazyLock<&'static str>,

    pub gspawn: fn(
        &Proc,
        &str,
        Data,
    ) -> Pin<Box<dyn Future<Output = Result<ActorId, anyhow::Error>> + Send>>,

    pub get_type_id: fn() -> TypeId,
}
```
- `name` is the actor's global type name, obtained from `Named::typename()`. This is the string that appears on the wire in a remote-spawn request.
- `gspawn` is the type-erased entry point for constructing the actor on a remote `Proc`. It is backed by the actor's `RemoteSpawn::gspawn` implementation and handles deserializing parameters and invoking `RemoteSpawn::new(...).await`.
- `get_type_id` returns the actor's `TypeId`, allowing the registry to map a concrete Rust type back to it's registration entry.

Users never construct a `SpawnableActor` manually; these records are generated automatically by the `remote!` macro.

The reason `remote!(MyActor)` works is that it only requires `MyActor: RemoteSpawn`. You can provide that either with an explicit `impl RemoteSpawn for MyActor`, or you get it for free from the blanket `impl<A: Actor + Referable + Binds<Self> + Default> RemoteSpawn for A`. In both cases, `remote!` can safely plug `<MyActor as RemoteSpawn>::gspawn` into the `SpawnableActor` record it generates.

## The `Remote` registry

The `Remote` type is the process-local registry of remote-spawnable actors. It is built from all `SpawnableActor` records submitted via `remote!` and exposed through two lookups: by global type name and by `TypeId`.
```rust
#[derive(Debug)]
pub struct Remote {
    by_name: HashMap<&'static str, &'static SpawnableActor>,
    by_type_id: HashMap<TypeId, &'static SpawnableActor>,
}
```

- `by_type_id` is used by `Remote::name_of::<A>()`, which starts from a concrete type `A: Actor` and looks up its `SpawnableActor` in order to read the registered name.
- `by_name` is used by `Remote::gspawn`, which starts from a global type name string received over the wire and looks up the corresponding `SpawnableActor` in order to call its `gspawn` function.

This is why the registry maintains two maps: one keyed by `TypeId` for caller-side APIs that start from a Rust type, and one keyed by string name for remote services that start from a serialized request.

### Building the registry: `Remote::collect`

```rust
impl Remote {
    pub fn collect() -> Self {
        let mut by_name = HashMap::new();
        let mut by_type_id = HashMap::new();
        for entry in inventory::iter::<SpawnableActor> {
            if by_name.insert(**entry.name, entry).is_some() {
                panic!("actor name {} registered multiple times", **entry.name);
            }
            let type_id = (entry.get_type_id)();
            if by_type_id.insert(type_id, entry).is_some() {
                panic!(
                    "type id {:?} ({}) registered multiple times",
                    type_id, **entry.name
                );
            }
        }
        Self { by_name, by_type_id }
    }
}
```
`Remote::collect` walks `inventory::iter::<SpawnableActor>` and builds two maps:
- `by_name` for lookup up actors by their global type name (the string that appears on the wire), and
- `by_type_id` for looking up the registration associated with a concrete Rust type.

It enforces that no two actors register the same global name or `TypeId` in a single binary.

The result is a process-local view of all remote-spawnable actors; callers are free to construct this registry once and reuse it or to rebuild it on demand, depending on their needs.

### Looking up names: `Remote::name_of`

```rust
impl Remote {
    pub fn name_of<A: Actor>(&self) -> Option<&'static str> {
        self.by_type_id
            .get(&TypeId::of::<A>())
            .map(|entry| **entry.name)
    }
}
```
`name_of` resolves a concrete `A: RemoteSpawn` to its registered global type name string.

Given a concrete `A: Actor`, `name_of` returns the string name that was registered via `remote!`. This is used by caller-side APIs that *start from a Rust type* and need to put a string type name on the wire for a remote spawn request.

For example, `spawn_with_name_inner`  constructs an `ActorSpec` by first resolving the type `A` to its global name:
```rust
impl ProcMeshRef {
  async fn spawn_with_name_inner<A: RemoteSpawn>(
      &self,
      cx: &impl context::Actor,
      name: Name,
      params: &A::Params,
  ) -> v1::Result<ActorMesh<A>>
  {
      let remote = Remote::collect();

      // Caller starts from the Rust type `A` → resolve to a global type name.
      let actor_type = remote
          .name_of::<A>()
          .ok_or(Error::ActorTypeNotRegistered(type_name::<A>().to_string()))?
          .to_string();

      let serialized_params = bincode::serialize(params)?;
      let agent_mesh = self.agent_mesh();

      agent_mesh.cast(
          cx,
          resource::CreateOrUpdate::<mesh_agent::ActorSpec> {
              name: name.clone(),
              rank: Default::default(),
              spec: mesh_agent::ActorSpec {
                  actor_type: actor_type.clone(),      // ← string name sent over the wire
                  params_data: serialized_params.clone(),
              },
          },
      )?;

      ...
  }
```
Here the caller begins with the Rust type `A` and uses `name_of::<A>()` to obtain the global name that will be sent to the remote `Proc`. On the receiving side, the registry takes the global type name string, resolves it to a `SpawnableActor`, and then invokes that entry's `gspawn` function to construct the actor.

### Spawning by name: `Remote::gspawn`
```rust
impl Remote {
    pub async fn gspawn(
        &self,
        proc: &Proc,
        actor_type: &str,
        actor_name: &str,
        params: Data,
    ) -> Result<ActorId, anyhow::Error> {
        let entry = self
            .by_name
            .get(actor_type)
            .ok_or_else(|| anyhow::anyhow!("actor type {} not registered", actor_type))?;
        (entry.gspawn)(proc, actor_name, params).await
    }
}
```
`gspawn` is the **name -> spawn**  path. It starts from a a global type name string (`actor_type`), looks up the corresponding `SpawnableActor` in `by_name`, and invokes its `gspawn` function. That function is the type-erased adapter provided by the actor's `RemoteSpawn` implementation: it deserializes `params` into `RemoteSpawn::Params`, calls `RemoteSpawn::new`, wires the actor into the given `Proc`, and returns the resulting `ActorId`.

In a typical setup, higher-level code in a separate crate starts from a generic `A: RemoteSpawn`, uses `Remote::name_of::<A>()` to obtain the global type name, serializes `A::Params`, and sends a request containing:
- `actor_type`: that global type name, and
- `params_data`: serialized `A::Params`.

On the receiving side, a control-plane or management actor calls:
```rust
self.remote.gspawn(&self.proc, &actor_type, &actor_name, params_data).await
```
to look up the corresponding `SpawnableActor` by `actor_type` and invoke its `gspawn` entry point. That call deserializes `params_data`, constructs the actor, wires it into `self.proc` and returns the new `ActorId`.

## Putting it together

Remote spawning in hyperactor involves two complementary pieces:
1. **Type-level registration** Each `A: RemoteSpawn` contributes a `SpawnableActor` record when the user writes `remote!(A)`. These records are collected at runtime by `Remote::collect()`.
2. **Data-level spawn requests** Higher-level code starts from a concrete actor type (e.g. `A: RemoteSpawn`), uses `Remote::name_of::<A>()` to obtain it's global type name, serializes `A::Params`, and sends a request containing those two pieces of data.

On the receiving side, a management component reconstructs the actor by calling:
```rust
remote.gspawn(&proc, &actor_type, &actor_name, params_data).await
```

`Remote::gspawn` uses the global type name to locate the correct `SpawnableActor` and invokes its type-erased `gspawn` function, which:
- deserializes `params_data`,
- calls `A::new(params).await`, and
- installs the actor into the provided `Proc`.

The `Remote` registry is thus the bridge between:
- **Rust types** implementing `RemoteSpawn` (which define how to construct the actor), and
- **runtime identifiers** (global type names) used in serialized remote-spawn requests.

This decoupling allows remote spawning to work across processes without requiring shared type information at compile time: all that crosses the wire is a global name and a parameter payload, and the receiving process uses its local registry to handle construction.
