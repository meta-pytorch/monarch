# Proc meshes & ProcMeshAgent

## What the `ProcMeshAgent` Is

Every proc in a mesh runs a `ProcMeshAgent`. It plays the same role on the proc side that the `HostMeshAgent` plays on the host side: it implements the control-plane interface for "managing this proc as part of a mesh".

The agent has several responsibilities, all of which will be documented on this page:
- wiring the proc into the mesh router,
- handling resource-style requests (`CreateOrUpdate`, `Stop`, `GetState`, `GetRankStatus`, ...),
- forwarding or recording supervision events,
- tracking the lifecycle of actors created on the proc,
- and supporting both the legacy v0 and the current v1 spawn APIs.

This chapter begins with the **v1 "resource-style" spawn path**:: how a request of the form
```rust
CreateOrUpdate<ActorSpec>
```
results in an actual actor being constructed inside the proc using the `Remote` registry.

To anchor that discussion, here is the essential shape of the agent:
```rust
pub struct ProcMeshAgent {
    proc: Proc, // local actor runtime
    remote: Remote,  // registry of SpawnableActor entries (built from RemoteSpawn + remote!(...))
    state: State, // v0/v1 bootstrapping mode
    actor_states: HashMap<Name, ActorInstanceState>, // per-actor spawn results & metadata
    record_supervision_events: bool,
    supervision_events: HashMap<ActorId, Vec<ActorSupervisionEvent>>,
}
```
- **`proc: Proc`** The proc-local runtime into which new actors will be installed
- **`remote: Remote`** A snapshot of the process-local registry of `SpawnableActor` entries (populated from `RemoteSpawn` impls via `remote!(A)`). This is the bridge between *global type names* and the actual constructors used by `Remote::gspawn`.
- **`actor_states`** The agent's bookkeeping: for each actor name in the mesh, what happened when this proc tried to spawn it.

The sections that follow walk the spawn flow end-to-end. Additional responsibilities (status, supervision, teardown) will be documented after the spawn discussion.

## The V1 Spawn Flow

At a high level, the v1 path for creating an actor on every proc looks like this:
```text
ProcMeshRef ──(CreateOrUpdate<ActorSpec>)──▶ ProcMeshAgent mesh
                      ProcMeshAgent ──(Remote::gspawn)──▶ Proc / Remote registry
```
("`ProcMeshRef` turns `spawn::<A>` into a broadcast `CreateOrUpdate<ActorSpec>` to the `ProcMeshAgent` mesh; each `ProcMeshAgent` then calls `Remote::gspawn` into its local `Proc` using the `Remote` registry.")

From the caller's point of view it starts as:
```rust
proc_mesh.spawn::<A>(cx, "name", &params).await
```
which is just a thin wrapper over:
```rust
proc_mesh
  .spawn_with_name::<A>(cx, Name::new("name"), &params)
  .await
```
The rest of this section unpacks what that call actually does.

### From `spawn` to `ActorSpec`

The real work happens in `spawn_with_name_inner`:
```rust
impl ProcMeshRef {
  async fn spawn_with_name_inner<A: Actor + Referable>(
      &self,
      cx: &impl context::Actor,
      name: Name,
      params: &A::Params,
  ) -> v1::Result<ActorMesh<A>>
  where
      A::Params: RemoteMessage,
  {
      let remote = Remote::collect();
        // `RemoteSpawn` + `remote!(A)` ensure that `A` has a
        // `SpawnableActor` entry in this registry, so
        // `name_of::<A>()` can resolve its global type name.
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
                  actor_type: actor_type.clone(),
                  params_data: serialized_params.clone(),
              },
          },
      )?;

      // ... wait on GetRankStatus and build ActorMesh<A> ...
  }
}
```
What this does, step by step:
1. **Resolve the Rust type `A` to a global type name**
   ```rust
   let remote = Remote::collect();
   let actor_type = remote
       .name_of::<A>()
       .ok_or(Error::ActorTypeNotRegistered(type_name::<A>().to_string()))?
       .to_string();
   ```
   This is the point where the *type-level* contract kicks in:
   - elsewhere, the user has written `remote!(MyActor)` for each `A: RemoteSpawn`,
   - that registration adds a `SpawnableActor` entry to the `Remote` registry,
   - `Remote::name_of::<A>()` looks up that entry and reads its **global type name**.

   If `A` was never registered with `remote!(A)`, this call fails with `ActorTypeNotRegistered`, and the spawn never leaves the caller's process.

2. **Serialize the spawn parameters**
   ```rust
   let serialized_params = bincode::serialize(params)?;
   ```
   Spawn parameters travel as opaque bytes. The API only enforces that `A::Params: RemoteMessage`, meaning the caller’s side can serialize them. On the remote side there is no trait bound — the generated `RemoteSpawn::gspawn` simply attempts to deserialize the incoming byte payload into `A::Params` and will return an error if it cannot.

3. **Broadcast a resource-style `CreateOrUpdate<ActorSpec>`**
   ```rust
   let agent_mesh = self.agent_mesh();

   agent_mesh.cast(
       cx,
       resource::CreateOrUpdate::<mesh_agent::ActorSpec> {
           name: name.clone(),
           rank: Default::default(),
           spec: mesh_agent::ActorSpec {
               actor_type: actor_type.clone(),
               params_data: serialized_params.clone(),
           },
       },
   )?;
   ```
   This is where the proc mesh turns a local method call into a **distributed control-plane request**:
   - `agent_mesh` is an `ActorMeshRef<ProcMeshAgent>` – one `ProcMeshAgent` per proc,
   - `cast` sends the same `CreateOrUpdate<ActorSpec>` to **every** `ProcMeshAgent`,
   - the `name` field is the *mesh-level* actor name ("this actor, on this mesh"),
   - `actor_type` is the *global* type name resolved via `Remote`,
   - `params_data` is the serialized `A::Params`.

At this point the proc mesh has done its part: it has told every proc in the mesh: "For mesh actor name, please ensure you have one local actor of type `actor_type`, constructed from `params_data`."

### How `ProcMeshAgent` handles `CreateOrUpdate<ActorSpec>`

Once the `ProcMeshRef` has broadcast a `CreateOrUpdate<ActorSpec>` to every proc, each proc's `ProcMeshAgent` receives that message and attempts to construct the actor locally.

The entry point is:
```rust
#[async_trait]
impl Handler<resource::CreateOrUpdate<ActorSpec>> for ProcMeshAgent {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        create_or_update: resource::CreateOrUpdate<ActorSpec>,
    ) -> anyhow::Result<()> {
         ...
    }
}
```

This handler performs four steps:

---

1. Idempotence: only the first `CreateOrUpdate` matters

```rust
if self.actor_states.contains_key(&create_or_update.name) {
    // There is no update.
    return Ok(());
}
```

The `CreateOrUpdate` resource verb supports "update" in principle, but actor meshes never update an existing actor by name. They only create a fresh actor mesh.

So the agent simply ignores subsequent requests for the same name.

---

2. Safety check: reject spawn if the proc has supervision errors

```rust
if !self.supervision_events.is_empty() {
    self.actor_states.insert(
        create_or_update.name.clone(),
        ActorInstanceState {
            spawn: Err(anyhow::anyhow!(
                "Cannot spawn new actors on mesh with supervision events"
            )),
            create_rank,
            stopped: false,
        },
    );
    return Ok(());
}
```

If this proc previously recorded **any** supervision events for **any** actor, the proc is considered "poisoned": it may be in a bad state, and spawning new actors would be unsafe.

The agent records the failure in `actor_states` and stops.

Later, when the `ProcMesh::spawn_with_name_inner` calls `GetRankStatus::wait` to aggregate per-rank results, this proc will contribute a `Failed` status for that actor name instead of ever reporting it as `Running`.

---

3. Unpack `ActorSpec` and call `remote.gspawn`

```rust
let ActorSpec {
    actor_type,
    params_data,
} = create_or_update.spec;

self.actor_states.insert(
    create_or_update.name.clone(),
    ActorInstanceState {
        create_rank,
        spawn: self
            .remote
            .gspawn(
                &self.proc,
                &actor_type,
                &create_or_update.name.to_string(),
                params_data,
            )
            .await,
        stopped: false,
    },
);
```
This is the core of v1 spawning. The agent:
- unpacks the `ActorSpec` (type name + parameter bytes), and
- passes those pieces into `remote.gspawn(...)` to construct the local actor.
- `actor_type: String` – the logical type name registered by `remote!(A)`, computed in `ProcMeshRef::spawn_with_name_inner` via `remote.name_of::<A>()`, and used by `Remote::gspawn` on each proc to find the right constructor.
- `params_data: Data` A raw byte buffer containing serialized `A::Params` (via `bincode::serialize`).
- `self.remote.gspawn(...)` This method looks up the `SpawnableActor` entry for `actor_type` in the local `Remote` registry then invoks:
```rust
SpawnableActor::spawn(proc, name, params_data)
```
Internally this calls the actor's `RemoteSpawn::new(params).await` construtor registers it under the given name in the proc's runtime, and returns an `ActorId`.

The result -- success or failure -- is recorded in:
```rust
ActorInstanceState {
    create_rank,     // this proc's rank in the mesh
    spawn: Result<ActorId, anyhow::Error>,
    stopped: false,
}
```
The `actor_states` map is later queried by `GetRankStatus` and `GetState`.

---

4. Return success locally (no direct reply)

Once the agent has updated `actor_states`, the handler simply returns:
```rust
Ok(())
```
There is no **direct reply** back to the caller for `CreateOrUpdate<ActorSpec>`.

From the agent's point of view, the work for the message is:
- decide whether to attempt a spawn (idempotence + supervision gate),
- call `remote.gspawn(...)` into the local `Proc`,
- record the outcome in `actor_states[name]` as `ActorInstanceState`.

That's it. The handler does **not** try to decide whether the *mesh-level* spawn "succeeded" or "failed" - it just persists the per-proc result.

Those per-proc results are later *read* by the resource query handlers (`Handler<GetRankStatus>`, `Handler<GetState<ActorState>>` on `ProcMeshAgent`).

## Completing the Spawn: How `GetRankStatus` Decides Success

Once every `ProcMeshAgent` has received the `CreateOrUpdate<ActorSpec>` message and updated its local `actor_states`, the caller still does not know:
- **Did every proc spawn the actor successfully?**
- **Did any proc report a supervision failure?**
- **Are all actors running, or did one terminate immediately?**

To answer these questions, the `ProcMeshRef` performs a *second* distributed query using the resource verb:
```rust
resource::GetRankStatus{ name, reply }
```
This message is broadcast to the same `ProcMeshAgent` mesh. Each agent replies with a small "overlay" describing *its* result for that actor name:
- no entry yet -> `NotExist`
- spawn failed -> `Failed(error)`
- spawned and running -> `Running`
- terminated -> `Stopped`/`Failed`,
- supervision events present -> `Failed`.

The reply port used to collect all `GetRankStatus` responses is opened via:
```rust
let (port, rx) = cx.mailbox().open_accum_port_opts(
    StatusMesh::from_single(region.clone(), Status::NotExist),
    Some(ReducerOpts { max_update_interval: Some(Duration::from_millis(50)) }),
);
```
Here, `cx` is the callers context. In tests this is typically `testing::instance()`, a tiny driver actor (`Instance<>()`), so the accumulation port (`port`/`rx`)-and thus all collected replies-live in that test instance's mailbox.

An accumulation port is just a mailbox port that keeps a running aggregate value. Each `GetRankStatus` reply is an overlay, and the mailbox's reducer merges those overlays into a single `StatusMesh`, with one final status per proc/rank.
