#  §3 HostMesh from an allocation (what `HostMesh::allocate(...)` actually does)

At this point we already have:

- a control-side proc + instance (from §1),
- a running allocation from the v0 `ProcessAllocator` (from §2),
- and we've seen how the child executable comes up in `Bootstrap::V0ProcMesh` and answers `Allocator2Process::StartProc(...)`.

The next line in the example was:

```rust
// from hyperactor_mesh/src/bootstrap.rs (`fn tests::bootstrap_canonical_simple`)
let host_mesh = HostMesh::allocate(&instance, Box::new(alloc), "test", None)
    .await
    .unwrap();
```

This is the step that turns "I have OS processes that can run a proc" into "I have real hosts I can talk to."

### 3.1 Inputs

The function is:

```rust
// from hyperactor_mesh/src/v1/host_mesh.rs
pub async fn allocate(
    cx: &impl context::Actor,
    alloc: Box<dyn Alloc + Send + Sync>,
    name: &str,
    bootstrap_params: Option<BootstrapCommand>,
) -> v1::Result<Self>
```

- `cx` is our local actor context — in the example this is the `instance` we made on the root proc.
- `alloc` is the allocation we just created with the process allocator.
- `name` is just a label for the mesh (`"test"` in the example).
- `bootstrap_params` can be forwarded to the hosts, but in the test it's `None`.

So this function **consumes** an allocation; it doesn't spawn OS processes by itself.

### 3.2 First: turn the allocation into a proc-mesh

The first real work `HostMesh::allocate(...)` does is to **consume** the `Alloc` you handed it and turn it into a `ProcMesh`:

```rust
// from hyperactor_mesh/src/v1/host_mesh.rs (`fn HostMesh::allocate`)
let transport = alloc.transport();
let extent = alloc.extent().clone();
let is_local = alloc.is_local();
let proc_mesh = ProcMesh::allocate(cx, alloc, name).await?;
```

That line hides the allocator-driving loop. Here is what's happening conceptually.

1. **`ProcMesh::allocate(...)` takes ownership of the `Alloc`.**
   At this point your `ProcessAlloc` (from §2) already has:
   - the per-allocation bootstrap address,
   - the extent (e.g. 1-D, replicas = 1),
   - the command template to spawn the child,
   - and a place to receive `Process2Allocator(...)` messages from children.

   But nothing is *running* yet — no child has been fully bootstrapped.

2. **It starts pulling events from the allocation.**
   A simplified view of what it does is:

   ```text
   // conceptual shape of what v1::ProcMesh::allocate(...) does
   loop {
       match alloc.next().await {
           Some(ProcState::Created { ... }) => { /* child OS process exists */ }
           Some(ProcState::Running { proc_id, mesh_agent, addr, .. }) => {
               // child ran its bootstrap_v0_proc_mesh(),
               // allocator sent StartProc(...),
               // child answered StartedProc(...)
               // -> we now know how to dial the proc
           }
           Some(ProcState::Stopped { .. }) => { /* error or teardown */ }
           None => break,
       }
   }
   ```

   The important part is the `ProcState::Running { .. }` arm — that is the one that corresponds exactly to the child sending

   ```text
   Process2Allocator(idx, Process2AllocatorMessage::StartedProc(proc_id, agent, proc_addr))
   ```

   in `bootstrap_v0_proc_mesh()`.

3. **It waits until it has "one running proc per rank."**
   Because the allocation knows its extent, `ProcMesh::allocate(...)` also knows how many "Running" events to wait for. In our example the extent is

   ```rust
   extent!(replicas = 1)
   ```

   so it only needs to see **one** such event. If the extent were bigger, it would keep pulling `alloc.next().await` until it had them all.

4. **It records exactly what the child told the allocator.**
   Each `ProcState::Running { ... }` contains:
   - the final `ProcId` (chosen by the parent, see the note earlier),
   - the channel address where the proc is serving,
   - the bound mesh-agent actor-ref.

   These are the three things we need later to turn the proc into a host.

5. **It packages that into a `ProcMesh`.**
   Once it has all ranks, it returns a `ProcMesh` that says, in effect:

   > "For rank 0, here is the direct-addressed proc, here is its agent, and here is where to dial it."

   That is what `HostMesh::allocate(...)` immediately uses in the next step (the trampoline spawn).

So when we say:

> "`HostMesh::allocate(...)` builds on top of `ProcMesh::allocate(...)`"

what we mean concretely is:

- **HostMesh** does *not* talk to the allocator protocol directly.
- It delegates that part to **ProcMesh**, which already knows how to drive an `Alloc` until every child has done:
  1. child: `Hello(...)`
  2. parent: `StartProc(...)`
  3. child: `StartedProc(...)`
- Then HostMesh turns *those* procs into actual hosts.

### 3.3 Then: tell each proc to become a host (the trampoline step)

Right after turning the allocation into a `ProcMesh`, the code does:

```rust
// from hyperactor_mesh/src/v1/host_mesh.rs `HostMesh::allocate()`
let (mesh_agents, mut mesh_agents_rx) = cx.mailbox().open_port();
let _trampoline_actor_mesh = proc_mesh
    .spawn::<HostMeshAgentProcMeshTrampoline>(
        cx,
        "host_mesh_trampoline",
        &(transport, mesh_agents.bind(), bootstrap_params, is_local),
    )
    .await?;
```

This is the key hop: we now have **one remote proc per rank**, but we want **one host per remote proc**. Rather than building the host from the parent, we send a tiny actor *to* each remote proc that will build the host *there*.

The long doc comment above `HostMesh::allocate()` in `hyperactor_mesh/src/v1/host_mesh.rs` explains the pattern. The short version:

1. we open a port locally (`open_port()`) so that the remote side can send back "I'm your host, here is my agent";
2. we ask **every proc in the proc-mesh** to spawn `HostMeshAgentProcMeshTrampoline`;
3. that trampoline (running *in the remote proc*) does:
   - `Host::serve(...)` in that process, using a `BootstrapProcManager`, so this host can later spawn procs as new OS children,
   - `host.system_proc().spawn::<HostMeshAgent>(...)` to put a `HostMeshAgent` on the host's service proc,
   - and finally `send(mesh_agents_port, that_host_agent_ref)` back to us.

The ASCII diagram from the source shows exactly that flow:

```text
                       ┌ ─ ─┌────────────────────┐
                            │allocated Proc:     │
                       │    │ ┌─────────────────┐│
                            │ │TrampolineActor  ││
                       │    │ │ ┌──────────────┐││
                            │ │ │Host          │││
              ┌────┬ ─ ┘    │ │ │ ┌──────────┐ │││
           ┌─▶│Proc│        │ │ │ │HostAgent │ │││
           │  └────┴ ─ ┐    │ │ │ └──────────┘ │││
           │  ┌────┐        │ │ │             ██████
┌────────┐ ├─▶│Proc│   │    │ │ └──────────────┘││ ▲
│ Client │─┤  └────┘        │ └─────────────────┘│ listening channel
└────────┘ │  ┌────┐   └ ─ ─└────────────────────┘
           ├─▶│Proc│
           │  └────┘
           │  ┌────┐
           └─▶│Proc│
              └────┘
                ▲

         `Alloc`-provided
               procs
```

So this line:

```rust
let (mesh_agents, mut mesh_agents_rx) = cx.mailbox().open_port();
```

means "I'm the parent, here is the port you will all report back to."

And this line:

```rust
proc_mesh
    .spawn::<HostMeshAgentProcMeshTrampoline>(...)
    .await?;
```

means "for each remote proc in the proc-mesh, run the trampoline actor that will stand up the host and send its agent back."

After this, the parent just waits on `mesh_agents_rx.recv()` once per rank (see §3.4).

### 3.4 Collect one agent per rank

After spawning those trampolines, `HostMesh::allocate()` does:

```rust
// from hyperactor_mesh/src/v1/host_mesh.rs `HostMesh::allocate()`
let mut hosts = Vec::new();
for _rank in 0..extent.num_ranks() {
    let mesh_agent = mesh_agents_rx.recv().await?;

    let Some((addr, _)) = mesh_agent.actor_id().proc_id().as_direct() else {
        return Err(...);
    };

    let host_ref = HostRef(addr.clone());
    if host_ref.mesh_agent() != mesh_agent {
        return Err(...);
    }
    hosts.push(host_ref);
}
```

That means:

- it expects **exactly one** reply per rank
- each reply is the remote host's agent
- it checks that the agent is **direct-addressed** (host listens on a channel) and that the id matches what it would derive from the host address

And this line:
```rust
if host_ref.mesh_agent() != mesh_agent { ... }
```

isn't decorative — it's proving that the agent we just got back is actually "the host-mesh agent that lives on the service proc at this address." We already know the service address (`addr`), so we can construct the actor id we expect for that host: `HostRef(addr).mesh_agent()`. The trampoline just sent us a real `ActorRef<HostMeshAgent>` from the child. We compare the expected id to the actual one; if they don't match, we bail, because that would mean we're about to assemble a host mesh with an agent that isn't actually running on that host's proc.

- and it turns that into a `HostRef` it can store

At the end of that loop we have a `Vec<HostRef>` — one per remote OS process we allocated.

### 3.5 What the trampoline actually does (remote side)

This is the bit from `hyperactor_mesh/src/v1/host_mesh.rs` right after we opened the port and before we start collecting agents:

```rust
// from hyperactor_mesh/src/v1/host_mesh.rs `HostMesh::allocate()`
let (mesh_agents, mut mesh_agents_rx) = cx.mailbox().open_port();
let _trampoline_actor_mesh = proc_mesh
    .spawn::<HostMeshAgentProcMeshTrampoline>(
        cx,
        "host_mesh_trampoline",
        &(transport, mesh_agents.bind(), bootstrap_params, is_local),
    )
    .await?;
```

We've already said "we spawn a trampoline on every allocated proc," but here's what that trampoline is doing and why we need it.

1. **It runs *in the remote proc***
   Remember: `proc_mesh.spawn::<...>(...)` sends a spawn to *each* of the procs that came from the allocator. So the code above is not running locally — it's telling each remote proc "please run this actor."

2. **Its job is to finish turning ‘bare proc' into ‘host'**
   At this point the remote OS process is only running a proc (the thing the allocator told it to start). That proc is reachable and can run actors, but it is not yet a *host* in the v1 sense. The trampoline actor's whole purpose is:
   - call `Host::serve(...)` **inside that remote process**
   - give it a `BootstrapProcManager` so it can later spawn *more* OS processes for procs
   - spawn the real `HostMeshAgent` on the host's service proc
   - report back to the parent with an `ActorRef<HostMeshAgent>`

3. **It reports back using the port we passed down**
   We gave it `mesh_agents.bind()` in the spawn args. That means the trampoline can do:
   - "here is the address of the host I just stood up"
   - "here is the actor id of the agent that manages it"
   on the channel the parent is already listening to.

4. **That's why the parent can later do the identity check**
   In §3.4 we loop over `mesh_agents_rx.recv().await?` and then do:

   ```rust
   let Some((addr, _)) = mesh_agent.actor_id().proc_id().as_direct() else { ... };
   let host_ref = HostRef(addr.clone());
   if host_ref.mesh_agent() != mesh_agent { ... }
   ```

   That works only because the trampoline, on the remote side, actually created the host at exactly that address and spawned the agent there.

So the trampoline is the "last mile" that runs *inside the child process* and upgrades "I am a proc the allocator asked for" → "I am a host with a service proc and a host-mesh agent, and here is my agent ref."

### 3.6 Assemble the `HostMesh`

At this point in `hyperactor_mesh/src/v1/host_mesh.rs` (inside `HostMesh::allocate(...)`) we've already:

- turned the `Alloc` into a `ProcMesh` (one proc per rank, running in the OS processes the allocator started),
- spawned a `HostMeshAgentProcMeshTrampoline` on each of those procs,
- received exactly one `ActorRef<HostMeshAgent>` back per rank over the port,
- verified that each agent really lives on the direct-addressed "service" proc for that host, and
- converted those into a `Vec<HostRef>`.

The function then just packages all of that into an owned `HostMesh`:

```rust
// from hyperactor_mesh/src/v1/host_mesh.rs `HostMesh::allocate(...)`
Ok(Self {
    name: name.clone(),
    extent: extent.clone(),
    allocation: HostMeshAllocation::ProcMesh {
        proc_mesh,
        proc_mesh_ref,
        hosts: hosts.clone(),
    },
    current_ref: HostMeshRef::new(name, extent.into(), hosts).unwrap(),
})
```

What each field means:

- `name: name.clone()`
  This is the name the caller passed to `HostMesh::allocate(...)` (in the canonical bootstrap it was `"test"`). We carry it through so the resulting mesh has a readable label.

- `extent: extent.clone()`
  The host mesh has the same shape as the allocation/proc-mesh we just consumed — e.g. if the alloc was `extent!(replicas = 1)`, the host mesh is 1-wide.

- `allocation: HostMeshAllocation::ProcMesh { ... }`
  This is the lifecycle anchor. The enum `HostMeshAllocation` tells the mesh "these hosts came from a proc-mesh allocation, you own them, and shutdown should walk that structure."
  In this variant we store:
  - `proc_mesh`: the owned proc-mesh we just built from the alloc
  - `proc_mesh_ref`: a reference view of that same proc-mesh
  - `hosts`: the `Vec<HostRef>` we collected from the trampolines
  Keeping both the proc-mesh and the hosts means we can later tear the whole thing down deterministically.

- `current_ref: HostMeshRef::new(...)`
  This builds the lightweight, non-owning view over the same hosts:
  ```rust
  HostMeshRef::new(name, extent.into(), hosts)
  ```
  so callers can work with a sliceable "host mesh ref" without taking ownership.

Put differently: **`HostMesh::allocate(...)` ends by freezing the dynamic bootstrap we just did (alloc → proc-mesh → trampolines → host agents) into a single owned value that remembers all the pieces it must later shut down.**

### 3.7 How this fits the flow

So now the flow looks like:

1. parent has a proc + instance (can send/receive)
2. parent allocates OS processes via `ProcessAllocator` (child runs v0 bootstrap)
3. `ProcMesh::allocate(...)` tells those children "start a proc" → we now have N procs
4. `HostMesh::allocate(...)` logs into each of those procs, has each one stand up a Host, and collects the `HostMeshAgent` refs
5. we now have an actual `HostMesh`
