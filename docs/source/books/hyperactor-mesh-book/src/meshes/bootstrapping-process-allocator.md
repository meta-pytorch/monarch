# 2. Process allocator & v0 bootstrap (get something that can spawn processes)

## 2. Get something that can spawn processes

In the canonical v1 flow we need a *thing in the parent* that can say "start another OS process that will come up as a hyperactor proc/host and talk back to me." In this codebase that "thing" is the **process allocator**, implemented as `hyperactor::alloc::ProcessAllocator` defined in `hyperactor/src/process.rs`.

In the unit-test version (`bootstrap_canonical_simple`) it looks like this:

```rust
let mut allocator = ProcessAllocator::new(Command::new(
    crate::testresource::get("monarch/hyperactor_mesh/bootstrap"),
));
let alloc = allocator
    .allocate(AllocSpec {
        // v1 usage: 1-D extent
        extent: extent!(replicas = 1),
        constraints: Default::default(),
        proc_name: None,
        transport: ChannelTransport::Unix,
    })
    .await
    .unwrap();
```

That's the high-level call. Under the covers, `allocate(...)` does a bit more, and it matters for understanding bootstrapping, so let's drill in.

### 2.1 What `allocate(...)` actually does

When you call `allocator.allocate(spec)`, the allocator first creates a **per-allocation bootstrap channel**:

```rust
let (bootstrap_addr, rx) =
    channel::serve(ChannelAddr::any(ChannelTransport::Unix))?;
```

So: every allocation gets its **own** address (`bootstrap_addr`) that children will dial back to, and an `rx` so the allocator can receive messages from those children.

Then it builds a `ProcessAlloc` that tracks:

- the `AllocSpec` you passed (which includes the extent and the transport),
- the `bootstrap_addr` it just created,
- the set of children it will spawn,
- and the world/rank bookkeeping (`Ranks`) so each child gets a proper ranked `ProcId`.

So far: **no process has been started yet**, we just prepared an allocation.

### 2.2 Spawning the child (inside `maybe_spawn()`)

Later, when the allocation runs its loop (`alloc.next().await`), it will eventually try to spawn a child here:

```rust
cmd.env(bootstrap::BOOTSTRAP_ADDR_ENV, self.bootstrap_addr.to_string());
cmd.env(bootstrap::CLIENT_TRACE_ID_ENV, self.client_context.trace_id.as_str());
cmd.env(bootstrap::BOOTSTRAP_INDEX_ENV, index.to_string());
let child = cmd.spawn()?;
```

Important points:

1. **The parent chooses the command.**
   In the unit test we pointed at a small bootstrap helper:
   ```rust
    ProcessAllocator::new(Command::new(
        crate::testresource::get("monarch/hyperactor_mesh/bootstrap")
   ```
   In real deployments this can be "re-exec myself" or "run this Python PAR/XAR with a different entrypoint." The allocator is just the *template*.

2. **This is where the initial env is attached.**
   From this file we can see the allocator always sets:
   - `BOOTSTRAP_ADDR_ENV` — "dial this address to talk to the parent allocator"
   - `BOOTSTRAP_INDEX_ENV` — "you are child #N in this allocation"
   - `CLIENT_TRACE_ID_ENV` — for log correlation
   That's the *actual* bootstrap data we can prove from the code.

3. **The spec's extent controls how many children we make.**
   The API is N-dimensional, but in the v1 shape we're describing we just do
   ```rust
   extent: extent!(replicas = 1)
   ```
   which the allocator interprets as "I need 1 rank in this allocation." If you asked for more, it would keep spawning until that extent is satisfied.

4. **The transport in the spec is what we tell the child later.**
   The `AllocSpec` has `transport: ChannelTransport::Unix` — that's what we eventually pass down to the child when we tell it "start a proc."

### 2.3 What the child actually does when it starts

So far we've been on the parent side: we built a `ProcessAllocator`, we called `allocate(...)`, and that code spawned **some other executable** (in tests: `monarch/hyperactor_mesh/bootstrap`). Now we need to look at the **child** side of that executable.

The test bootstrap binary's `main` is tiny:

```rust
// from hyperactor/test/bootstrap.rs
#[tokio::main]
async fn main() {
    hyperactor::initialize_with_current_runtime();
    unsafe {
        libc::signal(libc::SIGTERM, libc::SIG_DFL);
    }
    hyperactor_mesh::bootstrap_or_die().await;
}
```
So: every child just calls `hyperactor_mesh::bootstrap_or_die()`. That's the real entrypoint.


#### 2.3.1 `bootstrap_or_die` → `bootstrap` → "which mode am I in?"

In bootstrap.rs the entrypoint is:
```rust
// from hyperactor_mesh/src/bootstrap.rs
pub async fn bootstrap() -> anyhow::Error {
    let boot = ok!(Bootstrap::get_from_env()).unwrap_or_else(Bootstrap::default);
    boot.bootstrap().await
}
```
Key detail:
    - It first tries to read `HYPERACTOR_MESH_BOOTSTRAP_MODE` from the environment:
    - `Bootstrap::get_from_env()` looks for that env var.
    - If it exists, it decodes a base64 JSON into a `Bootstrap` value.
    - If it doesn't exist, it returns `None` and we fall back to `Bootstrap::default()`.

What's the default?
```rust
// from hyperactor_mesh/src/bootstrap.rs
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub enum Bootstrap {
    …
    #[default]
    V0ProcMesh,
}
```
So: if the parent did not set `HYPERACTOR_MESH_BOOTSTRAP_MODE`, the child runs in `Bootstrap::V0ProcMesh` mode.

This particular `ProcessAllocator` (the v0 one in `process.rs`) does **not** set `HYPERACTOR_MESH_BOOTSTRAP_MODE`. Instead it populates a small, fixed set of environment variables:

- `HYPERACTOR_MESH_BOOTSTRAP_ADDR`: the allocator's control channel
- `HYPERACTOR_MESH_INDEX`: the child's index inside this allocation
- optionally `BOOTSTRAP_LOG_CHANNEL`: for streaming the child's stdout/stderr back
- `MONARCH_CLIENT_TRACE_ID`: for log/trace correlation

Because `HYPERACTOR_MESH_BOOTSTRAP_MODE` is absent, the bootstrap entrypoint in the child falls back to its default and runs the `Bootstrap::V0ProcMesh` path. That is what causes the child to come up in the "say hello to the allocator, wait for `Allocator2Process` messages, start procs on demand" mode.

### 2.3.2 After the child picks `Bootstrap::V0ProcMesh`

At this point we are in the child process, running the `monarch/hyperactor_mesh/bootstrap` test binary (the one whose `main` just calls `hyperactor_mesh::bootstrap_or_die().await`).

Because the parent did **not** set `HYPERACTOR_MESH_BOOTSTRAP_MODE`, the call:

```rust
let boot = Bootstrap::get_from_env().unwrap_or_else(Bootstrap::default);
```

became `Bootstrap::V0ProcMesh`, so the child runs `hyperactor_mesh::bootstrap::bootstrap_v0_proc_mesh()`.

What that path does (summarizing the code in `bootstrap_v0_proc_mesh()`):

1. **Read the two env vars the allocator set**
   - `HYPERACTOR_MESH_BOOTSTRAP_ADDR` → this is the allocator's channel address.
   - `HYPERACTOR_MESH_INDEX` → this is "which child in this allocation am I?"

2. **Serve a channel for the allocator to talk to**
   It picks a fresh address on the same transport:
   ```rust
   let listen_addr = ChannelAddr::any(bootstrap_addr.transport());
   let (serve_addr, mut rx) = channel::serve(listen_addr)?;
   ```
   This is the child saying: "I will listen here for allocator commands."

3. **Dial the allocator and say Hello(index, my_addr)**
   ```rust
   let tx = channel::dial(bootstrap_addr.clone())?;
   tx.try_post(
       Process2Allocator(bootstrap_index, Process2AllocatorMessage::Hello(serve_addr)),
       ...
   )?;
   ```
   That `Process2AllocatorMessage::Hello(...)` is exactly what your parent side — the `ProcessAlloc` — is waiting for. It ties "child index N" to "this is the address you can send `Allocator2Process` messages to."

4. **Start a heartbeat task**
   The child also spawns a task that periodically sends:
   ```text
   Process2Allocator(index, Process2AllocatorMessage::Heartbeat)
   ```
   back to the allocator. If the allocator goes away or the heartbeat fails, the child exits. This is how the parent gets liveness.

5. **Enter the control loop**
   After Hello, the child just sits in:
   ```rust
   match rx.recv().await? {
       Allocator2Process::StartProc(proc_id, listen_transport) => { ... }
       Allocator2Process::StopAndExit(code) => { ... }
       Allocator2Process::Exit(code) => { ... }
   }
   ```
   This is the pivot point: the child is now "a remote executor that can start a hyperactor proc when told."

6. **On StartProc(...)**
   When the parent/allocator finally sends:
   ```text
   Allocator2Process::StartProc(proc_id, listen_transport)
   ```
   the child:
   - boots a real `Proc` **inside this process** via `ProcMeshAgent::bootstrap(proc_id.clone())`
   - serves that proc on a fresh address
   - and crucially sends **back** to the allocator:
     ```text
     Process2Allocator::StartedProc(proc_id, agent_ref, proc_addr)
     ```
     so the parent now knows:
     - which proc id is alive,
     - where to reach its mailbox (`proc_addr`),
     - and which mesh agent actor to talk to.

That's the full round-trip for the v0 allocator + test bootstrap binary: parent starts OS process → child says "hello, I'm #N, talk to me here" → parent says "start proc X on transport T" → child starts it and reports back.

#### Note: who picks the `ProcId`?

When the allocator receives the child's `Hello(...)` and decides to start a proc, **the allocator chooses the proc id**.

- If `AllocSpec.proc_name` is **`None`**, the allocator builds a ranked id:
  - `ProcId::Ranked(WorldId(<this allocation's uuid>), <child index>)`
  - meaning "you are proc #i in this allocation."
- If `AllocSpec.proc_name` is **`Some(name)`**, the allocator builds a direct id:
  - `ProcId::Direct(<child's hello address>, name)`

So: the parent always decides the identity; the child just accepts it.

#### 2.3.3 Driving the allocation (where the child actually starts)

Up to this point we've only *created* an allocation:

```rust
// from hyperactor_mesh/src/bootstrap.rs (`fn tests::bootstrap_canonical_simple`)
let alloc = allocator
    .allocate(AllocSpec {
        extent: extent!(replicas = 1),
        constraints: Default::default(),
        proc_name: None,
        transport: ChannelTransport::Unix,
    })
    .await
    .unwrap();
```

That call sets up the per-allocation state (bootstrap channel, ranks, etc.), but it does **not** by itself run the child's bootstrap flow. The part where the OS process is actually spawned and sends `Process2Allocator::Hello(...)` happens only when someone **drives** the allocation by pulling events from it (i.e. calling `alloc.next().await` in a loop).

In the canonical test we don't write that loop manually. Instead we hand the allocation to the mesh builder:

```rust
// from hyperactor_mesh/src/bootstrap.rs (`fn tests::bootstrap_canonical_simple`)
let host_mesh = HostMesh::allocate(&instance, Box::new(alloc), "test", None)
    .await
    .unwrap();
```

`HostMesh::allocate(...)` is what actually consumes the allocation: under the hood it waits for the spawned child to say "hello," sends back the `StartProc(...)` instruction, receives the child's `StartedProc(...)` (with the host/agent address), and then uses that to assemble the final `HostMesh`. In other words: `allocate(...)` gives us "a source of hosts," and `HostMesh::allocate(...)` is the step that turns that source into an actual running host in the new OS process.
