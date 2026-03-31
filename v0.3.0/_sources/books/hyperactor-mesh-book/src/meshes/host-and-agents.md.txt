# Host & agents (control plane & mux)

In the runtime, a **host** is the thing that owns "all the procs on this machine" *and* gives them a single front door. The Rust type looks like this:

```rust
// from hyperactor/src/host.rs

pub struct Host<M> {
    procs: HashSet<String>,
    frontend_addr: ChannelAddr,
    backend_addr: ChannelAddr,
    router: DialMailboxRouter,
    manager: M,
    service_proc: Proc,
    local_proc: Proc,
    frontend_rx: Option<ChannelRx<MessageEnvelope>>,
}
```
Visually, you can think of it like this:
```text
                      ┌────────────┐
                  ┌───▶  proc *,1  │
                  │ #1└────────────┘
                  │
  ┌──────────┐    │   ┌────────────┐
  │   Host   │◀───┼───▶  proc *,2  │
 *└──────────┘#   │ #2└────────────┘
                  │
                  │   ┌────────────┐
                  └───▶  proc *,3  │
                    #3└────────────┘
```

- `*` is the host's frontend address (`frontend_addr`). This is the address other mesh participants know.
- `#` is the host's backend address (`backend_addr`). Procs talk to the host here.
- `#1`, `#2`, `#3` are the per-proc backend channels the host records in `router: DialMailboxRouter`
- Each box `proc *,N` is a proc that is direct-addressed via the host — its id is essentially "proc at `*` named `N`".

## What the fields mean

- **`frontend_addr`**: the single, public entry point. Messages from the rest of the mesh arrive here.
- **`procs`**: a set of proc names managed by this host.
- **`router: DialMailboxRouter`**: the machinery that actually multiplexes/demultiplexes between `*` and the per-proc channels.
- **`manager: M`**: the thing that can create and destroy procs on this host. In the real bootstrapped case this is a `BootstrapProcManager`; in tests it can be a local manager.
- **`service_proc`**: the host's system proc handle, so the host can participate in the same message world it is hosting.
- **`local_proc`**: an additional local proc for in-process operations.
- **`frontend_rx`**: the optional channel receiver for the frontend address (consumed when serving starts).

## Why this matters for bootstrapping

When, in chapter 4, we say "now ask each host to spawn a proc," this is the piece that makes it possible. The host already has:

1. a public address (`*`),
2. a routing table for its existing procs,
3. and a manager capable of creating new ones.

So the host-mesh agent can receive a "create proc" request over the mesh protocol and hand it to the host, and the host will add another box to the diagram above.



---

## Code-level view

At the control plane we have the mesh-facing actor:

```rust
// hyperactor_mesh/src/v1/host_mesh/mesh_agent.rs
pub struct HostMeshAgent {
  host: Option<HostAgentMode>,
  created: HashMap<Name, ProcCreationState>,
}
```

The reason it's an `Option` is that the agent can exist before (or after) the host is actually running.

The `host` field is one of two shapes:

```rust
// "How are we running this host?"
pub enum HostAgentMode {
    // Real OS process, uses BootstrapProcManager underneath.
    Process(Host<BootstrapProcManager>),

    // In-process/testing host, uses a local proc manager.
    Local(Host<LocalProcManager<ProcManagerSpawnFn>>),
}
```

Both variants wrap a `Host<…>`, and that `Host` is the thing we drew earlier as the mux:

```rust
// hyperactor/src/host.rs (simplified)
pub struct Host<M> {
    procs: HashSet<String>,
    frontend_addr: ChannelAddr,
    backend_addr: ChannelAddr,
    router: DialMailboxRouter,
    manager: M,             // e.g. BootstrapProcManager
    service_proc: Proc,
    local_proc: Proc,
    frontend_rx: Option<ChannelRx<MessageEnvelope>>,
}
```

So the layering from the code's point of view is:

1. `HostMeshAgent` (actor you message over v1)
2. → maybe a `HostAgentMode`
3. → definitely a `Host<...>` once materialized
4. → which, through its manager (e.g. `BootstrapProcManager`), owns/spawns the procs and does the `*`/`#n` routing.

## HostMeshAgent message handling

The agent is exported with exactly these handlers:

```rust
#[hyperactor::export(
    handlers = [
        resource::CreateOrUpdate<ProcSpec>,
        resource::Stop,
        resource::GetState<ProcState>,
        resource::GetRankStatus { cast = true },
        resource::List,
        ShutdownHost,
    ]
)]
pub struct HostMeshAgent {
    host: Option<HostAgentMode>,
    created: HashMap<Name, ProcCreationState>,
    local_mesh_agent: OnceCell<anyhow::Result<ActorHandle<ProcMeshAgent>>>,
}
```

So everything it does is one of those 6 messages.

### 1. CreateOrUpdate<ProcSpec>

- If we already have `created[name]`, do nothing (idempotent).
- Otherwise call `host.spawn(name, ...)` — process-backed hosts get a `BootstrapProcConfig`, local hosts get `()`.
- Store `{ rank, created_result, stopped: false }` in `created`.

### 2. Stop

- Look up `created[name]`.
- If it was successfully created, call `host.terminate_proc(..., timeout)` and mark it `stopped = true`.
- Reply with a `StatusOverlay` for that rank (or empty if we never had it).

### 3. GetRankStatus

- Look up `created[name]`.
- If present, return that rank with `Running` / `Stopped` / `Failed` (depending on what we know).
- Otherwise return `NotExist`.

### 4. GetState<ProcState>

- Same lookup, but return the richer state:
  - the proc's direct id at the host,
  - the rank we used,
  - the proc's own `ProcMeshAgent` ref,
  - and any bootstrap/process status the host's manager could provide.

### 5. List

- Return all the proc names that have been created on this host (the keys from the `created` map).

### 6. ShutdownHost

- Ack first so the caller can await.
- Take the host out of `self`.
- Call `host.terminate_children(...)` (process vs local path) with the provided timeout and concurrency.

## Why this exists

`Host` is local; `HostMeshAgent` is the remote handle for it. Bootstrap code just sends `CreateOrUpdate/Stop/GetState` to the agent; the agent is the one that actually owns the `Host` and can spawn/stop procs. That’s why all handlers use the shared `resource` messages.

## 1. `ProcSpec` (what we tell the host to run)

In all the examples above we sent `resource::CreateOrUpdate<ProcSpec>`, and in the code that really is what's happening — but the current `ProcSpec` is intentionally very thin.

From `hyperactor_mesh/src/resource.rs`:

```rust
/// Spec for a host mesh agent to use when spawning a new proc.
#[derive(Clone, Debug, Serialize, Deserialize, Named, Default)]
pub(crate) struct ProcSpec {
    /// Config values to set on the spawned proc's global config,
    /// at the `ClientOverride` layer.
    pub(crate) client_config_override: Attrs,
}
```
So right now:
-  the spec is private (`pub(crate)`) and has exactly one field: `client_config_override: Attrs`;
- the rank is not here — it's on the outer message:
```rust
pub struct CreateOrUpdate<S> {
    pub name: Name,
    #[binding(include)]
    pub rank: Rank,
    pub spec: S,
}
```
What the `HostMeshAgent` actually does matches this shape:
- if the host is process-backed (`HostAgentMode::Process(...)`), it builds a `BootstrapProcConfig` using
- the rank from `CreateOrUpdate::<ProcSpec>`, and
- the `client_config_override` from `ProcSpec`, and passes that to `host.spawn(...);`
- if the host is local (`HostAgentMode::Local(...)`), it just calls `host.spawn(name, ())` and ignores the override.

Here is the bit of real code that does exactly that (abridged to just the decision):
```rust
// from hyperactor_mesh/src/v1/host_mesh/mesh_agent.rs (`impl Handler<resource::CreateOrUpdate<ProcSpec>> for HostMeshAgent`)

let created = match host {
    HostAgentMode::Process(host) => {
        host.spawn(
            msg.name.to_string(),
            BootstrapProcConfig {
                create_rank: msg.rank.unwrap(),
                client_config_override: msg.spec.client_config_override.clone(),
            },
        )
        .await
    }
    HostAgentMode::Local(host) => {
        host.spawn(msg.name.to_string(), ()).await
    }
};
```
That's why the current `ProcSpec` can stay small: the outer resource message carries the mesh-y things (name, rank), and the spec only has to carry the "what should this proc's client config look like" part.

## 2. How the host actually spawns

When the agent calls `host.spawn(name, …)`, the **host itself** is not doing the OS-level work. The host delegates to its configured *proc manager*:

- process-backed host → `Host<BootstrapProcManager>`
- in-proc/test host → `Host<LocalProcManager<...>>`

The manager is the thing that can "make a proc real" (fork/spawn, run the bootstrap command, wire the backchannel) and hand the host the proc name so the host can add it to the `procs: HashSet<String>` table and expose it as `ProcId::Direct(frontend_addr, name)`.

We're not going to unpack the process-backed path here — that lives in **"BootstrapProcManager (process-backed hosts)"** where we can talk about commands, ready signals, and termination.

## v1 bootstrap in one pass

The reason the `HostMeshAgent` has those five messages (create, stop, get-state, get-rank-status, shutdown) is that the v1 protocol treats "things on a host" as **resources**. A typical sequence is:

1. **Coordinator → hosts:** send `CreateOrUpdate<ProcSpec>` to every host agent in the mesh ("each of you should have a proc called `p0` with this rank/config").
2. **Coordinator → hosts (later):** send `GetState<ProcState>` (or `GetRankStatus`) to see which hosts actually brought that proc up and what address/command it got.
3. **Coordinator → hosts (teardown):** send `ShutdownHost` to have each agent tell its host to terminate all children and drop the host.

Because everyone speaks this same resource shape — `CreateOrUpdate<T>`, `GetState<T>`, `Stop`, `StopAll`/`ShutdownHost` — the handlers on `HostMeshAgent` all look the same, and the coordinator can fan the same message out to N hosts.
