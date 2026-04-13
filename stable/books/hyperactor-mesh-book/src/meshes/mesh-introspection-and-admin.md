# Mesh introspection & mesh-admin

This chapter is the "page me back in" overview of how mesh introspection works and how the mesh admin TUI sits on top of it.

The short version is:

- the mesh exposes a **reference-walking** API,
- every successful lookup returns a uniform `NodePayload`,
- `MeshAdminAgent` is the mesh-wide resolver and HTTP bridge,
- `HostAgent` and `ProcAgent` provide the proc-level and host-level facts,
- ordinary actors provide their own actor-level introspection through the blanket `IntrospectMessage` handler,
- and the TUI is "just" a lazy client of `GET /v1/{reference}` plus a couple of proc-oriented side endpoints.

If you remember only one idea, remember this one:

```
root
 -> host refs
 -> proc refs
 -> actor refs
```

The TUI does not have privileged topology knowledge. It starts at `root`, follows `children`, and asks the server to resolve whatever opaque reference string it was given next.

## The data model: `NodePayload`

The entire mesh-admin surface is built around one response type:

```
pub struct NodePayload {
 pub identity: String,
 pub properties: NodeProperties,
 pub children: Vec<String>,
 pub parent: Option<String>,
 pub as_of: String,
}
```

This is the key simplification. Clients do not get separate "host response", "proc response", and "actor response" endpoints with different shapes. Instead they always ask for "resolve this reference" and receive:

- `identity`: the canonical reference string for this node,
- `properties`: one of `Root`, `Host`, `Proc`, `Actor`, or `Error`,
- `children`: the next reference strings the client can walk,
- `parent`: a navigation hint for upward movement,
- `as_of`: when the data was captured.

`NodeProperties` is the typed payload:

- `Root` is synthetic and records high-level mesh metadata,
- `Host` describes a `HostAgent` and its proc children,
- `Proc` describes a proc node and its actor children,
- `Actor` describes an individual actor instance,
- `Error` is the sentinel shape used when resolution/decode fails in a controlled way.

The `introspect` module is the translation layer. Internally the runtime produces `IntrospectResult { attrs, children, ... }`; `derive_properties` decodes attrs into `NodeProperties`; `to_node_payload` wraps that into the API shape.

So the stack is:

```
published attrs / IntrospectResult
 -> derive_properties(...)
 -> NodeProperties
 -> NodePayload
```

## The mental model: what is actually introspectable?

The runtime rule is not "everything that exists is visible." The rule is "everything the routing layer can actually reach via actor messaging is visible."

That is why the module docs talk about **routable**, **introspectable**, and **opaque** nodes:

- if an actor can receive `IntrospectMessage`, it is introspectable,
- if a proc has at least one such actor, the admin layer can synthesize a proc node for it,
- infrastructure pieces with no routable mailbox sender are intentionally opaque and do not appear as first-class nodes.

This explains two important design choices:

1. procs are mostly **synthetic nodes** assembled from agent knowledge rather than directly introspected runtime objects;
2. the admin tree is a **navigation projection** of what the routing layer can actually observe, not a dump of every implementation detail in the process.

## Who provides what

There are three layers involved in a normal lookup.

### 1. `MeshAdminAgent`: mesh-wide resolver and HTTP bridge target

`MeshAdminAgent` owns:

- the map of host address -> `ActorRef<HostAgent>`,
- the reverse map of `HostAgent ActorId` -> host address,
- the synthetic root node,
- the `ResolveReferenceMessage` handler,
- the background Axum server started from `init`.

It does **not** directly know every actor in the mesh. Instead it is the orchestrator that decides which downstream actor to ask next.

The core resolver is:

```
resolve_reference(reference_string)
 "root" -> build_root_payload()
 "host:<actor_id>" -> resolve_host_node(...)
 ProcId -> resolve_proc_node(...)
 ActorId -> resolve_actor_node(...)
```

So `MeshAdminAgent` is the place where opaque references become typed navigation.

### 2. `HostAgent`: host nodes and system/local proc resolution

`HostAgent` publishes host attrs such as:

- `node_type = "host"`
- `addr`
- `num_procs`
- `children`
- `system_children`

More importantly, in `init` it registers a `QueryChild` callback for proc references that are **not** independently represented by a user `ProcAgent` path, notably:

- the service proc,
- the local proc.

That callback synthesizes a proc-level `IntrospectResult` for those system procs by enumerating actors in the proc and marking which are system actors.

So if `MeshAdminAgent` asks a `HostAgent` "tell me about this proc child", the host can answer directly for system/local procs.

### 3. `ProcAgent`: user proc nodes, terminated snapshots, proc-local services

`ProcAgent` does the corresponding job for user procs.

It publishes proc attrs such as:

- `node_type = "proc"`
- `proc_name`
- `num_actors`
- `system_children`
- `stopped_children`
- `stopped_retention_cap`
- `is_poisoned`
- `failed_actor_count`

In `init` it also registers a `QueryChild` callback with two especially important behaviors:

1. `QueryChild(Reference::Proc(proc_id))` builds a **fresh proc node from live proc state**, not from stale published snapshots. This is why a directly spawned actor can appear in the next admin query without waiting for a separate republish path.
2. `QueryChild(Reference::Actor(actor_id))` can return a **terminated snapshot** for dead actors, which lets the admin UI continue to resolve recently stopped actors by reference.

`ProcAgent` is also where the proc-oriented side services live today:

- `PySpyDump` is handled by delegating to a one-shot `PySpyWorker`,
- `ConfigDump` returns current CONFIG-marked settings for the proc.

## How a normal tree walk works

The runtime walk that underlies the TUI looks like this.

### Step 1: root

The client asks for `root`.

`MeshAdminAgent::build_root_payload()` returns a synthetic node whose children are host references:

```
root
 children = ["host:<host_agent_actor_id_0>", "host:<host_agent_actor_id_1>", ...]
```

The root is not a real actor or proc. It is purely a navigation anchor.

### Step 2: host

The client picks a host child and resolves it.

`MeshAdminAgent::resolve_host_node()` sends `IntrospectMessage::Query(Entity)` directly to that `HostAgent` and converts the returned attrs/children into `NodePayload`.

At that point the client sees:

- host metadata,
- system and user proc references under that host.

### Step 3: proc

The client picks a proc child and resolves it.

`MeshAdminAgent::resolve_proc_node()` does a two-stage attempt:

1. ask the owning `HostAgent` via `QueryChild(Reference::Proc(proc_id))`,
2. if that comes back as an error payload, fall back to the proc's `proc_agent[0]`.

Why the two stages?

- service/local/system procs are host-managed and answered by `HostAgent`,
- user procs are answered by `ProcAgent`,
- the client does not need to know the difference.

That is a recurring theme in this subsystem: the server hides topology quirks behind a single reference API.

### Step 4: actor

The client picks an actor child and resolves it.

`MeshAdminAgent::resolve_actor_node()` usually sends `IntrospectMessage::Query(Actor)` directly to that actor's introspection port.

There are some important special cases:

- if the actor being resolved is the admin actor itself, the agent snapshots itself directly to avoid self-deadlock;
- before querying a live user actor, it checks for a terminated snapshot via the proc agent, so recently dead actors remain queryable;
- if the actor lives on a standalone proc, parent handling is adjusted accordingly.

The end result is still a plain `NodePayload::Actor`, so the client sees a normal actor node.

## Why `QueryChild` matters so much

The reference API would be awkward without `QueryChild`.

The admin layer needs answers to questions like:

- "tell me about proc `P` under host `H`",
- "tell me about recently terminated actor `A` on proc `P`",
- "enumerate the children of this system proc even though it is not a user proc with its own agent path".

`QueryChild` is the escape hatch that lets a parent-ish actor answer these structural questions even when the child is synthetic, recently terminated, or otherwise not a normal live actor lookup.

That is why both `HostAgent` and `ProcAgent` install `set_query_child_handler(...)` callbacks during `init`.

Without those handlers, the admin layer could still introspect live actors directly, but proc-level navigation would be much poorer and much more special-cased.

## The HTTP layer is deliberately thin

The mesh admin HTTP server is not where the topology logic lives.

`MeshAdminAgent::init()`:

- binds its ports,
- binds a TCP listener,
- configures TLS/mTLS,
- creates a dedicated bridge `Instance<()>` mailbox,
- starts an Axum server in the background.

The important detail is the **bridge instance**. The HTTP handlers do not share `MeshAdminAgent`'s own actor mailbox. Instead they use a dedicated introspectable client mailbox so they can:

- open reply ports,
- send `ResolveReferenceMessage`, `PySpyDump`, and `ConfigDump`,
- await responses on Tokio tasks without hijacking the admin actor's own message loop.

So the HTTP server is really an actor-message bridge:

```
HTTP request
 -> Axum handler
 -> actor message / oneshot reply port
 -> MeshAdminAgent / HostAgent / ProcAgent / target actor
 -> response
```

The core routes today are:

- `GET /v1/schema`
- `GET /v1/schema/error`
- `GET /v1/openapi.json`
- `GET /v1/tree`
- `GET /v1/config/{proc_reference}`
- `GET /v1/pyspy/{proc_reference}`
- `GET /v1/{reference}`
- `GET /SKILL.md`

The navigation route the TUI cares about is `GET /v1/{reference}`.

## The TUI is a lazy reference walker

The admin TUI is intentionally not a "push the whole mesh over HTTP" client. It is a **lazy tree walker**.

Its core fetch path is in `fetch.rs`:

- `fetch_node_raw(client, base_url, reference)` issues `GET /v1/{urlencode(reference)}`,
- results are cached as `FetchState<NodePayload>`,
- cache updates go through `fetch_with_join`,
- `build_tree_node(...)` recursively builds the visible tree from cached payloads and follow-up fetches.

Two details matter when reading the TUI after time away:

### 1. It is reference-driven, not type-driven

The TUI does not infer topology from local heuristics. It trusts:

- `identity`
- `parent`
- `children`
- `properties`

That is why the navigation identity invariants in `mesh_admin.rs` matter so much: if `identity` and `parent` are wrong, the TUI cannot keep its tree coherent.

### 2. It is lazy

Proc and actor children are fetched when the user expands into them, not preloaded wholesale.

That is why the TUI code talks so much about:

- explicit tree recursion,
- placeholders,
- refresh generations,
- join semantics,
- cycle safety.

The TUI is not a mirror of server state. It is a cached, incrementally refreshed projection of the reference graph.

## Proc-oriented side endpoints

The TUI also uses two proc-oriented endpoints:

- `GET /v1/pyspy/{proc_reference}`
- `GET /v1/config/{proc_reference}`

These are intentionally **not** general reference-resolution endpoints. They require a proc reference because the server needs to decide whether to route the request to:

- `HostAgent` for the service proc,
- `ProcAgent` for non-service procs.

For py-spy, the flow is:

```
HTTP /v1/pyspy/{proc}
 -> choose HostAgent or ProcAgent by proc name
 -> probe reachability
 -> send PySpyDump
 -> ProcAgent/HostAgent delegates to PySpyWorker
 -> return structured PySpyResult
```

For config dump, the flow is analogous but simpler: send `ConfigDump` and await `ConfigDumpResult`.

These side endpoints are separate because they are operations on a proc, not tree navigation over `NodePayload`.

## Why this design is nice to work with

Three properties make this subsystem easier to reason about than it first appears.

### 1. Uniform API shape

Everything navigational becomes "resolve a reference to `NodePayload`". The client never has to switch protocols as it moves from host to proc to actor.

### 2. Topology knowledge stays server-side

The client does not need to know:

- which procs are system procs,
- which proc is answered by `HostAgent` versus `ProcAgent`,
- how terminated actor snapshots are recovered,
- how parent/identity normalization is done.

That logic sits in `MeshAdminAgent`, `HostAgent`, and `ProcAgent`.

### 3. It reuses the actor system instead of bypassing it

The HTTP surface is not a side database or a separate control plane. It is a projection built by sending actor messages through the same routing model the mesh already uses.

That is why the docs keep emphasizing "if you can send it a message, you can introspect it."

## Practical reading order in the code

When you need to page this back in after a few days, read in this order:

1. `hyperactor_mesh/src/introspect.rs`
This gives you the attrs keys, `NodePayload`, `NodeProperties`, and the decode rules.
2. `hyperactor_mesh/src/mesh_admin.rs`
This gives you the reference resolver, the router, and the HTTP bridge.
3. `hyperactor_mesh/src/host_mesh/host_agent.rs`
This shows how host nodes and system/local proc nodes are synthesized.
4. `hyperactor_mesh/src/proc_agent.rs`
This shows how user proc nodes, terminated snapshots, py-spy, and config dump work.
5. `hyperactor_mesh_admin_tui/src/fetch.rs` and `.../src/lib.rs`
This shows how the TUI lazily walks the reference graph and turns responses into a visible tree.

That sequence maps cleanly onto the runtime layering:

```
attrs -> NodePayload -> resolve reference -> HTTP bridge -> TUI walker
```

## A compact end-to-end call flow

This is the full "what happens when I expand a node in the TUI?" summary:

```
TUI
 -> GET /v1/{reference}
 -> resolve_reference_bridge
 -> MeshAdminAgent::resolve_reference
 root -> build_root_payload
 host ref -> HostAgent introspect query
 proc ref -> HostAgent QueryChild, else ProcAgent QueryChild
 actor ref -> direct actor introspect query, maybe via terminated snapshot
 -> IntrospectResult / attrs
 -> derive_properties -> NodeProperties
 -> NodePayload
 -> TUI cache / tree builder
 -> visible row in the tree
```

That is the subsystem in one page.