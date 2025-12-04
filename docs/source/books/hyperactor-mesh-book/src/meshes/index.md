# Meshes

This section explains the *shape* of meshes in v1 — hosts, procs, and actor meshes — and then shows how we bring them up (bootstrapping) from both Rust and Python.

By "mesh" here we don't just mean "a bunch of actors." We mean the whole layered thing:

1. **Host mesh**: "these are the machines / host runtimes I can ask to start procs."
2. **Proc mesh**: "these are the proc runtimes that actually host actors."
3. **Actor mesh**: "these are user actors arranged over that proc mesh so you can broadcast/call/select."

The bootstrapping work is mostly about getting those first two layers to exist so the third one is easy.

## What we want to show

- there is a bootstrap entrypoint in Rust that can make **a host** and put a **host agent** in it
- that host can start **procs** (OS children or in-proc), and each proc has a **proc agent**
- once procs exist, we can **spawn actors** on them as a mesh
- the same path is exposed from Python (`this_host() → spawn_procs(...) → mesh.spawn(...)`)
- it's all done in-band with the same resource messages (`CreateOrUpdate<ProcSpec>`, `GetState<...>`, `Stop`, …)

So the through line is: *one process that can speak hyperactor → host → procs → actors*.

## Pieces (conceptual)

- **Host**: a long-lived runtime that owns "all procs on this machine" and gives them a single front door (`*` / mux). It also runs a **`HostMeshAgent`** in its system proc so other parts of the mesh can tell it "start/stop this proc."
- **Proc**: an actor runtime. In v1 the proc also runs a **`ProcMeshAgent`** so it can be managed the same way as the host — that's why the agent handlers all look like the resource ones you saw.
- **Actor mesh**: the thing you actually care about as a user — N copies of your actor (often one per proc), callable as a group.

That's why the messages look uniform — `CreateOrUpdate<T>`, `GetState<T>`, `Stop`, `ShutdownHost` — the same resource shape works at both the host level and the proc level.

## Flow we'll describe

1. start from a single process (the Rust test / the Python runtime does this)
2. create a **host** in that process
3. have the host **spawn one or more procs** (OS children or in-proc)
4. each proc calls back and is **collected into a proc mesh**
5. create an **actor mesh** on top of that proc mesh

After that, it's just "send messages to the mesh."

## How the pages line up

- **Bootstrapping Overview** — the story version: host → procs → actors.
    - **1. Proc and instance** — what "a proc with an actor in it" even is.
    - **2. Process allocator & v0 bootstrap** — the older path / allocator angle.
    - **3. HostMesh from an allocation** — taking an allocation and saying "these are my hosts."
    - **4. Doing real work (hosts → procs → actors)** — actually spawning actors once procs exist.
- **Host & agents (control plane & mux)** — deep dive on the thing the host runs (`HostMeshAgent`), how it maps `CreateOrUpdate<ProcSpec>` to `host.spawn(...)`, and why all the handlers look the same.
- **Proc meshes & ProcMeshAgent** — deep dive on the proc-level agent: how it turns `CreateOrUpdate<ActorSpec>` and `MeshAgentMessage::Gspawn` into actor spawns via hyperactor's `Remote` registry.
- **Process-backed hosts: BootstrapProcManager** — the "real OS child, real bootstrap command" path the host delegates to.
- **Bootstrapping from Python** — show that `this_host().spawn_procs(...).spawn(...)` is using the same Rust v1 path, just through the Python bindings.
- **Appendix: `bootstrap_canonical_simple`** — the Rust teaching example all of this is mirroring.

So: this section isn't "a test walkthrough." It's "here's how v1 meshes are built, and here are the two front doors (Rust and Python) that call the same code."
