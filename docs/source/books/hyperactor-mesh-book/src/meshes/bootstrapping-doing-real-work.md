#  §4 Doing real work (hosts → procs → actors)

We can now line up the rest of `bootstrap_cannonical_simple` and see that the remaining calls are just "use the host we just created."

Source we're explaining:

```rust
let host_mesh = HostMesh::allocate(&instance, Box::new(alloc), "test", None)
    .await
    .unwrap();
```

was the part we just broke down in §3 — it consumed the allocation, talked to the children, ran the trampoline, and gave us a real `HostMesh`.

The test immediately does two more things:

1. spawn a **proc mesh** *on* that host mesh
2. spawn an **actor mesh** *on* that proc mesh

Then it proves messages can go through, and shuts everything down.

---

### 4.1 Spawn a proc on each host

```rust
let proc_mesh = host_mesh
    .spawn(&instance, "p0", Extent::unity())
    .await
    .unwrap();
```

This is **not** the same as the earlier `ProcMesh::allocate(...)` we saw inside `HostMesh::allocate(...)`. That earlier one was "take an `Alloc` of OS processes and make them into controllable procs." This new call is "now that I *have* hosts, please create *another* layer of procs on those hosts."

What actually happens here, per `HostMeshRef::spawn(...)` in `hyperactor_mesh/src/v1/host_mesh.rs`:

1. For **each host** we already have, it sends a `create_or_update(...)` to that host's **HostMeshAgent** saying "you should have a proc named `p0_0` (then `p0_1`, ...) with this create-rank."
2. Each host uses its embedded **BootstrapProcManager** to do the *real* OS-level thing: spawn a new child process, run `bootstrap_or_die()` in it, and have it come back as a proc for that host.
3. The parent waits for status from every host (so it doesn't return too early).
4. Finally it gathers all those per-host procs into a `ProcMesh`.

So this line in the test:

```rust
let proc_mesh = host_mesh
    .spawn(&instance, "p0", Extent::unity())
    .await
    .unwrap();
```

means: "for every host we just created from the allocation, start one real proc in its *own* OS process, and give me back a mesh of those procs."

Because the test uses `Extent::unity()`, that just means "1 per host."

---

### 4.2 Spawn actors into those procs

Next line of the test:

```rust
let actor_mesh: ActorMesh<testactor::TestActor> =
    proc_mesh.spawn(&instance, "a0", &()).await.unwrap();
```

Now we're fully inside the already-running procs. This call tells each proc:

- spawn an actor of type `TestActor`
- call it `"a0"`
- give me back a mesh of those actors

No new OS processes here — this is inside the existing proc that the host just spawned for us.

---

### 4.3 Prove messages flow

The test then opens a port on the **client** instance and broadcasts a message to the actor mesh:

```rust
let (port, mut rx) = instance.mailbox().open_port();
actor_mesh
    .cast(&instance, testactor::GetActorId(port.bind()))
    .unwrap();
let got_id = rx.recv().await.unwrap();
assert_eq!(
    got_id,
    actor_mesh.values().next().unwrap().actor_id().clone()
);
```

This is just "did we actually reach the actor we spawned in that far-away OS process?"

- the client opens a port
- sends `GetActorId` to all actors
- one replies
- we assert it's the one we expected

That proves the whole stack we just built (client → allocated proc → host → host-spawned proc → actor) is actually wired.

---

### 4.4 Shutdown

Last part of the test:

```rust
host_mesh.shutdown(&instance).await.expect("host shutdown");
```

This is important: the host is holding a `BootstrapProcManager`, and that thing is the one that really owns the PIDs of the procs it spawned. `shutdown(...)` walks the hosts, tells each agent to terminate its children, and drops the host. If you don't do this, you can leak the OS children.

---

### 4.5 Recap of layers

At this point the whole test looks like:

1. **client proc** in our test process
2. **process allocator** spawns N "v0 bootstrap" processes
3. **HostMesh::allocate(...)** turns each of those into a real **host** (via the trampoline)
4. **host_mesh.spawn(...)** uses each host's proc manager to spawn *another* OS process that becomes the service proc for our app
5. **proc_mesh.spawn(...)** (the actor step) runs entirely inside those service procs

One allocator pass to get remote runtimes, one host pass to make them into hosts, then normal host → proc → actor operations.
