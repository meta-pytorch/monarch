# Appendix: `bootstrap_canonical_simple`

This is the exact test from the codebase that the walkthrough above is unpacking.

```rust
// from hyperactor_mesh/src/bootstrap.rs (tests)
#[tokio::test]
async fn bootstrap_canonical_simple() {
    // SAFETY: unit-test scoped
    unsafe {
        std::env::set_var("HYPERACTOR_MESH_BOOTSTRAP_ENABLE_PDEATHSIG", "false");
    }

    // 1) Create a "root" direct-addressed proc.
    let proc = Proc::direct(ChannelTransport::Unix.any(), "root".to_string())
        .await
        .unwrap();

    // 2) Create an actor instance we'll use to send and receive messages.
    let (instance, _handle) = proc.instance("client").unwrap();

    // 3) Configure a ProcessAllocator with the bootstrap binary.
    let mut allocator = ProcessAllocator::new(Command::new(crate::testresource::get(
        "monarch/hyperactor_mesh/bootstrap",
    )));

    // 4) Request a new allocation of procs from the ProcessAllocator.
    let alloc = allocator
        .allocate(AllocSpec {
            extent: extent!(replicas = 1),
            constraints: Default::default(),
            proc_name: None,
            transport: ChannelTransport::Unix,
        })
        .await
        .unwrap();

    // 5) Build a HostMesh with explicit OS-process boundaries (per rank):
    //
    // (1) Allocator → bootstrap proc [OS process #1]
    //     `ProcMesh::allocate(..)` starts one OS process per
    //     rank; each runs our runtime and the trampoline actor.
    //
    // (2) Host::serve(..) sets up a Host in the same OS process
    //     (no new process). It binds front/back channels, creates
    //     an in-process service proc (`Proc::new(..)`), and
    //     stores the `BootstrapProcManager` for later spawns.
    //
    // (3) Install HostMeshAgent (still no new OS process).
    //     `host.system_proc().spawn::<HostMeshAgent>("agent",
    //     host).await?` creates the HostMeshAgent actor in that
    //     service proc.
    //
    // (4) Collect & assemble. The trampoline returns a
    //     direct-addressed `ActorRef<HostMeshAgent>`; we collect
    //     one per rank and assemble a `HostMesh`.
    //
    // Note: When the Host is later asked to start a proc
    // (`host.spawn(name)`), it calls `ProcManager::spawn` on the
    // stored `BootstrapProcManager`, which does a
    // `Command::spawn()` to launch a new OS child process for
    // that proc.
    let host_mesh = HostMesh::allocate(&instance, Box::new(alloc), "test", None)
        .await
        .unwrap();

    // 6) Spawn a ProcMesh named "p0" on the host mesh:
    //
    // (1) Each HostMeshAgent (running inside its host's service
    //     proc) receives the request.
    //
    // (2) The Host calls into its `BootstrapProcManager::spawn`,
    //     which does `Command::spawn()` to launch a brand-new OS
    //     process for the proc.
    //
    // (3) Inside that new process, bootstrap runs and a
    //     `ProcMeshAgent` is started to manage it.
    //
    // (4) We collect the per-host procs into a `ProcMesh` and
    //     return it.
    let proc_mesh = host_mesh
        .spawn(&instance, "p0", Extent::unity())
        .await
        .unwrap();

    // 7) Spawn an ActorMesh<TestActor> named "a0" on the proc mesh:
    //
    // (1) For each proc (already running in its own OS process),
    //     the `ProcMeshAgent` receives the request.
    //
    // (2) It spawns a `TestActor` inside that existing proc (no
    //     new OS process).
    //
    // (3) The per-proc actors are collected into an
    //     `ActorMesh<TestActor>` and returned.
    let actor_mesh: ActorMesh<testactor::TestActor> =
        proc_mesh.spawn(&instance, "a0", &()).await.unwrap();

    // 8) Open a fresh port on the client instance and send a
    //    GetActorId message to the actor mesh. Each TestActor will
    //    reply with its actor ID to the bound port. Receive one
    //    reply and assert it matches the ID of the (single) actor in
    //    the mesh.
    let (port, mut rx) = instance.mailbox().open_port();
    actor_mesh
        .cast(&instance, testactor::GetActorId(port.bind()))
        .unwrap();
    let got_id = rx.recv().await.unwrap();
    assert_eq!(
        got_id,
        actor_mesh.values().next().unwrap().actor_id().clone()
    );

    // 9) Important: shut down, or we'll leak the OS children the hosts spawned.
    host_mesh.shutdown(&instance).await.expect("host shutdown");
}
```
