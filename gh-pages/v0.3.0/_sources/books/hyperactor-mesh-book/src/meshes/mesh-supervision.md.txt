#  Error Handling by Supervision

When you spawn procs and actors, you need to know when they encounter errors, and
have a chance to recover from them. This process is called "supervision", because
the owner of the resource (meshes of hosts, procs, actors) supervises them
to make sure they are making progress.

## Principles of ownership
We start with a set of principles we want to be true about ownership of resources,
such that we can make the guarantees for a good user experience:

1. Creating new meshes always results in an owned mesh. For example, if the holder of a ProcMeshRef spawns an actor, that operation returns an owned ActorMesh.
2. All meshes are owned by at most one actor. We do not support transfer or suspension of ownership. An actor can own multiple resources.
3. A mesh cannot outlive its owner – that is, when the owner dies or stops, so does the mesh.
4. If a mesh dies, it will no longer handle any messages except for a final "cleanup" message. A stopped mesh will accept no new message, but drain all of its existing pending messages before cleanup. An owned mesh will run cleanup before its owner runs cleanup.

From these principles, we can also derive that ownership follows a strict hierarchy.
We can always draw a single path from any mesh to some "root" owner. This makes
the supervision hierarchy a tree.

The upshot of these is that all meshes form a single hierarchy, and there are no orphan (managed) objects in the system: all failures must propagate.

The following is a further axiomitization, separating actor supervision from mesh lifecycle management:

### Actors
Actors form a strict hierarchy of ownership. Typically the root of the hierarchy is the main script's client actor. We say that an actor is owned by its parent. A root actor is not owned.
When an actor fails, the failure propagates to its parent. The parent can handle the failure. When a failure is not handled, the parent actor fails. (Which causes its failure to be propagated in turn.) When the root actor fails, it invokes the user fault handler, which by default will exit the process.

### Meshes:
All (host, proc, actor) meshes are owned by an actor. In turn, the lifecycle of the mesh is tied to that actor. If the actor stops (whether through failure or grace), then its own meshes are also stopped.
Mesh lifecycle events (for example, the failure of an actor in an actor mesh) generate failures that are also propagated to its owner actor; the rules from above apply.

### Notes
It is possible for there to be multiple roots. For example, in “SPMD mode”, each rank could have their own root client. (In this case, the fate of the clients are all tied together, since SPMD schedulers will kill/restart the whole job on a single-node failure.)


## Supervision
From these follow the rules of supervision. We say that the owner of a mesh _supervises_ the mesh. This means that it is responsible for responding to _failures_ in that mesh. In principle, it does not matter how failures are instigated; the supervisor is always responsible for determining how to handle it.
Another important principle at play here is _encapsulation_: only the owner of a mesh knows how to handle mesh failure. No other component should need to know.
Thus, when a mesh fails in some way (for example, an actor failed), the owning actor receives a failure notification. In response to such failures, the actor must recover from the failure. If it fails to do so (either because it did not handle the event, or because it failed to recover the mesh properly), the _actor itself must fail_, cascading the failure up the ownership hierarchy. This causes all meshes owned by the actor to also fail. It is then up to the owner of the mesh to which that actor belongs to recover again.
This is similar to exception handling mechanisms: one can choose to handle exceptions at the appropriate level in the hierarchy (call stack), and failure to handle an exception propagates the failure (and thus terminates the remainder of functions in the call stack).

## Supervision Python API
Actors can handle failures by providing an implementation of the `__supervise__` method:
```py
class ManagerActor(Actor):
  def __init__(self, worker_procs: ProcMesh):
    # "workers" is now owned by this actor. If this actor fails, then
    # the "workers" mesh will also fail.
    self.workers = worker_procs.spawn("workers", WorkerActor)

  def __supervise__(self, failure: MeshFailure) -> bool:
    # This is invoked whenever an owned mesh fails.
    # The failure object can be stringified, and will contain the error message,
    # the actor name, and (sometimes) the rank that failed, if it can be determined.
    # The full API can be found on the MeshFailure typing stub:
    # monarch/python/monarch/_rust_bindings/monarch_hyperactor/supervision.pyi

    logging.error(f"failure encountered: {failure}")

    # To handle the event, return a truthy object. To consider it unhandled,
    # return a falsey object (such as None or False).
    return None
```

`__supervise__` is special: Because it handles "exceptions", we have to be able
to invoke it at any (safe) point. This is because otherwise we might run into a
deadlock: for example, an actor might be waiting for a result from a failed actor.
Thus, we define safe points (e.g., waiting for channel receives) at which we may
safely invoke the supervision handler. This means that __supervise__ handlers have
to be written carefully: it can potentially change the state of the actor in the middle of handling a message.

If `__supervise__` returns a truthy value, the failure will be considered handled
and not delivered further up the chain. If it returns a falsey value (including None, if there is no return),
the failure will be delivered to that Actor’s owner recursively until it reaches
the original client. If it reaches the original client with no handling, it will crash.
If `__supervise__` raises an Exception of any kind, it will be considered a new supervision event to be delivered to that Actor’s owner. Its cause will be set to the supervision event it was handling. This behavior matches the special method `__exit__` for context managers.

We make no guarantees about how many times `__supervise__` will be called.
If you own a mesh of N actors, each of which generates a supervision error, it may be called anywhere between 1 and N times inclusive.


## Supervision Rust API
Add the following to your Rust Actor:
```rust
#[hyperactor::export(handlers = [MeshFailure])]
struct MyActor {
    // ...
}

#[async_trait]
impl Handler<MeshFailure> for MyActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        failure: MeshFailure,
    ) -> Result<(), anyhow::Error> {
        // Any Ok result will consider the message handled. Any Err result will
        // propagate to the owner of this Actor.
        // You can use this default handler which returns an Err with
        // UnhandledSupervisionEvent, which chains the messages together.
        failure.default_handler()
    }
}
```

In order to spawn an actor mesh, you need to have this handler implemented on
the context you pass in to ProcMesh::spawn. Else, there will be a type error.


## Actor Lifecycle
Actors transition once and only once from running to either stopped or died.
An actor is considered to have died if:
- An endpoint for the actor throws an exception when there is no response port.  All code running in the main thread of the client counts as an 'endpoint' for the purpose of failure.
- The process on which the actor is running fails.
- It broadcasts a message (no response port), and that message is not delivered.
- For the purpose of failure handling a Proc is treated as an actor with no endpoints, and no children. Its parent is the spawning actor. A Proc fails by exiting with a non-zero error code, or by failing to respond to a heartbeat.

An actor is considered stopped if:
- Someone has called stop() on the actor.

A stopped actor is not an error, and is part of the normal lifecycle. It becomes
an error only if some reference tries to use a stopped actor (but that is an error
on the user actor, not the stopped one).

## Liveness
A related problem is _liveness_: an actor can get stuck if it is waiting for a
message that will never arrive. For example, if it is waiting for a response from an actor that is dead.
WARNING: This whole section is not yet implemented.

Monarch should provide the guarantee that, in these circumstances, awaiting the receive will result in an error.
We should do this while also allowing for different communication patterns:
direct RPC, cast/accumulate, and actors doing more esoteric things like passing ports to other actors, or storing port references for later reply. Because of Monarch’s flexibility, there isn’t a single, straight-forward definition of "liveness" for the purposes of a port, so we need to instead be in the business of providing a sensible mechanism, and have good default behavior.

### Port Linking
In order to ensure liveness, we propose to implement _port linking_:
```rust
impl PortRef {
  /// Link the lifetime of an actor to this port.
  fn link(&self, caps: &impl IsActor);
  /// Unlink this actor from the port.
  fn unlink(&self, caps: &impl IsActor);
  /// Atomically re-link the port from `from` to this actor.
  fn relink(&self, caps: &impl IsActor, from: ActorId);
}
```
When a port is linked, the port handle is alive when all (usually just one) of its linked actors are alive. Linkage also modifies a port’s receive behavior:

```rust
impl<T> PortHandle<T> {
  /// Returns whether the port is alive.
  fn is_alive(&self) -> bool;

  /// Returns a RecvError::LinkFailure when a linked actor fails while waiting
  /// for a message from this port.
  async fn recv(&self) -> Result<T, RecvError>;
}

```

With these primitives, we can flexibly support all of the above communication patterns. For example:
1. Simple RPC: we simply link the recipient actor, which subsequently does not modify linkage.
2. Multicast: we link the root of the tree. Each node in turn rewrites (through bind/unbind) the ports to maintain a hierarchy of links. Link failure propagates through the hierarchy.


## Implementation
This supervision hierarchy is handled in two parts:
- `ActorSupervisionEvent`: which is events happening within a single proc
  or an Actor. These are the root causes of errors
- `MeshFailure`: which are events happening to a mesh of hosts, procs, or actors.
  These are caused by ActorSupervisionEvents.

You can remember this via their names + module paths: ActorSupervisionEvent is in hyperactor,
and MeshFailure is in hyperactor_mesh.

This supervision hierarchy is handled in two parts:
- `ActorSupervisionEvent`: which is events happening within a single proc
  and an Actor. These are the root causes of errors
- `MeshFailure`: which are events happening to a mesh of hosts, procs, or actors.
  These are caused by ActorSupervisionEvents.

You can remember this via their module paths: ActorSupervisionEvent is in hyperactor,
and MeshFailure is in hyperactor_mesh.

ActorSupervisionEvent is propagated in hyperactor/src/proc.rs, in InstanceCell::send_supervision_event_or_crash
and its callers. It can be handled by an actor that implements `handle_supervision_event`.
Events are first bubbled up to parent actors in the same proc, before being sent
to the `ProcMeshAgent` which collects all the events on a proc.

MeshSupervisionEvents are created by the set of MeshControllers, such as ActorMeshController
and ProcMeshController in hyperactor_mesh/src/v1/mesh_controller.rs.
They aggregate events from ProcMeshAgents they query, and deliver the events to
their owners.

The MeshController also ensures the lifecycle principles, when an actor stops,
part of its cleanup is running the cleanup of all child MeshControllers, which
are spawned for any new mesh.

Finally, the MeshController is also responsible for sending out these events
to subscribers, which are not owners of a mesh but are using it as a reference
and want to know when there is an error that occurs.
