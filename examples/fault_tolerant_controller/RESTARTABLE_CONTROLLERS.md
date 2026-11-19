# Restartable controllers

A restartable controller needs a surviving owner that can decide what to rebuild and enough recoverable application state to do so correctly. The useful boundary is the unit of work that can be restarted consistently. Keeping a Python process alive, or reconnecting to an actor reference, does not establish that boundary.

The two examples explore different choices. [Checkpoint recovery](fault_tolerant_controller.py) restarts the controller and its workers together. [Preserved workers](preserved_workers.py) keeps workers under a surviving owner and replaces only the controller. The first is the recommended starting point for training. The second requires a worker protocol designed for controller replacement.

## Ownership defines the failure boundary

Monarch's supervised ownership tree gives each child one owner. The normal stop-on-owner-loss policy makes the owner's death a reason to stop its descendants. Remote cleanup is asynchronous and depends on failure detection and orphan keepalives; the process-startup gap described below limits the current implementation's guarantee. A child's failure reaches its parent's `__supervise__` handler. Handling it keeps the parent alive; leaving it unhandled allows failure to escalate. See the [actor guide](../../docs/source/actors.md), [Python spawn implementation](../../python/monarch/_src/actor/proc_mesh.py), and [data mesh supervision](../../hyperactor_mesh/src/proc_mesh.rs).

These rules make a controller that owns every application actor a natural application failure boundary. Restarting that controller reconstructs a subtree. It cannot recover its old local variables, pending calls, or descendants merely by running its constructor again. Recovery needs an explicit source of truth and a surviving actor responsible for choosing the replacement.

This is also the relevant lesson from Erlang/OTP. An OTP supervisor starts, monitors, stops, and restarts its children according to child specifications; when it terminates, its children terminate, and a higher supervisor may restart it. Those specifications describe how to start processes, not how to restore arbitrary application state. A newly started supervisor obtains its initial specifications through `init/1`; specifications added dynamically to the old supervisor are not a durable inventory automatically restored with it. Applications must reconstruct that inventory when needed. See the official [supervision principles](https://www.erlang.org/doc/system/sup_princ.html) and [`supervisor` API](https://www.erlang.org/doc/apps/stdlib/supervisor.html).

## Separate the keeper from the application controller

A keeper owns the job's lifetime, allocation references, and root supervision policy. An application controller is its real supervised child and owns whichever application work should share its fate. Controller death means the keeper may recover the application. An explicit `stop_job` decision means the keeper stops the job, including any preserved workers, and must not interpret that stop as a reason to restart it.

In these examples an independent Python process connects through a private token and offers its `this_proc()` mesh. The keeper invokes `spawn` on that offered mesh from inside its own endpoint. The resulting application Controller actor therefore belongs to the keeper even though it runs in the offered process. The controller can spawn workers on a borrowed keeper-host reference: the host remains alive while the controller's worker subtree is cleaned up.

[Token connection](../../python/monarch/_src/actor/token.py) only exchanges references to existing actors. It does not adopt the connecting root actor. Likewise, an operating system parent-child relationship is not Monarch supervision. The ownership established by the keeper's spawn is what makes application failure reporting meaningful.

For now, the Controller actor plays the client role and defines the shutdown boundary. Stopping that actor, including through keeper teardown, stops its owned descendants under Monarch's existing ownership policy. All application work that should share the client's lifetime must be spawned from that actor's context. Other actors on the joining process's `this_proc()`, including its independent bootstrap root, are explicitly outside this boundary and are ignored by the example's ownership policy. There is no guarantee that the joining process itself exits when the keeper stops. Workers deliberately preserved across controller replacement are instead owned directly by the keeper. Adopting the joining root or process is deferred Monarch core work; these examples make no such core change.

These scoped application examples use ordinary actors, ownership, and a failure handler, without controller-specific recovery machinery in the framework. The application chooses whether workers belong beneath the controller or beside it, and the keeper implements the corresponding restart policy.

## Recommended scenario: restart the whole training domain

For training whose workers communicate or advance together, put the controller, trainers, generators, and communication state in one restartable domain. Keep a consistent checkpoint outside that subtree. On failure, the keeper waits until the old generation has stopped, or fences it from every shared resource before authorizing another generation. It then starts fresh workers and restores the last committed checkpoint.

A complete checkpoint includes the model, optimizer, progress, random number generator state, and data cursor as applicable. Restoring only a model weight can repeat or skip input, change random sampling, or apply an update twice. The checkpoint must represent a consistent boundary across participants. Old communicators, pending futures, channels, and reply ports are discarded; they are not part of the restored application state.

The checkpoint example uses a small deterministic training loop, including the generator's complete random state, so recovery can be compared with uninterrupted execution. Its local checkpoint directory survives the application controller and worker subtree. This illustrates controller recovery on a surviving host; it is not a claim of recovery after losing that directory or its machine. A production application needs a checkpoint store that survives its intended failures.

Uncheckpointed external effects can repeat after rollback. Metrics, object writes, billing, and published results need their own idempotency or commit rules if repeats matter. A successful restart establishes the application's chosen state invariant; successful actor creation alone is insufficient.

## Preserved workers need an explicit recovery protocol

A different topology puts workers and controllers beneath the keeper as siblings. Controller failure then leaves the workers alive. This can be useful for expensive worker initialization, independent services, or bounded operations whose completion can be queried. Survival alone does not make interrupted work recoverable.

A controller's receive port and waiter disappear with its process. A `PortRef` contains the process incarnation (`ProcId`), so a replacement cannot transparently reopen that same address. Dropping remote sender references does not close a worker's receiver. Sending a late reply to a vanished receiver can also fail the worker unless the protocol handles that path. Shared or serialized references preserve addresses, not the old controller's execution context.

A recoverable protocol must invalidate the old session, reconnect reply paths, reject stale authority, and expose enough worker state to determine what completed. Give each logical operation a stable identity across controller retries and retain the required deduplication result. A controller generation or epoch identifies authority; it does not identify a logical operation. Incrementing the epoch on every restart cannot by itself prevent repeating an already completed operation.

The preserved-workers example demonstrates a deliberately bounded protocol and checks worker identities across replacements. It is designed for repeated hard controller-process kills, where the old process is known dead. It does not establish fencing against a live controller isolated by a network partition. Nor does per-worker deduplication repair a collective dispatched to only some ranks: those workers may already be blocked waiting for participants that never entered. Existing arbitrary workers should not be assumed recoverable without reviewing their communication and side-effect protocols.

## Keeper failover is a separate design problem

These examples keep the keeper alive. Adding another supervisor above it only moves the ownership boundary: if keeper death still cascades through its descendants, its replacement cannot preserve those descendants through policy alone.

Preserving a job across keeper replacement would require a recoverable logical owner scope with durable inventory and intent, retained failure reporting and reconciliation, and exclusive authority enforced by fencing on every control path. The infrastructure scheduler must also survive the relevant failure. A terminal job tombstone must prevent a later owner from reviving an explicitly stopped job. There should be exactly one logical root and at most one authorized keeper; a period with no authorized keeper may be necessary during transfer.

That design changes the intended owner-death cleanup contract for the retained scope. Existing detach or relink primitives may support parts of it, but they do not supply durable ownership, authority transfer, or recovery reconciliation by themselves. Keeper migration and partition-tolerant ownership are outside this work.

## Process cleanup has a startup gap

The current [host-agent implementation](../../hyperactor_mesh/src/host_mesh/host_agent.rs) creates pending process state with `expiry_time: None` and carries that value into `ProcCreationState::Created`. An owner heartbeat through `KeepaliveGetState` arms the deadline; `HostAgent`'s `SelfCheck` only reaps created processes with an expired deadline. A crash before that initial heartbeat can therefore leave process service roots without an orphan expiry, even when application actors have stopped.

The hard-kill validation waits for heartbeat startup before injecting failure. It demonstrates cleanup after that step; arbitrary crashes during process startup are not proven safe. This is a limitation of the current process cleanup path, not a recovery guarantee supplied by the examples.

## What verification should establish

Verification should kill the actual controller process, observe the keeper's `__supervise__` notification, and inspect the chosen worker boundary. For checkpoint recovery, old worker processes must exit before replacement, and final complete state must match an uninterrupted reference. For preserved workers, identities must stay unchanged while the protocol converges correctly after interrupted operations. Repeated kills should cover different points between dispatch, mutation, reply, and checkpoint commit.

Failure detection and cleanup have separate timers; shortening the orphan timeout alone does not shorten data mesh supervision. Check process exit rather than assuming stopped application actors imply stopped process services. Consult each example's commands and reported checks for its validation. Passing a hard-kill test supports the specified recovery boundary and injection points, not arbitrary failures or transparent continuation of lost calls.
