# Restart the controller and its training subtree

This CPU-only example restarts an entire training failure domain. The keeper
survives; every replacement controller creates fresh trainer and generator
processes and restores a complete checkpoint from outside that failure domain.
The final model, step, and generator RNG states must exactly match an
uninterrupted local reference.

See [the shared design write-up](RESTARTABLE_CONTROLLERS.md) for both recovery
models. The companion [preserved-worker example](preserved_workers.py) and its
[run guide](PRESERVED_WORKERS.md) keep workers under the keeper instead. The
[implementation plan](CHECKPOINT_PLAN.md) describes the checkpoint model here.

From the repository root, with Monarch installed in the active interpreter:

```bash
python examples/fault_tolerant_controller/fault_tolerant_controller.py
python examples/fault_tolerant_controller/fault_tolerant_controller.py --restarts 3 --steps 14 --random-kills --seed 7
python -m pytest examples/fault_tolerant_controller/test_fault_tolerant_controller.py -v
```

For a source checkout, put `python/` on `PYTHONPATH` and build the matching native
extension first. The source CLI adds its own example directory to the worker
import path. The same interpreter and Monarch version run every process.
Linux `/proc` is required for process identity and cleanup checks.

## Ownership and joining

```text
keeper OS process
  root client actor
    Keeper actor                         application supervision root
      Controller actor generation N      on an external controller process
        trainer ProcMesh + ActorMesh     on the keeper's surviving host
        generator ProcMesh + ActorMesh   on the keeper's surviving host
    token rendezvous actor

external controller OS process
  bootstrap root client                  infrastructure; not adopted
  Controller actor                       owned remotely by Keeper above
```

The keeper publishes its `Keeper` actor through the experimental private
`monarch._src.actor.token` API. An independently started controller bootstrap
joins that token and passes `this_proc()` to `Keeper.join`. **Inside its own
endpoint context**, Keeper spawns Controller on that offered process. That
spawn establishes application supervision. Token join only exchanges actor
references; the token peer is the bootstrap root, which is never adopted.
Launching an OS subprocess does not establish Monarch ownership either.

The `Controller` actor is the client shutdown boundary for this example. All
application children must be spawned from its actor context so stopping it also
stops their subtree. Other actors on the joining process's `this_proc()`, including
the independent bootstrap root, are outside this boundary and are ignored by
this ownership policy. Keeper teardown does not guarantee exit of that entire
process. Root/process adoption is deferred Monarch core work.

Controller borrows a reference to the keeper's host, then spawns both process
and actor meshes inside its own endpoint. Borrowing a host does not transfer
ownership: the actor performing each spawn owns the newly created resource.
The underlying keeper-owned host survives the controller, so Monarch's host
agent can reap orphaned worker processes after a hard controller-process SIGKILL.

## Recovery checks

Each checkpoint atomically replaces a trusted local pickle in the keeper's
temporary directory. It includes the next trainer step, model weight, and
`random.Random.getstate()` for every generator, including the Gaussian cache.
The controller commits after each training step, then deliberately dies after
sampling the next step without committing it. Recreating generators from only
their initial seeds would produce a different result and fail verification.

Before replacement, the keeper requires the old subprocess's SIGKILL exit,
a `MeshFailure` for exactly that controller mesh, and natural exit of every old
worker identified by pid and Linux process start time. It also searches for
unrecorded processes with the unique run marker. Replacement identities must
be distinct. Every intermediate checkpoint and the final complete checkpoint
are compared with the uninterrupted reference. The normal run exits zero only
when all checks pass and cleanup needs no emergency signals.

The deterministic mode crashes after two committed steps per failed generation.
`--random-kills` chooses one to three committed steps per generation from the
specified seed. Both modes perform all kills after the heartbeat startup wait.
Use `--steps` greater than three times `--restarts` to leave recovery work.
`--restarts 0` runs the uninterrupted actor baseline.

## Timing and limits

The example sets delivery timeout to 3 seconds, data supervision and orphan
timeouts to 8 seconds, process-exit timeout to 3 seconds, and supervision polling
to 1 second. Conflicting environment overrides are rejected. Ordinary calls
have a 30-second deadline, natural subtree cleanup has 45 seconds, and each
controller generation has `--controller-timeout` seconds (default 90).

Fault injection waits three seconds after workers start so initial ownership
heartbeats can arm host-side orphan expiry. There is currently no public
readiness acknowledgement for that heartbeat. This wait follows the existing
hard-exit cleanup test; it is not a guarantee that an arbitrary crash during
worker startup is safe. In particular, a proc whose owner dies before the host
receives its first keepalive can lack an armed orphan-expiry timer. Such a leak
fails this example and triggers emergency cleanup; it never counts as recovery.
The example does not fix that runtime startup edge case.

The keeper remains alive on the same host throughout. There is no keeper
migration, durable storage service, concurrent-controller fencing, or recovery
from arbitrary partial external side effects. Tokens grant access to private
endpoints and appear in process arguments; use trusted processes and a trusted
network. Checkpoint pickles must also come from trusted writers.

## Negative checks and cleanup

These commands must exit nonzero:

```bash
# Erase generator RNG progress in the persisted checkpoint after the first kill.
python examples/fault_tolerant_controller/fault_tolerant_controller.py --inject checkpoint
# Final controller stalls; keeper kills it at its deadline and cleans up.
python examples/fault_tolerant_controller/fault_tolerant_controller.py --restarts 0 --inject hang --controller-timeout 10
```

Every exit path attempts Monarch context shutdown. If live processes with the
run's marker remain, emergency cleanup validates each pid/start-time identity
before SIGKILL and forces a failing result. The temporary checkpoint directory
is removed on exit. The tests cover complete RNG replay, atomic-write failure,
invalid checkpoints, successful repeated recovery, corrupted recovery, timeout,
and the absence of leftover run processes.
