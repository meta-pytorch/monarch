# Keep workers alive across controller crashes

`preserved_workers.py` is an independent, CPU-only companion to
`fault_tolerant_controller.py`. It hard-kills four controller processes during
outstanding worker calls and lets a fifth controller finish. Two worker actors
retain their original process IDs, full actor addresses, optimizer values, and
operation ledgers throughout the restarts.

See the [shared design write-up](RESTARTABLE_CONTROLLERS.md) for the tradeoffs,
the [checkpoint example](README.md) for whole-subtree recovery, and the
[implementation plan](PRESERVED_WORKERS_PLAN.md) for this example's scope.

From a checkout with a current Monarch installation:

```sh
python examples/fault_tolerant_controller/preserved_workers.py --seed 7
python examples/fault_tolerant_controller/stress_preserved_workers.py --negative
```

The stress driver runs seeds 7, 11, and 29, then checks that a hung final
controller and a skipped worker-failure probe both fail nonzero. Each integration
run has bounded endpoint, generation, supervision, and cleanup waits; the stress
driver also enforces an outer 150-second deadline. Use `--help` for individual
seed, kill-count, step-count, and deadline options. Linux `/proc` and a trusted
local machine are required. No GPU, Slurm, or external service is needed.

## Ownership and replacement

The actual Monarch actor ownership is:

```text
root client
└── Keeper
    ├── Worker[0] (persistent process)
    ├── Worker[1] (persistent process)
    └── Controller (external bootstrap process, replaced each generation)
```

Keeper also owns the worker process mesh. The token publishes only Keeper. A
fresh bootstrap connects and sends its `this_proc()` process mesh to
`Keeper.join`. That endpoint executes `procs.spawn(name, Controller, ...)` in Keeper's
actor context. Consequently Controller is supervised by Keeper even though its
OS process was started by `subprocess.Popen`. Connecting a token alone does not
establish that ownership, and this example does not adopt a bootstrap root.

The `Controller` actor plays the client role. Any children intended to stop with
that client must be spawned from its actor context. The persistent workers are
deliberately keeper-owned siblings. Unrelated actors on the joining process's
`this_proc()`, including its bootstrap root, are outside this shutdown boundary
and are ignored by this ownership policy. Keeper teardown does not guarantee
exit of the whole joining process; root/process adoption is deferred core work.

The driver waits for join and observes an outstanding worker request before
starting a seeded random timer. Timers alternate between short and longer ranges
so the run exercises both work before commit and committed updates awaiting a
reply. The kill itself is external SIGKILL, including when the application actor
is awaiting worker replies. It is not a self-kill at a safe loop boundary. The
JSON kill records show whether an operation was committed while still active.
The final generation runs uninterrupted.

At join, Keeper registers the actual Controller actor address with the workers
before any work can begin. The kill is unannounced to the workers. Before
replacement, the driver kills and reaps its OS process, waits for the matching
Controller mesh failure in `Keeper.__supervise__`, and drains old worker requests.
Only then does Keeper admit a new controller. This provides a single active controller for
this local hard-kill experiment; it does not fence a partitioned controller that
remains alive. Data supervision uses a three-second deadline to keep the example
quick. The driver rejects conflicting delivery or data-supervision environment
overrides so its deadlines stay meaningful.

## Lost replies and replay

Each worker has a toy optimizer value plus a ledger keyed by stable logical step,
independent of controller generation. Payloads come from stateless seeded replay.
The commit checks the existing payload and updates value and ledger without an
`await` between them. Replaying an identical operation returns its original
prefix result; reusing its ID with a changed payload raises `ValueError`.

A new controller queries every worker and replays from the smallest committed
step. This handles partial commits across ranks, as well as a commit whose reply
died with the previous controller. Old requests finish before replacement rather
than cancelling an update whose commit status is uncertain. An explicit final
probe also replays step zero and verifies its original result,
then rejects a changed payload with that same ID, without changing state. Final
checks require the exact expected ledger and optimizer value on both workers,
preventing missing or double-applied updates.

Workers use a delivery-failure hook only for selected recipient-loss and transport
failures whose destination port belongs to a registered full Controller actor
address. Controller-directed sends in this worker are replies; Keeper-directed
status and validation replies retain the default delivery policy. Registration
precedes work, so the policy also covers a reply that fails before supervision
detects the crash. Registration does not prove
that a recipient died. The hook counts accepted failures, and success requires
observing at least one. Handler, protocol, unallocated-port, oversized-frame, and
expiry failures retain Monarch's default policy, as do other actor addresses.
This application policy permits losing replies to registered sessions; raw
`PortRef` addresses do not survive process loss, and old reply receivers cannot
be reused. Replacement calls allocate fresh reply receivers.

After training, Keeper starts a long-running worker call and waits until every
rank confirms it is running. It then deliberately fails the worker actors. The
already-outstanding mesh call must raise `SupervisionError`, and Keeper must
receive the worker mesh's failure. This avoids sending new requests to actors
that are already known to have failed. Unexpected failures are not absorbed. The
probe fails actors rather than killing their OS processes, which would also fail
implicit runtime logging actors.

## Limits and cleanup

This is a bounded in-memory protocol demonstration, not a durable training
checkpoint or a complete optimizer implementation. Keeper failure, migration,
worker recovery, network partitions, untrusted clients, and arbitrary concurrent
controllers are outside its scope. An unbounded training job would need a bounded
ledger, explicit state retention, and an application-specific commit protocol.

The Python token API and actor mesh name access are private. Tokens grant access
to endpoints; use this only with trusted processes. All processes must use the
same Monarch version. The example stops owned processes and shuts down Monarch
in `finally`. Fallback cleanup signals only recorded worker PIDs that still match
both their Linux start time and a unique inherited run marker. A final check
requires those processes to be gone, including on the negative probes. A run that
needs emergency SIGKILL cleanup fails. The success marker is printed only after
explicit shutdown and cleanup checks pass.
