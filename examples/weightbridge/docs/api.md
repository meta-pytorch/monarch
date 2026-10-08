# API Reference

Practical reference for the classes and helpers an integrator uses. See the source modules for
lower-level transport internals. Import the **frontend adapters** for the normal integration path;
they don't pull in any framework dependency.

API signatures are deployment-neutral. Select transport and buffer settings for your environment before
starting workers; see [Environment Variables](environment.md) and the validation checklist in
[Assumptions, Limitations & Pitfalls](limitations.md).

```python
from wbridge.frontend.adapters import AdapterContext, SenderAdapter, ReceiverAdapter
from wbridge.backend.sender import SenderArgs
from wbridge.backend import coordinator
from wbridge.backend.control_channel import coordinator_ipc, coordinator_tcp_port
from wbridge.utils.specgen import hf_weights_from_checkpoint
```

---

## `AdapterContext`

Framework-neutral inputs; the same dataclass is used on both sides. Fields (all required):

| field | type | meaning |
|---|---|---|
| `hf_weights` | `dict[str, Callable[[], Tensor]]` | HF tensor name → zero-arg factory. Called many times; must be re-callable and side-effect-free. |
| `hf_shapes` | `dict[str, tuple[int, ...]]` | HF tensor name → shape. |
| `wksd_factory` | `Callable[[], dict[str, Tensor]]` | returns a **fresh** GPU worker-state-dict snapshot each call. |
| `load_weights` | `Callable[[HFWeightFetcher], Any]` | your native loader, adapted to take the fetcher dict; pure bijective copy/scatter for same-dtype elements. |
| `rank` | `int` | this adapter's rank. |

See [integration.md](integration.md#the-integration-contract-adaptercontext) for the exact
requirements. (Older docs mentioned `hf_iter_factory`, `wksd`, or `load_spec_path` — those no longer
exist.)

## `SenderArgs`

Transport configuration for the sender.

| field | type | meaning |
|---|---|---|
| `world_size` | `int` | number of trainer sender ranks in the connect group. |
| `receiver_urls` | `list[str]` | `tcp://host:port` of each rollout engine's coordinator (one per engine). |
| `master_addr` | `str` | trainer rank-0 host for the one-time Gloo metadata rendezvous. |
| `master_port` | `int` | TCP port for that Gloo group. |
| `protocol` | `str = "efa"` | RDMA backend: `"efa"` (Mooncake on an EFA cluster), `"tcp"` (Mooncake, local/toy runs), or `"monarch"` (libibverbs via Monarch; requires Monarch-actor workers and rules out staging). See [transport backends](architecture.md#transport-backends). |
| `sender_staging` | `bool = False` | pack → CPU offload → RDMA-from-CPU pipeline; off = straight-from-GPU (validated default). |

## `SenderAdapter`

Trainer-side integration. Construction runs `LoadSpec` inference + verification and builds a
`WeightSender`.

```python
adapter = SenderAdapter(ctx, sender_args)
adapter.connect()                     # once, before the first update
record_send_start()                   # optional application EWTT marker
ev = adapter.send_weights()           # per update; returns a CUDA event (or None off-GPU)
if ev is not None: ev.synchronize()   # weights safe to overwrite
# Record the application's trainer block_end here, then:
adapter.flush_profile_outputs()
```

- `connect() -> None` — queries each coordinator, joins the Gloo rendezvous, registers RDMA buffers,
  starts the background RDMA thread. Call **once**; reuse for all updates.
- `send_weights() -> torch.cuda.Event | None` — packs on the calling thread, hands RDMA to the
  background thread, returns an event that fires when the packed weights are safe to overwrite. One
  call = one update.
- `wait_send_complete() -> None` — blocks until the last update is delivered and consumed by all
  receivers. Only needed for debug/equality checks; production skips it so RDMA overlaps the next step.
- `flush_profile_outputs() -> None` — release Gantt/control profiling output for the latest epoch. Call
  only after recording the application's trainer `block_end`; it does not wait for background delivery.

## `ReceiverAdapter`

Rollout-side integration. Construction runs `LoadSpec` inference + verification and builds a
`WeightReceiver` (eagerly, so it can handshake before transfer).

```python
adapter = ReceiverAdapter(ctx, controller_ipc_name, num_workers=TP*PP, receiver_staging=False)
updated = adapter.poll_requests(before_receive=quiesce_runtime)
if updated:
    record_consume_end()              # optional application EWTT marker
    # Record the application's rollout block_end here, then:
    adapter.flush_profile_outputs()
```

- `poll_requests(before_receive: Callable[[int], None] | None = None) -> bool` — performs one control
  round and services its action on the main thread, called every scheduler tick on all ranks. Rank 0
  peeks its first-round CPU flag and broadcasts KICK/GO. For a real update, `before_receive(epoch)` runs
  synchronously on every model-parallel rank after GO but before model weights are mutated, allowing the
  runtime to quiesce inference and cancel admitted requests. The callback must preserve rank lockstep and
  must not start asymmetric TP/EP work. Empty polls, staging KICKs, and deferred connection setup return
  `False`; a completed receive+load returns `True`.
- **EWTT timing hook:** record a trainer `send_start` immediately before `send_weights()` and a rollout
  `consume_end` immediately after `poll_requests()` returns `True`. Across a distributed deployment,
  EWTT is the earliest trainer `send_start` to the latest rollout-rank `consume_end`. Use a cross-host
  wall clock for those endpoints; a process-local monotonic clock cannot be subtracted across nodes.
- `flush_profile_outputs() -> None` — release Gantt JSONL, control-profile logs, and a requested receiver
  chrome trace for the latest epoch. Call after recording rollout `block_end`.
- `num_workers` = the engine's model-parallel process count (`TP × PP`); `receiver_staging` enables the
  full-epoch CPU landing pipeline. KICK leaves generation running; GO follows rank 0's completed first-round
  H2D, and every peer then waits for its own corresponding host-side flag.

## Coordinator (control plane)

A standalone ZMQ relay **process**, one per rollout engine. It is not HTTP and holds no torch.

```python
from wbridge.backend import coordinator
from wbridge.backend.control_channel import coordinator_ipc, coordinator_tcp_port

ipc = coordinator_ipc(port)                                   # rank0-facing IPC endpoint
proc = coordinator.spawn(ipc, coordinator_tcp_port(port))     # detached subprocess (Popen)
```

- `coordinator.spawn(ipc_name, tcp_port) -> subprocess.Popen` — launches the coordinator; it self-exits
  when the spawning process dies.
- `coordinator_ipc(port)`, `coordinator_tcp_port(port)`, `hub_addr(port)` — derive the IPC path, the
  trainer-facing TCP port, and the rank0↔peers hub endpoint deterministically from one per-engine
  integer, so no address handoff is needed. The trainer connects to `tcp://{host}:{coordinator_tcp_port(port)}`.
- `WeightReceiverController` — the class the coordinator process runs (`__init__(ipc_name, tcp_endpoint)`,
  `serve()`, `close()`). You normally use `coordinator.spawn`, not this directly. There is **no**
  `set_worker_num` and **no** HTTP route — rank 0 auto-registers the receiver count.

## Backend endpoints

`SenderAdapter`/`ReceiverAdapter` own these; use them directly only for a bespoke integration.

- `WeightSender(args, rank, shard_spec, load_spec, wksd)` — `connect()`, `send()`, `wait_send_complete()`.
- `WeightReceiver(controller_ipc_name, rank, shard_spec, dtype_spec, load_spec, wksd, *, num_workers, receiver_staging=False)`
  — `poll_requests()`, `stop()`.

## Metadata helpers

From `wbridge.utils.specgen`:

- `hf_weights_from_checkpoint(hf_path) -> (HFWeightFetcher, dict[str, shape])` — builds `hf_weights` +
  `hf_shapes` from a safetensors (or single-`.bin`) checkpoint via a metadata-only scan.
- `infer_load_spec(hf_weights, hf_shapes, wksd_factory, load_weights) -> LoadSpec` — probe-based layout
  inference (run for you by the adapters).
- `verify_load_spec(hf_weights, wksd_factory, load_spec) -> None` — reconstructs and asserts
  byte-equality (also run by the adapters).
- Type aliases: `HFWeightFetcher = dict[str, Callable[[], Tensor]]`, `LoadWeightsFn`, `WksdFactory`.

`ShardSpec`, `BoundShardSpec`, `LoadSpec`, `CopyPlan` (in `wbridge.utils.data`) are the internal
data-plane types; the adapters produce and consume them — integrators rarely touch them directly.

## Reference adapters

These are **concrete examples** of the adapter pattern, not part of the framework-agnostic API. Read
them as copy-me templates when writing an adapter for your own runtime; import them directly so
environments without those dependencies don't import them.

WeightBridge does not declare any inference or training framework as a package dependency. A framework
adapter subclasses `SenderAdapter` or `ReceiverAdapter` and supplies `hf_weights`, `wksd_factory`, and
`load_weights` for that runtime.

Each shows the same pattern: wrap an existing `load_weights`, expose HF factories + a state-dict
snapshot, then defer to `SenderAdapter`/`ReceiverAdapter`.
