# Integration Guide

This guide is framework-agnostic: follow it to wire WeightBridge into **any** well-formed RL stack
that has a training runtime (the *trainer*, which produces updated weights) and an
inference/generation runtime (the *rollout engine*, which must periodically refresh its weights).
WeightBridge does not parse your model or own your scheduler — you describe your runtime through a
small **adapter context**, and drive two calls from your loops.

If you learn best from code, read this alongside [`examples/`](examples.md), a complete, runnable
2-node Ray demo that implements everything below on a toy model.

The toy example is an API demonstration, not a performance configuration. Before benchmarking, freeze
the target cluster's placement, transport environment, library versions, and measurement window in the
experiment artifact rather than in the reusable library tree.

---

## The three pieces

| Piece | How many | What it is |
|---|---|---|
| **Coordinator** | one per rollout engine | a standalone ZMQ relay **process** you spawn; not part of any worker |
| **Receiver** (`ReceiverAdapter`) | one per rollout **model-parallel rank** | lives inside each rollout worker process; receives + loads weights |
| **Sender** (`SenderAdapter`) | one per trainer **shard-owning rank** | lives inside each trainer worker process; packs + sends weights |

You keep these long-lived: build once, reuse for every update. Never construct them per update, and
never lazily from a request handler.

---

## The integration contract: `AdapterContext`

WeightBridge never introspects your model. On **both** sides you supply an `AdapterContext` — five
things — and it infers everything else (the layout mapping, the routing, the RDMA plan) from them:

```python
from wbridge.frontend.adapters import AdapterContext

ctx = AdapterContext(
    hf_weights=hf_weights,        # dict[str, Callable[[], Tensor]]  — HF name → zero-arg factory
    hf_shapes=hf_shapes,          # dict[str, tuple[int, ...]]       — HF name → shape
    wksd_factory=wksd_factory,    # Callable[[], dict[str, Tensor]]  — fresh GPU worker-state-dict
    load_weights=load_weights,    # Callable[[HFWeightFetcher], Any] — your native loader, adapted
    rank=rank,                    # int                              — this adapter's rank
)
```

Requirements — each is load-bearing:

1. **`hf_weights`** — maps every HuggingFace checkpoint tensor name to a **zero-arg factory** returning
   that tensor (on CPU is fine). The factory is called **many times** (probe, restore, verify) and must
   be re-callable and side-effect-free — yield a fresh/cloned tensor if the underlying storage is
   shared. For a standard safetensors checkpoint, don't write this yourself:
   `hf_weights, hf_shapes = wbridge.utils.specgen.hf_weights_from_checkpoint(path)` builds both from a
   metadata-only scan (O(1) per-tensor reads).
2. **`hf_shapes`** — HF name → shape; used to size the index-probe tensors.
3. **`wksd_factory`** — returns a **fresh snapshot of your live GPU worker state dict** on each call.
   It must be a *factory*, not a captured dict, because inference happens after the loader runs and the
   loader may *replace* parameter tensors (e.g. MoE expert fusion). If your loader does strictly
   in-place `copy_`, `lambda: wksd` is sufficient; if it swaps tensors, pass the model's `state_dict`
   method. Values must be CUDA tensors.
4. **`load_weights`** — your framework's **existing** weight loader, adapted to the signature
   `load_weights(hf_weights: dict[str, Callable[[], Tensor]]) -> Any` and writing into your params.
   WeightBridge probes it to learn your layout, so it must satisfy two properties:
   - a **pure, bijective copy/scatter** for same-dtype elements — only layout ops (`copy_`, `narrow`,
     `view`, `reshape`, `permute`, `transpose`, `split`, `cat`, `index_select`, `scatter_`, …), never
     arithmetic on values (no scaling, quantization, normalization inside `load_weights`);
   - it accepts the **complete** name→factory dict each call (some loaders do cross-rank comms, e.g.
     broadcasting tied embeddings, and break on a partial dict).
   Most mainstream dense/MoE loaders already satisfy this in bf16; see
   [limitations](limitations.md#loadspec-inference) for the assumptions and known exceptions.
5. **`rank`** — this adapter's rank (see [rank rules](#rank--worker-count-rules)).

Constructing a `SenderAdapter`/`ReceiverAdapter` with the context runs the whole metadata plane
(`infer_load_spec` → `verify_load_spec`) at construction. `verify_load_spec` reconstructs your worker
tensors from the checkpoint through the inferred spec and asserts byte-equality, so a bad adapter
context fails loudly at build time, not silently at transfer time.

> **Both sides must present the same source format.** specgen infers one canonical spec (from the HF
> checkpoint) and assumes the trainer and the rollout each expose weights that map to *that same*
> reference. If the two sides expose **different source formats** — e.g. one presents its raw
> parallel/native layout while the other presents the HF reference layout — specgen aligns them against
> mismatched references: the transfer completes but **merged and grouped tensors silently come out
> wrong** (fused QKV, fused gate/up, and especially **grouped MoE experts**), failing the equality
> check while simpler tensors still pass. Note that `verify_load_spec` validates the *receiver* against
> the checkpoint, so it will **not** catch a trainer that exposes an unexpected source format — you must
> ensure the trainer exposes weights in the **same reference format** the rollout loads from.

> Writing an adapter for a new framework is essentially: wrap your existing `load_weights`, expose the
> HF factories + a state-dict snapshot. You write **no** shard/layout math — it is inferred.

---

## Rank & worker-count rules

- **Trainer:** every rank that owns a shard of the model is a sender; `SenderArgs.world_size` is the
  number of such ranks. `rank` is that rank.
- **Rollout:** one receiver per **model-parallel** process. `rank` and `num_workers` must count the
  full model-parallel grid: **`num_workers = TP × PP`**, and `rank = TP_size × PP_rank + TP_rank`
  (the global model-parallel rank). Do **not** multiply by data-parallel size (DP replicas share the
  TP processes) or expert-parallel size (experts reshard within TP ranks). Keying on bare `TP_rank`
  with PP > 1 collides ranks and spawns a coordinator per stage.

---

## Step-by-step

### Rollout side (receiver)

1. **Choose one deterministic integer per engine** (e.g. your inference server's port) and derive the
   control endpoints from it so trainer and all ranks agree with no handoff:
   ```python
   from wbridge.backend.control_channel import coordinator_ipc, coordinator_tcp_port
   ipc = coordinator_ipc(port)
   ```
2. **On rank 0 only, spawn the coordinator process** (once per engine):
   ```python
   from wbridge.backend import coordinator
   coord_proc = coordinator.spawn(ipc, coordinator_tcp_port(port))  # detached subprocess
   ```
3. **In every model-parallel worker process**, after weights are loaded but **before** other large
   allocations (so specgen + RDMA registration have free HBM), build the context and the receiver:
   ```python
   from wbridge.frontend.adapters import ReceiverAdapter
   adapter = ReceiverAdapter(ctx, ipc, num_workers=TP*PP)   # rank came from ctx
   ```
4. **Drive the receiver from your scheduler/generation loop, on every tick, on all ranks in
   lockstep:**
   ```python
   pending_epoch = [None]

   def before_receive(epoch):
       pending_epoch[0] = epoch
       quiesce_and_cancel_requests(epoch)

   if adapter.poll_requests(before_receive=before_receive):
       record_rollout_consume_end(pending_epoch[0])
       record_rollout_block_end()
       adapter.flush_profile_outputs()
   ```
   Put this call in *every* path the loop can take — including any idle/paused wait where no batches
   are running — so an update can land whenever the trainer sends one. `poll_requests()` performs the
   control round, may complete deferred connect setup, and performs the blocking receive+load only when
   there is work. Its optional hook receives the update epoch and runs after GO but before weights are
   mutated. The call returns `True` only when weights were applied.

### Trainer side (sender)

5. **Expose each engine's coordinator URL to trainer rank 0** as `tcp://{host}:{coordinator_tcp_port(port)}`
   (rank 0 collects one per engine).
6. **Rank 0 picks a Gloo rendezvous** (its own host + a free port) and broadcasts
   `[receiver_urls, master_addr, master_port]` to all trainer ranks (over your existing trainer group).
7. **Every trainer rank builds the context and the sender:**
   ```python
   from wbridge.backend.sender import SenderArgs
   from wbridge.frontend.adapters import SenderAdapter
   sender_args = SenderArgs(
       world_size=num_trainer_ranks,
       receiver_urls=receiver_urls,     # ["tcp://host:port", ...], one per engine
       master_addr=master_addr,
       master_port=master_port,
       protocol=rdma_protocol,           # "efa" for Mooncake/EFA or "monarch" for Monarch RDMA
   )
   adapter = SenderAdapter(ctx, sender_args)
   ```
8. **All trainer ranks call `connect()` once** (barrier first). While they do, the **rollout side must
   already be calling** `poll_requests()`, because connect is driven through the poll loop: the
   coordinator relays `connect` to receiver rank 0, which joins the same Gloo rendezvous during a
   `poll_requests()` call. WeightBridge handles the ordering internally (fire connect → join Gloo → gather
   acks); your only obligation is that the rollout loop is running.

### Per update

9. **Trainer:** each time the rollout engine should get a fresh policy version:
   ```python
   record_trainer_send_start(epoch)
   ev = adapter.send_weights()   # packs on the calling thread; RDMA runs in the background
   if ev is not None:
       ev.synchronize()          # weights are now safe to overwrite for the next training step
   record_trainer_block_end()
   adapter.flush_profile_outputs()
   ```
   That's it — no collective with the rollout engine, no pause.
10. **Rollout:** nothing extra — the sender's first data-ready flag wakes rank 0, and the poll from step 4
    applies the update. There is no normal per-update trainer→coordinator doorbell.

### EWTT measurement hooks

For an end-to-end weight-transfer measurement, use application-owned structured events around the two
API calls above:

- Emit `send_start(epoch)` on every participating trainer rank immediately before its
  `send_weights()` call, after any pre-send barrier. Do not place it at the outer update entry or before
  unrelated pause/preparation work.
- Emit `consume_end(epoch)` on every rollout model-parallel rank immediately after
  `poll_requests()` returns `True`, before generation resumes, scheduler bookkeeping runs, or profiling
  output is flushed.
- Compute `EWTT(epoch) = max(consume_end wall time) - min(send_start wall time)` across all trainer
  ranks, rollout ranks, and rollout engines. Use a synchronized cross-host wall clock such as
  `time.time_ns()` for the endpoints; retain `time.monotonic_ns()` only for within-process diagnostics.
- Measure performance with expensive equality/debug gates disabled. Use a separate gate-on run as
  correctness evidence, because readback and completion waits change the measured interval.

This boundary includes packing, background delivery, receive/assemble, and the native weight load. It
does not confuse the sender's pack-handoff event with completion, which is important because
`send_weights()` is intentionally asynchronous.

---

## Ordering & correctness constraints

These follow directly from the [threading model](architecture.md#threading-model). Violating them
deadlocks or silently corrupts weights.

- **Enter in lockstep.** Call `poll_requests()` on **all** model-parallel ranks
  every tick, driven by rank 0's broadcast. Never self-schedule per rank, never skip ranks.
- **Treat GO as quiescence.** Rank 0 emits GO on its own first GPU-ingress readiness. Every peer stops
  scheduler/TP work on that decision, then waits for its own readiness before assemble/repack; it must not
  continue inference independently after rank 0 leaves the TP path. Each external-slot internal-consume kernel
  then depends only on that exact slot's READY. A per-slot cyclic aggregate CONS protects its next external
  generation after every local reader kernel finishes; unrelated slots and ranks impose no exchange barrier.
- **Never pause the rollout engine around the transfer.** A blanket "pause generation" bracket inside
  the update path deadlocks the flag handshake (a paused receiver won't complete its lockstep role).
  Enforce on-policy freshness at the **orchestration layer** — drain in-flight rollouts *before*
  triggering the update — not by pausing the engine.
- **Keep the rollout loop running during `connect()`.** Connect is a rendezvous between trainer rank 0
  and receiver rank 0; if the rollout side isn't polling, connect blocks.
- **`send_weights()` returns after packing, not after delivery.** Call `ev.synchronize()` before you
  overwrite the packed weights. Delivery overlaps your next step; only debug flows need
  `wait_send_complete()`.

## Reconnect & teardown

- **Reconnect** (e.g. new rollout engines appear): re-run the connect path. WeightBridge tears down
  the old RDMA engine, unregisters buffers, and destroys the Gloo group before rebuilding, so
  `connect()` is idempotent.
- **Teardown** currently relies on process death: the sender's RDMA thread and the receiver's daemons
  are `daemon=True`, and the coordinator process self-exits when its spawning parent dies. For
  deterministic shutdown call `WeightReceiver.stop()` and terminate the coordinator `Popen` yourself.

## Sync vs. async

The same calls serve both modes; only *where you place them* differs:

- **Sync / on-policy:** the trainer calls `send_weights()` at the update barrier; the rollout applies
  it before continuing. Achieve on-policy by draining rollouts before the update.
- **Async / off-policy:** the trainer returns after `ev.synchronize()` and continues training while
  the RDMA overlaps; the rollout applies the update whenever its poll next reports ready.

## Redundancy checklist (common mistakes)

- **Don't** call `connect()` before every send — connect once, then only `send_weights()`.
- **Don't** create an HTTP server or route weight readiness through your engine's internal messaging —
  the coordinator is a process and the receiver polls.
- **Don't** construct adapters/coordinator lazily from request handlers — build them at init.
- **Don't** wrap the transfer in a generation pause (see above).
- **Don't** hand-write shard math — provide a correct `load_weights` and let specgen infer it.

For the pitfalls that cause silent failures (transport zero-transfer, dedup correctness, transpose/dtype),
read [limitations.md](limitations.md) before your first real run. For per-class API details, see
[api.md](api.md).
