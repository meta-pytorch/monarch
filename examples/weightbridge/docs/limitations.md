# Assumptions, Limitations & Pitfalls

Read this before your first real run. Several failure modes here are **silent** — the job exits 0 while
no weights actually moved — so the section ends with how to validate.

Treat the values in this document as library defaults or observed operating points, not as a portable
benchmark profile. Freeze and validate the complete environment in the experiment artifact for each
deployment.

---

## `LoadSpec` inference

WeightBridge learns your layout by probing your `load_weights` (see
[architecture](architecture.md#metadata-plane-gloo-once)). That probe is valid only when:

1. **`load_weights` is a pure, bijective copy/scatter for same-dtype elements.** Each destination
   element is an exact bitwise copy of exactly one source element. Only layout operations are allowed
   (`copy_`, `narrow`, `select`, `view`, `reshape`, `permute`, `transpose`, `split`, `chunk`, `cat`,
   `index_select`, `scatter_`, `contiguous`). **Any arithmetic on values** — scaling, addition,
   clamping, quantization, normalization — corrupts the probe. Post-load hooks that requantize are
   fine *as long as* they run **after** `load_weights` returns (they usually do).
2. **Same dtype on both sides** for each `(hf_name, worker_name)` pair. The probe encodes indices using
   the worker tensor's element size; a differing HF element size corrupts them. Trust an inferred spec
   only for same-dtype source/dest pairs.
3. **The full name→factory dict is passed each call** (some loaders do cross-rank comms, e.g.
   broadcasting tied embeddings under PP > 1).
4. **Shard mappings are rectangular**, between tensors of equal dimensionality; each contiguous
   component is a rectangular box; >2D tensors map as a single component.

**Verified** for mainstream dense and MoE models (Llama, Qwen, Mistral, DeepSeek, GLM) in bf16.
**Known exceptions** (arithmetic inside `load_weights`, so incompatible unless bypassed): Gemma-1
(`+= 1.0` on norms), AMD-HIP INT4 MoE scaling, DeepSeek NvFP4 requant, `flash_rl` per-token-group FP8
quant, BitsAndBytes 4-bit. `verify_load_spec` (run at adapter construction) will fail loudly if your
loader violates these — trust that assertion.

---

## Pitfalls

### 1. The in-process receiver must enter every step in lockstep
The receiver is a thread inside each rollout worker process, not a separate process. If model-parallel
ranks enter WeightBridge's blocking step at different times, one rank can be inside a WeightBridge
collective while another is still inside your runtime's own TP/EP collective → two communicators
entered in opposite order → a **silent, intermittent deadlock**. **Avoid:**
drive `poll_requests()` on **all** ranks every tick via rank 0's hub broadcast
(the built-in behavior) — never per-rank/self-scheduled, and keep the control notification on the main
thread. This is *the* headline constraint.

### 2. Never pause the rollout engine around the transfer
A "pause generation" bracket inside the update path deadlocks the flag handshake (a paused receiver
won't complete its lockstep role); `pause`/`flush` return 200 OK, masking it. **Avoid:** enforce
on-policy freshness by draining in-flight rollouts at the orchestration layer *before* the update.

### 3. Mooncake on EFA
- **Mooncake must be built with EFA support.** Mooncake is the default RDMA backend; on an EFA fabric
  select it with `protocol="efa"` (native RC-QP `"rdma"` does not work on EFA — no reliable-connection
  QPs), or `protocol="tcp"` for a local/toy run (the engine sets `MC_FORCE_TCP=1`). Supplying an
  EFA-capable Mooncake build, and a runtime in which it can actually reach the EFA fabric, is an
  **environment/deployment responsibility** that is intentionally out of scope for this library doc —
  see your stack's own setup/deployment guide for the concrete steps.
- **Use Mooncake `>=0.3.12.post1` and keep `MC_EFA_CQ_THREADS=1`.** Older EFA builds start one
  busy-polling completion-queue thread for every discovered EFA device. With one WeightBridge endpoint
  per GPU this can produce hundreds of pollers per node, contend with NCCL and trainer CPU work, and make
  the integration appear to slow compute even while weight transfer itself is fast. The upstream bounded
  worker pool removes that cliff; `MC_EFA_CQ_THREADS=0` is only a diagnostic legacy-behavior control.
- **A mis-deployed transport can silently move zero bytes** — Mooncake can report `initialize`
  success yet enumerate no usable device, so transfers no-op while the run stays green. Never treat a
  clean exit as proof bytes moved; always gate with the equality check (see *Validation*).
- **Bandwidth needs striping.** EFA round-robins one NIC per transfer request, so a single big write can
  ride only one path. WeightBridge stripes large writes according to `WBRIDGE_EFA_SUBSLICE_BYTES`;
  the default performed well on the evaluated cluster, but should be retuned for a different fabric.
- **Host (CPU-staged) RDMA can be substantially slower than GPUDirect** and needs each rank pinned to a
  distinct switch-local NIC with a NUMA-local staging buffer; bulk and flags must use separate NICs or a
  saturating bulk write can starve the flag handshake. Prefer the default straight-from-GPU path unless
  HBM headroom forces staging, and measure the tradeoff on the target cluster.

### 4. Monarch transport
Selected with `protocol="monarch"`. It is only reachable from inside a Monarch actor, and it imposes
constraints on the *surrounding process* that Mooncake does not. Each of the first three fails **without
a usable error** — a hang, or a panic that is not an exception.

- **Do not enable `PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True`** — despite Monarch warning at
  import that it is "required to maximize RDMA performance with CUDA tensors". A CUDA VMM segment is
  mapped from physical granules piecewise and one ibverbs MR cannot span such a range, so large writes
  fail in a repeatable periodic pattern (`IBV_WC_REM_ACCESS_ERR` / `IBV_WC_LOC_PROT_ERR`). **Small
  transfers do not trip it**, so a toy example passes and the
  real model does not — and the failure reads like an addressing bug, not an allocator one. This also
  puts the bandwidth that warning promises out of reach.
- **Never block the actor's event loop.** An RDMA completion arrives as a *message to the submitting
  actor*, so an endpoint that blocks its own loop waiting on that completion can never observe it: the
  transfer wedges forever with nothing in any log. Run all WeightBridge work on a **dedicated thread per
  actor** — one thread, not a pool, because `torch.cuda.set_device` is thread-local and a pool scatters
  ranks onto device 0. `MonarchEngine.wait()` raises rather than hangs if called on a live loop.
- **The actor context is a `contextvar` and does not reach WeightBridge's own threads** (the sender's
  RDMA thread and the receiver's progress/waiter threads). Calling from a thread without it trips a Rust
  panic that is **not an `Exception` subclass** — `except Exception` will not catch it — and it poisons
  the actor's dispatch loop, making it unrecoverable rather than degradable. `MonarchEngine` installs the
  context on every thread it is called from, and installs the context *value* rather than reusing a
  shared captured `Context`: a `Context` cannot be entered twice, so two threads submitting concurrently
  would raise `cannot enter context: already entered`.
- **Staging is unsupported** — `protocol="monarch"` with sender or receiver staging raises at connect.
  Beyond the missing plumbing it would be a poor trade: Monarch hash-spreads *host* memory across NICs
  instead of pinning it to the GPU-local one, and a CPU-pinned destination measured substantially worse.
- **One NIC per GPU, by design.** Device memory is bound to the NIC co-located with its GPU, so there is
  no equivalent of Mooncake's configurable sub-slicing across the switch's NICs. Per-rank bandwidth is one
  NIC's worth. Monarch wins a like-for-like comparison at equal NIC count; on a fabric with several NICs
  per GPU, striping is worth more than that margin.
- **Bring-up quirks** — mesh transport configuration, passing peer meshes as constructor arguments,
  awaiting mesh initialization before pickling — belong to the orchestration layer rather than the
  transport; see [examples](examples.md#orchestrator-monarch-default-or-ray).

### 5. Transport performance traps
Latent in any backend whose completions are delivered through Python, and both are easy to measure past.

- **Batch throughput and peer-progress latency are different objectives.** A synchronized fan-out may run
  faster as one backend action: on Monarch, splitting destinations barely moved median bandwidth but
  materially reduced the slowest rank's throughput. WeightSender instead submits each
  destination independently because its ACK and DATA-ready boundary is peer-specific; otherwise one slow
  receiver stalls every unrelated receiver. Always measure end-to-end worker blocking as well as aggregate
  bandwidth when changing this tradeoff; a median-only benchmark misses both peer head-of-line blocking and
  the batched-action straggler spread.
- **CPU-bound Python in the same process can throttle RDMA.** Where a completion is delivered as an actor
  message it needs the GIL to be observed: the bytes have landed but the wait cannot return. On a
  synthetic ladder, competing CPU-bound Python work severely reduced Monarch's bandwidth. The
  representative workload was *not* in this regime — `sys.setswitchinterval` recovered the synthetic
  loss and changed nothing meaningful on the real run — but any deployment that adds a busy Python thread
  to a rollout worker should re-measure. It is GIL *handoff latency*, not CPU starvation, so shortening
  the switch interval is the lever (`WBRIDGE_MONARCH_SWITCH_INTERVAL`, off by default because it is
  process-global and costs context switches in compute-heavy Python). Mooncake polls its completion queue
  in C++ with the GIL released and is structurally immune.

### 6. De-duplication correctness
Replica-aware transfer is subtle; the built-in implementation handles it, but if you touch it:
receiver dedup must key on the **whole** receive spec (not per-tensor), consume must use an
**intersection** mapping (a partial `1/m` slice is not "contained"), the receiver↔receiver exchange
must preserve exact source ownership, and reused external ingress slots need an aggregate
**consumed-flag** handshake (a fast writer must not overwrite a slot until self and every local
internal-consume reader kernel have finished).
Reductions must be deterministic across ranks (sorted keys) or the flag ping-pong desyncs.

### 7. Layout / transpose / dtype
- **Transpose** must be honored on **every** copy path (it is encoded as a negative width in the shard
  tuple). A model that stores a tensor transposed relative to the checkpoint (e.g. a RowParallel
  `down_proj`) will be silently wrong if a copy path ignores it. The bundled path handles transpose;
  the example deliberately includes a transposed tensor to exercise it.
- **Split shards on a contiguous axis.** Splitting the longest axis of a wide tensor yields strided
  sub-shards and a substantially slower copy kernel. (Correctness-neutral, large perf trap; the planner
  auto-picks contiguous vs. strided.)
- **Multi-destination HF names use the widest destination dtype** for the wire buffer.

### 8. Ordering & synchronization
- There is no transport notify primitive: cross-node "done"/"consumed" signals are tiny flag writes the
  peer polls. A blocking write returns only after data lands, which makes the subsequent flag safe —
  preserve **data write → flag write** and the globally-monotonic sequence. Same-node replica peers publish
  that sequence through shared CPU memory, but must still preserve **CUDA event record → CPU sequence →
  imported event wait → internal-consume kernel**.
- The RDMA engine is unordered w.r.t. CUDA kernels — synchronize after the fused pack before writing.
- Cached copy plans bind to **address-stable** buffers (in-place params, reused wire buffers).

### 9. Memory / OOM
Wire buffers and the arena scale with the round cap, all inside the rollout's free HBM. At receiver depth
`D`, the topology arena is `D × (RECV + fused own/send prepare)` plus up to `D` exact cross-node GRECV ingress
slots per active source; same-node internal-consume columns have no local staging allocation. The generic
fallback retains the same peer-partitioned, parity-buffered GRECV bank.
Tune `WBRIDGE_ROUND_CAP_BYTES` to free HBM; allocations near 80% of the remaining free HBM emit a warning.
Keep logical/pack buffers transient or fused — don't cache many persistent ones. Real-weight validation can also hit a **cgroup** memory limit
(not host OOM) mid-registration; lean the config if so.

---

## Validation — a green run is not a successful transfer

The single most dangerous failure class is a **silent no-op** (zero-transfer from a libfabric conflict,
or a swallowed transport return code). Do not trust timings or exit codes alone:

- Validate with a **zero-init equality check**: on the rollout, snapshot params → reset → transfer →
  compare. Receive buffers are zero-initialized, so a no-op yields zeros and the compare fails. (The
  example does exactly this via `verify_all`.)
- Add hard asserts on every transport return code (`initialize`/`register_memory`: `!= 0` is error;
  `batch_transfer_sync_write`: `< 0` is error).
- Confirm the fabric is actually in use before trusting bandwidth: under Mooncake, `FI_PROVIDER=efa` and
  the `efa` provider in the logs. Under Monarch this one is already enforced — `MonarchEngine.init`
  refuses to start unless the backend reports `ibverbs`, because a silent TCP fallback measures the wrong
  thing while looking healthy.
- Benchmark **steady state**, not the first transfer — connect, registration, and warmup impose a large
  one-time cost, while later transfers stabilize.

---

## Current simplifications

These are implementation choices, not fundamental limits:

- **Transfers are split into byte-capped rounds, run in sequence.** Within a round, packing and consume
  are *fully fused* — the transpose-aware model↔wire copy (both packing stages collapsed into one plan)
  replays as O(1) Triton launches per dtype group, not tensor-by-tensor. The remaining granularity is
  the round: a transfer is chunked to `WBRIDGE_ROUND_CAP_BYTES` (default 2 GiB) and the cap must be
  sized to the rollout's free HBM (wire buffers are double-buffered, the dedup arena scales with the
  cap; see the Memory / OOM pitfall above).
- **Pipelining is partial.** The sender's RDMA overlaps the next training step (background thread +
  double-buffered wire buffers), and on the receiver the sender's RDMA-fill of the next round's landing
  zone overlaps current work (the zones are disjoint). The depth-2/3-stage dispatcher may keep both prepared
  parity rounds' E+C work active; exact per-destination generation gates serialize only reuse of the same
  stable slot, while source-slot kernels remain readiness-driven and independent. The **pack runs synchronously on the
  caller's thread**, so its cost sits on the training critical path. Optional CPU **staging** (off by default) overlaps the
  transfer with generation at the cost of substantially lower host-RDMA bandwidth; the validated default transfers
  straight from GPU.
- **Cross-engine / data-parallel matching is the caller's responsibility.** The router computes shard
  overlaps and deduplicates *within* a replica class, but does not infer cross-engine replica
  equivalence or decide which trainer ranks feed which rollout engine. A tensor replicated wider than
  its class is under-deduplicated (sent more than once) — never incorrect, just less bandwidth saved.
- **Teardown relies on process death** (daemon threads + the coordinator's parent-death watchdog); there
  is no wired-in graceful shutdown — call `WeightReceiver.stop()` and terminate the coordinator `Popen`
  if you need one.
