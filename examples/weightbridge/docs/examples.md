# Examples

`examples/` is a complete, **runnable** 2-node Ray demo that transfers weights from a trainer layout to
a different rollout layout on a toy model. It is the concrete companion to
[integration.md](integration.md): every step of the contract appears here in real code, small enough to
read end to end.

This example demonstrates the API and validates data movement; it is not a production benchmark recipe.
Choose topology, transport, and memory settings for the target cluster rather than treating its defaults
as a performance configuration.

## What it demonstrates

- A trainer runtime and a rollout runtime with **different parameter names and layouts**, bridged with
  no hand-written shard math.
- `LoadSpec` inference discovering, by probing each side's `load_weights`: QKV merge, gate/up merge,
  row-parallel and column-parallel slices, vocab slices, and a **transpose** (the rollout stores
  `down_proj` transposed, exercising the negative-width transpose path).
- The standalone coordinator process, the poll-driven receiver, and connect-once / send-per-update on
  the trainer.
- End-to-end **correctness verification** (see [below](#what-it-verifies)).

## Files

| file | role |
|---|---|
| `qwen_tiny.py` | a one-block Qwen2-style toy: builds the HF checkpoint and the two runtime layouts (trainer = Megatron-style packed `linear_qkv`/`linear_fc1`; rollout = SGLang-style stacked `qkv_proj`/`gate_up_proj`, transposed `down_proj`) plus each side's `load_weights`. |
| `utils.py` | `make_hf_weights` (build the `hf_weights` factory dict + `hf_shapes` from a CPU checkpoint) and Ray node discovery. |
| `workers.py` | the Ray actors: `RolloutEngine` (spawns the coordinator + `RolloutWorker`s), `RolloutWorker` (builds a `ReceiverAdapter`, polls), `TrainerWorker`/`TrainerEngine` (build a `SenderAdapter`, connect + send). This is the integration reference. |
| `train.py` | entrypoint: pins Ray nodes, starts both engines, runs one transfer, verifies correctness and (via `check_transports`) that each leg used the transport its placement implies. |

## Running it

The example needs **2 nodes with ≥ 4 GPUs total** (2 trainer + 2 rollout workers). Install Ray and a
Mooncake build suitable for the selected transport before starting it. If EFA libraries are not on the
system loader path, set `LD_LIBRARY_PATH` before launching Ray; the example forwards the existing value
unchanged rather than assuming a vendor-specific installation directory.

From the directory containing `pyproject.toml`, `pip install -e '.[examples]'` installs the declared
example dependencies. An EFA run may preinstall its fabric-specific Mooncake wheel first.

```bash
# on each node, after the environment is set up:
ray start --head              # node A
ray start --address=<A:port>  # node B

# transfer over Mooncake TCP (default; works without EFA):
python examples/train.py

# on an AWS EFA cluster, use the RDMA fabric:
python examples/train.py --network-provider efa --network-interface <efa-iface>
```

Useful flags/env: `--rollout-ip`/`--trainer-ip` (pin which Ray node is which), `--rollout-port`
(the per-engine integer the coordinator endpoints derive from), `--network-interface`
(`NCCL_/GLOO_SOCKET_IFNAME` for the Gloo rendezvous), and `WB_VISIBLE_DEVICES` for replay utilities that
manually place several ranks inside one Ray actor. See [Environment Variables](environment.md) for the
buffering, topology, transport, and diagnostic switches inherited by workers.

### Orchestrator: Ray (default) or Monarch

`--orchestrator {ray,monarch}` selects the actor framework the workers run in; **Ray is the default**.
This is not cosmetic — the Monarch wbridge transport is only reachable from inside a Monarch actor
(`RDMABuffer`/`RDMAAction` resolve `context()`), so Ray workers can never host it. Ray is the reference
path for the Mooncake transports.

Monarch is not a WeightBridge package dependency. Install the `torchmonarch` version supplied by the
target runtime only when using this backend or its examples.

The roles are identical either way: `worker_bodies.py` holds all the actual work, and `workers.py` /
`workers_monarch.py` are thin actor wrappers around it.

A Monarch run needs worker loops already serving on each node. Start those loops with the scheduler or
process manager used by your cluster, then pass their addresses explicitly:

```bash
python examples/train.py \
  --orchestrator monarch \
  --protocol monarch \
  --monarch-workers tcp://<trainer-host>:<port>,tcp://<rollout-host>:<port>
```

`--protocol {auto,tcp,efa,monarch}` picks the wbridge backend independently of the orchestrator; `auto`
follows `--network-provider`. `--protocol monarch` requires `--orchestrator monarch`.

Two Monarch quirks the example handles, both of which cost a debugging cycle to find: worker `init` is a
single **broadcast** (`.call()`), because `ControlChannel`'s constructor is a rendezvous and initializing
ranks one at a time deadlocks; and each actor explicitly `set_device`s its own GPU, because Monarch's
`spawn_procs({"gpus": N})` — unlike Ray's `num_gpus=1` — does not set `CUDA_VISIBLE_DEVICES`, so every
rank would otherwise land on device 0 (and share one NIC).

### Co-located config: validating the NVLink bypass

`--colocate` (or `WB_COLOCATE=1`) pins **both** engines to one Ray node — the placement in which
WeightBridge skips the network RDMA backend and moves weights with a direct CUDA-IPC copy over NVLink
([architecture](architecture.md#data-plane-one-sided-rdma)). It needs **1 node with ≥ 4 free GPUs**:

```bash
ray start --head
python examples/train.py --colocate                     # asserts the bypass engaged
WBRIDGE_SAME_NODE_IPC=0 python examples/train.py --colocate   # A/B: forced through the RDMA backend
```

Either way the run ends with a `check_transports` report and a hard assertion. The evidence is byte
counters, not log lines: `wire_rdma_bytes` is incremented at the `engine.write_async` call site and
`wire_ipc_bytes` at the CUDA-IPC `copy_`, so a co-located run must report `wire_rdma_bytes == 0` with
`wire_ipc_bytes > 0` on **every** trainer and rollout worker — i.e. the transfer engine carried no
weight bytes at all. `agh_*` is the same split for the rollout↔rollout dedup exchange. Flags are
excluded: they are 8 bytes and use the selected control path. Per-GPU NVLink link counts (via NVML) are
printed alongside so you can see what the IPC copies ran over; they are reported rather than asserted because
NVML reports NVSwitch endpoints, not peer GPUs, on HGX-class nodes.

Running **without** `--colocate` exercises the negative control: the same assertion inverts and
requires that nothing took the IPC path.

## How it maps to the integration contract

Reading `workers.py` against [integration.md](integration.md):

- **Adapter context (both sides)** — each worker builds the four data fields from the toy model:
  `make_hf_weights(...)` → `hf_weights` + `hf_shapes`; `wksd_factory=lambda: self.state_dict` (the toy
  loader copies in place); `load_weights` = the toy loader wrapped to take the fetcher dict
  (`_wrap_load_weights`). Constructing the adapter runs inference + verification.
- **Coordinator** — `RolloutEngine.init` derives `coordinator_ipc(port)` and calls
  `coordinator.spawn(ipc, port)` once; workers connect their `ReceiverAdapter` to the same IPC.
- **Receiver polling** — `RolloutWorker.recv_weights` loops on `poll_requests()`; a production runtime
  passes `before_receive` to quiesce local inference before weights are mutated
  (the example polls until ready; a real engine calls this once per scheduler tick, on all ranks).
- **Sender** — `TrainerWorker` builds `SenderArgs` with `receiver_urls=["tcp://host:rollout_port"]`,
  constructs a `SenderAdapter`, calls `connect()` once, then `send_weights()`.
- **Rank rules** — `num_workers=num_rollout_workers` and per-worker `rank` (the toy is TP-only, so
  `num_workers = TP`).

To adapt this to a real framework, replace `qwen_tiny.py`'s toy layouts/loaders with your runtime's
real `state_dict` and `load_weights`, and place the receiver poll in your scheduler loop instead of the
`recv_weights` loop. A framework adapter does exactly that.

## What it verifies

Each `RolloutWorker` snapshots its loaded weights, then after the transfer `verify()` checks every
rollout shard against the expected value. Because receive buffers are zero-initialized, a silent
no-op transfer produces zeros and fails the check — so a passing run is real evidence that bytes moved
and landed in the right place (see [validation](limitations.md#validation--a-green-run-is-not-a-successful-transfer)).
