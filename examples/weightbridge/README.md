# WeightBridge

WeightBridge is a reusable RL weight-transfer library. It moves updated policy weights from **Trainer
Workers** (the training runtime) to **Rollout Workers** (the inference/generation runtime) when the two
sides use different parameter names, tensor layouts, or parallelism strategies. It infers how each
runtime maps to a common HuggingFace-checkpoint coordinate system, computes exactly which byte ranges
each receiver needs, and moves only those bytes over one-sided RDMA.

## Why WeightBridge?

- **Bandwidth-efficient.** Replaces all-gather/broadcast weight sync with shard-routed, deduplicated,
  one-sided RDMA transfer — each receiver gets only the slices it needs.
- **Layout- and framework-agnostic.** Trainer and rollout may use different names, tensor-parallel or
  expert-parallel layouts, and merged tensors (QKV, gate/up). No model- or engine-specific mapping is
  hard-coded; the layout is *inferred* by probing your existing `load_weights`.
- **One integration surface** for collocated, disaggregated-sync, and disaggregated-async modes.
- **Narrow scope.** It is the transfer layer only — not an RL framework, scheduler, or inference engine.

## Installation

From this directory:

```bash
pip install -e .
```

The base install provides the framework-independent package. Optional imports are grouped by use case:

```bash
pip install -e '.[examples]'          # runnable Ray examples
pip install -e '.[mooncake,cuda]'     # WeightBridge transfer runtime
pip install -e '.[checkpoints]'       # direct safetensors checkpoint loading
pip install -e '.[test]'
```

The `mooncake` extra installs Mooncake `>= 0.3.12.post1`; EFA deployments may instead preinstall a
compatible EFA-enabled wheel with the same distribution name.

Monarch is deliberately not a package dependency. It is needed only when importing the Monarch
backend/examples. Install the version supplied by the surrounding training or serving environment;
WeightBridge's framework-independent imports do not load it.

WeightBridge intentionally does not ship a paper- or cluster-specific deployment profile. Record exact
model, topology, container, and transport settings with the experiment that uses them. The examples inherit
the caller's loader environment and accept node/interface choices explicitly.

## Tiny integration sketch

WeightBridge never parses your model — you describe each runtime with an `AdapterContext` (HF weight
factories, a fresh worker-state-dict snapshot, and your native `load_weights`), then poll from the
rollout scheduler and send from the trainer.

```python
# --- Rollout engine: one coordinator process per engine ---
from wbridge.backend import coordinator
from wbridge.backend.control_channel import coordinator_ipc, coordinator_tcp_port
ipc = coordinator_ipc(server_port)
coordinator.spawn(ipc, coordinator_tcp_port(server_port))   # rank 0 only

# --- Rollout worker: one per model-parallel rank; poll every scheduler tick, all ranks ---
from wbridge.frontend.adapters import AdapterContext, ReceiverAdapter
receiver = ReceiverAdapter(ctx, ipc, num_workers=TP * PP)
updated = receiver.poll_requests(before_receive=quiesce_runtime)
if updated:
    record_consume_end()            # receive + load has completed on this rank

# --- Trainer worker: connect once, send per update ---
from wbridge.backend.sender import SenderArgs
from wbridge.frontend.adapters import SenderAdapter
sender = SenderAdapter(ctx, SenderArgs(world_size=N, receiver_urls=[f"tcp://{host}:{port}"],
                                       master_addr=addr, master_port=pg_port,
                                       protocol=rdma_protocol))  # e.g. "efa" or "monarch"
sender.connect()                   # once
ev = sender.send_weights()         # per update
if ev is not None: ev.synchronize()
```

See the [Integration Guide](docs/integration.md) for the full contract and constraints,
the [Environment Variables](docs/environment.md) reference for deployment tuning, and
[`examples/`](docs/examples.md) for a complete runnable demo.

## Documentation

- [Motivation](docs/motivation.md) — why RL weight transfer needs a reusable, shard-routed layer.
- [Architecture](docs/architecture.md) — the Control, Metadata, and Data planes, and the threading model.
- [Integration Guide](docs/integration.md) — the framework-agnostic contract + step-by-step checklist.
- [API Reference](docs/api.md) — the public classes and helpers.
- [Examples](docs/examples.md) — the runnable 2-node Ray walkthrough.
- [Environment Variables](docs/environment.md) — defaults, dependencies, transport tuning, and diagnostics.
- [Assumptions, Limitations & Pitfalls](docs/limitations.md) — read before your first real run.

## Development

```bash
python -m pytest tests/test_shard_compatibility.py     # overlap / ShardSpec math
python -m pytest tests/test_query_receivers_metadata.py # receiver metadata routing
python -m pytest tests/test_fuse_copy.py                # fused transpose-aware copy plans
python -m pytest tests/test_arena_planner.py            # dedup arena layout
python -m pytest tests/test_rdma_protocol.py            # flag ping-pong / one-sided protocol
python -m pytest tests/test_receive_trigger.py          # data-driven KICK/GO state machine
python -m pytest tests/test_control_channel.py          # ZMQ coordinator + hub
python -m pytest tests/test_mooncake_engine.py          # Mooncake engine wrapper
python -m pytest tests/test_local_staging.py            # CPU staging path
```
