# Installation

WeightBridge installs as the distribution `weightbridge` and is imported as `wbridge`. Dependency declarations are in [pyproject.toml](../pyproject.toml).

## Prerequisites

Use Python 3.10 or newer. Install the PyTorch build appropriate for your GPUs and CUDA environment before installing WeightBridge. Generic imports and some tests can run without GPUs; actual sender construction and LoadSpec probing use CUDA. See [sender.py](../wbridge/backend/sender.py) and [specgen.py](../wbridge/utils/specgen.py).

Install dependencies on every worker node. Python extras do not provision GPU drivers, NICs, or an EFA fabric.

## Base installation

Run from the Monarch repository root:

```bash
cd examples/weightbridge
pip install -e .
```

Run the remaining commands in this guide from `examples/weightbridge/`, which has its own
`pyproject.toml`. Installing Monarch from the repository root does not install this example package.

`-e` creates an editable installation, so imports use the checkout and source changes take effect without reinstalling. Reinstall when dependency declarations change. Use `pip install .` for a regular installation.

| Dependency | Purpose | Code reference |
|---|---|---|
| `torch` | Tensor operations, CUDA buffers/events, distributed metadata rendezvous. | [sender.py](../wbridge/backend/sender.py), [distributed.py](../wbridge/utils/distributed.py) |
| `pyzmq` | Coordinator messages and rollout-rank synchronization. | [coordinator.py](../wbridge/backend/coordinator.py), [control_channel.py](../wbridge/backend/control_channel.py) |
| `packaging` | Parse versions for PyTorch compatibility handling. | [distributed.py](../wbridge/utils/distributed.py) |

## Optional extras

Extras add dependencies to the base installation. Combine them with commas:

```bash
pip install -e '.[mooncake,cuda,checkpoints]'
```

| Extra | Dependencies | Purpose and code reference |
|---|---|---|
| `cuda` | `cuda-python>=12.6` | CUDA runtime bindings for CUDA IPC and GPU coordination; [router.py](../wbridge/backend/router.py) imports `cuda.bindings.runtime`. This does not install GPU drivers or CUDA-enabled PyTorch. |
| `mooncake` | `mooncake-transfer-engine>=0.3.12.post1,<0.4` | Data-plane transport for `protocol="tcp"` or `"efa"`; [mooncake.py](../wbridge/backend/rdma/mooncake.py) imports `mooncake.engine.TransferEngine`. |
| `ray` | `ray`, `nvidia-ml-py` | Ray actor orchestration and NVML GPU topology discovery in [workers.py](../examples/workers.py) and [utils.py](../examples/utils.py). Not required by the generic adapter API. |
| `checkpoints` | `safetensors` | Read HF safetensors checkpoints with `hf_weights_from_checkpoint()` in [specgen.py](../wbridge/utils/specgen.py). Skip if you provide your own tensor factories and shapes. |
| `examples` | `cuda-python>=12.6`, `mooncake-transfer-engine>=0.3.12.post1,<0.4`, `nvidia-ml-py`, `ray`, `triton` | Bundled Ray/Mooncake examples and fused CUDA copies. See [train.py](../examples/train.py), [workers.py](../examples/workers.py), [data.py](../wbridge/utils/data.py). Does not include Monarch or safetensors. |
| `test` | `pytest` | Run [tests](../tests); individual tests may also need GPUs, transports, or multiple workers/nodes. |
| `dev` | `pytest` | Currently identical to `test`; no additional linter, formatter, or development runtime. See [pyproject.toml](../pyproject.toml). |

Common commands:

```bash
# Mooncake runtime with CUDA IPC bindings:
pip install -e '.[mooncake,cuda]'

# Safetensors checkpoint helper:
pip install -e '.[checkpoints]'

# Bundled Ray/Mooncake examples:
pip install -e '.[examples]'

# Examples that also read safetensors checkpoints:
pip install -e '.[examples,checkpoints]'

# Test runner:
pip install -e '.[test]'
```

The checkpoint helper scans safetensors metadata for names/shapes and builds factories that load individual tensors on demand. It can also read a single `pytorch_model*.bin` file through PyTorch without safetensors; multiple .bin shards are not supported by that fallback. See [hf_weights_from_checkpoint](../wbridge/utils/specgen.py).

Triton is explicitly included in `examples`, but has no standalone WeightBridge extra. [data.py](../wbridge/utils/data.py) uses it for fused CUDA copies when available, falling back to per-pair PyTorch `copy_` otherwise. If needed, install it separately with `pip install triton`.

## Transport selection

Installation extras and runtime selection are separate. Configure [SenderArgs.protocol](../wbridge/backend/sender.py); [WeightRouter._init_engine](../wbridge/backend/router.py) constructs the transport. The default is `efa`, so choose `tcp` explicitly for Mooncake TCP runs.

### Mooncake TCP

Install `.[mooncake,cuda]` or `.[examples]` and choose `protocol="tcp"`. [MooncakeEngine.init](../wbridge/backend/rdma/mooncake.py) sets `MC_FORCE_TCP=1` by default. EFA is not required; GPU worker requirements still apply.

### Mooncake EFA

Use an EFA-enabled Mooncake build and choose `protocol="efa"`. The extra requires `>=0.3.12.post1,<0.4`; you may preinstall a compatible EFA-enabled wheel under the same distribution name. The extra alone does not establish EFA support in the wheel or host.

Configure fabric and library paths before launching workers. Mooncake and NCCL must resolve compatible libfabric libraries. See [transport tuning](environment.md#transport-tuning) for `FI_PROVIDER`, `FI_EFA_USE_DEVICE_RDMA`, `MC_EFA_CQ_THREADS`, and striping settings, and [EFA limitations](limitations.md#3-mooncake-on-efa) for deployment requirements.

### Monarch

Monarch is not a declared dependency or extra. Install the `torchmonarch` version supplied by your training/serving environment and choose `protocol="monarch"`. CUDA IPC paths still need the `cuda` extra; Triton can enable fused copies as described above.

Run inside a Monarch actor, with WeightBridge on a dedicated thread off its event loop. Sender/receiver staging is unsupported. Avoid `PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True`; the backend must report `ibverbs`. See [monarch.py](../wbridge/backend/rdma/monarch.py), [Monarch examples](examples.md#orchestrator-ray-default-or-monarch), and [Monarch limitations](limitations.md#4-monarch-transport).

## Verify and continue

```bash
python -c 'import wbridge; from wbridge.frontend.adapters import SenderAdapter, ReceiverAdapter'
python -m pip check
```

These check imports and declared dependencies, not GPU/fabric readiness. Run the [examples](examples.md#running-it) and validate transferred weight equality using the [validation guidance](limitations.md#validation--a-green-run-is-not-a-successful-transfer). The default two-node example needs two trainer and two rollout GPUs; the colocated option needs four GPUs on one node.

After installing `.[test]`, a lightweight subset is:

```bash
python -m pytest tests/test_logical_tensor_split.py tests/test_monarch_engine.py tests/test_mooncake_engine.py
```

Continue with the [Integration Guide](integration.md) for adapter initialization, coordinator setup, and worker synchronization. Record exact package versions, container, CUDA, transport build, and environment settings with each deployment.
