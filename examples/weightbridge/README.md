# WeightBridge

WeightBridge is a flexible, efficient weight-transfer library designed to support diverse
reinforcement learning (RL) settings for large language models. It moves updated policy weights
from **Trainer Workers** to **Rollout Workers** across different models, parallelization layouts,
and synchronization modes, including fully synchronous, fully asynchronous, and partially
synchronous execution.

WeightBridge automatically extracts the correspondence between trainer and rollout weight layouts,
then plans and executes transfers that avoid redundant communication and balance load across workers.
A small, general API lets RL frameworks integrate weight transfer while retaining control of training
and rollout scheduling. See our [paper](https://arxiv.org/abs/2609.25442) for the design and evaluation.

This directory is a self-contained example package under `examples/weightbridge/`. See the
[Installation Guide](docs/installation.md) for setup; install this package separately from Monarch.

## Why WeightBridge?

- **Bandwidth-efficient.** Replaces all-gather/broadcast weight sync with shard-routed, deduplicated,
  one-sided RDMA transfer — each receiver gets only the slices it needs.
- **Layout- and framework-agnostic.** Trainer and rollout may use different names, tensor-parallel or
  expert-parallel layouts, and merged tensors (QKV, gate/up). No model- or engine-specific mapping is
  hard-coded; the layout is *inferred* by probing the existing `load_weights` function in the backend frameworks.
- **One integration surface** for fully synchronous, fully asynchronous, and partially synchronous
  modes (see [the paper](https://arxiv.org/html/2609.25442v1#S3.SS3)).
- **Tested across training and inference frameworks**, including
  [Megatron-LM](https://github.com/NVIDIA/Megatron-LM),
  [FSDP](https://docs.pytorch.org/docs/stable/fsdp.html),
  [vLLM](https://github.com/vllm-project/vllm), and
  [SGLang](https://github.com/sgl-project/sglang).
- **Multiple communication backends**, supporting
  [Mooncake](https://github.com/kvcache-ai/Mooncake) and
  [Monarch](https://github.com/meta-pytorch/monarch).

## Tiny integration sketch

WeightBridge never parses your model — you describe each runtime with an `AdapterContext` (HF weight
factories, a fresh worker-state-dict snapshot, and your native `load_weights`), then poll from the
rollout scheduler and send from the trainer.

```python
from wbridge.frontend.adapters import SenderAdapter, ReceiverAdapter


def trainer_worker_process():
    adapter = SenderAdapter(...)              # WeightBridge
    adapter.connect()                         # WeightBridge

    for _ in range(num_iters):
        train_one_step()
        adapter.send_weights()                # WeightBridge


def rollout_worker_process():
    adapter = ReceiverAdapter(...)            # WeightBridge

    while True:
        generate_one_step()
        updated = adapter.poll_requests()     # WeightBridge
```

See the [Integration Guide](docs/integration.md) for initialization arguments, staging,
CUDA event synchronization, and rollout-rank lockstep requirements,
the [Environment Variables](docs/environment.md) reference for deployment tuning, and
[`examples/`](docs/examples.md) for a complete runnable demo.

## Documentation

- [Installation Guide](docs/installation.md) — prerequisites, package extras, and transport setup.
- [Architecture](docs/architecture.md) — the Control, Metadata, and Data planes, and the threading model.
- [Integration Guide & API Reference](docs/integration.md) — integration contract, setup workflow, and public APIs.
- [Examples](docs/examples.md) — the runnable 2-node Ray walkthrough.
- [Environment Variables](docs/environment.md) — defaults, dependencies, transport tuning, and diagnostics.
- [Assumptions, Limitations & Pitfalls](docs/limitations.md) — read before your first real run.

## Citation

If WeightBridge is useful in your research or your product, please cite our
[paper](https://arxiv.org/abs/2609.25442):

```bibtex
@misc{jiang2026weightbridge,
  title         = {WeightBridge: An Efficient Weight Transfer Library for Reinforcement Learning},
  author        = {Xuanlin Jiang and Samuel Hsia and Michael Kuchnik and Zachary DeVito and Minlan Yu and Carole-Jean Wu},
  year          = {2026},
  eprint        = {2609.25442},
  archivePrefix = {arXiv},
  primaryClass  = {cs.DC},
  url           = {https://arxiv.org/abs/2609.25442}
}
```
