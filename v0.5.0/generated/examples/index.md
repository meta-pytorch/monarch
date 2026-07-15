# Examples

- [ping_pong.py](ping_pong.html): Demonstrates the basics of Monarch's Actor/endpoint API with a ping-pong communication example
- [crawler.py](crawler.html): Demonstrates Monarch's actor API and many-to-one communications with a web crawler example
- [spmd_ddp.py](ddp/spmd_ddp.html): Shows how to run PyTorch's Distributed Data Parallel (DDP) using SPMDActor
- [Interactive SPMD Job](ddp/spmd_job.html): Shows how to use `serve()` and `run_spmd()` for interactive SPMD training with job caching and debugging
- [kubernetes_ddp.py](ddp/kubernetes_ddp.html): Extends the DDP example to run on Kubernetes using MonarchMesh CRD and operator
- [grpo_actor.py](grpo_actor.html): Implements a distributed PPO-like reinforcement learning algorithm using the Monarch actor framework
- [grpo_yield.py](grpo_yield.html): Same GRPO task as `grpo_actor.py`, wired with direct port messaging instead of queue actors and `RDMABuffer`; each actor's `run` endpoint is structured like a Python generator that yields values to, and receives values from, the next actor in the ring
- [kubernetes_grpo.py](grpo/kubernetes_grpo.html): Extends the GRPO example to run on Kubernetes using MonarchMesh CRDs, fine-tuning the open-source Qwen3.5-0.8B model on the GSM8K math dataset
- [distributed_tensors.py](distributed_tensors.html): Shows how to dispatch tensors and tensor level operations to a distributed mesh of workers and GPUs
- [debugging.py](debugging.html): Shows how to use the Monarch debugger to debug a distributed program
- [otel_collector.py](otel_collector.html): Exports Monarch metrics and logs to an OpenTelemetry Collector deployed on Kubernetes, with Grafana for visualization
- [Multinode Slurm Tutorial](https://docs.pytorch.org/tutorials/intermediate/monarch_distributed_tutorial.html): Multinode distributed training tutorial using Monarch and Slurm to run an SPMD training job.
- [Running on Kubernetes using Skypilot](https://github.com/meta-pytorch/monarch/tree/main/examples/skypilot): Run Monarch on Kubernetes and cloud VMs via SkyPilot.