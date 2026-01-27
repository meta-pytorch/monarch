Examples
================

- :doc:`ping_pong.py </generated/examples/ping_pong>`: Demonstrates the basics of Monarch's Actor/endpoint API with a ping-pong communication example
- :doc:`crawler.py </generated/examples/crawler>`: Demonstrates Monarch's actor API and many-to-one communications with a web crawler example
- :doc:`spmd_ddp.py </generated/examples/spmd_ddp>`: Shows how to run PyTorch's Distributed Data Parallel (DDP) within Monarch actors
- :doc:`kubernetes_ddp.py </generated/examples/ddp/kubernetes_ddp>`: Extends the DDP example to run on Kubernetes using MonarchMesh CRD and operator
- :doc:`grpo_actor.py </generated/examples/grpo_actor>`: Implements a distributed PPO-like reinforcement learning algorithm using the Monarch actor framework
- :doc:`distributed_tensors.py </generated/examples/distributed_tensors>`: Shows how to dispatch tensors and tensor level operations to a distributed mesh of workers and GPUs
- :doc:`debugging.py </generated/examples/debugging>`: Shows how to use the Monarch debugger to debug a distributed program
- `Multinode Slurm Tutorial <https://docs.pytorch.org/tutorials/intermediate/monarch_distributed_tutorial.html>`_: Multinode distributed training tutorial using Monarch and Slurm to run an SPMD training job.
- `Running on Kubernetes using Skypilot <https://github.com/pytorch-labs/monarch/tree/main/examples/skypilot>`_: Run Monarch on Kubernetes and cloud VMs via SkyPilot.

.. toctree::
   :hidden:

   /generated/examples/ping_pong
   /generated/examples/crawler
   /generated/examples/spmd_ddp
   /generated/examples/ddp/kubernetes_ddp
   /generated/examples/grpo_actor
   /generated/examples/distributed_tensors
   Multinode Slurm Tutorial <https://docs.pytorch.org/tutorials/intermediate/monarch_distributed_tutorial.html>
