Examples
================

- :doc:`ping_pong.py <ping_pong>`: Demonstrates the basics of Monarch's Actor/endpoint API with a ping-pong communication example
- :doc:`crawler.py <crawler>`: Demonstrates Monarch's actor API and many-to-one communications with a web crawler example
- :doc:`spmd_ddp.py <spmd_ddp>`: Shows how to run PyTorch's Distributed Data Parallel (DDP) within Monarch actors
- :doc:`grpo_actor.py <grpo_actor>`: Implements a distributed PPO-like reinforcement learning algorithm using the Monarch actor framework
- :doc:`distributed_tensors.py <distributed_tensors>`: Shows how to dispatch tensors and tensor level operations to a distributed mesh of workers and GPUs
- :doc:`debugging.py <debugging>`: Shows how to use the Monarch debugger to debug a distributed program
- `Multinode Slurm Tutorial <https://docs.pytorch.org/tutorials/intermediate/monarch_distributed_tutorial.html>`_: Multinode distributed training tutorial using Monarch and Slurm to run an SPMD training job.
- `SkyPilot Integration <https://github.com/pytorch-labs/monarch/tree/main/examples/skypilot>`_: Run Monarch on Kubernetes and cloud VMs via SkyPilot.

.. toctree::
   :hidden:

   ping_pong
   crawler
   spmd_ddp
   grpo_actor
   distributed_tensors
   Multinode Slurm Tutorial <https://docs.pytorch.org/tutorials/intermediate/monarch_distributed_tutorial.html>
