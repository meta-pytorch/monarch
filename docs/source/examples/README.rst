Examples
================

This directory contains examples demonstrating how to use Monarch for distributed computing.

Python Script Examples
---------------------

These examples are formatted for sphinx-gallery and will be automatically converted to HTML documentation:

- :doc:`ping_pong.py <ping_pong>`: Demonstrates the basics of Monarch's Actor/endpoint API with a ping-pong communication example
- :doc:`spmd_ddp.py <spmd_ddp>`: Shows how to run PyTorch's Distributed Data Parallel (DDP) within Monarch actors
- :doc:`grpo_actor.py <grpo_actor>`: Implements a distributed PPO-like reinforcement learning algorithm using the Monarch actor framework
- :doc:`distributed_tensors.py <distributed_tensors>`: Shows how to dispatch tensors and tensor level operations to a distributed mesh of workers and GPUs

.. toctree::
   :hidden:

   ping_pong
   spmd_ddp
   grpo_actor
   distributed_tensors
