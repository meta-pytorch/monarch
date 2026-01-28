# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
DDP on Kubernetes Using Monarch
===============================

This tutorial extends the :doc:`../spmd_ddp` tutorial to run PyTorch's Distributed
Data Parallel (DDP) on Kubernetes using Monarch's ``KubernetesJob``.

This example shows:

- How to provision GPU workers on Kubernetes using the MonarchMesh CRD
- How to connect to Kubernetes pods using ``KubernetesJob``
- How to set up torch.distributed environment variables across pods
- How to run multi-node DDP training on Kubernetes

Prerequisites
-------------

Before running this example, you need:

1. A Kubernetes cluster with GPU nodes (``nvidia.com/gpu`` resources)
2. The `MonarchMesh CRD and operator <https://github.com/meta-pytorch/monarch-kubernetes/>`_ installed
3. NVIDIA device plugin deployed
4. ``kubectl`` configured to access the cluster

Kubernetes Manifest
-------------------

The following manifest (``ddp_mesh.yaml``) provisions the worker pods::

    apiVersion: monarch.pytorch.org/v1alpha1
    kind: MonarchMesh
    metadata:
      name: ddpmesh # Name of MonarchMesh
      namespace: monarch-tests
    spec:
      replicas: 2  # Number of worker pods (hosts)
      port: 26600
      podTemplate:
        containers:
        - name: worker
          image: ghcr.io/meta-pytorch/monarch:latest
          resources:
            limits:
              nvidia.com/gpu: 4
            requests:
              nvidia.com/gpu: 4
          command:
            - python
            - -u
            - -c
            - |
              from monarch.actor import run_worker_loop_forever
              import socket
              address = f"tcp://{socket.getfqdn()}:26600"
              run_worker_loop_forever(address=address, ca="trust_all_connections")

Deploy with::

    kubectl apply -f manifests/ddp_mesh.yaml

See the `complete manifest on GitHub <https://github.com/meta-pytorch/monarch/tree/main/docs/source/examples/ddp/manifests>`_
including RBAC configuration and controller pod.
"""

# %%
# Imports
# -------
# We import the standard PyTorch DDP components along with Monarch's Kubernetes
# support and distributed environment setup utilities.

import asyncio
import os

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.optim as optim
from monarch.actor import Actor, current_rank, endpoint
from monarch.job.kubernetes import KubernetesJob
from monarch.spmd import setup_torch_elastic_env_async
from monarch.tools.network import AddrType
from torch.nn.parallel import DistributedDataParallel as DDP


# %%
# Model Definition
# ----------------
# We use a simple toy model, identical to the local DDP example.


class ToyModel(nn.Module):
    """A simple toy model for demonstration purposes."""

    def __init__(self):
        super(ToyModel, self).__init__()
        self.net1 = nn.Linear(10, 10)
        self.relu = nn.ReLU()
        self.net2 = nn.Linear(10, 5)

    def forward(self, x):
        return self.net2(self.relu(self.net1(x)))


# %%
# DDP Actor
# ---------
# The ``DDPActor`` wraps PyTorch DDP functionality. Unlike the local example,
# we use ``nccl`` backend for GPU communication and read environment variables
# set by ``setup_torch_elastic_env_async``.


class DDPActor(Actor):
    """Actor that wraps PyTorch DDP functionality for Kubernetes.

    This actor reads distributed environment from environment variables
    (RANK, LOCAL_RANK, WORLD_SIZE, MASTER_ADDR, MASTER_PORT) that are
    configured by Monarch's ``setup_torch_elastic_env_async``.
    """

    def __init__(self):
        self.rank = current_rank().rank

    def _rprint(self, msg):
        """Helper method to print with rank information."""
        print(f"rank={self.rank} {msg}", flush=True)

    @endpoint
    async def setup(self):
        """Initialize the PyTorch distributed process group.

        Uses NCCL backend for efficient GPU-to-GPU communication.
        Environment variables are set by setup_torch_elastic_env_async.
        """
        self._rprint("Initializing torch distributed")

        world_size = int(os.environ["WORLD_SIZE"])
        rank = int(os.environ["RANK"])

        dist.init_process_group("nccl", rank=rank, world_size=world_size)
        self._rprint("Finished initializing torch distributed")

    @endpoint
    async def cleanup(self):
        """Clean up the PyTorch distributed process group."""
        self._rprint("Cleaning up torch distributed")
        dist.destroy_process_group()

    @endpoint
    async def demo_basic(self):
        """Run a basic DDP training example."""
        self._rprint("Running basic DDP example")

        local_rank = int(os.environ["LOCAL_RANK"])
        self._rprint(f"local_rank={local_rank}")

        # Create model and move to GPU
        model = ToyModel().to(local_rank)
        ddp_model = DDP(model, device_ids=[local_rank])

        loss_fn = nn.MSELoss()
        optimizer = optim.SGD(ddp_model.parameters(), lr=0.001)

        # Forward pass
        optimizer.zero_grad()
        outputs = ddp_model(torch.randn(20, 10).to(local_rank))
        labels = torch.randn(20, 5).to(local_rank)

        # Backward pass
        loss_fn(outputs, labels).backward()
        optimizer.step()

        self._rprint("Finished running basic DDP example")


# %%
# Main Function
# -------------
# The main function connects to Kubernetes pods and runs DDP training.


async def main(num_hosts: int = 2, gpus_per_host: int = 4, mesh_name: str = "ddpmesh"):
    """Run DDP training on Kubernetes.

    Args:
        num_hosts: Number of worker pods (must match MonarchMesh replicas)
        gpus_per_host: GPUs per pod (must match nvidia.com/gpu in MonarchMesh)
        mesh_name: Name of the MonarchMesh resource
    """
    print("=" * 60)
    print("Kubernetes DDP Example")
    print(f"Configuration: {num_hosts} hosts, {gpus_per_host} GPUs/host")
    print("=" * 60)

    # %%
    # Connect to Kubernetes
    # ~~~~~~~~~~~~~~~~~~~~~
    # Create a ``KubernetesJob`` that connects to pre-provisioned pods in the
    # ``monarch-tests`` namespace. The MonarchMesh CRD provisions worker pods
    # with labels that ``KubernetesJob`` uses for discovery.

    k8s_job = KubernetesJob(namespace="monarch-tests")
    k8s_job.add_mesh(mesh_name, num_replicas=num_hosts)

    # %%
    # Create Process Mesh
    # ~~~~~~~~~~~~~~~~~~~
    # Get the job state and spawn processes on the workers. Each host gets
    # ``gpus_per_host`` processes, one per GPU.

    job_state = k8s_job.state()
    host_mesh = getattr(job_state, mesh_name)
    proc_mesh = host_mesh.spawn_procs({"gpus": gpus_per_host})

    # %%
    # Setup Distributed Environment
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Configure environment variables for torch.distributed:
    #
    # - ``RANK``: Global rank (0 to world_size-1)
    # - ``LOCAL_RANK``: Rank within the node (0 to gpus_per_host-1)
    # - ``WORLD_SIZE``: Total number of processes
    # - ``MASTER_ADDR``: Address of rank 0 for rendezvous
    # - ``MASTER_PORT``: Port for rendezvous
    #
    # We use IPv4 addresses since short hostnames may not resolve across pods.

    await setup_torch_elastic_env_async(proc_mesh, use_ipaddr=AddrType.IPv4)

    # %%
    # Run DDP Training
    # ~~~~~~~~~~~~~~~~
    # Spawn the DDP actor on the process mesh and run the training steps.

    ddp_actor = proc_mesh.spawn("ddp_actor", DDPActor)

    await ddp_actor.setup.call()
    await ddp_actor.demo_basic.call()
    await ddp_actor.cleanup.call()

    print("=" * 60)
    print("DDP example completed successfully!")
    print("=" * 60)

    # Clean up
    proc_mesh.stop().get()


# %%
# Running the Example
# -------------------
# To run this example:
#
# 1. Deploy the MonarchMesh::
#
#        kubectl apply -f manifests/ddp_mesh.yaml
#
# 2. Wait for pods to be ready::
#
#        kubectl get pods -n monarch-tests -l app.kubernetes.io/name=monarch-worker
#
# 3. Run from the controller pod::
#
#        kubectl exec -it ddp-controller -n monarch-tests -- \
#            python kubernetes_ddp.py
#
# 4. View worker logs::
#
#        kubectl logs -n monarch-tests -l app.kubernetes.io/name=monarch-worker
#
# 5. Clean up::
#
#        kubectl delete -f manifests/ddp_mesh.yaml
#
# For the full instruction, see the
# `Kubernetes DDP README <https://github.com/meta-pytorch/monarch/tree/main/docs/source/examples/ddp>`_.

if __name__ == "__main__":
    asyncio.run(main())
