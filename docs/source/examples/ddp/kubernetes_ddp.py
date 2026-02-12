# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
DDP on Kubernetes
===============================

This tutorial extends the :doc:`spmd_ddp` tutorial to run PyTorch's Distributed
Data Parallel (DDP) on Kubernetes using Monarch's ``SPMDActor`` and ``KubernetesJob``.

This example shows:

- How to provision GPU workers on Kubernetes using ``KubernetesJob`` with Python-native provisioning
- How to connect to Kubernetes pods using ``KubernetesJob``
- How to run multi-node DDP training using ``SPMDActor``

Prerequisites
-------------

Before running this example, you need:

1. A Kubernetes cluster with GPU nodes (``nvidia.com/gpu`` resources)
2. The `MonarchMesh CRD and operator <https://github.com/meta-pytorch/monarch-kubernetes/>`_ installed
3. NVIDIA device plugin deployed
4. ``kubectl`` configured to access the cluster

Python-Native Provisioning
--------------------------

With ``--provision``, the script creates MonarchMesh CRDs directly from Python.
No YAML manifests for worker pods are needed -- only a controller pod with
RBAC permissions to create CRDs and watch pods::

    # Deploy the controller with CRD permissions
    kubectl apply -f manifests/ddp_provision.yaml

    # Copy scripts and run
    kubectl cp kubernetes_ddp.py monarch-tests/ddp-controller:/tmp/kubernetes_ddp.py
    kubectl cp train.py monarch-tests/ddp-controller:/tmp/train.py
    kubectl exec -it ddp-controller -n monarch-tests -- \\
        python /tmp/kubernetes_ddp.py --provision --num_hosts 2 --gpus_per_host 4

YAML Manifest Provisioning
---------------------------

Alternatively, you can pre-provision workers with YAML manifests::

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

Training Script
---------------

The training script (``train.py``) is a standard PyTorch DDP script::

    import os

    import torch
    import torch.distributed as dist
    import torch.nn as nn
    import torch.optim as optim
    from torch.nn.parallel import DistributedDataParallel as DDP


    def main():
        dist.init_process_group("nccl")
        rank = dist.get_rank()
        local_rank = int(os.environ["LOCAL_RANK"])
        torch.cuda.set_device(local_rank)

        model = nn.Linear(10, 1).cuda()
        ddp_model = DDP(model)

        optimizer = optim.SGD(ddp_model.parameters(), lr=0.01)

        for step in range(5):
            inputs = torch.randn(4, 10).cuda()
            outputs = ddp_model(inputs)
            loss = outputs.sum()
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            print(f"[Rank {rank}] Step {step} loss={loss.item()}")

        dist.destroy_process_group()


    if __name__ == "__main__":
        main()

This script:

- Initializes NCCL process group (environment variables set by ``SPMDActor``)
- Creates a simple linear model wrapped in DDP
- Runs 5 training steps
- Cleans up the process group
"""

# %%
# Imports
# -------
# We import Monarch's Kubernetes job support and SPMDActor.

import argparse
import asyncio
import textwrap

from monarch._src.job.kubernetes import _WORKER_BOOTSTRAP_SCRIPT
from monarch.config import configure
from monarch.job.kubernetes import KubernetesJob
from monarch.spmd import SPMDActor
from monarch.tools.network import AddrType

configure(enable_log_forwarding=True)

# Path to train.py on worker pods
TRAIN_SCRIPT = "/tmp/train.py"

# Training script content — written to worker pods at startup when provisioning
_TRAIN_SCRIPT_CONTENT = textwrap.dedent("""\
    import os
    import torch
    import torch.distributed as dist
    import torch.nn as nn
    import torch.optim as optim
    from torch.nn.parallel import DistributedDataParallel as DDP

    def main():
        dist.init_process_group("nccl")
        rank = dist.get_rank()
        local_rank = int(os.environ["LOCAL_RANK"])
        torch.cuda.set_device(local_rank)
        model = nn.Linear(10, 1).cuda()
        ddp_model = DDP(model)
        optimizer = optim.SGD(ddp_model.parameters(), lr=0.01)
        for step in range(5):
            inputs = torch.randn(4, 10).cuda()
            outputs = ddp_model(inputs)
            loss = outputs.sum()
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            print(f"[Rank {rank}] Step {step} loss={loss.item()}")
        dist.destroy_process_group()

    if __name__ == "__main__":
        main()
""")


def build_gpu_pod_spec(gpus_per_host: int) -> dict:
    """Build a PodSpec with GPU resources and shared memory for NCCL.

    The bootstrap command writes train.py to the worker filesystem
    before starting the Monarch worker loop, so the SPMDActor can
    find and execute it.
    """
    # Write train.py then start the worker loop
    bootstrap = (
        "import pathlib\n"
        f"pathlib.Path({TRAIN_SCRIPT!r}).write_text({_TRAIN_SCRIPT_CONTENT!r})\n"
        + _WORKER_BOOTSTRAP_SCRIPT
    )
    return {
        "containers": [
            {
                "name": "worker",
                "image": "ghcr.io/meta-pytorch/monarch:latest",
                "command": ["python", "-u", "-c", bootstrap],
                "env": [{"name": "MONARCH_PORT", "value": "26600"}],
                "resources": {
                    "limits": {"nvidia.com/gpu": str(gpus_per_host)},
                    "requests": {"nvidia.com/gpu": str(gpus_per_host)},
                },
                "volumeMounts": [{"name": "dshm", "mountPath": "/dev/shm"}],
            }
        ],
        "volumes": [
            {
                "name": "dshm",
                "emptyDir": {"medium": "Memory", "sizeLimit": "16Gi"},
            }
        ],
    }


# %%
# Main Function
# -------------
# The main function connects to Kubernetes pods and runs DDP training
# using ``SPMDActor`` to execute the training script.


async def main(
    num_hosts: int = 2,
    gpus_per_host: int = 4,
    mesh_name: str = "ddpmesh",
    provision: bool = False,
) -> None:
    """Run DDP training on Kubernetes.

    Args:
        num_hosts: Number of worker pods (must match MonarchMesh replicas)
        gpus_per_host: GPUs per pod (must match nvidia.com/gpu in MonarchMesh)
        mesh_name: Name of the MonarchMesh resource
        provision: If True, create MonarchMesh CRDs from Python
    """
    print("=" * 60)
    print("Kubernetes DDP Example")
    print(f"Configuration: {num_hosts} hosts, {gpus_per_host} GPUs/host")
    print("=" * 60)

    # %%
    # Connect to Kubernetes
    # ~~~~~~~~~~~~~~~~~~~~~
    # Create a ``KubernetesJob`` in the ``monarch-tests`` namespace.
    # With ``--provision``, the job creates MonarchMesh CRDs via the K8s API
    # using ``pod_spec`` for full control over the pod template (needed for
    # the shared memory volume that NCCL requires). Without ``--provision``,
    # it attaches to pre-provisioned pods.

    k8s_job = KubernetesJob(namespace="monarch-tests")
    if provision:
        k8s_job.add_mesh(
            mesh_name,
            num_replicas=num_hosts,
            pod_spec=build_gpu_pod_spec(gpus_per_host),
        )
    else:
        k8s_job.add_mesh(mesh_name, num_replicas=num_hosts)

    # %%
    # Create Process Mesh
    # ~~~~~~~~~~~~~~~~~~~
    # Get the job state and spawn processes on the workers. Each host gets
    # ``gpus_per_host`` processes, one per GPU.

    job_state = k8s_job.state()
    host_mesh = getattr(job_state, mesh_name)
    proc_mesh = host_mesh.spawn_procs({"gpus": gpus_per_host})

    # Stream logs from all processes to the client
    await proc_mesh.logging_option(stream_to_client=True)

    # %%
    # Run DDP Training with SPMDActor
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Spawn ``SPMDActor`` on the process mesh. The actor configures torch elastic
    # environment variables (RANK, LOCAL_RANK, WORLD_SIZE, MASTER_ADDR, MASTER_PORT)
    # and executes the training script.

    spmd_actors = proc_mesh.spawn("_SPMDActor", SPMDActor)

    # Get master address/port from first actor (all coordinates = 0)
    # We use IPv4 addresses since short hostnames may not resolve across pods.
    first_values = dict.fromkeys(proc_mesh._labels, 0)
    master_addr, master_port = await spmd_actors.slice(
        **first_values
    ).get_host_port.call_one(AddrType.IPv4)

    # Execute training script across the mesh
    await spmd_actors.main.call(master_addr, master_port, [TRAIN_SCRIPT])

    print("=" * 60)
    print("DDP example completed successfully!")
    print("=" * 60)

    # Clean up
    proc_mesh.stop().get()

    if provision:
        k8s_job.kill()


# %%
# Running the Example
# -------------------
#
# **Option 1: Python-native provisioning (recommended)**
#
# With ``--provision``, train.py is embedded in the worker pod spec and
# written to the filesystem at startup. No manual file copying to workers
# is needed.
#
# 1. Deploy the controller with CRD permissions::
#
#        kubectl apply -f manifests/ddp_provision.yaml
#
# 2. Run from the controller::
#
#        kubectl cp kubernetes_ddp.py monarch-tests/ddp-controller:/tmp/kubernetes_ddp.py
#        kubectl exec -it ddp-controller -n monarch-tests -- \
#            python /tmp/kubernetes_ddp.py --provision --num_hosts 2 --gpus_per_host 4
#
# 3. Clean up::
#
#        kubectl delete -f manifests/ddp_provision.yaml
#
# **Option 2: YAML manifest provisioning**
#
# 1. Deploy the MonarchMesh::
#
#        kubectl apply -f manifests/ddp_mesh.yaml
#
# 2. Wait for pods to be ready::
#
#        kubectl get pods -n monarch-tests -l app.kubernetes.io/name=monarch-worker
#        kubectl get pods -n monarch-tests ddp-controller
#
# 3. Copy train.py to each worker pod::
#
#        for pod in $(kubectl get pods -n monarch-tests -l app.kubernetes.io/name=monarch-worker -o name); do
#            kubectl cp train.py monarch-tests/${pod#pod/}:/tmp/train.py
#        done
#
# 4. Run from the controller pod::
#
#        kubectl cp kubernetes_ddp.py monarch-tests/ddp-controller:/tmp/kubernetes_ddp.py
#        kubectl exec -it ddp-controller -n monarch-tests -- \
#            python /tmp/kubernetes_ddp.py --num_hosts 2 --gpus_per_host 4
#
# 5. Clean up::
#
#        kubectl delete -f manifests/ddp_mesh.yaml
#
# Command-line Arguments
# ~~~~~~~~~~~~~~~~~~~~~~
# - ``--provision``: Create MonarchMesh CRDs from Python (no worker YAML needed)
# - ``--num_hosts``: Number of worker pods
# - ``--gpus_per_host``: GPUs per pod
# - ``--mesh_name``: Name of the MonarchMesh resource

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run DDP training on Kubernetes")
    parser.add_argument(
        "--num_hosts",
        type=int,
        default=2,
        help="Number of worker pods",
    )
    parser.add_argument(
        "--gpus_per_host",
        type=int,
        default=4,
        help="GPUs per pod",
    )
    parser.add_argument(
        "--mesh_name",
        type=str,
        default="ddpmesh",
        help="Name of the MonarchMesh resource",
    )
    parser.add_argument(
        "--provision",
        action="store_true",
        help="Provision MonarchMesh CRDs from Python (no YAML manifests needed)",
    )
    args = parser.parse_args()
    asyncio.run(
        main(args.num_hosts, args.gpus_per_host, args.mesh_name, args.provision)
    )
