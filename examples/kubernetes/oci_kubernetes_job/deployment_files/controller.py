# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# %%
# Imports
# -------
# We import Monarch's Kubernetes job support and SPMDActor.

import argparse
import asyncio
import textwrap

from kubernetes.client import (
    V1Capabilities,
    V1Container,
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1HostPathVolumeSource,
    V1PodSpec,
    V1ResourceRequirements,
    V1SecurityContext,
    V1Volume,
    V1VolumeMount,
)
from monarch.config import configure
from monarch.job.kubernetes import KubernetesJob
from monarch.spmd import SPMDActor
from monarch.tools.network import AddrType

configure(enable_log_forwarding=True, message_delivery_timeout="2m")

_WORKER_BOOTSTRAP_SCRIPT: str = textwrap.dedent("""\
    import os
    import socket
    from monarch.actor import run_worker_loop_forever
    port = os.environ.get("MONARCH_PORT", "26600")
    hostname = socket.getfqdn()
    address = f"tcp://{hostname}:{port}"
    run_worker_loop_forever(address=address, ca="trust_all_connections")
""")

# Path to train.py on worker pods
TRAIN_SCRIPT = "/tmp/train.py"

# Training script content — read from TRAIN_SCRIPT path on the controller pod and
# written to worker pods at startup when provisioning
with open(TRAIN_SCRIPT, "r") as _f:
    _TRAIN_SCRIPT_CONTENT = _f.read()


def build_gpu_pod_spec(
    image: str, gpus_per_host: int, gpu_vendor: str, rdma: bool
) -> V1PodSpec:
    """Build a V1PodSpec for a MonarchMesh worker pod.

    Without ``rdma``: GPU resources and shared memory for NCCL.

    With ``rdma`` (NVIDIA only): adds RDMA/InfiniBand access for NCCL collectives
    across pods. IPC_LOCK plus privileged mode are required to pin memory for
    RDMA. RDMA is provided via SR-IOV: requesting ``nvidia.com/sriov-rdma-vf``
    triggers the monarch-sriov-vf-injector webhook, which adds the Multus
    annotation ``k8s.v1.cni.cncf.io/networks=sriov-rdma-vf`` so a Mellanox VF
    netdev is plumbed into the pod netns for NCCL. hostNetwork is deliberately
    NOT set on worker pods — the Monarch K8s operator discovers workers via
    pod DNS, and hostNetwork breaks that (socket.getfqdn() resolves to the
    node FQDN, not the pod DNS name).

    The bootstrap command writes train.py to the worker filesystem before
    starting the Monarch worker loop, so the SPMDActor can find and execute it.
    """
    # Write train.py then start the worker loop
    bootstrap = (
        "import pathlib\n"
        f"pathlib.Path({TRAIN_SCRIPT!r}).write_text({_TRAIN_SCRIPT_CONTENT!r})\n"
        + _WORKER_BOOTSTRAP_SCRIPT
    )

    env = [V1EnvVar(name="MONARCH_PORT", value="26600")]
    volume_mounts = [V1VolumeMount(name="dshm", mount_path="/dev/shm")]
    volumes = [
        V1Volume(
            name="dshm",
            empty_dir=V1EmptyDirVolumeSource(medium="Memory", size_limit="16Gi"),
        )
    ]
    security_context = None

    if rdma:
        gpu_resources = {
            "nvidia.com/gpu": str(gpus_per_host),
            # Request one VF per physical RDMA port on the node (16 CX-6 data-plane
            # ports per a100-sriov-policy.yaml), not one per GPU. Requires one
            # Monarch pod per node since each pod consumes the node's full VF pool.
            "nvidia.com/sriov-rdma-vf": "16",
        }
        env.extend(
            [
                V1EnvVar(name="NCCL_IB_DISABLE", value="0"),
                V1EnvVar(name="NCCL_IB_HCA", value="mlx5"),
                V1EnvVar(name="NCCL_IB_GID_INDEX", value="3"),
                V1EnvVar(name="NCCL_IB_TC", value="41"),
                V1EnvVar(name="NCCL_IB_SL", value="0"),
                V1EnvVar(name="NCCL_IB_QPS_PER_CONNECTION", value="4"),
                V1EnvVar(name="NCCL_NET_GDR_LEVEL", value="PHB"),
                V1EnvVar(name="NCCL_DEBUG", value="INFO"),
            ]
        )
        security_context = V1SecurityContext(
            privileged=True,
            capabilities=V1Capabilities(add=["IPC_LOCK"]),
        )
        volume_mounts.append(
            V1VolumeMount(name="infiniband", mount_path="/dev/infiniband")
        )
        volumes.append(
            V1Volume(
                name="infiniband",
                host_path=V1HostPathVolumeSource(path="/dev/infiniband"),
            )
        )
    else:
        gpu_resources = {f"{gpu_vendor}.com/gpu": str(gpus_per_host)}

    return V1PodSpec(
        containers=[
            V1Container(
                name="worker",
                image=image,
                command=["python", "-u", "-c", bootstrap],
                env=env,
                security_context=security_context,
                resources=V1ResourceRequirements(
                    limits=gpu_resources,
                    requests=gpu_resources,
                ),
                volume_mounts=volume_mounts,
            )
        ],
        volumes=volumes,
    )


# %%
# Main Function
# -------------
# The main function connects to Kubernetes pods and runs DDP training
# using ``SPMDActor`` to execute the training script.


async def main(
    image: str,
    gpu_vendor: str,
    num_hosts: int = 2,
    gpus_per_host: int = 8,
    mesh_name: str = "monarchmesh",
    provision: bool = False,
    rdma: bool = False,
) -> None:
    """Run DDP training on Kubernetes.

    Args:
        image: Worker container image (e.g. NVIDIA CUDA or AMD ROCm build)
        gpu_vendor: "nvidia" or "amd" — selects the K8s GPU resource key
        num_hosts: Number of worker pods (must match MonarchMesh replicas)
        gpus_per_host: GPUs per pod (must match the GPU resource in MonarchMesh)
        mesh_name: Name of the MonarchMesh resource
        provision: If True, create MonarchMesh CRDs from Python
        rdma: If True, provision worker pods with RDMA/InfiniBand (NVIDIA only)
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
            pod_spec=build_gpu_pod_spec(image, gpus_per_host, gpu_vendor, rdma),
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
# Command-line Arguments
# ~~~~~~~~~~~~~~~~~~~~~~
# - ``--image``: Worker container image (e.g. NVIDIA CUDA or AMD ROCm build)
# - ``--gpu-vendor``: "nvidia" or "amd" — selects the K8s GPU resource key
# - ``--provision``: Create MonarchMesh CRDs from Python (no worker YAML needed)
# - ``--rdma``: Provision worker pods with RDMA/InfiniBand (NVIDIA only)
# - ``--num_hosts``: Number of worker pods
# - ``--gpus_per_host``: GPUs per pod
# - ``--mesh_name``: Name of the MonarchMesh resource

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run DDP training on Kubernetes")
    parser.add_argument(
        "--image",
        type=str,
        required=True,
        help="Worker container image",
    )
    parser.add_argument(
        "--gpu-vendor",
        type=str,
        choices=["nvidia", "amd"],
        required=True,
        help="GPU vendor — selects nvidia.com/gpu or amd.com/gpu",
    )
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
        default="monarchmesh",
        help="Name of the MonarchMesh resource",
    )
    parser.add_argument(
        "--provision",
        action="store_true",
        help="Provision MonarchMesh CRDs from Python (no YAML manifests needed)",
    )
    parser.add_argument(
        "--rdma",
        action="store_true",
        help="Provision worker pods with RDMA/InfiniBand (NVIDIA only)",
    )
    args = parser.parse_args()
    if args.rdma and args.gpu_vendor != "nvidia":
        parser.error("--rdma is only supported with --gpu-vendor nvidia")
    asyncio.run(
        main(
            args.image,
            args.gpu_vendor,
            args.num_hosts,
            args.gpus_per_host,
            args.mesh_name,
            args.provision,
            args.rdma,
        )
    )
