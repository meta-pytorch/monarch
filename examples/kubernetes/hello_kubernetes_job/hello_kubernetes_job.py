# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import argparse
import socket

from monarch.actor import Actor, endpoint
from monarch.config import configure
from monarch.job.kubernetes import ImageSpec, KubeConfig, KubernetesJob
from monarch.rdma import RDMABuffer


RDMA_PAYLOAD = b"hello from tcp rdma"


class SimpleActor(Actor):
    @endpoint
    def run(self, x):
        return x()


class RDMASourceActor(Actor):
    def __init__(self) -> None:
        self.data = bytearray(RDMA_PAYLOAD)
        self.buffer = None

    @endpoint
    def get_buffer(self) -> RDMABuffer:
        self.buffer = RDMABuffer(memoryview(self.data))
        return self.buffer


class RDMASinkActor(Actor):
    def __init__(self) -> None:
        self.data = bytearray(len(RDMA_PAYLOAD))

    @endpoint
    async def read_buffer(self, buffer: RDMABuffer) -> bytes:
        await buffer.read_into(memoryview(self.data))
        return bytes(self.data)


def greet_from_mesh(mesh_name: str, mesh_procs):
    """Spawn a SimpleActor actor and print a greeting from the mesh."""
    simple_actor = mesh_procs.spawn("simple_actor", SimpleActor).slice(hosts=0)
    message = simple_actor.run.call_one(
        lambda: f"hello from {socket.gethostname()}"
    ).get()
    print(f"From MonarchMesh {mesh_name}: {message}")


def rdma_smoke(procs1, procs2):
    """Read an RDMABuffer from mesh1 into mesh2 using TCP fallback."""
    source = procs1.spawn("rdma_source", RDMASourceActor).slice(hosts=0)
    sink = procs2.spawn("rdma_sink", RDMASinkActor).slice(hosts=0)

    buffer = source.get_buffer.call_one().get()
    result = sink.read_buffer.call_one(buffer).get()
    if result != RDMA_PAYLOAD:
        raise RuntimeError(f"RDMA payload mismatch: {result!r}")
    print(f"RDMABuffer TCP fallback smoke: {result.decode()}")


def main():
    parser = argparse.ArgumentParser(description="Monarch Kubernetes example")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--volcano",
        action="store_true",
        help="Use Volcano scheduler with manifests/volcano_workers.yaml",
    )
    group.add_argument(
        "--provision",
        action="store_true",
        help="Provision MonarchMesh CRDs from Python (no YAML manifests needed)",
    )
    parser.add_argument(
        "--kueue",
        type=str,
        default=None,
        help="Kueue local queue name",
    )
    parser.add_argument(
        "--image",
        type=str,
        default="ghcr.io/meta-pytorch/monarch:latest",
        help="Container image to use for provisioned meshes if --provision is set",
    )
    parser.add_argument(
        "--out-of-cluster",
        action="store_true",
        help="Set to true if this script is running outside of the kubernetes cluster.",
    )
    parser.add_argument(
        "--kubeconfig",
        type=str,
        default="~/.kube/config",
        help="Path to kubeconfig file (default: ~/.kube/config)",
    )
    parser.add_argument(
        "--attach-to",
        type=str,
        default=None,
        help="Monarch address for out-of-cluster access (e.g. tcp://127.0.0.1:26600). "
        "Requires kubectl port-forward to be running. Cannot be used in combination with --provision. ",
    )
    parser.add_argument(
        "--rdma-smoke",
        action="store_true",
        help="Run a small RDMABuffer transfer between mesh1 and mesh2 using TCP fallback.",
    )
    args = parser.parse_args()
    if args.provision and args.attach_to:
        raise ValueError(
            "Cannot use --provision and --attach-to in combination. "
            "Use --attach-to only without provision."
        )

    if args.volcano and args.kueue:
        parser.error("Arguments --volcano and --kueue are mutually exclusive")

    job = KubernetesJob(
        namespace="monarch-tests",
        kubeconfig=KubeConfig.from_path(args.kubeconfig)
        if args.out_of_cluster
        else None,
        attach_to=args.attach_to,
    )
    if args.volcano:
        # Volcano adds volcano.sh/job-name and volcano.sh/task-index labels to pods
        job.add_mesh(
            "mesh1",
            2,
            label_selector="volcano.sh/job-name=mesh1",
            pod_rank_label="volcano.sh/task-index",
        )
        job.add_mesh(
            "mesh2",
            2,
            label_selector="volcano.sh/job-name=mesh2",
            pod_rank_label="volcano.sh/task-index",
        )
    elif args.provision:
        # Provision MonarchMesh CRDs directly from Python.
        # The Monarch operator (must be pre-installed) creates the
        # StatefulSets and headless Services automatically.
        # The monarch port doubles as the attach endpoint, so no
        # extra duplex configuration is needed.
        labels = {"kueue.x-k8s.io/queue-name": args.kueue} if args.kueue else None
        job.add_mesh(
            "mesh1",
            2,
            image_spec=ImageSpec(args.image),
            labels=labels,
        )
        job.add_mesh(
            "mesh2",
            2,
            image_spec=ImageSpec(args.image),
            labels=labels,
        )
    else:
        job.add_mesh("mesh1", 2)
        job.add_mesh("mesh2", 2)

    if args.rdma_smoke:
        configure(rdma_disable_ibverbs=True, rdma_allow_tcp_fallback=True)

    state = job.state(cached_path=None)

    procs1 = state.mesh1.spawn_procs()
    greet_from_mesh("mesh1", procs1)

    procs2 = state.mesh2.spawn_procs()
    greet_from_mesh("mesh2", procs2)

    if args.rdma_smoke:
        rdma_smoke(procs1, procs2)

    procs1.stop().get()
    procs2.stop().get()

    if args.provision:
        job.kill()


if __name__ == "__main__":
    main()
