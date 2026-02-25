# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import argparse
import socket

from monarch.actor import Actor, endpoint
from monarch.job.kubernetes import ImageSpec, KubernetesJob


class SimpleActor(Actor):
    @endpoint
    def run(self, x):
        return x()


def greet_from_mesh(mesh_name: str, mesh_procs):
    """Spawn a SimpleActor actor and print a greeting from the mesh."""
    simple_actor = mesh_procs.spawn("simple_actor", SimpleActor).slice(hosts=0)
    message = simple_actor.run.call_one(
        lambda: f"hello from {socket.gethostname()}"
    ).get()
    print(f"From MonarchMesh {mesh_name}: {message}")


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
    args = parser.parse_args()

    job = KubernetesJob(namespace="monarch-tests")
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
        job.add_mesh(
            "mesh1", 2, image_spec=ImageSpec("ghcr.io/meta-pytorch/monarch:latest")
        )
        job.add_mesh(
            "mesh2", 2, image_spec=ImageSpec("ghcr.io/meta-pytorch/monarch:latest")
        )
    else:
        job.add_mesh("mesh1", 2)
        job.add_mesh("mesh2", 2)

    state = job.state()

    procs1 = state.mesh1.spawn_procs()
    greet_from_mesh("mesh1", procs1)

    procs2 = state.mesh2.spawn_procs()
    greet_from_mesh("mesh2", procs2)

    procs1.stop().get()
    procs2.stop().get()

    if args.provision:
        job.kill()


if __name__ == "__main__":
    main()
