# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import socket

from monarch.actor import Actor, endpoint
from monarch.job import KubernetesJob


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
    job = KubernetesJob(namespace="monarch-tests")
    job.add_mesh("mesh1", 2)
    job.add_mesh("mesh2", 2)

    state = job.state()

    procs1 = state.mesh1.spawn_procs()
    greet_from_mesh("mesh1", procs1)

    procs2 = state.mesh2.spawn_procs()
    greet_from_mesh("mesh2", procs2)

    procs1.stop().get()
    procs2.stop().get()


if __name__ == "__main__":
    main()
