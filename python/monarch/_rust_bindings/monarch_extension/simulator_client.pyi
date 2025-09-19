# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import final

@final
class SimulatorClient:
    """
    A wrapper around [simulator_client::Simulatorclient] to expose it to python.
    It is a client to communicate with the simulator service.

    Arguments:
    - `system_addr`: Address of the system.
    - `world_size`: Number of workers in a given mesh.
    """

    def __init__(self, system_addr: str, world_size: int) -> None: ...
    def kill_world(self, world_name: str) -> None:
        """
        Kill the world with the given name.

        Arguments:
        - `world_name`: Name of the world to kill.
        """
        ...
    def spawn_mesh(
        self, system_addr: str, controller_actor_id: str, worker_world: str
    ) -> None:
        """
        Spawn a mesh actor.

        Arguments:
        - `system_addr`: Address of the system to spawn the mesh in.
        - `controller_actor_id`: Actor id of the controller to spawn the mesh in.
        - `worker_world`: World of the worker to spawn the mesh in.
        """
        ...

    def set_training_script_state_running(self) -> None:
        """
        Let the simulator know that the training script is actively sending
        commands to the backend
        """
        ...

    def set_training_script_state_waiting(self) -> None:
        """
        Let the simulator know that the training script is waiting for the
        backend to resolve a future
        """
        ...
