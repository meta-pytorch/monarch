# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import Dict

from monarch.actor import HostMesh


class JobState:
    """
    Container for the current state of a job.

    Provides access to the HostMesh objects for each mesh requested in the job
    specification. Each mesh is accessible as an attribute.

    Example::

        state = job.state()
        state.trainers    # HostMesh for the "trainers" mesh
        state.dataloaders # HostMesh for the "dataloaders" mesh
    """

    def __init__(self, hosts: Dict[str, HostMesh]):
        self._hosts = hosts

    def __getattr__(self, attr: str) -> HostMesh:
        try:
            return self._hosts[attr]
        except KeyError:
            available = ", ".join(sorted(self._hosts.keys()))
            raise AttributeError(
                f"'{attr}' is not a valid host mesh name. Available names: {available}"
            )
