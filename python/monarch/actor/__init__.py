# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""
Monarch Actor API - Public interface for actor functionality.
"""

from typing import TYPE_CHECKING

from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._src.actor.actor_mesh import (
    Accumulator,
    Actor,
    ActorError,
    as_endpoint,
    Channel,
    context,
    Context,
    current_actor_name,
    current_rank,
    current_size,
    enable_transport,
    Endpoint,
    Point,
    Port,
    PortReceiver,
    send,
    ValueMesh,
)
from monarch._src.actor.bootstrap import attach_to_workers, run_worker_loop_forever
from monarch._src.actor.debugger.debug_controller import debug_controller
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.future import Future

from monarch._src.actor.proc_mesh import local_proc_mesh, proc_mesh, sim_proc_mesh

from monarch._src.actor.v1 import enabled as v1_enabled

if not v1_enabled or TYPE_CHECKING:
    from monarch._src.actor.host_mesh import (
        HostMeshV0 as HostMesh,
        hosts_from_config_v0 as hosts_from_config,
        this_host_v0 as this_host,
        this_proc_v0 as this_proc,
    )
    from monarch._src.actor.proc_mesh import (
        get_or_spawn_controller_v0 as get_or_spawn_controller,
        ProcMeshV0 as ProcMesh,
    )
else:
    from monarch._src.actor.v1.host_mesh import (
        HostMesh,
        hosts_from_config,
        this_host,
        this_proc,
    )
    from monarch._src.actor.v1.proc_mesh import get_or_spawn_controller, ProcMesh


__all__ = [
    "Accumulator",
    "Actor",
    "ActorError",
    "current_actor_name",
    "as_endpoint",
    "current_rank",
    "current_size",
    "endpoint",
    "Future",
    "local_proc_mesh",
    "Point",
    "proc_mesh",
    "ProcMesh",
    "Channel",
    "send",
    "sim_proc_mesh",
    "ValueMesh",
    "debug_controller",
    "get_or_spawn_controller",
    "this_host",
    "this_proc",
    "HostMesh",
    "context",
    "hosts_from_config",
    "Port",
    "PortReceiver",
    "Endpoint",
    "Extent",
    "run_worker_loop_forever",
    "attach_to_workers",
    "enable_transport",
    "Context",
    "ChannelTransport",
]
