# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""
Monarch Actor API - Public interface for actor functionality.
"""

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._rust_bindings.monarch_hyperactor.supervision import MeshFailure
from monarch._src.actor import config
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
    shutdown_context,
    ValueMesh,
)
from monarch._src.actor.bootstrap import attach_to_workers, run_worker_loop_forever
from monarch._src.actor.debugger.debug_controller import debug_controller
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.future import Future
from monarch._src.actor.host_mesh import (
    HostMesh,
    hosts_from_config,
    this_host,
    this_proc,
)
from monarch._src.actor.proc_mesh import (
    get_or_spawn_controller,
    local_proc_mesh,
    proc_mesh,
    ProcMesh,
    sim_proc_mesh,
)
from monarch._src.actor.supervision import unhandled_fault_hook

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
    "shutdown_context",
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
    "unhandled_fault_hook",
    "MeshFailure",
    "config",
]
