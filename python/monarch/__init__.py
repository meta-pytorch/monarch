# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from importlib import import_module as _import_module
from typing import TYPE_CHECKING

# Import before monarch to pre-load torch DSOs as, in exploded wheel flows,
# our RPATHs won't correctly find them.
try:
    import torch  # noqa: F401
except ImportError:
    pass

# submodules of monarch should not be imported in this
# top-level file because it will cause them to get
# loaded even if they are not actually being used.
# for instance if we import monarch.common.functions,
# we might not want to also import monarch.common.tensor,
# which recursively imports torch.

# Instead to expose functionality as part of the
# monarch.* API, import it inside the TYPE_CHECKING
# guard (so typechecker works), and then add it
# to the _public_api dict and __all__ list. These
# entries will get loaded on demand.


if TYPE_CHECKING:
    from monarch import timer
    from monarch._src.actor.allocator import LocalAllocator, ProcessAllocator
    from monarch._src.actor.shape import NDSlice, Shape
    from monarch.common._coalescing import coalescing

    from monarch.common.device_mesh import (
        DeviceMesh,
        get_active_mesh,
        no_mesh,
        RemoteProcessGroup,
        slice_mesh,
        to_mesh,
    )

    from monarch.common.function import resolvers as function_resolvers

    from monarch.common.future import Future

    from monarch.common.invocation import RemoteException
    from monarch.common.opaque_ref import OpaqueRef
    from monarch.common.pipe import create_pipe, Pipe, remote_generator
    from monarch.common.remote import remote
    from monarch.common.selection import Selection
    from monarch.common.stream import get_active_stream, Stream
    from monarch.common.tensor import reduce, reduce_, Tensor
    from monarch.fetch import fetch_shard, inspect, show
    from monarch.gradient_generator import grad_function, grad_generator
    from monarch.notebook import mast_mesh, reserve_torchx as mast_reserve
    from monarch.python_local_mesh import python_local_mesh
    from monarch.rust_backend_mesh import (
        rust_backend_mesh,
        rust_backend_meshes,
        rust_mast_mesh,
    )
    from monarch.rust_local_mesh import local_mesh, local_meshes, SocketType
    from monarch.simulator.config import set_meta  # noqa
    from monarch.simulator.interface import Simulator
    from monarch.world_mesh import world_mesh


_public_api = {
    "coalescing": ("monarch.common._coalescing", "coalescing"),
    "remote": ("monarch.common.remote", "remote"),
    "DeviceMesh": ("monarch.common.device_mesh", "DeviceMesh"),
    "get_active_mesh": ("monarch.common.device_mesh", "get_active_mesh"),
    "no_mesh": ("monarch.common.device_mesh", "no_mesh"),
    "RemoteProcessGroup": ("monarch.common.device_mesh", "RemoteProcessGroup"),
    "function_resolvers": ("monarch.common.function", "resolvers"),
    "Future": ("monarch.common.future", "Future"),
    "RemoteException": ("monarch.common.invocation", "RemoteException"),
    "Shape": ("monarch._src.actor.shape", "Shape"),
    "NDSlice": ("monarch._src.actor.shape", "NDSlice"),
    "Selection": ("monarch.common.selection", "Selection"),
    "OpaqueRef": ("monarch.common.opaque_ref", "OpaqueRef"),
    "create_pipe": ("monarch.common.pipe", "create_pipe"),
    "Pipe": ("monarch.common.pipe", "Pipe"),
    "remote_generator": ("monarch.common.pipe", "remote_generator"),
    "get_active_stream": ("monarch.common.stream", "get_active_stream"),
    "Stream": ("monarch.common.stream", "Stream"),
    "Tensor": ("monarch.common.tensor", "Tensor"),
    "reduce": ("monarch.common.tensor", "reduce"),
    "reduce_": ("monarch.common.tensor", "reduce_"),
    "to_mesh": ("monarch.common.device_mesh", "to_mesh"),
    "slice_mesh": ("monarch.common.device_mesh", "slice_mesh"),
    "call_on_shard_and_fetch": ("monarch.fetch", "call_on_shard_and_fetch"),
    "fetch_shard": ("monarch.fetch", "fetch_shard"),
    "inspect": ("monarch.fetch", "inspect"),
    "show": ("monarch.fetch", "show"),
    "grad_function": ("monarch.gradient_generator", "grad_function"),
    "grad_generator": ("monarch.gradient_generator", "grad_generator"),
    "python_local_mesh": ("monarch.python_local_mesh", "python_local_mesh"),
    "mast_mesh": ("monarch.notebook", "mast_mesh"),
    "mast_reserve": ("monarch.notebook", "reserve_torchx"),
    "rust_backend_mesh": ("monarch.rust_backend_mesh", "rust_backend_mesh"),
    "rust_backend_meshes": ("monarch.rust_backend_mesh", "rust_backend_meshes"),
    "local_mesh": ("monarch.rust_local_mesh", "local_mesh"),
    "local_meshes": ("monarch.rust_local_mesh", "local_meshes"),
    "SocketType": ("monarch.rust_local_mesh", "SocketType"),
    "rust_mast_mesh": ("monarch.rust_backend_mesh", "rust_mast_mesh"),
    "set_meta": ("monarch.simulator.config", "set_meta"),
    "Simulator": ("monarch.simulator.interface", "Simulator"),
    "world_mesh": ("monarch.world_mesh", "world_mesh"),
    "timer": ("monarch.timer", "timer"),
    "ProcessAllocator": ("monarch._src.actor.allocator", "ProcessAllocator"),
    "LocalAllocator": ("monarch._src.actor.allocator", "LocalAllocator"),
    "SimAllocator": ("monarch._src_actor.allocator", "SimAllocator"),
    "ActorFuture": ("monarch.future", "ActorFuture"),
    "builtins": ("monarch.builtins", "builtins"),
}


def __getattr__(name):
    if name in _public_api:
        module_path, attr_name = _public_api[name]
        module = _import_module(module_path)
        result = getattr(module, attr_name)
        globals()[name] = result
        return result
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


try:
    from __manifest__ import fbmake  # noqa

    IN_PAR = bool(fbmake.get("par_style"))
except ImportError:
    IN_PAR = False

# we have to explicitly list this rather than just take the keys of the _public_api
# otherwise tools think the imports are unused
__all__ = [
    "coalescing",
    "DeviceMesh",
    "get_active_mesh",
    "no_mesh",
    "remote",
    "RemoteProcessGroup",
    "function_resolvers",
    "Future",
    "RemoteException",
    "Shape",
    "Selection",
    "NDSlice",
    "OpaqueRef",
    "create_pipe",
    "Pipe",
    "remote_generator",
    "get_active_stream",
    "Stream",
    "Tensor",
    "reduce",
    "reduce_",
    "to_mesh",
    "slice_mesh",
    "call_on_shard_and_fetch",
    "fetch_shard",
    "inspect",
    "show",
    "grad_function",
    "grad_generator",
    "python_local_mesh",
    "mast_mesh",
    "mast_reserve",
    "rust_backend_mesh",
    "rust_backend_meshes",
    "local_mesh",
    "local_meshes",
    "SocketType",
    "rust_mast_mesh",
    "set_meta",
    "Simulator",
    "world_mesh",
    "timer",
    "ProcessAllocator",
    "LocalAllocator",
    "SimAllocator",
    "ActorFuture",
    "builtins",
]
assert sorted(__all__) == sorted(_public_api)
