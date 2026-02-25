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
    import monarch._rust_bindings  # @manual  # noqa: F401
except ImportError:
    try:
        import torch  # @manual  # noqa: F401
    except ImportError:
        pass
    import monarch._rust_bindings  # @manual  # noqa: F401

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
    from monarch._src.actor.shape import Extent, NDSlice, Shape
    from monarch.common._coalescing import coalescing
    from monarch.common.device_mesh import (
        get_active_mesh,
        no_mesh,
        RemoteProcessGroup,
        slice_mesh,
        to_mesh,
    )
    from monarch.common.function import resolvers as function_resolvers
    from monarch.common.opaque_ref import OpaqueRef
    from monarch.common.remote import remote
    from monarch.common.selection import Selection
    from monarch.common.stream import get_active_stream, Stream
    from monarch.common.tensor import reduce, reduce_, Tensor
    from monarch.config import (  # noqa
        clear_runtime_config,
        configure,
        configured,
        get_global_config,
        get_runtime_config,
    )
    from monarch.fetch import fetch_shard, inspect, show
    from monarch.gradient_generator import grad_function, grad_generator
    from monarch.simulator.config import set_meta  # noqa
    from monarch.simulator.interface import Simulator
    from monarch.world_mesh import world_mesh


_public_api = {
    "coalescing": ("monarch.common._coalescing", "coalescing"),
    "clear_runtime_config": ("monarch.config", "clear_runtime_config"),
    "configure": ("monarch.config", "configure"),
    "configured": ("monarch.config", "configured"),
    "get_global_config": ("monarch.config", "get_global_config"),
    "get_runtime_config": ("monarch.config", "get_runtime_config"),
    "remote": ("monarch.common.remote", "remote"),
    "get_active_mesh": ("monarch.common.device_mesh", "get_active_mesh"),
    "no_mesh": ("monarch.common.device_mesh", "no_mesh"),
    "RemoteProcessGroup": ("monarch.common.device_mesh", "RemoteProcessGroup"),
    "function_resolvers": ("monarch.common.function", "resolvers"),
    "Extent": ("monarch._src.actor.shape", "Extent"),
    "Future": ("monarch.common.future", "Future"),
    "Shape": ("monarch._src.actor.shape", "Shape"),
    "NDSlice": ("monarch._src.actor.shape", "NDSlice"),
    "Selection": ("monarch.common.selection", "Selection"),
    "OpaqueRef": ("monarch.common.opaque_ref", "OpaqueRef"),
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
    "mast_reserve": ("monarch.notebook", "reserve_torchx"),
    "set_meta": ("monarch.simulator.config", "set_meta"),
    "Simulator": ("monarch.simulator.interface", "Simulator"),
    "world_mesh": ("monarch.world_mesh", "world_mesh"),
    "timer": ("monarch.timer", "timer"),
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
    "clear_runtime_config",
    "configure",
    "configured",
    "get_global_config",
    "get_runtime_config",
    "get_active_mesh",
    "no_mesh",
    "remote",
    "RemoteProcessGroup",
    "function_resolvers",
    "Extent",
    "Future",
    "Shape",
    "Selection",
    "NDSlice",
    "OpaqueRef",
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
    "mast_reserve",
    "set_meta",
    "Simulator",
    "world_mesh",
    "timer",
    "ActorFuture",
    "builtins",
]
assert sorted(__all__) == sorted(_public_api)
