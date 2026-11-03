# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import monarch.builtins as builtins
import monarch.timer as timer
from monarch._src.actor.shape import (
    Extent as Extent,
    NDSlice as NDSlice,
    Shape as Shape,
)
from monarch.common._coalescing import coalescing as coalescing
from monarch.common.device_mesh import (
    get_active_mesh as get_active_mesh,
    no_mesh as no_mesh,
    RemoteProcessGroup as RemoteProcessGroup,
    slice_mesh as slice_mesh,
    to_mesh as to_mesh,
)
from monarch.common.function import resolvers as function_resolvers
from monarch.common.future import Future as Future
from monarch.common.opaque_ref import OpaqueRef as OpaqueRef
from monarch.common.remote import remote as remote
from monarch.common.selection import Selection as Selection
from monarch.common.stream import (
    get_active_stream as get_active_stream,
    Stream as Stream,
)
from monarch.common.tensor import reduce as reduce, reduce_ as reduce_, Tensor as Tensor
from monarch.config import (
    clear_runtime_config as clear_runtime_config,
    configure as configure,
    configured as configured,
    get_global_config as get_global_config,
    get_runtime_config as get_runtime_config,
)
from monarch.fetch import (
    call_on_shard_and_fetch as call_on_shard_and_fetch,
    fetch_shard as fetch_shard,
    inspect as inspect,
    show as show,
)
from monarch.gradient_generator import (
    grad_function as grad_function,
    grad_generator as grad_generator,
)
from monarch.notebook import reserve_torchx as mast_reserve
from monarch.simulator.config import set_meta as set_meta
from monarch.simulator.interface import Simulator as Simulator
from monarch.world_mesh import world_mesh as world_mesh

IN_PAR: bool
__all__: list[str]
