# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import importlib
from functools import partial
from typing import Any, Optional, Sequence, TYPE_CHECKING

"""
This file provides a type annoated shim for using tensor engine functions
from within the actor module which only optionally includes the tensor engine.

Each function that is needed should have a @shim entry below which gives the name,
module, and type of the function. Each function is resolved dynamically the first
time it is used.
"""

if TYPE_CHECKING:
    from monarch._src.actor.actor_mesh import ActorEndpoint, Port, Selection

from monarch._rust_bindings.monarch_hyperactor.buffers import FrozenBuffer


def shim(fn=None, *, module=None):
    if fn is None:
        return partial(shim, module=module)

    impl = None
    name = fn.__name__

    def wrap(*args, **kwargs):
        nonlocal impl
        if impl is None:
            impl = getattr(importlib.import_module(module), name)
        return impl(*args, **kwargs)

    return wrap


@shim(module="monarch.mesh_controller")
def actor_send(
    endpoint: "ActorEndpoint",
    args_kwargs_tuple: bytes,
    refs: "Sequence[Any]",
    port: "Optional[Port[Any]]",
    selection: "Selection",
) -> None: ...


@shim(module="monarch.mesh_controller")
def actor_rref(endpoint, args_kwargs_tuple: FrozenBuffer, refs: Sequence[Any]): ...


@shim(module="monarch.common.remote")
def _cached_propagation(_cache, rfunction, args, kwargs) -> Any: ...


@shim(module="monarch.common.fake")
def fake_call(fn, *args, **kwargs): ...
