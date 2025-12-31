# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import importlib
from functools import partial
from typing import (
    Any,
    Callable,
    Optional,
    overload,
    ParamSpec,
    Sequence,
    TYPE_CHECKING,
    TypeVar,
)

"""
This file provides a type annoated shim for using tensor engine functions
from within the actor module which only optionally includes the tensor engine.

Each function that is needed should have a @shim entry below which gives the name,
module, and type of the function. Each function is resolved dynamically the first
time it is used.
"""

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.actor import MethodSpecifier
    from monarch._src.actor.actor_mesh import Port

from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer
from monarch._rust_bindings.monarch_hyperactor.pytokio import PendingPickleState

P = ParamSpec("P")
F = TypeVar("F", bound=Callable[..., Any])


@overload
def shim(fn: F, *, module: Optional[str] = None) -> F: ...


@overload
def shim(
    fn: None = None, *, module: Optional[str] = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...


def shim(
    fn: Optional[Callable[..., Any]] = None, *, module: Optional[str] = None
) -> Any:
    if fn is None:
        return partial(shim, module=module)

    impl: Optional[Callable[..., Any]] = None
    name: str = fn.__name__

    def wrap(*args: Any, **kwargs: Any) -> Any:
        nonlocal impl
        if impl is None:
            # TODO: See if there's a reasonable way to assert that the module name is not none
            # pyre-ignore Incompatible parameter type [6]: In call `importlib.import_module`, for 1st positional argument, expected `str` but got `Optional[str]`
            impl = getattr(importlib.import_module(module), name)
        return impl(*args, **kwargs)

    return wrap


@shim(module="monarch.mesh_controller")
def create_actor_message(
    method_name: "MethodSpecifier",
    proc_mesh: "Optional[Any]",
    args_kwargs_tuple: Buffer,
    refs: "Sequence[Any]",
    port: "Optional[Port[Any]]",
    pending_pickle_state: Optional[PendingPickleState],
) -> "Any": ...


@shim(module="monarch.mesh_controller")
def actor_rref(
    endpoint: Any,
    args_kwargs_tuple: Buffer,
    refs: Sequence[Any],
    pending_pickle_state: Optional[PendingPickleState],
) -> Any: ...


@shim(module="monarch.common.remote")
def _cached_propagation(_cache: Any, rfunction: Any, args: Any, kwargs: Any) -> Any: ...


@shim(module="monarch.common.fake")
def fake_call(fn: Any, *args: Any, **kwargs: Any) -> Any: ...
