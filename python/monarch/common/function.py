# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import importlib
import logging
import sys
import warnings
from logging import Logger

# pyre-ignore
from pickle import _getattribute, PickleError, whichmodule
from types import BuiltinFunctionType, FunctionType
from typing import (
    Any,
    Callable,
    Dict,
    NamedTuple,
    Optional,
    Protocol,
    runtime_checkable,
)

import cloudpickle

logger: Logger = logging.getLogger(__name__)


@runtime_checkable
class ResolvableFunction(Protocol):
    def resolve(self) -> Callable: ...


ConvertsToResolvable = Any


def _string_resolver(arg: Any) -> Optional[ResolvableFunction]:
    if isinstance(arg, str) and "." in arg:
        return ResolvableFunctionFromPath(arg)


def _torch_resolver(arg: Any) -> Optional[ResolvableFunction]:
    import torch

    if isinstance(arg, torch._ops.OpOverload):
        return ResolvableFunctionFromPath("torch.ops." + str(arg))


def function_to_import_path(arg: BuiltinFunctionType | FunctionType) -> Optional[str]:
    # code replicated from pickler to check if we
    # would successfully be able to pickle this function.
    name = getattr(arg, "__qualname__", None)
    if name is None:
        name = arg.__name__
    try:
        module_name = whichmodule(arg, name)
        __import__(module_name, level=0)
        module = sys.modules[module_name]
        if module_name == "__main__":
            return None  # the workers will not have the same main

        # pytest installs its own custom loaders that do not
        # survive process creation
        try:
            if "pytest" in module.__loader__.__class__.__module__:
                return None
        except AttributeError:
            pass

        obj2, parent = _getattribute(module, name)
        # support annotations that cover up the global impl
        if obj2 is arg or getattr(obj2, "_remote_impl", None) is arg:
            return f"{module_name}.{name}"
    except (PickleError, ImportError, KeyError, AttributeError):
        pass
    return None


def _function_resolver(arg: Any):
    if isinstance(arg, (FunctionType, BuiltinFunctionType)):
        if path := function_to_import_path(arg):
            return ResolvableFunctionFromPath(path)


def _cloudpickle_resolver(arg: Any):
    # @lint-ignore PYTHONPICKLEISBAD
    return ResolvableFromCloudpickle(cloudpickle.dumps(arg))


resolvers = [
    _torch_resolver,
    _string_resolver,
    _function_resolver,
    _cloudpickle_resolver,
]


_cached_resolvers = {}


def maybe_resolvable_function(arg: Any) -> Optional[ResolvableFunction]:
    if arg == "__test_panic":
        return ResolvableFunctionFromPath("__test_panic")
    r = _cached_resolvers.get(arg)
    if r is not None:
        return r
    for resolver in resolvers:
        r = resolver(arg)
        if r is not None:
            _cached_resolvers[arg] = r
            return r
    return None


def resolvable_function(arg: ConvertsToResolvable) -> ResolvableFunction:
    if isinstance(arg, ResolvableFunction):
        return arg
    r = maybe_resolvable_function(arg)
    if r is None:
        raise ValueError(f"Unsupported target for a remote call: {arg!r}")
    return r


class ResolvableFunctionFromPath(NamedTuple):
    path: str

    def resolve(self):
        first, *parts = self.path.split(".")
        if first == "torch":
            function = importlib.import_module("torch")
            for p in parts:
                function = getattr(function, p)
            assert isinstance(function, Callable)
        else:
            modulename, funcname = self.path.rsplit(".", 1)
            module = importlib.import_module(modulename)
            function = getattr(module, funcname)
            # support annotations that cover up the global impl
            actual = getattr(function, "_remote_impl", None)
            return function if actual is None else actual
        return function

    def __str__(self):
        return self.path


class ResolvableFromCloudpickle(NamedTuple):
    data: bytes

    def resolve(self):
        # @lint-ignore PYTHONPICKLEISBAD
        return cloudpickle.loads(self.data)


Propagator = Any
