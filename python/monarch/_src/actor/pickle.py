# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import collections.abc as abc
import io
import os
import pickle
import sys
from contextlib import contextmanager, ExitStack
from typing import Any, Callable, Iterable, List, Tuple

import cloudpickle
from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer, FrozenBuffer


def maybe_torch():
    """
    XXX: there is a minor bug if we are sending a gpu tensor to a host that hasn't loaded
    torch yet: the patch to load the tensor on the cpu will not have been applied and it
    will end up on the gpu. The right fix is to switch cpu loading to the save side,
    or to delete the custom behavior entirely.
    """
    return sys.modules.get("torch")


_orig_function_getstate = cloudpickle.cloudpickle._function_getstate


# To ensure that the debugger and tracebacks work on remote hosts
# running code that was pickled by value, we need to monkeypatch
# cloudpickle to set the `__loader__` attribute inside `__globals__`
# for the unpickled function. That way, when the remote host tries
# to load the source code for the function, it will use the RemoteImportLoader
# to retrieve the source code from the root client, where it *ostensibly*
# exists.
def _function_getstate(func):
    from monarch._src.actor.source_loader import RemoteImportLoader

    state, slotstate = _orig_function_getstate(func)
    slotstate["__globals__"]["__loader__"] = RemoteImportLoader(
        func.__code__.co_filename
    )
    return state, slotstate


cloudpickle.cloudpickle._function_getstate = _function_getstate


class _Pickler(cloudpickle.Pickler):
    def __init__(self, filter, f: Buffer | io.BytesIO):
        self.f = f
        super().__init__(self.f)
        self._filter = filter
        self._saved = []

    def persistent_id(self, obj):
        if not self._filter(obj):
            return None
        self._saved.append(obj)
        return len(self._saved) - 1


class _Unpickler(pickle.Unpickler):
    def __init__(self, data: bytes | FrozenBuffer, sequence: Iterable[Any]):
        if isinstance(data, FrozenBuffer):
            super().__init__(data)
        else:
            super().__init__(io.BytesIO(data))
        self._iter = iter(sequence)
        self._values = []

    def persistent_load(self, id):
        while id >= len(self._values):
            self._values.append(next(self._iter))
        return self._values[id]


def flatten(obj: Any, filter: Callable[[Any], bool]) -> Tuple[List[Any], FrozenBuffer]:
    buffer = Buffer()
    pickler = _Pickler(filter, buffer)
    pickler.dump(obj)

    return pickler._saved, buffer.freeze()


def unflatten(data: FrozenBuffer | bytes, values: Iterable[Any]) -> Any:
    with ExitStack() as stack:
        torch = maybe_torch()
        if torch is not None:
            stack.enter_context(load_tensors_on_cpu())
            stack.enter_context(torch.utils._python_dispatch._disable_current_modes())
        up = _Unpickler(data, values)
        return up.load()


@contextmanager
def load_tensors_on_cpu():
    # Ensure that any tensors load from CPU via monkeypatching how Storages are
    # loaded.
    import torch

    old = torch.storage._load_from_bytes
    try:
        torch.storage._load_from_bytes = lambda b: torch.load(
            io.BytesIO(b) if isinstance(b, bytes) else b,
            map_location="cpu",
            weights_only=False,
        )
        yield
    finally:
        torch.storage._load_from_bytes = old
