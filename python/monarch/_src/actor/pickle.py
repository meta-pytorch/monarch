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
from collections import ChainMap
from contextlib import ExitStack
from typing import Any, Callable, Iterable, List, Tuple

import cloudpickle
from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer, FrozenBuffer


def maybe_torch():
    """
    Returns the torch module if it has been loaded, otherwise None.
    """
    if "torch" in sys.modules:
        # we avoid eagerly loading torch because it
        # takes a long time to import and slows down startup
        # for programs that do not use it.

        # But once it has been loaded, we know we might need it.
        # We have to explicitly import now because torch
        # might now be completely loaded yet.
        import torch

        return torch
    return None


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


def _load_from_bytes(b):
    import torch  # if we haven't loaded it

    # we have to now
    return torch.load(
        io.BytesIO(b) if isinstance(b, bytes) else b,
        map_location="cpu",
        weights_only=False,
    )


def _torch_storage(obj):
    import torch  # we only get here if torch is already imported

    b = io.BytesIO()
    torch.save(obj, b, _use_new_zipfile_serialization=False)
    return (_load_from_bytes, (b.getvalue(),))


class _Pickler(cloudpickle.Pickler):
    _torch_initialized = False
    _dispatch_table = {}

    dispatch_table = ChainMap(_dispatch_table, cloudpickle.Pickler.dispatch_table)

    def __init__(self, filter, f: Buffer | io.BytesIO):
        self.f = f
        super().__init__(self.f)
        self._filter = filter
        self._saved = []
        _Pickler._init_torch_dispatch()

    @classmethod
    def _init_torch_dispatch(cls):
        # already initialized
        if cls._torch_initialized:
            return
        torch = maybe_torch()
        if torch is not None:
            keys = [torch.storage.UntypedStorage, torch.storage.TypedStorage]
            scan = 0
            while scan < len(keys):
                keys.extend(keys[scan].__subclasses__())
                scan += 1
            for key in keys:
                cls._dispatch_table[key] = _torch_storage
            cls._torch_initialized = True

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
            stack.enter_context(torch.utils._python_dispatch._disable_current_modes())
        up = _Unpickler(data, values)
        return up.load()
