# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import functools
import itertools
import os
from typing import Any, Iterator

import torch
from torch._subclasses.fake_tensor import FakeTensor
from torch.utils._pytree import register_pytree_node
from torch.utils.weak import WeakTensorKeyDictionary

_key_table: WeakTensorKeyDictionary = WeakTensorKeyDictionary()
_key_counter: Iterator[int] = itertools.count(1)

# check that we are for sure running on the worker process
_on_worker = os.environ.get("LOCAL_RANK") is not None


def wrap_create(create, xs):
    return create(xs[0])


class OpaqueRef:
    """
    OpaqueRef is a reference to an object that is only resolvable on the worker
    This is used to pass objects from the controller to the worker across User Defined Functions

    Example::
        def init_udf_worker():
            model = nn.Linear(3, 4)
            model_ref = OpaqueRef(model)
            return model_ref

        def run_step_worker(model_ref: OpaqueRef):
            model = model_ref.value
            # do something with model (e.g. forward pass

        # on Controller
        model_ref = init_udf()
        run_step(model_ref)

    """

    def __init__(self, value=None):
        self._key = torch.tensor(next(_key_counter), dtype=torch.int64)
        self.check_worker("create")
        _key_table[self._key] = value

    @classmethod
    def _create(cls, key: torch.Tensor):
        c = cls.__new__(cls)
        c._key = key
        return c

    # like NamedTuple, just pass the call to reconstruct this
    # rather than the dict. This also ensures the OpaqueObject
    # subclass degrades into this class when sent to the worker
    def __reduce_ex__(self, protocol):
        return OpaqueRef._create, (self._key,)

    def __repr__(self):
        return f"OpaqueRef({repr(self._key)})"

    @property
    def value(self) -> Any:
        self.check_worker("access")
        return _key_table[self._key]

    @value.setter
    def value(self, v: Any) -> None:
        self.check_worker("set")
        _key_table[self._key] = v

    def check_worker(self, what):
        # both checks are needed for the case where OpaqueRef() is
        # called on the client with no mesh active.
        in_worker_or_propagate = _on_worker or isinstance(self._key, FakeTensor)
        if not in_worker_or_propagate:
            raise RuntimeError(
                f"Client is attempting to {what} an OpaqueRef. This can only be done in a remote function."
            )


def _flatten(x: OpaqueRef):
    return (x._key,), functools.partial(wrap_create, x._create)


def _unflatten(xs, ctx):
    return ctx(xs)


# pyrefly: ignore [bad-argument-type]
register_pytree_node(OpaqueRef, _flatten, _unflatten)
