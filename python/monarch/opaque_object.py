# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import functools

import torch
from monarch.common.function import (
    ConvertsToResolvable,
    resolvable_function,
    ResolvableFunction,
)
from monarch.common.opaque_ref import OpaqueRef
from monarch.common.remote import call_on_shard_and_fetch, remote


def _invoke_method(obj: OpaqueRef, method_name: str, *args, **kwargs):
    return getattr(obj.value, method_name)(*args, **kwargs)


def _fresh_opaque_ref():
    return OpaqueRef(torch.zeros(0, dtype=torch.int64))


@remote(propagate=lambda *args, **kwargs: _fresh_opaque_ref())
def _construct_object(
    constructor_resolver: ResolvableFunction, *args, **kwargs
) -> OpaqueRef:
    constructor = constructor_resolver.resolve()
    return OpaqueRef(constructor(*args, **kwargs))


def opaque_method(fn):
    method_name = fn.__name__

    @functools.wraps(fn)
    def impl(self, *args, **kwargs):
        return self.call_method(method_name, fn, *args, **kwargs)

    return impl


class OpaqueObject(OpaqueRef):
    """
    Provides syntax sugar for working with OpaqueObjRef objects on the controller.

    class MyWrapperObject(OpaqueObject):

        # Declare that the object has a_remote_add method.
        # The definition provides the shape propagation rule.
        @opaque_method
        def a_remote_add(self, t: torch.Tensor):
            return t + t

    # on the controller you can now create the wrapper
    obj: MyWrapperObject = MyWrapperObject.construct("path.to.worker.constructor", torch.rand(3, 4))

    # and call its methods
    t: monarch.Tensor = obj.a_remote_add(torch.rand(3, 4))

    This interface can be used to build (unsafe) wrappers around stateful things such torch.nn.Modules
    in order to make porting them to monarch-first structures easier.
    """

    def __init__(self, constructor: ConvertsToResolvable | OpaqueRef, *args, **kwargs):
        if isinstance(constructor, OpaqueRef):
            self._key = constructor._key
        else:
            self._key = _construct_object(
                resolvable_function(constructor), *args, **kwargs
            )._key

    def call_method(self, method_name, propagation, *args, **kwargs):
        endpoint = remote(
            _invoke_method,
            propagate=lambda self, method_name, *args, **kwargs: propagation(
                self, *args, **kwargs
            ),
        )
        return endpoint(self, method_name, *args, **kwargs)

    def call_method_on_shard_and_fetch(self, method_name, *args, **kwargs):
        return call_on_shard_and_fetch(
            remote(_invoke_method), self, method_name, *args, **kwargs
        )
