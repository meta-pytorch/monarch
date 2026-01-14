# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from unittest import IsolatedAsyncioTestCase

from monarch._rust_bindings.monarch_hyperactor.alloc import (  # @manual=//monarch/monarch_extension:monarch_extension
    AllocConstraints,
    AllocSpec,
)
from monarch._src.actor.allocator import ProcessAllocator
from monarch._src.actor.proc_mesh import _get_bootstrap_args


class TestAlloc(IsolatedAsyncioTestCase):
    async def test_basic(self) -> None:
        allocator = ProcessAllocator(*_get_bootstrap_args())
        spec = AllocSpec(AllocConstraints(), replica=2)
        alloc = allocator.allocate(spec)
        await alloc.initialized

        print(alloc)
