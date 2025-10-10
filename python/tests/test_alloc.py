# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from unittest import IsolatedAsyncioTestCase

import pytest

from monarch import ProcessAllocator
from monarch._rust_bindings.monarch_hyperactor.alloc import (  # @manual=//monarch/monarch_extension:monarch_extension
    AllocConstraints,
    AllocSpec,
)
from monarch._src.actor.v1 import enabled as v1_enabled


pytestmark: pytest.MarkDecorator = pytest.mark.skipif(
    not v1_enabled, reason="no v0/v1 dependency, so only run with v1"
)


class TestAlloc(IsolatedAsyncioTestCase):
    async def test_basic(self) -> None:
        cmd = "echo hello"
        allocator = ProcessAllocator(cmd)
        spec = AllocSpec(AllocConstraints(), replica=2)
        alloc = allocator.allocate(spec)

        print(alloc)
