# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, final

@final
class TestStruct:
    """Minimal Rust struct for testing @rust_struct mixin patching."""

    def __init__(self, value: int) -> None: ...
    def rust_method(self) -> int: ...
    def shared_method(self) -> str: ...

def _make_test_struct(value: int) -> Any: ...
