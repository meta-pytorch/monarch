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

# The value a successful probe publishes.
_PROBE_SUCCESS_VALUE: int

@final
class _HandleProbe:
    """Closed control object over one Rust-produced ``Handle``.

    Private contract-test support. The handle comes from the real
    ``PyHandle::spawn`` path, not from ``PythonTask.spawn_handle()``. The probe
    accepts no coroutine, awaitable, callable, future or producer function: it
    only puts a genuine ``Handle`` into one of three reviewed terminal states.

    ``_handle`` is typed ``Any`` rather than ``Handle[Any]`` so this stub does
    not import the legacy ``pytokio`` module solely for typing; the
    characterization suite casts it privately.
    """

    @property
    def _handle(self) -> Any: ...
    @property
    def _started(self) -> bool: ...
    @property
    def _completed(self) -> bool: ...
    def _release(self) -> None: ...
    def _wait_completed(self) -> None: ...

def _make_handle_probe(outcome: str) -> _HandleProbe:
    """Build a probe whose handle reaches ``outcome`` once released.

    ``outcome`` is one of ``"success"``, ``"exception"`` (an ordinary
    ``Exception``) or ``"base_exception"``.
    """
    ...
