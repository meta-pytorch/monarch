# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from typing import Any, final

from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask

@final
class _RdmaMemoryRegionView:
    def __init__(self, addr: int, size_in_bytes: int) -> None: ...

@final
class _LocalMemoryHandle:
    def __init__(self, obj: Any, addr: int, size: int) -> None: ...
    @property
    def addr(self) -> int: ...
    @property
    def size(self) -> int: ...
    def read_at(self, offset: int, size: int) -> bytes: ...
    def write_at(self, offset: int, data: bytes) -> None: ...
    def __repr__(self) -> str: ...

@final
class _RdmaManager:
    device: str
    def __repr__(self) -> str: ...
    @classmethod
    def create_rdma_manager_nonblocking(
        self,
        proc_mesh: Any,
        client: Any,
    ) -> PythonTask[_RdmaManager | None]: ...

@final
class _RdmaBuffer:
    name: str

    @classmethod
    def create_rdma_buffer_blocking(
        cls, local: _LocalMemoryHandle, client: Any
    ) -> _RdmaBuffer: ...
    @classmethod
    def create_rdma_buffer_nonblocking(
        cls, local: _LocalMemoryHandle, client: Any
    ) -> PythonTask[Any]: ...
    def drop(self, client: Any) -> PythonTask[None]: ...
    def read_into(
        self,
        dst: _LocalMemoryHandle,
        client: Any,
        timeout: int,
    ) -> PythonTask[Any]: ...
    def write_from(
        self,
        src: _LocalMemoryHandle,
        client: Any,
        timeout: int,
    ) -> PythonTask[Any]: ...
    def size(self) -> int: ...
    def owner_actor_id(self) -> str: ...
    def __reduce__(self) -> tuple[Any, ...]: ...
    def __repr__(self) -> str: ...
    @staticmethod
    def new_from_json(json: str) -> _RdmaBuffer: ...

@final
class _RdmaAction:
    """Builder for a batched RDMA action. Stage `read_into` / `write_from`
    ops then call `submit` to dispatch them. Local-memory races (writes
    overlapping anything, two writes from disjoint local sources are
    fine) are caught eagerly in the `add_*` calls.
    """

    def __init__(self) -> None: ...
    def add_read_into_local(
        self, remote: _RdmaBuffer, local: _LocalMemoryHandle
    ) -> None: ...
    def add_write_from_local(
        self, remote: _RdmaBuffer, local: _LocalMemoryHandle
    ) -> None: ...
    def submit(self, client: Any, timeout: int) -> PythonTask[None]: ...

def is_ibverbs_available() -> bool: ...
def rdma_supported() -> bool: ...
