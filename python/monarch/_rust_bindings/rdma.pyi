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

def is_ibverbs_available() -> bool: ...
def rdma_supported() -> bool: ...
