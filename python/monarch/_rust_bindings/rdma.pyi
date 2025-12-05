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
        cls, addr: int, size: int, proc_id: str, client: Any
    ) -> _RdmaBuffer: ...
    @classmethod
    def create_rdma_buffer_nonblocking(
        cls, addr: int, size: int, proc_id: str, client: Any
    ) -> PythonTask[Any]: ...
    def drop(self, local_proc_id: str, client: Any) -> PythonTask[None]: ...
    def read_into(
        self,
        addr: int,
        size: int,
        local_proc_id: str,
        client: Any,
        timeout: int,
    ) -> PythonTask[Any]: ...
    def write_from(
        self,
        addr: int,
        size: int,
        local_proc_id: str,
        client: Any,
        timeout: int,
    ) -> PythonTask[Any]: ...
    def size(self) -> int: ...
    def owner_actor_id(self) -> str: ...
    def __reduce__(self) -> tuple[Any, ...]: ...
    def __repr__(self) -> str: ...
    @staticmethod
    def new_from_json(json: str) -> _RdmaBuffer: ...
    @classmethod
    def rdma_supported(cls) -> bool: ...
