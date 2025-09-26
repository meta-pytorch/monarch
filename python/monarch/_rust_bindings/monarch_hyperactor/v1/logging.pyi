# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import final, TYPE_CHECKING

from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask

from monarch._rust_bindings.monarch_hyperactor.v1.proc_mesh import ProcMesh

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.context import Instance

@final
class LoggingMeshClient:
    """
    Python binding for the Rust LoggingMeshClient.
    """
    @staticmethod
    def spawn(
        instance: Instance, proc_mesh: ProcMesh
    ) -> PythonTask[LoggingMeshClient]: ...
    def set_mode(
        self,
        instance: Instance,
        stream_to_client: bool,
        aggregate_window_sec: int | None,
        level: int,
    ) -> None: ...
    def flush(self, instance: Instance) -> PythonTask[None]: ...
