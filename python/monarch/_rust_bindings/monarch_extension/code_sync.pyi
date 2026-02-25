# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from pathlib import Path
from typing import Any, Dict, final

from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh

class WorkspaceLocation:
    """
    Python binding for the Rust WorkspaceLocation enum.
    """
    @final
    class Constant(WorkspaceLocation):
        def __init__(self, path: str | Path) -> None: ...

    @final
    class FromEnvVar(WorkspaceLocation):
        def __init__(self, env: str, relpath: str | Path) -> None: ...

    def resolve(self) -> Path:
        """
        Resolve the workspace location to a Path.
        """
        ...

@final
class WorkspaceShape:
    """
    Python binding for the Rust WorkspaceShape struct.
    """
    @staticmethod
    def shared(label: str) -> "WorkspaceShape": ...
    @staticmethod
    def exclusive() -> "WorkspaceShape": ...

class CodeSyncMethod:
    """
    Python binding for the Rust CodeSyncMethod enum.
    """

    @final
    class Rsync(CodeSyncMethod):
        def __init__(self) -> None: ...

    @final
    class CondaSync(CodeSyncMethod):
        def __init__(
            self, prefix_path_replacements: Dict[str | Path, WorkspaceLocation]
        ) -> None: ...

@final
class RemoteWorkspace:
    """
    Python binding for the Rust RemoteWorkspace struct.
    """
    def __init__(self, location: WorkspaceLocation, shape: WorkspaceShape) -> None: ...

@final
class WorkspaceConfig:
    """
    Python binding for the Rust WorkspaceConfig struct.
    """
    def __init__(
        self,
        *,
        local: Path,
        remote: RemoteWorkspace,
        method: CodeSyncMethod = ...,
    ) -> None: ...

@final
class CodeSyncMeshClient:
    """
    Python binding for the Rust CodeSyncMeshClient.
    """
    @staticmethod
    def spawn_blocking(
        client: Any,
        proc_mesh: ProcMesh,
    ) -> CodeSyncMeshClient: ...
    async def sync_workspace(
        self,
        *,
        instance: Instance,
        local: str,
        remote: RemoteWorkspace,
        auto_reload: bool = False,
    ) -> None: ...
    async def sync_workspaces(
        self,
        *,
        instance: Instance,
        workspaces: list[WorkspaceConfig],
        auto_reload: bool = False,
    ) -> None: ...
