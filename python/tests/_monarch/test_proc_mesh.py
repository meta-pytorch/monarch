# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import contextlib
import importlib.resources
import os
import shutil
import subprocess
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Generator, Optional

from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints, AllocSpec
from monarch._rust_bindings.monarch_hyperactor.channel import (
    ChannelAddr,
    ChannelTransport,
)
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._src.actor.allocator import (
    AllocateMixin,
    RemoteAllocator,
    StaticRemoteAllocInitializer,
)
from monarch._src.actor.host_mesh import HostMesh

from monarch._src.actor.proc_mesh import ProcMesh
from monarch._src.actor.v1 import enabled as v1_enabled
from monarch.tools.config.workspace import Workspace


def proc_mesh_from_alloc(
    allocator: AllocateMixin,
    spec: AllocSpec,
    setup: Optional[Callable[[], None]] = None,
    constraints: Optional[AllocConstraints] = None,
) -> ProcMesh:
    if not v1_enabled:
        alloc = allocator.allocate(spec)
        return ProcMesh.from_alloc(alloc, setup)
    else:
        return HostMesh.allocate_nonblocking(
            "hosts", Extent(*zip(*list(spec.extent.items()))), allocator, constraints
        ).spawn_procs(bootstrap=setup)


@contextlib.contextmanager
def remote_process_allocator(env: dict[str, str]) -> Generator[str, None, None]:
    if __package__:
        cm = importlib.resources.as_file(importlib.resources.files(__package__))
    else:  # running as script (e.g. pytest test_proc_mesh.py)
        cm = contextlib.nullcontext()

    with cm as package_path:
        if package_path is None:
            package_path = ""

        addr = ChannelAddr.any(ChannelTransport.Unix)
        args = ["process_allocator", f"--addr={addr}"]

        env = {
            # prefix PATH with this test module's directory to
            # give 'process_allocator' and 'monarch_bootstrap' binary resources
            # in this test module's directory precedence over the installed ones
            # useful in BUCK where these binaries are added as 'resources' of this test target
            "PATH": f"{package_path}:{os.getenv('PATH', '')}",
            "RUST_LOG": "debug",
        } | env

        process_allocator = subprocess.Popen(
            args=args,
            env=env,
        )
        try:
            yield addr
        finally:
            process_allocator.terminate()
            try:
                five_seconds = 5
                process_allocator.wait(timeout=five_seconds)
            except subprocess.TimeoutExpired:
                process_allocator.kill()


class TestSyncWorkspace(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    async def test_sync_workspace(self) -> None:
        local_workspace_dir = self.tmpdir / "local" / "github" / "torch"
        local_workspace_dir.mkdir(parents=True)

        remote_workspace_root = self.tmpdir / "remote" / "workspace"
        remote_workspace_dir = remote_workspace_root / "torch"
        workspace = Workspace(dirs=[local_workspace_dir])

        with remote_process_allocator(
            env={"WORKSPACE_DIR": str(remote_workspace_root)}
        ) as host:
            allocator = RemoteAllocator(
                world_id="test_sync_workspace",
                initializer=StaticRemoteAllocInitializer(host),
            )
            spec = AllocSpec(AllocConstraints(), hosts=1, gpus=1)
            proc_mesh = proc_mesh_from_alloc(allocator, spec)

            # local workspace dir is empty & remote workspace dir hasn't been primed yet
            self.assertFalse(remote_workspace_dir.is_dir())

            # create a README file locally and sync workspace
            with open(local_workspace_dir / "README.md", mode="w") as f:
                f.write("hello world")

            await proc_mesh.sync_workspace(workspace)

            # validate README has been created remotely
            with open(remote_workspace_dir / "README.md", mode="r") as f:
                self.assertListEqual(["hello world"], f.readlines())
