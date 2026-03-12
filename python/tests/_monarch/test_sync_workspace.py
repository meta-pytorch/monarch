# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import shutil
import tempfile
import unittest
from pathlib import Path

from monarch._src.job.process import ProcessJob
from monarch.tools.config.workspace import Workspace


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

        with ProcessJob(
            {"hosts": 1},
            env={"WORKSPACE_DIR": str(remote_workspace_root)},
        ).scoped_state(cached_path=None) as state:
            host = state.hosts

            # local workspace dir is empty & remote workspace dir hasn't been primed yet
            self.assertFalse(remote_workspace_dir.is_dir())

            # create a README file locally and sync workspace
            with open(local_workspace_dir / "README.md", mode="w") as f:
                f.write("hello world")

            await host.sync_workspace(workspace)

            # validate README has been created remotely
            with open(remote_workspace_dir / "README.md", mode="r") as f:
                self.assertListEqual(["hello world"], f.readlines())

            host.shutdown().get()
