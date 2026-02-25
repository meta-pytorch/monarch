# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import shutil
import tempfile
import unittest
from pathlib import Path

from monarch.tools.config.environment import CondaEnvironment
from monarch.tools.config.workspace import Workspace
from torchx import specs


class TestWorkspace(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="TestWorkspace_"))

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    def touch(self, *path: str) -> Path:
        f = Path(os.path.join(self.tmpdir, *path))
        f.parent.mkdir(parents=True, exist_ok=True)
        f.touch()
        return f

    def test_workspace_dir_list(self) -> None:
        w = Workspace(
            dirs=[
                self.tmpdir / "foo",
                self.tmpdir / "github" / "bar",
            ]
        )
        self.assertDictEqual(
            {
                self.tmpdir / "foo": "foo",
                self.tmpdir / "github" / "bar": "bar",
            },
            w.dirs,
        )

    def test_workspace_dir_map(self) -> None:
        w = Workspace(
            dirs={
                self.tmpdir / "foo": "github/foo",
                self.tmpdir / "github" / "bar": "github/bar",
                self.tmpdir / "github" / "torchx": "",
            }
        )
        self.assertDictEqual(
            {
                self.tmpdir / "foo": "github/foo",
                self.tmpdir / "github" / "bar": "github/bar",
                self.tmpdir / "github" / "torchx": "",
            },
            w.dirs,
        )

    def test_merge(self) -> None:
        def assert_exists(outdir: Path, *path: str) -> None:
            f = outdir / os.path.join(*path)
            self.assertTrue(f.exists())
            self.assertTrue(f.is_file())

        self.touch("github", "torch", "pyproject.toml")
        self.touch("github", "torch", "torch", "__init__.py")

        self.touch("torchtitan", "pyproject.toml")
        self.touch("torchtitan", "torchtitan", "__init__.py")

        self.touch("github", "torchx", "README.md")
        self.touch("github", "torchx", "torchx", "__init__.py")

        outdir = self.tmpdir / "out"

        Workspace(
            dirs={
                self.tmpdir / "github" / "torch": "torch",
                self.tmpdir / "torchtitan": "torchtitan",
                self.tmpdir / "github" / "torchx": "",
            }
        ).merge(outdir)

        assert_exists(outdir, "torch", "pyproject.toml")
        assert_exists(outdir, "torch", "torch", "__init__.py")

        assert_exists(outdir, "torchtitan", "pyproject.toml")
        assert_exists(outdir, "torchtitan", "torchtitan", "__init__.py")

        assert_exists(outdir, "README.md")
        assert_exists(outdir, "torchx", "__init__.py")

    def test_null(self) -> None:
        self.assertDictEqual({}, Workspace.null().dirs)
        self.assertIsNone(Workspace.null().env)

    def test_set_env_vars(self) -> None:
        w = Workspace(
            dirs={
                self.tmpdir / "github" / "torch": "torch",
                self.tmpdir / "torchtitan": "torchtitan",
                self.tmpdir / "github" / "torchx": "",
            },
            env=None,
        )

        appdef = specs.AppDef(
            name="test",
            roles=[
                specs.Role("0", "N/A", env={}),
                specs.Role("1", "N/A", env={"WORKSPACE_DIR": "/tmp/workspace"}),
                specs.Role("2", "N/A", env={"PYTHONPATH": "/do/not:/overwrite"}),
                specs.Role(
                    "3",
                    "N/A",
                    env={
                        "PYTHONPATH": f"{specs.macros.img_root}/workspace/torch:/tmp/workspace"
                    },
                ),
            ],
        )

        w.set_env_vars(appdef)

        PYTHONPATH = ":".join(
            [
                f"{specs.macros.img_root}/workspace/torch",
                f"{specs.macros.img_root}/workspace/torchtitan",
                f"{specs.macros.img_root}/workspace/",
            ]
        )
        WORKSPACE_DIR = f"{specs.macros.img_root}/workspace"

        # -- check role0
        role0 = appdef.roles[0]
        self.assertEqual(WORKSPACE_DIR, role0.env["WORKSPACE_DIR"])
        self.assertEqual(PYTHONPATH, role0.env["PYTHONPATH"])
        self.assertNotIn("CONDA_DIR", role0.env)

        # -- check role1
        role1 = appdef.roles[1]
        self.assertEqual("/tmp/workspace", role1.env["WORKSPACE_DIR"])
        self.assertEqual(
            "/tmp/workspace/torch:/tmp/workspace/torchtitan:/tmp/workspace/",
            role1.env["PYTHONPATH"],
        )
        self.assertNotIn("CONDA_DIR", role1.env)

        # -- check role2
        role2 = appdef.roles[2]
        self.assertEqual(WORKSPACE_DIR, role2.env["WORKSPACE_DIR"])
        self.assertEqual(f"/do/not:/overwrite:{PYTHONPATH}", role2.env["PYTHONPATH"])
        self.assertNotIn("CONDA_DIR", role2.env)

        # -- check role3
        role3 = appdef.roles[3]
        self.assertEqual(WORKSPACE_DIR, role3.env["WORKSPACE_DIR"])
        self.assertEqual(
            ":".join(
                [
                    f"{specs.macros.img_root}/workspace/torch",
                    "/tmp/workspace",
                    f"{specs.macros.img_root}/workspace/torchtitan",
                    f"{specs.macros.img_root}/workspace/",
                ]
            ),
            role3.env["PYTHONPATH"],
        )
        self.assertNotIn("CONDA_DIR", role2.env)

    def test_set_CONDA_DIR(self) -> None:
        w = Workspace(env=CondaEnvironment(conda_prefix="_IGNORED_"))
        appdef = specs.AppDef(
            name="test",
            roles=[
                specs.Role("0", "N/A", env={}),
                specs.Role("1", "N/A", env={"CONDA_DIR": "/do/not/overwrite"}),
            ],
        )

        w.set_env_vars(appdef)

        self.assertEqual(
            f"{specs.macros.img_root}/conda", appdef.roles[0].env["CONDA_DIR"]
        )
        self.assertEqual("/do/not/overwrite", appdef.roles[1].env["CONDA_DIR"])
