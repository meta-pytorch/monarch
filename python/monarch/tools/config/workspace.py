# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import shutil
from pathlib import Path

from monarch.tools.config.environment import CondaEnvironment, Environment

ACTIVE_CONDA_ENV = CondaEnvironment()


class Workspace:
    """
    A workspace is one or more local directories that contains your project(s).
    Workspaces can specify an "environment" on which projects are developed and run locally.
    A currently active conda environment is an example of such environment.

    At the time of job submission an ephemeral version of the "image" is built and the
    new job is configured to run on this image. The "image" is the one specified by
    `Role.image` attribute in the job's `AppDef`
    (see `monarch.tools.components.hyperactor.host_mesh()`).

    For example when launching onto Kubernetes, "image" is interpreted as a Docker image (e.g. "name:tag")

    Specifically the ephemeral image contains:

    1. A copy of the workspace directories
    2. (If Applicable) A copy of the currently active environment

    This effectively one-time mirrors the local codebase and environment on the remote machines.

    Workspaces can also be sync'ed interactively on-demand (post job launch) by using
    `monarch.actor.HostMesh.sync_workspace(Workspace)`.

    Usage:

    .. doc-test::

        import pathlib
        from monarch.tools.config import Workspace
        from monarch.tools.config import Config

        HOME = pathlib.Path().home()

        # 1. single project workspace
        config = Config(
            workspace=Workspace(dirs=[HOME / "github" / "torchtitan"]),
        )

        # 2. multiple projects (useful for cross-project development)
        config = Config(
            workspace=Workspace(
                dirs=[
                    # $HOME/torch             (local) -> $WORKSPACE_DIR/torch      (remote)
                    # $HOME/github/torchtitan (local) -> $WORKSPACE_DIR/torchtitan (remote)
                    HOME() / "torch",
                    HOME() / "github" / "torchtitan",
                ]
            ),
        )

        # 3. with explicit local -> remote mappings
        config = Config(
            workspace=Workspace(
                dirs={
                    # $HOME/torch             (local) -> $WORKSPACE_DIR/github/pytorch    (remote)
                    # $HOME/github/torchtitan (local) -> $WORKSPACE_DIR/github/torchtitan (remote)
                    HOME() / "torch" : "github/pytorch"
                    HOME() / "github" / "torchtitan" : "github/torchtitan"
                }
            )
        )
        # -- or flat into WORKSPACE_DIR
        config = Config(
            workspace=Workspace(
                # $HOME/github/torchtitan  (local) -> $WORKSPACE_DIR/  (remote)
                dirs={HOME() / "github" / "torchtitan": ""},
            )
        )

        # 3. no project, everything is installed in my environment (but sync my env)
        config = Config(
            workspace=Workspace(),
        )

        # 4. disable project and environment sync
        config = Config(
            workspace=Workspace.null(),
        )
    """

    def __init__(
        self,
        dirs: list[Path | str] | dict[Path | str, str] | None = None,
        env: Environment | None = ACTIVE_CONDA_ENV,
    ) -> None:
        self.env = env
        self.dirs: dict[Path, str] = {}  # src -> dst

        if dirs is None:
            pass
        elif isinstance(dirs, list):
            for d in dirs:
                assert d, (
                    f"{d} must note be empty as this may have unintended consequences"
                )
                d = Path(d)
                self.dirs[d] = d.name
        else:  # dict
            for src, dst in dirs.items():
                assert src, (
                    f"{src} must note be empty as this may have unintended consequences"
                )
                self.dirs[Path(src)] = dst

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Workspace):
            return False

        return self.env == other.env and self.dirs == other.dirs

    def merge(self, outdir: str | Path) -> None:
        """Merges the dirs of this workspace into the given outdir."""

        outdir = Path(outdir)
        outdir.mkdir(parents=True, exist_ok=True)

        for src, dst in self.dirs.items():
            shutil.copytree(src, outdir / dst, dirs_exist_ok=True)

    # pyre-ignore[2] skip type-hint to avoid torchx dep
    def set_env_vars(self, appdef) -> None:
        """For each role in the appdef, sets the following env vars (if not already set):

        1. `WORKSPACE_DIR`: the root directory of the remote workspace
        2. `PYTHONPATH`: include all the remote workspace dirs for all the roles in the appdef
                (dedups and appends to existing `PYTHONPATH`)
        3. `CONDA_DIR`: (if env is conda) the remote path to the conda env to activate
        """

        # typically this macro comes from torchx.specs.macros.img_root
        # but we use the str repr instead to avoid taking a dep to torchx from this module
        # unittest (test_workspace.py) asserts against torchx.specs.macros.img_root
        # guarding against changes to the macro value
        img_root_macro = "${img_root}"

        for role in appdef.roles:
            remote_workspace_root = role.env.setdefault(
                "WORKSPACE_DIR",
                f"{img_root_macro}/workspace",
            )

            PYTHONPATH = [p for p in role.env.get("PYTHONPATH", "").split(":") if p]
            for dst in self.dirs.values():
                remote_dir = f"{remote_workspace_root}/{dst}"
                if remote_dir not in PYTHONPATH:
                    PYTHONPATH.append(remote_dir)
            role.env["PYTHONPATH"] = ":".join(PYTHONPATH)

            if isinstance(self.env, CondaEnvironment):
                role.env.setdefault("CONDA_DIR", f"{img_root_macro}/conda")

    @staticmethod
    def null() -> "Workspace":
        """Returns a "null" workspace; a workspace with no project dirs and no environment."""
        return Workspace(env=None)
