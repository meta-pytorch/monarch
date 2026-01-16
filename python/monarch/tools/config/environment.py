# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from monarch.tools import utils


class Environment:
    """An environment holds the necessary dependencies for the projects (directories)
    in a `monarch.tools.workspace.Workspace`. When specified as part of a Workspace,
    the local environment is packed into an ephemeral "image" (e.g. Docker) to mirror
    the locally installed packages on the remote job.
    """

    pass


class CondaEnvironment(Environment):
    """Reference to a conda environment.
    If no `conda_prefix` is specified, then defaults to the currently active conda environment.
    """

    def __init__(self, conda_prefix: str | None = None) -> None:
        self._conda_prefix = conda_prefix

    @property
    def conda_prefix(self) -> str:
        """Returns the `conda_prefix` this object was instantiated with or the currently active conda environment
        if no `conda_prefix` was specified in the constructor."""
        if not self._conda_prefix:
            active_conda_prefix = utils.conda.active_env_dir()
            assert active_conda_prefix, (
                "No currently active conda environment. Either specify a `conda_prefix` or activate one."
            )
            return active_conda_prefix
        else:
            return self._conda_prefix

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CondaEnvironment):
            return False

        return self._conda_prefix == other._conda_prefix
