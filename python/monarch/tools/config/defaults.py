# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Defines defaults for ``monarch.tools``"""

import warnings
from typing import Callable

from monarch.tools.components import hyperactor
from monarch.tools.config import Config
from monarch.tools.config.workspace import Workspace
from torchx import specs
from torchx.schedulers import (
    docker_scheduler,
    kubernetes_scheduler,
    local_scheduler,
    SchedulerFactory,
    slurm_scheduler,
)


def component_fn(scheduler: str) -> Callable[..., specs.AppDef]:
    """The default TorchX component function for the scheduler"""
    return hyperactor.host_mesh


def scheduler_factories() -> dict[str, SchedulerFactory]:
    """Supported schedulers (name -> scheduler static factory method)"""
    return {  # pyre-ignore[7]
        # --- local schedulers (no multi-host support) ---
        "local_cwd": local_scheduler.create_scheduler,
        "local_docker": docker_scheduler.create_scheduler,
        # --- remote schedulers (yes multi-host support) ---
        "slurm": slurm_scheduler.create_scheduler,
        "k8s": kubernetes_scheduler.create_scheduler,
    }


def config(scheduler: str, workspace: str | None = None) -> Config:
    """The default :py:class:`~monarch.tools.config.Config` to use when submitting to the provided ``scheduler``."""
    warnings.warn(
        "`defaults.config()` is deprecated, prefer instantiating `Config()` directly",
        FutureWarning,
        stacklevel=2,
    )
    return Config(
        scheduler=scheduler,
        workspace=Workspace(dirs={workspace: ""}) if workspace else Workspace.null(),
    )


def dryrun_info_formatter(dryrun_info: specs.AppDryRunInfo) -> Callable[..., str]:
    """Used to attach a formatter to the dryrun info when running
    :py:function:`~monarch.tools.commands.create` in ``dryrun`` mode so that
    the returned ``AppDryrunInfo`` can be printed to console.
    """
    # no-op, use the default formatter already attached to the dryrun info
    return dryrun_info._fmt
