# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import warnings
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from monarch.tools.config.workspace import Workspace

# Gracefully handle cases where torchx might not be installed
# NOTE: this can be removed once torchx.specs moves to monarch.session
try:
    from torchx import specs
except ImportError:
    pass

if TYPE_CHECKING:
    from torchx.specs import CfgVal

NOT_SET: str = "__NOT_SET__"


def _empty_appdef() -> "specs.AppDef":
    from torchx import specs

    return specs.AppDef(name=NOT_SET)


@dataclass
class Config:
    """
    All configs needed to schedule a mesh of allocators.

    Args:
        scheduler: the name of the scheduler to use, must be one of the registered schedulers in
          `monarch.tools.config.defaults.scheduler_factories`.
        scheduler_args: additional arguments to pass to the scheduler. Scheduler args are different
           for each scheduler. You can run `torchx runopts {scheduler}` from the commandline to get
           a help string. For additional details refer to the scheduler documentation
           in https://docs.pytorch.org/torchx/latest/schedulers.
        workspace: the local workspace to package and mirror on the remote side.
        dryrun: useful for debugging job specs, if `True`, will return the actual scheduler request that
          would've been used to submit the job.
        appdef: the job spec to submit to the scheduler.

    """

    scheduler: str = NOT_SET
    scheduler_args: dict[str, "CfgVal"] = field(default_factory=dict)
    workspace: Workspace = field(default_factory=Workspace.null)
    dryrun: bool = False
    appdef: "specs.AppDef" = field(default_factory=_empty_appdef)

    def __post_init__(self) -> None:
        # workspace used to be Optional[str]
        # while we type it as class Workspace now, handle workspace=None and str for BC
        if self.workspace is None or self.workspace == "":
            deprecation_msg = (
                "Setting `workspace=None` is deprecated."
                " Use `workspace=monarch.tools.config.workspace.Workspace(env=None)` instead."
            )
            warnings.warn(deprecation_msg, FutureWarning, stacklevel=2)
            self.workspace = Workspace.null()
        elif isinstance(self.workspace, str):
            deprecation_msg = (
                f"Setting `workspace='{self.workspace}'` is deprecated."
                f" Use `workspace=monarch.tools.config.workspace.Workspace(dirs=['{self.workspace}'])` instead."
            )
            warnings.warn(deprecation_msg, FutureWarning, stacklevel=2)
            # previous behavior (when workspace was a str pointing to the local project dir)
            # was to copy the local dir into $WORKSPACE_DIR. For example:
            # ~/github/torch/** (local) -> $WORKSPACE_DIR/** (remote)
            # so we map it to "".
            self.workspace = Workspace(dirs={self.workspace: ""})
