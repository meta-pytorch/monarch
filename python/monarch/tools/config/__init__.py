# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import warnings
from dataclasses import dataclass, field
from typing import Any

from monarch.tools.config.workspace import Workspace

# Gracefully handle cases where torchx might not be installed
# NOTE: this can be removed once torchx.specs moves to monarch.session
try:
    from torchx import specs
except ImportError:
    pass

NOT_SET: str = "__NOT_SET__"


def _empty_appdef() -> "specs.AppDef":
    return specs.AppDef(name=NOT_SET)


@dataclass
class Config:
    """
    All configs needed to schedule a mesh of allocators.
    """

    scheduler: str = NOT_SET
    scheduler_args: dict[str, Any] = field(default_factory=dict)
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
