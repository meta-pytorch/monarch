# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Optional

from monarch._rust_bindings.monarch_hyperactor.actor import PickledMessageClientActor
from monarch._rust_bindings.monarch_hyperactor.proc import Proc

def init_proc(
    *,
    proc_id: str,
    bootstrap_addr: str,
    timeout: int = 5,
    supervision_update_interval: int = 0,
    listen_addr: Optional[str] = None,
) -> Proc:
    """
    Helper function to bootstrap a new Proc with multiprocess supervision.

    Arguments:
    - `proc_id`: String representation of the ProcId eg. `"world_name[0]"`
    - `bootstrap_addr`: String representation of the channel address of the system
        actor. eg. `"tcp![::1]:2345"`
    - `timeout`: Number of seconds to wait to successfully connect to the system.
    - `supervision_update_interval`: Number of seconds between supervision updates.
    - `listen_addr`: String representation of the channel address of the proc
        actor. eg. `"tcp![::1]:2345"`
    """
    ...

def world_status(actor: PickledMessageClientActor) -> dict[str, str]:
    """
    Get the status of all worlds from the system actor.

    Arguments:
    - `actor`: A client actor to use for querying the system.

    Returns:
    - A dictionary mapping world IDs to their status strings.
    """
    ...
