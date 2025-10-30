# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import inspect

from monarch._src.actor.actor_mesh import context, DebugContext
from monarch._src.actor.debugger.debug_controller import debug_controller
from monarch._src.actor.debugger.pdb_wrapper import PdbWrapper


def remote_breakpointhook() -> None:
    frame = inspect.currentframe()
    assert frame is not None
    frame = frame.f_back
    assert frame is not None

    ctx = context()
    rank = ctx.message_rank
    pdb_wrapper = PdbWrapper(
        rank.rank,
        {k: rank[k] for k in rank},
        ctx.actor_instance.actor_id,
        debug_controller(),
    )
    DebugContext.set(DebugContext(pdb_wrapper))
    pdb_wrapper.set_trace(frame)
