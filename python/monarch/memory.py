# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import itertools
import os
from pathlib import Path

import torch
from monarch.common.remote import remote


PATH_KEY = "dir_snapshots"
_counter = itertools.count()


@remote(propagate="inspect")
def record_memory_history() -> None:
    torch.cuda.memory._record_memory_history()


def dump_memory_snapshot(*args, **kwargs) -> None:
    """
    This function wraps torch.cuda.memory._dump_snapshot() to dump memory snapshot remotely.
    """
    assert isinstance(kwargs.get(PATH_KEY, None), str), (
        f"{PATH_KEY} must be passed and must be a string to represent the path to save the memory snapshots."
    )
    id = next(_counter)
    _memory_controller_dump(id, *args, **kwargs)


@remote(propagate="inspect")
def _memory_controller_dump(ident, *args, **kwargs) -> None:
    dir_path = Path(kwargs[PATH_KEY]).absolute()
    os.makedirs(dir_path, exist_ok=True)
    # This is not a synchronized call, so it is okay to call without device mesh.
    rank = torch.distributed.get_rank() if torch.distributed.is_initialized() else 0
    snapshot_path = f"{dir_path}/snapshot_{rank}.pickle"
    torch.cuda.memory._dump_snapshot(filename=snapshot_path)
