# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import socket
from datetime import timedelta
from typing import Any

import torch
from packaging.version import parse
from torch.distributed import rendezvous
from torch.distributed.distributed_c10d import (
    _new_process_group_helper,
    _world,
    Backend,
    default_pg_timeout,
    PrefixStore,
)


def get_local_ip() -> str:
    """Return this host's local IPv4 address (UDP connect trick to ``8.8.8.8``)."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]


def get_full_group_port() -> int:
    return 60000 + int(os.environ.get("CUDA_VISIBLE_DEVICES", "0")[0]) * 100


def init_custom_process_group(
    backend: Backend | str | None = None,
    init_method: str | None = None,
    timeout: timedelta | None = None,
    world_size: int = -1,
    rank: int = -1,
    store=None,
    group_name: str | None = None,
    pg_options: Any | None = None,
):
    """Create a named process group without touching the default group.

    Mirrors ``slime.utils.distributed_utils.init_process_group`` and
    ``sglang.srt.utils.common.init_custom_process_group``.

    .. note::

       ``device_id`` is intentionally **not** passed to
       ``_new_process_group_helper``.  Passing it triggers
       ``eagerConnectSingleDevice`` which deadlocks when the process already
       holds many NCCL communicators (e.g. 14+ from SGLang TP/EP groups or
       23+ from Megatron parallelism groups).  Without ``device_id``, NCCL
       communicators are created lazily on the first collective — which works
       reliably in all tested configurations (16 ranks, 2 nodes, existing PGs
       on both sides).
    """
    assert (store is None) or (init_method is None), (
        "Cannot specify both init_method and store."
    )

    if store is not None:
        assert world_size > 0, "world_size must be positive if using store"
        assert rank >= 0, "rank must be non-negative if using store"
    elif init_method is None:
        init_method = "env://"

    if backend:
        backend = Backend(backend)
    else:
        backend = Backend("undefined")

    if timeout is None:
        timeout = timedelta(seconds=1800)  # 30 min — inter-node NCCL PG can be slow

    if store is None:
        rendezvous_uri = init_method
        assert rendezvous_uri is not None
        print(
            f"[wbridge-pg] rank {rank}/{world_size} rendezvous at {rendezvous_uri} group={group_name}",
            flush=True,
        )
        rendezvous_iterator = rendezvous(
            rendezvous_uri, rank, world_size, timeout=timeout
        )
        store, rank, world_size = next(rendezvous_iterator)
        print(
            f"[wbridge-pg] rank {rank}/{world_size} rendezvous done, creating group",
            flush=True,
        )
        store.set_timeout(timeout)
        gn = group_name
        assert gn is not None
        store = PrefixStore(gn, store)

    # NOTE: The pg_options parameter was renamed into backend_options in PyTorch 2.6.0
    pg_options_param_name = (
        "backend_options" if parse(torch.__version__) >= parse("2.6") else "pg_options"
    )
    pg, _ = _new_process_group_helper(
        world_size,
        rank,
        [],
        backend,
        store,
        group_name=group_name,
        **{pg_options_param_name: pg_options},
        timeout=timeout,
        # NOTE: no device_id — see docstring for why eager connect is avoided.
    )
    print(f"[wbridge-pg] rank {rank}/{world_size} process group created", flush=True)

    _world.pg_group_ranks[pg] = {i: i for i in range(world_size)}
    return pg
