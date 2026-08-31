# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Helpers for propagating per-host ``service`` ProcIds.

Each Monarch host runs a singleton ``HostAgent`` at ``service``. To avoid
frontend-address disambiguation (host A dials host B via different hostname/
IP strings), every host is given a unique instance ProcId at launch. Each
launcher owns its identity-allocation policy and passes the resulting ProcIds
to workers via env. At the worker and controller boundaries, the ProcId is
combined with the channel URL into the ProcAddr string accepted by the public
bootstrap APIs.

Env transport:
* Per-host launchers (ProcessJob, host_mesh_from_store) put the complete
  ProcAddr in their worker-address transport.
* Role-level launchers (MAST, Kubernetes, Slurm, SPMD) can only set env once
  per Role/StatefulSet/sbatch for many replicas. They share
  ``MONARCH_SERVICE_PROC_IDS`` as a JSON list plus
  ``MONARCH_SERVICE_PROC_RANK`` (replica_id/SLURM_NODEID/pod-index) and
  workers read ``ranked_service_proc_id_from_env()`` → ``list[rank]``.
  The worker always receives a single ProcId; the JSON list is only a
  transport format and does not prescribe how launchers allocate identities.
"""

import json
import os
import secrets
from collections.abc import Sequence

from monarch._rust_bindings.monarch_hyperactor.proc import ProcId, Uid


# Role-level launchers (MAST, Kubernetes, Slurm, SPMD) set env once per
# Role/StatefulSet/sbatch for many hosts. The scheduler only interpolates
# rank (replica_id/SLURM_NODEID/pod-index) per replica, not per-host
# arbitrary values, so all replicas share the same MONARCH_SERVICE_PROC_IDS
# JSON list and each host selects its single ID via MONARCH_SERVICE_PROC_RANK
# (ranked_service_proc_id_from_env). Per-host launchers (ProcessJob,
# host_mesh_from_store) set MONARCH_SERVICE_PROC_ID directly per Popen and
# use service_proc_id_from_env. Worker always receives a single ProcId; the
# list is only transport; launchers remain responsible for allocating the IDs.
SERVICE_PROC_IDS_ENV = "MONARCH_SERVICE_PROC_IDS"
SERVICE_PROC_RANK_ENV = "MONARCH_SERVICE_PROC_RANK"


def new_service_proc_id() -> ProcId:
    """Return a fresh instance ProcId for a worker's host service."""
    return ProcId(Uid.instance("service"))


def allocate_service_proc_ids(count: int) -> list[ProcId]:
    """Return related, distinct service ProcIds for a ranked worker group."""
    namespace = secrets.randbits(64)
    return [
        ProcId(Uid.instance_from_value((namespace + rank) & ((1 << 64) - 1), "service"))
        for rank in range(count)
    ]


def serialize_service_proc_ids(proc_ids: Sequence[ProcId]) -> str:
    """Serialize service ProcIds for a launcher environment variable."""
    return json.dumps([str(proc_id) for proc_id in proc_ids])


def deserialize_service_proc_ids(serialized: str) -> list[ProcId]:
    """Deserialize service ProcIds at a launcher boundary."""
    return [ProcId.from_string(proc_id) for proc_id in json.loads(serialized)]


def service_proc_addr(address: str, proc_id: ProcId | None) -> str:
    """Combine a channel URL with its service identity when one is available."""
    return address if proc_id is None else f"{proc_id}@{address}"


def service_proc_addrs(
    addresses: Sequence[str], proc_ids: Sequence[ProcId] | None
) -> list[str]:
    """Combine corresponding channel URLs and service identities."""
    if proc_ids is None:
        return list(addresses)
    if len(addresses) != len(proc_ids):
        raise ValueError(
            f"got {len(addresses)} worker addresses and {len(proc_ids)} service ProcIds"
        )
    return [
        service_proc_addr(address, proc_id)
        for address, proc_id in zip(addresses, proc_ids)
    ]


def ranked_service_proc_id_from_env(
    *, rank_env: str = SERVICE_PROC_RANK_ENV
) -> ProcId | None:
    """Select a service ProcId from a ranked launcher environment.

    Returns None if the env var is not set, allowing callers to fall back
    to legacy address-based routing.
    """
    serialized = os.environ.get(SERVICE_PROC_IDS_ENV)
    if serialized is None:
        return None
    proc_ids = deserialize_service_proc_ids(serialized)
    rank_str = os.environ.get(rank_env)
    if rank_str is None:
        return None
    rank = int(rank_str)
    return proc_ids[rank]
