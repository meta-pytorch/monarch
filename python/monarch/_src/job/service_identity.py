# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Helpers for per-host ``service`` ProcId generation and propagation.

Each Monarch host runs a singleton ``HostAgent`` at ``service``. To avoid
frontend-address disambiguation (host A dials host B via different hostname/
IP strings), every host is given a unique ``ProcId(Uid.instance("service"))``
at launch. The launcher allocates one ProcId per host, passes it to the
worker via env, and the client passes the same IDs to ``attach_to_workers``.

Env transport:
* Per-host launchers (ProcessJob, host_mesh_from_store) set
  ``MONARCH_SERVICE_PROC_ID`` per Popen and workers read it via
  ``service_proc_id_from_env()`` (returns ``None`` if absent for legacy
  fallback).
* Role-level launchers (MAST, Kubernetes, Slurm, SPMD) can only set env once
  per Role/StatefulSet/sbatch for many replicas. They share
  ``MONARCH_SERVICE_PROC_IDS`` as a JSON list plus
  ``MONARCH_SERVICE_PROC_RANK`` (replica_id/SLURM_NODEID/pod-index) and
  workers read ``ranked_service_proc_id_from_env()`` → ``list[rank]``.
  The worker always receives a single ProcId; the JSON list is just
  transport because there is no pre-determined per-host random source
  both sides can derive without extra coordination.
"""

import json
import os
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
# list is just transport. JSON list is used because there is no
# pre-determined per-host random source both client and hosts can derive
# reliably without extra coordination.
SERVICE_PROC_ID_ENV = "MONARCH_SERVICE_PROC_ID"
SERVICE_PROC_IDS_ENV = "MONARCH_SERVICE_PROC_IDS"
SERVICE_PROC_RANK_ENV = "MONARCH_SERVICE_PROC_RANK"


def new_service_proc_id() -> ProcId:
    """Return a fresh instance ProcId for a worker's host service."""
    return ProcId(Uid.instance("service"))


def serialize_service_proc_ids(proc_ids: Sequence[ProcId]) -> str:
    """Serialize service ProcIds for a launcher environment variable."""
    return json.dumps([str(proc_id) for proc_id in proc_ids])


def deserialize_service_proc_ids(serialized: str) -> list[ProcId]:
    """Deserialize service ProcIds at a launcher boundary."""
    return [ProcId.from_string(proc_id) for proc_id in json.loads(serialized)]


def service_proc_id_from_env() -> ProcId | None:
    """Load one service ProcId passed through a launcher environment.

    Returns None if the env var is not set, allowing callers to fall back
    to legacy address-based routing.
    """
    serialized = os.environ.get(SERVICE_PROC_ID_ENV)
    if serialized is None:
        return None
    return ProcId.from_string(serialized)


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
