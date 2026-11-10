# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Helpers for explicit per-host ``service`` ProcIds.

Each Monarch host runs a singleton ``HostAgent`` in its ``service`` proc.
Launchers normally start workers from a bare channel URL: the worker mints a
fresh service ProcId at startup, and ``attach_to_workers`` discovers it by
querying the same URL. Use these helpers only when the caller must choose the
identity itself.
"""

from monarch._rust_bindings.monarch_hyperactor.proc import ProcId, Uid


def new_service_proc_id() -> ProcId:
    """Return a fresh instance ProcId for a worker's host service."""
    return ProcId(Uid.instance("service"))


def service_proc_addr(address: str, proc_id: ProcId) -> str:
    """Combine a channel URL with its service identity."""
    return f"{proc_id}@{address}"
