# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from monarch._rust_bindings.monarch_distributed_telemetry.database_scanner import (
    DatabaseScanner,
)
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.host_mesh import PyMeshAdminRef

def _pre_register_snapshot_schemas(scanner: DatabaseScanner) -> None: ...
def _start_periodic_snapshots(
    scanner: DatabaseScanner,
    admin_ref: PyMeshAdminRef,
    instance: Instance,
    interval_secs: float,
) -> None: ...
