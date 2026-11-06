# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from typing import List, Tuple

from monarch._rust_bindings.monarch_distributed_telemetry import (
    database_scanner as database_scanner,
    query_engine as query_engine,
)
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.mailbox import PortId
from monarch._rust_bindings.monarch_hyperactor.proc import ActorAddr

def _start_socket_ingest(
    scanner: database_scanner.DatabaseScanner, socket_path: str
) -> None:
    """Start Unix-socket ingest for a database scanner."""
    ...

def _register_telemetry_schemas(
    scanner: database_scanner.DatabaseScanner,
) -> None:
    """Register trace, entity, and metric schemas for a database scanner."""
    ...

def _set_unix_socket_sink_path(socket_path: str) -> None:
    """Activate the process-global Unix socket sink."""
    ...

def _post_scan_counts(
    instance: Instance, dest: PortId, counts: List[Tuple[ActorAddr, int]]
) -> None:
    """Post every collector's batch count to a distributed query's port."""
    ...
