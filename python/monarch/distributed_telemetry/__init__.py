# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Distributed Telemetry - SQL queries over distributed MemTable databases.

Three-component architecture:
1. DatabaseScanner (Rust): Local MemTable operations with child stream merging
2. DistributedTelemetryActor (Python): Orchestrates children, wraps DatabaseScanner
3. QueryEngine (Rust): DataFusion query execution

Usage:
    from monarch.distributed_telemetry import start_telemetry

    engine = start_telemetry().get()
    # ... spawn procs, they're automatically tracked ...
    result = engine.query("SELECT * FROM metrics")
"""

from monarch.distributed_telemetry.actor import (
    DistributedTelemetryActor,
    start_telemetry,
)
from monarch.distributed_telemetry.engine import QueryEngine

__all__ = ["DistributedTelemetryActor", "QueryEngine", "start_telemetry"]
