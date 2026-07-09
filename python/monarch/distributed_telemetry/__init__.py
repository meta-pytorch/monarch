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
    from monarch.job import ProcessJob, TelemetryConfig

    state = ProcessJob({"hosts": 1}).enable_telemetry(TelemetryConfig()).state()
    client = state.query_engine_client
    assert client is not None
    # ... spawn procs, they're automatically tracked ...
    result = client.query("SELECT * FROM metrics")
"""
