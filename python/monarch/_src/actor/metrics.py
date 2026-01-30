# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Telemetry metrics for endpoint operations.

This module defines histograms and counters used to track endpoint
performance metrics. Metrics (call_one, choose, call, stream) are now implemented in Rust
"""

from monarch._src.actor.telemetry import METER
from opentelemetry.metrics import Counter


endpoint_broadcast_error_counter: Counter = METER.create_counter(
    name="endpoint_broadcast_error.count",
    description="Count of errors in endpoint broadcast operations",
)

endpoint_broadcast_throughput_counter: Counter = METER.create_counter(
    name="endpoint_broadcast_throughput.count",
    description="Count of endpoint broadcast invocations for throughput measurement",
)
