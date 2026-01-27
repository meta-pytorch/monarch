# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Telemetry metrics for endpoint operations.

This module defines histograms and counters used to track endpoint
performance metrics. Most metrics (call, call_one, choose, stream)
are now implemented in Rust; only broadcast metrics remain in Python.
"""

from monarch._src.actor.telemetry import METER
from opentelemetry.metrics import Counter, Histogram

# Histogram for measuring endpoint message size
endpoint_message_size_histogram: Histogram = METER.create_histogram(
    name="endpoint_message_size",
    description="Size of endpoint messages",
)

# Broadcast metrics (still in Python, not yet moved to Rust)
endpoint_broadcast_error_counter: Counter = METER.create_counter(
    name="endpoint_broadcast_error.count",
    description="Count of errors in endpoint broadcast operations",
)

endpoint_broadcast_throughput_counter: Counter = METER.create_counter(
    name="endpoint_broadcast_throughput.count",
    description="Count of endpoint broadcast invocations for throughput measurement",
)
