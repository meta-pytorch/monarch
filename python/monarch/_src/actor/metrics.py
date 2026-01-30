# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Telemetry metrics for endpoint operations.

This module defines histograms and counters used to track endpoint
performance metrics. Metrics (call_one, choose, call) are now implemented in Rust
"""

from monarch._src.actor.telemetry import METER
from opentelemetry.metrics import Counter, Histogram


# Histogram for measuring endpoint stream latency per yield
endpoint_stream_latency_histogram: Histogram = METER.create_histogram(
    name="endpoint_stream_latency.us",
    description="Latency of endpoint stream operations per yield in microseconds",
)


# Histogram for measuring endpoint message size
endpoint_message_size_histogram: Histogram = METER.create_histogram(
    name="endpoint_message_size",
    description="Size of endpoint messages",
)

endpoint_broadcast_error_counter: Counter = METER.create_counter(
    name="endpoint_broadcast_error.count",
    description="Count of errors in endpoint broadcast operations",
)

endpoint_stream_throughput_counter: Counter = METER.create_counter(
    name="endpoint_stream_throughput.count",
    description="Count of endpoint stream invocations for throughput measurement",
)

endpoint_broadcast_throughput_counter: Counter = METER.create_counter(
    name="endpoint_broadcast_throughput.count",
    description="Count of endpoint broadcast invocations for throughput measurement",
)
