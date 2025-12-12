# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Telemetry metrics for endpoint operations.

This module defines all histograms and counters used to track endpoint
performance metrics including latency, errors, and throughput.
"""

from monarch._src.actor.telemetry import METER
from opentelemetry.metrics import Counter, Histogram

# Histogram for measuring endpoint call latency
endpoint_call_latency_histogram: Histogram = METER.create_histogram(
    name="endpoint_call_latency.us",
    description="Latency of endpoint call operations in microseconds",
)

# Histogram for measuring endpoint call_one latency
endpoint_call_one_latency_histogram: Histogram = METER.create_histogram(
    name="endpoint_call_one_latency.us",
    description="Latency of endpoint call_one operations in microseconds",
)

# Histogram for measuring endpoint stream latency per yield
endpoint_stream_latency_histogram: Histogram = METER.create_histogram(
    name="endpoint_stream_latency.us",
    description="Latency of endpoint stream operations per yield in microseconds",
)

# Histogram for measuring endpoint choose latency
endpoint_choose_latency_histogram: Histogram = METER.create_histogram(
    name="endpoint_choose_latency.us",
    description="Latency of endpoint choose operations in microseconds",
)

# Histogram for measuring endpoint message size
endpoint_message_size_histogram: Histogram = METER.create_histogram(
    name="endpoint_message_size",
    description="Size of endpoint messages",
)

# Counters for measuring endpoint errors
endpoint_call_error_counter: Counter = METER.create_counter(
    name="endpoint_call_error.count",
    description="Count of errors in endpoint call operations",
)

endpoint_call_one_error_counter: Counter = METER.create_counter(
    name="endpoint_call_one_error.count",
    description="Count of errors in endpoint call_one operations",
)

endpoint_choose_error_counter: Counter = METER.create_counter(
    name="endpoint_choose_error.count",
    description="Count of errors in endpoint choose operations",
)

endpoint_broadcast_error_counter: Counter = METER.create_counter(
    name="endpoint_broadcast_error.count",
    description="Count of errors in endpoint broadcast operations",
)

# Counters for measuring endpoint throughput (call counts)
endpoint_call_throughput_counter: Counter = METER.create_counter(
    name="endpoint_call_throughput.count",
    description="Count of endpoint call invocations for throughput measurement",
)

endpoint_call_one_throughput_counter: Counter = METER.create_counter(
    name="endpoint_call_one_throughput.count",
    description="Count of endpoint call_one invocations for throughput measurement",
)

endpoint_choose_throughput_counter: Counter = METER.create_counter(
    name="endpoint_choose_throughput.count",
    description="Count of endpoint choose invocations for throughput measurement",
)

endpoint_stream_throughput_counter: Counter = METER.create_counter(
    name="endpoint_stream_throughput.count",
    description="Count of endpoint stream invocations for throughput measurement",
)

endpoint_broadcast_throughput_counter: Counter = METER.create_counter(
    name="endpoint_broadcast_throughput.count",
    description="Count of endpoint broadcast invocations for throughput measurement",
)
