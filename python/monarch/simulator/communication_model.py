# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Communication latency models for collective operations.

Provides latency-bandwidth models for estimating communication time of
collective operations (all_reduce, reduce_scatter, all_gather, all_to_all)
and point-to-point sends, based on tensor size, dtype, device count, and
network parameters.
"""

import math
from dataclasses import dataclass
from typing import List, Optional


# Bytes per element for common dtypes.
DTYPE_BYTES = {
    "float32": 4,
    "float64": 8,
    "float16": 2,
    "bfloat16": 2,
    "int8": 1,
    "int16": 2,
    "int32": 4,
    "int64": 8,
    "bool": 1,
    "uint8": 1,
}


@dataclass
class NetworkConfig:
    """Network parameters for communication modeling.

    Attributes:
        intra_node_bandwidth_gbs: NVLink bandwidth in GB/s (default: H100 NVLink 4.0).
        inter_node_bandwidth_gbs: Inter-node bandwidth in GB/s (default: 400Gbps IB/RoCE bidirectional).
        single_hop_latency_us: Per-hop latency in microseconds.
        gpus_per_node: Number of GPUs per node. Determines whether intra-node
            or inter-node bandwidth is used.
        contention_factor: Multiplicative factor (0, 1] applied to effective
            bandwidth in contention-sensitive collectives like AllToAll.
            Lower values model heavier contention. Default 0.5.
    """

    intra_node_bandwidth_gbs: float = 900.0
    inter_node_bandwidth_gbs: float = 50.0
    single_hop_latency_us: float = 2.0
    gpus_per_node: int = 8
    contention_factor: float = 0.5


def tensor_size_bytes(shape: List[int], dtype: str) -> int:
    """Compute tensor size in bytes from shape and dtype string."""
    num_elements = 1
    for d in shape:
        num_elements *= d
    return num_elements * DTYPE_BYTES.get(dtype, 4)


# ---------------------------------------------------------------------------
# Low-level latency functions (seconds). Ported from aiperf_modeling.
# ---------------------------------------------------------------------------


def _bandwidth_bytes_per_sec(num_devices: int, config: NetworkConfig) -> float:
    """Select bandwidth based on whether communication is intra- or inter-node."""
    if num_devices <= config.gpus_per_node:
        return config.intra_node_bandwidth_gbs * 1e9
    return config.inter_node_bandwidth_gbs * 1e9


def _hop_latency_s(config: NetworkConfig) -> float:
    return config.single_hop_latency_us * 1e-6


def p2p_latency(
    message_size: int,
    bandwidth_bps: float,
    single_hop_latency_s: float = 2e-6,
) -> float:
    """Point-to-point send/recv latency in seconds."""
    return single_hop_latency_s + (message_size / bandwidth_bps)


def ring_allreduce_latency(
    num_nodes: int,
    data_size: int,
    single_hop_latency: float,
    bandwidth_bps: Optional[float] = None,
) -> float:
    """Ring-based AllReduce latency in seconds."""
    num_hops = num_nodes // 2  # bidirectional
    latency = num_hops * single_hop_latency
    if bandwidth_bps is not None:
        data_volume = 2 * data_size * (num_nodes - 1) // num_nodes
        latency = max(latency, data_volume / bandwidth_bps)
    return latency


def reduce_scatter_latency(
    num_nodes: int,
    data_size: int,
    single_hop_latency: float,
    bandwidth_bps: Optional[float] = None,
) -> float:
    """ReduceScatter latency in seconds (one phase of ring AllReduce)."""
    latency = (num_nodes - 1) * single_hop_latency
    if bandwidth_bps is not None:
        data_volume = data_size * (num_nodes - 1) // num_nodes
        latency = max(latency, data_volume / bandwidth_bps)
    return latency


def allgather_latency(
    num_nodes: int,
    data_size: int,
    single_hop_latency: float,
    bandwidth_bps: Optional[float] = None,
) -> float:
    """AllGather latency in seconds (same comm pattern as ReduceScatter)."""
    return reduce_scatter_latency(
        num_nodes, data_size, single_hop_latency, bandwidth_bps
    )


def alltoall_latency(
    num_nodes: int,
    data_size: int,
    single_hop_latency: float,
    bandwidth_bps: Optional[float] = None,
    *,
    contention_factor: float = 0.5,
) -> float:
    """AllToAll latency in seconds."""
    chunk_size = data_size // num_nodes
    total_data = chunk_size * (num_nodes - 1)
    latency = single_hop_latency
    if bandwidth_bps is not None:
        effective_bw = bandwidth_bps * contention_factor
        latency = max(latency, total_data / effective_bw)
    return latency


# ---------------------------------------------------------------------------
# High-level estimators used by ir.py
# ---------------------------------------------------------------------------

# Map IR reduce_type strings to the corresponding latency function.
_COLLECTIVE_FN = {
    "all_reduce": ring_allreduce_latency,
    "reduce_scatter": reduce_scatter_latency,
    "all_gather": allgather_latency,
    "all_to_all": alltoall_latency,
}


def estimate_collective_time_us(
    reduce_type: str,
    tensor_shape: List[int],
    dtype_str: str,
    num_devices: int,
    config: Optional[NetworkConfig] = None,
) -> int:
    """Estimate collective communication time in microseconds.

    Args:
        reduce_type: Collective type string from the IR timing key
            (e.g. "all_reduce", "reduce_scatter").
        tensor_shape: Tensor dimensions, e.g. [1024, 1024].
        dtype_str: Dtype string, e.g. "float32".
        num_devices: Number of participating devices.
        config: Network configuration. Uses defaults if None.

    Returns:
        Estimated latency in microseconds (int).
    """
    if config is None:
        config = NetworkConfig()

    if num_devices <= 1:
        return 0

    data_size = tensor_size_bytes(tensor_shape, dtype_str) if tensor_shape else 0
    if data_size == 0:
        return 0

    bw = _bandwidth_bytes_per_sec(num_devices, config)
    hop = _hop_latency_s(config)

    fn = _COLLECTIVE_FN.get(reduce_type, ring_allreduce_latency)
    kwargs = {}
    if reduce_type == "all_to_all":
        kwargs["contention_factor"] = config.contention_factor
    latency_s = fn(num_devices, data_size, hop, bw, **kwargs)
    return max(1, int(math.ceil(latency_s * 1e6)))


def estimate_send_time_us(
    tensor_shape: List[int],
    dtype_str: str,
    config: Optional[NetworkConfig] = None,
) -> int:
    """Estimate point-to-point send time in microseconds.

    Args:
        tensor_shape: Tensor dimensions, e.g. [512, 512].
        dtype_str: Dtype string, e.g. "float32".
        config: Network configuration. Uses defaults if None.

    Returns:
        Estimated latency in microseconds (int).
    """
    if config is None:
        config = NetworkConfig()

    data_size = tensor_size_bytes(tensor_shape, dtype_str) if tensor_shape else 0
    if data_size == 0:
        return 0

    # Conservative: assume inter-node BW since the IR doesn't carry
    # source/destination topology information.
    # Latency model: single_hop_latency + data_size / bandwidth.
    bw = config.inter_node_bandwidth_gbs * 1e9
    hop = config.single_hop_latency_us * 1e-6
    latency_s = p2p_latency(data_size, bw, hop)
    return max(1, int(math.ceil(latency_s * 1e6)))
