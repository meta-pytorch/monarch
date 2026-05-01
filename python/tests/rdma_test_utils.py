# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""
Shared helpers for the RDMA test modules.

``rdma_backends`` is a decorator that runs a test once per available RDMA
backend. ``ibverbs`` is only collected when the host has compatible hardware;
``tcp`` always runs, and is forced even on RDMA-capable hosts via
``rdma_disable_ibverbs=True``.
"""

from monarch.config import parametrize_config_pointwise
from monarch.rdma import is_ibverbs_available


RDMA_BACKENDS: list[str] = []
if is_ibverbs_available():
    RDMA_BACKENDS.append("ibverbs")
RDMA_BACKENDS.append("tcp")


if is_ibverbs_available():
    rdma_backends = parametrize_config_pointwise(
        rdma_disable_ibverbs=[True, False],
        rdma_allow_tcp_fallback=[True, False],
    )
else:
    rdma_backends = parametrize_config_pointwise(
        rdma_disable_ibverbs=[True],
        rdma_allow_tcp_fallback=[True],
    )
