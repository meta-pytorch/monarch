# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""
Shared helpers for the RDMA test modules.

``rdma_backends`` runs a test under both RDMA backends: ``ibverbs``
(``rdma_disable_ibverbs=False``) and ``tcp`` (``rdma_disable_ibverbs=True``,
which forces TCP even on ibverbs-capable hosts).
"""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable

import pytest
from monarch.config import get_global_config, parametrize_config_pointwise
from monarch.rdma import is_ibverbs_available


RDMA_BACKENDS: list[str] = ["ibverbs", "tcp"]


def skip_if_ibverbs_unavailable() -> None:
    """Skip the running test when no ibverbs device is available on this host."""
    if not is_ibverbs_available():
        pytest.skip("ibverbs backend requested but no ibverbs device on this host")


def _skip_ibverbs_case_when_unavailable(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap ``fn`` so the ibverbs parametrization is skipped at runtime when the
    executing host lacks an ibverbs device.

    Runs inside the ``configured(...)`` context that ``parametrize_config_pointwise``
    applies, so it reads the active backend from the merged config.
    """

    def _is_ibverbs_case() -> bool:
        # rdma_disable_ibverbs=False is the ibverbs case.
        return not get_global_config().get("rdma_disable_ibverbs", False)

    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            if _is_ibverbs_case():
                skip_if_ibverbs_unavailable()
            return await fn(*args, **kwargs)

        return async_wrapper

    @functools.wraps(fn)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        if _is_ibverbs_case():
            skip_if_ibverbs_unavailable()
        return fn(*args, **kwargs)

    return sync_wrapper


def rdma_backends(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Parametrize a test across the ibverbs and tcp RDMA backends.
    The ibverbs case is skipped at runtime on hosts without an ibverbs
    device.
    """
    return parametrize_config_pointwise(
        rdma_disable_ibverbs=[True, False],
        rdma_allow_tcp_fallback=[True, False],
    )(_skip_ibverbs_case_when_unavailable(fn))
