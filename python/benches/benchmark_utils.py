#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import logging
import statistics
from collections.abc import Iterator, Sequence
from contextlib import contextmanager

from monarch.actor import HostMesh, ProcMesh
from monarch.config import configured, get_global_config


logger: logging.Logger = logging.getLogger(__name__)

Metric = str | int | float
BenchmarkRecord = dict[str, Metric]


def first_host(host_mesh: HostMesh) -> HostMesh:
    """Select one host from a one-dimensional host mesh."""
    labels = host_mesh.region.labels
    if len(labels) != 1:
        raise ValueError(f"expected one host dimension, got {labels}")
    return host_mesh.slice(**{labels[0]: 0})


def summarize_samples(samples: Sequence[float]) -> dict[str, float]:
    if len(samples) < 2:
        raise ValueError("at least two samples are required")
    ordered = sorted(samples)
    percentiles = statistics.quantiles(ordered, n=100, method="inclusive")
    return {
        "min": ordered[0],
        "mean": statistics.fmean(ordered),
        "p50": percentiles[49],
        "p90": percentiles[89],
        "p99": percentiles[98],
        "max": ordered[-1],
        "stdev": statistics.stdev(ordered),
    }


@contextmanager
def data_actor_mesh_config(enabled: bool) -> Iterator[None]:
    if not enabled:
        yield
        return

    if "use_direct_spawn" not in get_global_config():
        raise RuntimeError(
            "data actor mesh mode is unavailable: use_direct_spawn is not configured"
        )
    with configured(use_direct_spawn=True):
        yield


def stop_proc_mesh(proc_mesh: ProcMesh) -> None:
    try:
        proc_mesh.stop().get()
    except Exception as error:
        logger.warning(f"failed to stop benchmark proc mesh: {error}", exc_info=True)
