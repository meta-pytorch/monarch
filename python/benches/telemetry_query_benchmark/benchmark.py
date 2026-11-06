#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Benchmark fixed-data queries through telemetry query backends."""

from __future__ import annotations

import argparse
import functools
import socket
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from typing import Callable, cast

from monarch.python.benches.telemetry_query_benchmark import (
    pyspy_fixture,
    query_backend,
)

_QueryRows = query_backend.QueryRows
_Validator = Callable[[_QueryRows], None]
_QUERY_ITERATIONS = 100
_CONCURRENT_ROUNDS = 20


@dataclass(frozen=True)
class _QueryCase:
    name: str
    sql: str
    validate: _Validator


def _expect_scalar(
    rows: _QueryRows,
    *,
    column: str,
    expected: int,
) -> None:
    if len(rows) != 1:
        raise RuntimeError(f"expected one row for {column}; got {len(rows)}")
    value = rows[0].get(column)
    if type(value) is not int:
        raise RuntimeError(f"expected integer {column}; got {value!r}")
    if value != expected:
        raise RuntimeError(f"expected {column}={expected}; got {value!r}")


def _expect_rows(
    rows: _QueryRows,
    *,
    expected: _QueryRows,
) -> None:
    if rows != expected:
        raise RuntimeError("query rows differ from the deterministic fixture")
    for row, expected_row in zip(rows, expected):
        if any(
            type(value) is not type(expected_row[column])
            for column, value in row.items()
        ):
            raise RuntimeError("query row types differ from the deterministic fixture")


def _scalar_case(
    name: str,
    sql: str,
    column: str,
    expected: int,
) -> _QueryCase:
    return _QueryCase(
        name,
        sql,
        functools.partial(_expect_scalar, column=column, expected=expected),
    )


def _query_cases(
    shape: pyspy_fixture.DatasetShape,
) -> tuple[_QueryCase, ...]:
    return (
        _scalar_case(
            "fanout_floor",
            "SELECT COUNT(*) AS dump_count FROM pyspy_dumps",
            "dump_count",
            shape.dumps,
        ),
        _scalar_case(
            "frame_count",
            "SELECT COUNT(*) AS frame_count FROM pyspy_frames",
            "frame_count",
            shape.frames,
        ),
        _scalar_case(
            "filtered_count",
            "SELECT COUNT(*) AS frame_count FROM pyspy_frames "
            "WHERE filename = 'module_0.py'",
            "frame_count",
            shape.filename_zero_frames,
        ),
        _QueryCase(
            "group_by_filename",
            "SELECT filename, COUNT(*) AS frame_count FROM pyspy_frames "
            "GROUP BY filename ORDER BY filename",
            functools.partial(
                _expect_rows,
                expected=pyspy_fixture.expected_filename_group_rows(shape),
            ),
        ),
        _scalar_case(
            "four_table_join",
            "SELECT COUNT(*) AS local_count FROM pyspy_local_variables l "
            "JOIN pyspy_frames f ON l.dump_id = f.dump_id "
            "AND l.thread_id = f.thread_id AND l.frame_depth = f.frame_depth "
            "JOIN pyspy_stack_traces s ON f.dump_id = s.dump_id "
            "AND f.thread_id = s.thread_id "
            "JOIN pyspy_dumps d ON f.dump_id = d.dump_id WHERE l.arg = TRUE",
            "local_count",
            shape.frames,
        ),
        _QueryCase(
            "ordered_projection",
            "SELECT dump_id, thread_id, frame_depth, name, filename, line "
            "FROM pyspy_frames ORDER BY dump_id, thread_id, frame_depth "
            "LIMIT 1000",
            functools.partial(
                _expect_rows,
                expected=pyspy_fixture.expected_projection_rows(
                    shape,
                    1_000,
                ),
            ),
        ),
    )


def _execute_query(
    backend: query_backend.QueryBackend,
    case: _QueryCase,
) -> float:
    start = time.monotonic()
    data = backend.query(case.sql)
    elapsed = time.monotonic() - start
    case.validate(backend.rows(data))
    return elapsed


@dataclass(frozen=True)
class _CaseResult:
    # Median latency of sequential queries.
    warm: float
    # Median wall time for a round of `concurrency` simultaneous queries.
    concurrent: float


def _concurrent_round(
    backend: query_backend.QueryBackend,
    case: _QueryCase,
    pool: ThreadPoolExecutor,
    concurrency: int,
) -> float:
    start = time.monotonic()
    results = list(pool.map(lambda _: backend.query(case.sql), range(concurrency)))
    elapsed = time.monotonic() - start
    for data in results:
        case.validate(backend.rows(data))
    return elapsed


def _benchmark_case(
    backend: query_backend.QueryBackend,
    case: _QueryCase,
    concurrency: int,
) -> _CaseResult:
    warm = statistics.median(
        _execute_query(backend, case) for _ in range(_QUERY_ITERATIONS)
    )
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        concurrent = statistics.median(
            _concurrent_round(backend, case, pool, concurrency)
            for _ in range(_CONCURRENT_ROUNDS)
        )
    return _CaseResult(
        warm=warm,
        concurrent=concurrent,
    )


def _run_benchmark(
    shape: pyspy_fixture.DatasetShape,
    backend_name: query_backend.BackendName,
    workers: int,
    concurrency: int,
) -> dict[str, _CaseResult]:
    cases = _query_cases(shape)
    backend = query_backend.create_backend(backend_name, workers)
    try:
        backend.preload(shape)

        results = {}
        for case in cases:
            results[case.name] = _benchmark_case(backend, case, concurrency)
    except BaseException:
        with suppress(Exception):
            backend.close()
        raise
    else:
        backend.close()
        return results


def _format_ms(seconds: float) -> str:
    return f"{seconds * 1000.0:.3f}"


def _print_summary(results: dict[str, _CaseResult], concurrency: int) -> None:
    print(f"| Query | Warm (ms) | {concurrency} concurrent (ms) |")
    print("|---|---:|---:|")
    for name, result in results.items():
        print(
            f"| `{name}` | {_format_ms(result.warm)} "
            f"| {_format_ms(result.concurrent)} |"
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="benchmark fixed-data telemetry SQL queries",
    )
    parser.add_argument(
        "--backend",
        choices=("query-engine", "sidecar-http"),
        default="query-engine",
        help="Execution boundary to measure; use sidecar-http for end-to-end integration coverage.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Local worker collectors that share the fixture with the query root (query-engine only).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="Simultaneous queries per concurrent round.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    backend = cast(query_backend.BackendName, args.backend)
    workers = cast(int, args.workers)
    concurrency = cast(int, args.concurrency)
    shape = pyspy_fixture.DatasetShape(
        dumps=20,
        threads_per_dump=4,
        frames_per_thread=1_250,
    )
    print(f"hostname={socket.gethostname()}")
    print(f"backend={backend}")
    print(f"workers={workers}")
    print(f"concurrency={concurrency}")
    _print_summary(
        _run_benchmark(shape, backend, workers, concurrency),
        concurrency,
    )


if __name__ == "__main__":
    main()
