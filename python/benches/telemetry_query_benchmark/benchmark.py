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


def _benchmark_case(
    backend: query_backend.QueryBackend,
    case: _QueryCase,
) -> float:
    return statistics.median(
        _execute_query(backend, case) for _ in range(_QUERY_ITERATIONS)
    )


def _run_benchmark(
    shape: pyspy_fixture.DatasetShape,
    backend_name: query_backend.BackendName,
) -> dict[str, float]:
    cases = _query_cases(shape)
    backend = query_backend.create_backend(backend_name)
    try:
        backend.preload(shape)

        query_medians = {}
        for case in cases:
            query_medians[case.name] = _benchmark_case(
                backend,
                case,
            )
    except BaseException:
        with suppress(Exception):
            backend.close()
        raise
    else:
        backend.close()
        return query_medians


def _print_summary(query_medians: dict[str, float]) -> None:
    print("| Query | Median (ms) |")
    print("|---|---:|")
    for name, median in query_medians.items():
        print(f"| `{name}` | {median * 1000.0:.3f} |")


def _parse_backend() -> query_backend.BackendName:
    parser = argparse.ArgumentParser(
        description="benchmark fixed-data telemetry SQL queries",
    )
    parser.add_argument(
        "--backend",
        choices=("query-engine", "sidecar-http"),
        default="query-engine",
        help="Execution boundary to measure; use sidecar-http for end-to-end integration coverage.",
    )
    return cast(query_backend.BackendName, parser.parse_args().backend)


def main() -> None:
    backend = _parse_backend()
    shape = pyspy_fixture.DatasetShape(
        dumps=20,
        threads_per_dump=4,
        frames_per_thread=1_250,
    )
    print(f"hostname={socket.gethostname()}")
    print(f"backend={backend}")
    _print_summary(_run_benchmark(shape, backend))


if __name__ == "__main__":
    main()
