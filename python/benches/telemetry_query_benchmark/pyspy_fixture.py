# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Deterministic PySpy data for telemetry query benchmarks."""

from __future__ import annotations

import json
from dataclasses import dataclass

_FILENAME_GROUPS = 16


@dataclass(frozen=True)
class DatasetShape:
    """Cardinalities for a normalized PySpy fixture."""

    dumps: int
    threads_per_dump: int
    frames_per_thread: int

    @property
    def traces(self) -> int:
        return self.dumps * self.threads_per_dump

    @property
    def frames(self) -> int:
        return self.traces * self.frames_per_thread

    @property
    def filename_zero_frames(self) -> int:
        frames_per_trace = (
            self.frames_per_thread + _FILENAME_GROUPS - 1
        ) // _FILENAME_GROUPS
        return self.traces * frames_per_trace


def dump_id(index: int) -> str:
    """Return the stable identifier for one fixture dump."""
    return f"dump-{index:06d}"


def make_dump(shape: DatasetShape) -> str:
    """Build one compact PySpy JSON payload."""
    payload = {
        "Ok": {
            "capture_mode": "python_only",
            "stack_traces": [
                _make_trace(shape, thread_index)
                for thread_index in range(shape.threads_per_dump)
            ],
        }
    }
    return json.dumps(payload, separators=(",", ":"))


def expected_projection_rows(
    shape: DatasetShape,
    row_count: int,
) -> list[dict[str, object]]:
    """Return the rows expected from the ordered frame projection."""
    return [_frame_identity(shape, index) for index in range(row_count)]


def expected_filename_group_rows(
    shape: DatasetShape,
) -> list[dict[str, object]]:
    """Return the ordered frame count for each generated filename."""
    counts = _expected_filename_counts(shape)
    return [
        {"filename": filename, "frame_count": counts[filename]}
        for filename in sorted(counts)
    ]


def _make_frame(
    frame_index: int,
) -> dict[str, object]:
    filename = f"module_{frame_index % _FILENAME_GROUPS}.py"
    return {
        "name": f"function_{frame_index % 64}",
        "filename": filename,
        "line": frame_index + 1,
        "locals": [{"arg": True}],
    }


def _make_trace(
    shape: DatasetShape,
    thread_index: int,
) -> dict[str, object]:
    return {
        "thread_id": thread_index,
        "frames": [
            _make_frame(frame_index) for frame_index in range(shape.frames_per_thread)
        ],
    }


def _frame_identity(shape: DatasetShape, index: int) -> dict[str, object]:
    frames_per_dump = shape.threads_per_dump * shape.frames_per_thread
    dump_index, dump_offset = divmod(index, frames_per_dump)
    thread_index, frame_index = divmod(dump_offset, shape.frames_per_thread)
    return {
        "dump_id": dump_id(dump_index),
        "thread_id": thread_index,
        "frame_depth": frame_index,
        "name": f"function_{frame_index % 64}",
        "filename": f"module_{frame_index % _FILENAME_GROUPS}.py",
        "line": frame_index + 1,
    }


def _expected_filename_counts(shape: DatasetShape) -> dict[str, int]:
    counts = {}
    for filename_index in range(min(_FILENAME_GROUPS, shape.frames_per_thread)):
        per_trace = (
            shape.frames_per_thread - 1 - filename_index
        ) // _FILENAME_GROUPS + 1
        counts[f"module_{filename_index}.py"] = shape.traces * per_trace
    return counts
