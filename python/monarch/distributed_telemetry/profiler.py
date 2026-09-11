# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from __future__ import annotations

import os
import time
from pathlib import Path
from types import TracebackType
from typing import Callable

from monarch._rust_bindings.monarch_extension.trace import export_profile


class profile:
    """Collect distributed user traces for one block of Python code."""

    def __init__(
        self,
        telemetry_url: str,
        *,
        on_trace_ready: Callable[[profile], object] | None = None,
    ) -> None:
        self._telemetry_url = telemetry_url
        self._on_trace_ready = on_trace_ready
        self._start_us: int | None = None
        self._end_us: int | None = None
        self.path: Path | None = None

    def __enter__(self) -> "profile":
        if self._start_us is not None:
            raise RuntimeError("profile context cannot be reused")
        self._start_us = time.time_ns() // 1_000
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del exc_value, traceback
        if self._start_us is None or self._end_us is not None:
            raise RuntimeError("profile context is not active")

        self._end_us = time.time_ns() // 1_000
        if exc_type is None and self._on_trace_ready is not None:
            self._on_trace_ready(self)
        return False

    def export_perfetto_trace(
        self, output: os.PathLike[str] | str | None = None
    ) -> Path:
        """Export the completed profile window to a Perfetto trace file."""
        if self._start_us is None or self._end_us is None:
            raise RuntimeError("profile context has not completed")

        self.path = Path(
            export_profile(
                self._telemetry_url,
                self._start_us,
                self._end_us,
                os.fspath(output) if output is not None else None,
            )
        )
        return self.path
