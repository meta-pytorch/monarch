# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Wall-clock event recorder for building a per-component weight-transfer Gantt (gated WBRIDGE_GANTT=1),
plus optional torch.profiler labels for the same regions.

Records ``(role, rank, wt, op, round, t0, t1)`` with ``time.time()`` — wall clock, so sender-node and
receiver-node events share one timeline (modulo NTP skew across nodes). Adds no cuda syncs, so it does not
perturb the pipeline being measured. RDMA waits/polls/drain bracket blocking calls, so those intervals are
exact — which is what reveals the pipeline fill/drain bubble.

Output: one JSONL file per process under ``$WBRIDGE_GANTT_DIR``. We write files rather than log lines
because Ray's log forwarding dedups/rate-limits the high-frequency events (collapsing many rounds into
one). Falls back to the logger when no directory is configured or writable.

Torch profiler labels: every :func:`span` also opens ``torch.profiler.record_function("wbridge::"+op)``
when profiling is enabled (``WBRIDGE_PROFILE=1``, or inside :func:`capture`), so a chrome trace attributes
CUDA kernels to wbridge phases. The two switches are independent — the wall-clock recorder needs no
profiler, and the label needs no ``WBRIDGE_GANTT`` — so one ``with span(...)`` at each call site covers
both. Each is a true no-op (shared ``nullcontext``, zero allocation) when its switch is off.
"""

import json
import logging
import os
import threading
import time
from contextlib import contextmanager, nullcontext

ON = os.environ.get("WBRIDGE_GANTT") == "1"
_prof = os.environ.get("WBRIDGE_PROFILE") == "1"
_events: list = []  # (role, rank, wt, op, round, t0, t1)
_events_lock = threading.Lock()
_dump_lock = threading.Lock()
_logger = logging.getLogger("wbridge.gantt")
_NULL = nullcontext()


def _record_function(op: str):
    """``torch.profiler.record_function("wbridge::"+op)`` when profiling is on, else a no-op context."""
    if _prof:
        import torch

        return torch.profiler.record_function(f"wbridge::{op}")
    return _NULL


@contextmanager
def capture():
    """Force torch.profiler labels on for the duration, restoring the prior state afterward.

    Wrap the region where a profiler is actively recording (a targeted capture) so the :func:`span`
    labels appear in the trace regardless of the ``WBRIDGE_PROFILE`` env. Independent of ``WBRIDGE_GANTT``.
    """
    global _prof
    prev = _prof
    _prof = True
    try:
        yield
    finally:
        _prof = prev


def _outdir() -> str | None:
    path = os.environ.get("WBRIDGE_GANTT_DIR")
    return os.path.abspath(os.path.expanduser(path)) if path else None


def rec(role: str, rank: int, wt: int, op: str, rnd: int, t0: float, t1: float) -> None:
    if ON:
        with _events_lock:
            _events.append((role, rank, wt, op, rnd, t0, t1))


class _Span:
    """Times its ``with`` block (records a Gantt span on exit) and, when profiling, also opens a
    ``record_function`` label around it. Created only when :data:`ON`."""

    __slots__ = ("_meta", "_t0", "_rf")

    def __init__(self, meta: tuple) -> None:
        self._meta = meta

    def __enter__(self) -> "_Span":
        self._t0 = time.time()
        self._rf = (
            _record_function(self._meta[3]) if _prof else None
        )  # op label; only when profiling
        if self._rf is not None:
            self._rf.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._rf is not None:
            self._rf.__exit__(exc_type, exc, tb)
        if (
            exc_type is None
        ):  # skip on exception (matches recording only after the work completes)
            rec(*self._meta, self._t0, time.time())
        return False


def span(role: str, rank: int, wt: int, op: str, rnd: int):
    """Context manager: time the wrapped block, record it as a Gantt span, and (when profiling) label
    it ``wbridge::<op>`` in the torch profiler trace.

    A no-op (shared ``nullcontext``, zero allocation) unless ``WBRIDGE_GANTT=1`` or profiling is on.
    Replaces the ``t0 = now(); <work>; rec(..., t0, now())`` idiom with ``with span(...):  <work>``.
    """
    if ON:
        return _Span((role, rank, wt, op, rnd))
    if _prof:
        return _record_function(
            op
        )  # profiler label only (no wall-clock record when gantt is off)
    return _NULL


def take() -> list:
    """Atomically detach the events recorded so far, without performing any output.

    WeightBridge uses this at the end of an epoch while all of that epoch's producer threads are
    quiescent.  The immutable snapshot can then be written only after the caller has recorded its
    externally visible ``block_end`` metric, without accidentally taking events from the next epoch.
    """
    global _events
    if not ON:
        return []
    with _events_lock:
        events = _events
        _events = []
    return events


def dump(events: list | None = None) -> None:
    """Write an event snapshot.

    With no argument this retains the legacy drain-and-write behavior.  Passing a snapshot returned by
    :func:`take` separates event collection from file/log output, which is required to keep profiling I/O
    outside an application's reported blocking interval.
    """
    if events is None:
        events = take()
    if not events:
        return
    # A process normally has one endpoint, but the explicit lock also keeps fallback/direct users from
    # interleaving two JSONL appends when post-block emission races an asynchronous sender completion.
    with _dump_lock:
        d = _outdir()
        if d:
            try:
                os.makedirs(d, exist_ok=True)
                # The Gantt directory is commonly shared by several physical nodes. Include the
                # hostname so equal container PIDs cannot interleave records into the same file.
                host = os.uname().nodename
                path = os.path.join(d, f"gantt_pid{os.getpid()}_{host}.jsonl")
                with open(path, "a") as f:
                    for role, rank, wt, op, rnd, t0, t1 in events:
                        f.write(
                            json.dumps(
                                {
                                    "role": role,
                                    "rank": rank,
                                    "wt": wt,
                                    "op": op,
                                    "round": rnd,
                                    "t0": t0,
                                    "t1": t1,
                                    "host": host,
                                    "pid": os.getpid(),
                                }
                            )
                            + "\n"
                        )
                return
            except OSError:
                pass  # fall through to logger
        for role, rank, wt, op, rnd, t0, t1 in events:
            _logger.info(
                "wbridge-gantt role=%s rank=%d wt=%d op=%s round=%d t0=%.6f t1=%.6f",
                role,
                rank,
                wt,
                op,
                rnd,
                t0,
                t1,
            )
