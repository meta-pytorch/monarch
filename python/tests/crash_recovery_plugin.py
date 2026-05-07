# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Pytest plugin for crash-safe sequential test execution.

Spawns one persistent worker subprocess. The worker overrides pytest_runtestloop
to replace the normal test loop with an IPC loop: it builds a nodeid->item dict
after collection, signals ready, then runs whatever test the controller asks for
by node ID.  Order and collection sync between controller and worker are never
assumed.

Worker stdout/stderr are inherited from the controller (same fds), so -s output
and direct Rust/C writes go straight to the terminal with no buffering or IPC
in the way.

Usage:
    pytest --crash-recovery [--max-crashes=N] ...
"""

from __future__ import annotations

import os
import pickle
import subprocess
import sys
from typing import NamedTuple

import pytest
from _pytest.reports import TestReport

_in_worker = False


# ---------------------------------------------------------------------------
# Wire protocol: pickle's own framing on buffered file objects
# ---------------------------------------------------------------------------


def _send(f, obj: object) -> None:
    pickle.dump(obj, f, protocol=4)
    f.flush()


def _recv(f) -> object:
    """Read one message; return WorkerCrashedMsg on EOF or pipe error."""
    try:
        return pickle.load(f)
    except (EOFError, OSError, pickle.UnpicklingError):
        return WorkerCrashedMsg()


# ---------------------------------------------------------------------------
# Message types
# ---------------------------------------------------------------------------


# Controller -> Worker
class RunMsg(NamedTuple):
    nodeid: str


class StopMsg(NamedTuple):
    pass


# Worker -> Controller
class ReadyMsg(NamedTuple):
    pass


class NotFoundMsg(NamedTuple):
    nodeid: str


class PickleReportsMsg(NamedTuple):
    reports: list  # list[TestReport]


class WorkerCrashedMsg(NamedTuple):
    pass


# ---------------------------------------------------------------------------
# Worker plugin — runs inside the worker subprocess
# ---------------------------------------------------------------------------


class _WorkerPlugin:
    def __init__(self, rf, wf) -> None:
        self._rf = rf
        self._wf = wf

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtestloop(self, session: pytest.Session) -> bool:
        """Replace the normal test loop with an IPC-driven loop.

        After collection the worker builds a nodeid->item dict, signals ready,
        then runs whatever test the controller requests by node ID.  There is
        no assumed ordering and no synchronisation needed between controller
        and worker collections.
        """
        import _pytest.runner as runner

        items: dict[str, pytest.Item] = {item.nodeid: item for item in session.items}
        _send(self._wf, ReadyMsg())

        while True:
            match _recv(self._rf):
                case StopMsg() | WorkerCrashedMsg():
                    break
                case RunMsg(nodeid=nodeid):
                    item = items.get(nodeid)
                    if item is None:
                        _send(self._wf, NotFoundMsg(nodeid=nodeid))
                        continue
                    # log=False: suppress worker-side logreport calls so
                    # PASSED/FAILED markers don't appear from the worker;
                    # the controller fires them after receiving the reports.
                    reports = runner.runtestprotocol(item, log=False, nextitem=None)
                    _send(self._wf, PickleReportsMsg(reports=reports))

        return True  # handled; skip the default test loop


def _worker_main() -> None:
    """Entry point when this file is run directly as the worker subprocess."""
    global _in_worker
    _in_worker = True

    from _pytest.config import _prepareconfig

    read_fd, write_fd = int(sys.argv[1]), int(sys.argv[2])
    rf = open(read_fd, "rb")
    wf = open(write_fd, "wb")

    # Receive the controller's invocation args and reconstruct a config.
    # Strip -v/--verbose because we suppress the terminal plugin (-p no:terminal)
    # and those flags are only registered by the terminal plugin.
    raw_args: list[str] = pickle.load(rf)
    args = [
        a
        for a in raw_args
        if a not in {"-v", "--verbose"} and not a.startswith("--verbose=")
    ] + ["-p", "no:terminal", "-p", "no:cacheprovider"]

    config = _prepareconfig(args)
    config.pluginmanager.register(_WorkerPlugin(rf, wf), "crash-worker")
    try:
        config.hook.pytest_cmdline_main(config=config)
    finally:
        rf.close()
        wf.close()


# ---------------------------------------------------------------------------
# Controller plugin — runs in the main pytest process
# ---------------------------------------------------------------------------


class CrashRecoveryPlugin:
    def __init__(self, max_crashes: int, config: pytest.Config) -> None:
        self._max_crashes = max_crashes
        self._config = config
        self._crashes = 0
        self._crashed: list[str] = []
        self._worker: subprocess.Popen | None = None
        self._rf = None
        self._wf = None

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        self._spawn_worker()

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_protocol(
        self, item: pytest.Item, nextitem: pytest.Item | None
    ) -> bool:
        if self._crashes >= self._max_crashes:
            self._emit_reports(item, _crash_reports(item, "crash limit reached, test not run"))
            return True

        _send(self._wf, RunMsg(nodeid=item.nodeid))

        match _recv(self._rf):
            case WorkerCrashedMsg():
                self._on_crash(item)
            case NotFoundMsg(nodeid=nodeid):
                self._emit_reports(
                    item,
                    _crash_reports(item, f"worker could not find {nodeid!r} in its collection"),
                )
            case PickleReportsMsg(reports=reports):
                self._emit_reports(item, reports)

        return True

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        if self._worker is not None:
            try:
                _send(self._wf, StopMsg())
            except OSError:
                pass
            self._reap_worker()

    def pytest_terminal_summary(self, terminalreporter, exitstatus: int) -> None:
        if not self._crashed:
            return
        terminalreporter.write_sep("=", "CRASHED TESTS", red=True, bold=True)
        for nodeid in self._crashed:
            terminalreporter.write_line(f"  CRASHED: {nodeid}", red=True)
        terminalreporter.write_line(
            f"  {len(self._crashed)} test(s) crashed the runner", red=True
        )

    # ------------------------------------------------------------------

    def _emit_reports(self, item: pytest.Item, reports: list[TestReport]) -> None:
        item.ihook.pytest_runtest_logstart(nodeid=item.nodeid, location=item.location)
        for r in reports:
            item.ihook.pytest_runtest_logreport(report=r)
        item.ihook.pytest_runtest_logfinish(nodeid=item.nodeid, location=item.location)

    def _on_crash(self, item: pytest.Item) -> None:
        self._crashes += 1
        self._crashed.append(item.nodeid)
        print(
            f"\n[crash-recovery] crash {self._crashes}/{self._max_crashes}: {item.nodeid}",
            file=sys.stderr,
            flush=True,
        )
        self._reap_worker()
        if self._crashes < self._max_crashes:
            self._spawn_worker()
        self._emit_reports(item, _crash_reports(item, "test runner crashed"))

    def _spawn_worker(self) -> None:
        worker_r, ctrl_w = os.pipe()
        ctrl_r, worker_w = os.pipe()
        self._rf = open(ctrl_r, "rb")
        self._wf = open(ctrl_w, "wb")
        self._worker = subprocess.Popen(
            [sys.executable, __file__, str(worker_r), str(worker_w)],
            pass_fds=(worker_r, worker_w),
        )
        os.close(worker_r)
        os.close(worker_w)
        _send(self._wf, list(self._config.invocation_params.args))
        match _recv(self._rf):
            case ReadyMsg():
                pass
            case WorkerCrashedMsg():
                raise RuntimeError("worker crashed before sending ready")
            case other:
                raise RuntimeError(f"worker sent unexpected message during startup: {other!r}")

    def _reap_worker(self) -> None:
        self._rf.close()
        self._rf = None
        self._wf.close()
        self._wf = None
        try:
            self._worker.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._worker.kill()
            self._worker.wait()
        self._worker = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _crash_reports(item: pytest.Item, reason: str) -> list[TestReport]:
    def _r(when: str, outcome: str, longrepr: str | None) -> TestReport:
        return TestReport(
            nodeid=item.nodeid,
            location=item.location,
            keywords=dict(item.keywords),
            outcome=outcome,
            longrepr=longrepr,
            when=when,
            sections=[],
            duration=0.0,
        )

    return [
        _r("setup", "passed", None),
        _r("call", "failed", f"CRASHED: {reason}"),
        _r("teardown", "passed", None),
    ]


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


def pytest_addoption(parser: pytest.Parser) -> None:
    g = parser.getgroup("crash-recovery", "crash recovery")
    g.addoption(
        "--crash-recovery",
        action="store_true",
        default=False,
        help="Run tests in a worker subprocess; restart on crash.",
    )
    g.addoption(
        "--max-crashes",
        type=int,
        default=10,
        metavar="N",
        help="Abort after N worker crashes (default: 10).",
    )


def pytest_configure(config: pytest.Config) -> None:
    if _in_worker:
        return  # running as worker; _WorkerPlugin is registered by _worker_main
    if config.getoption("--crash-recovery", default=False):
        max_c = config.getoption("--max-crashes", default=10)
        config.pluginmanager.register(
            CrashRecoveryPlugin(max_c, config), "crash-recovery"
        )


if __name__ == "__main__":
    # When run directly as the worker subprocess, register this module under
    # its importable name so that pickle uses "crash_recovery_plugin.Foo"
    # consistently on both sides rather than "__main__.Foo".
    sys.modules["crash_recovery_plugin"] = sys.modules["__main__"]
    for _cls in (RunMsg, StopMsg, ReadyMsg, NotFoundMsg, PickleReportsMsg, WorkerCrashedMsg):
        _cls.__module__ = "crash_recovery_plugin"
    _worker_main()


