# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Pytest plugin for crash-safe sequential test execution.

Spawns one persistent worker subprocess inside a fresh PID namespace
(via unshare --user --pid --fork --mount-proc).  The worker overrides
pytest_runtestloop to replace the normal test loop with an IPC loop: it
builds a nodeid->item dict after collection, signals ready, then runs
whatever test the controller requests by node ID.

When the worker exits (crash or graceful stop), the kernel kills every
other process in its PID namespace, cleaning up any leaked subprocesses.

Worker stdout/stderr are inherited from the controller (same fds), so -s
output and direct Rust/C writes go straight to the terminal.

Usage:
    pytest --crash-recovery [--max-crashes=N] [--max-leaked-procs=N] ...
"""

from __future__ import annotations

import ctypes
import os
import pickle
import signal
import subprocess
import sys
from typing import NamedTuple

import _pytest.runner as runner
import pytest
from _pytest.outcomes import OutcomeException
from _pytest.reports import TestReport

_in_worker = False


def _check(*cmd: str) -> bool:
    return (
        subprocess.run(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        ).returncode
        == 0
    )


# --mount-proc remaps /proc to only show processes inside the PID namespace.
# Some container environments block the mount syscall even inside a user
# namespace, so we also check --pid --fork alone as a fallback.
_unshare_mount_proc_available: bool = _check(
    "unshare", "--user", "--pid", "--fork", "--mount-proc", "true"
)
_unshare_pid_available: bool = _unshare_mount_proc_available or _check(
    "unshare", "--user", "--pid", "--fork", "true"
)


def _pids_in_my_namespace() -> set[int]:
    """Return PIDs of all processes sharing our PID namespace.

    Works whether or not /proc was remounted with --mount-proc:
    - With --mount-proc: /proc only has namespace-local entries, so a plain
      scandir suffices; the ns/pid check is redundant but harmless.
    - Without --mount-proc: /proc has all host processes; we filter by
      comparing each process's /proc/<pid>/ns/pid symlink to our own.
    """
    try:
        my_ns = os.readlink("/proc/self/ns/pid")
    except OSError:
        return set()
    result = set()
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit():
            continue
        try:
            if os.readlink(f"/proc/{entry.name}/ns/pid") == my_ns:
                result.add(int(entry.name))
        except OSError:
            pass
    return result


# prctl(PR_SET_PDEATHSIG, SIGKILL): tell the kernel to deliver SIGKILL to this
# process when its parent exits.  Used to chain kills through the unshare →
# Python worker hierarchy without needing process groups.
_libc = ctypes.CDLL("libc.so.6", use_errno=True)
_PR_SET_PDEATHSIG = 1


def _set_pdeathsig() -> None:
    _libc.prctl(_PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0)


# ---------------------------------------------------------------------------
# Wire protocol: pickle framing on buffered file objects
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
    proc_count: int
    newly_leaked: int  # PIDs that appeared during this test


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
        items: dict[str, pytest.Item] = {item.nodeid: item for item in session.items}
        # Track leaked processes whenever we have a PID namespace (with or
        # without --mount-proc).  _pids_in_my_namespace() filters /proc by
        # namespace symlink so it's accurate in both cases.
        track_procs = _unshare_pid_available
        known_pids: set[int] = _pids_in_my_namespace() if track_procs else set()
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
                    if track_procs:
                        after = _pids_in_my_namespace()
                        newly_leaked = len(after - known_pids)
                        known_pids = after
                        proc_count = len(after)
                    else:
                        newly_leaked = 0
                        proc_count = 0
                    _send(
                        self._wf,
                        PickleReportsMsg(
                            reports=reports,
                            proc_count=proc_count,
                            newly_leaked=newly_leaked,
                        ),
                    )

        return True  # handled; skip the default test loop


def _worker_main() -> None:
    """Entry point when this file is run directly as the worker subprocess."""
    global _in_worker
    _in_worker = True
    # Die when our parent (unshare) exits for any reason.
    _set_pdeathsig()

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
    ] + ["-p", "no:terminal", "-p", "no:logging", "-p", "no:cacheprovider"]

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
    def __init__(
        self,
        max_crashes: int,
        max_leaked_procs: int,
        restart_every: int,
        config: pytest.Config,
    ) -> None:
        self._max_crashes = max_crashes
        self._max_leaked_procs = max_leaked_procs
        self._restart_every = restart_every
        self._config = config
        self._crashes = 0
        self._crashed: list[str] = []
        self._worker: subprocess.Popen | None = None
        self._rf = None
        self._wf = None
        self._tests_since_restart = 0

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        self._spawn_worker()

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_protocol(
        self, item: pytest.Item, nextitem: pytest.Item | None
    ) -> bool:
        if self._crashes >= self._max_crashes:
            self._emit_reports(
                item, _crash_reports(item, "crash limit reached, test not run")
            )
            return True

        _send(self._wf, RunMsg(nodeid=item.nodeid))

        try:
            msg = _recv(self._rf)
        except OutcomeException:
            # A pytest plugin (e.g. pytest-timeout) interrupted the wait.
            # SIGKILL the worker first so its PID namespace is cleaned up and
            # both pipe ends are definitely closed before we touch them.
            # (Re-raising the OutcomeException is not an option: it would
            # propagate out of pytest_runtest_protocol with no CallInfo to
            # catch it, causing INTERNALERROR.)
            self._kill_worker()
            if self._crashes < self._max_crashes:
                self._spawn_worker()
            self._emit_reports(
                item, _crash_reports(item, "timed out waiting for worker")
            )
            return True

        match msg:
            case WorkerCrashedMsg():
                self._on_crash(item)
            case NotFoundMsg(nodeid=nodeid):
                self._emit_reports(
                    item,
                    _crash_reports(
                        item, f"worker could not find {nodeid!r} in its collection"
                    ),
                )
            case PickleReportsMsg(
                reports=reports, proc_count=proc_count, newly_leaked=newly_leaked
            ):
                self._emit_reports(item, reports)
                self._tests_since_restart += 1
                if newly_leaked:
                    print(
                        f"[crash-recovery] {item.nodeid} leaked {newly_leaked} process(es)"
                        f" ({proc_count} total in namespace)",
                        file=sys.stderr,
                        flush=True,
                    )
                if proc_count > self._max_leaked_procs:
                    print(
                        f"[crash-recovery] {proc_count} processes exceeds threshold"
                        f" {self._max_leaked_procs}, restarting worker",
                        file=sys.stderr,
                        flush=True,
                    )
                    self._kill_worker()  # SIGKILL cleans up the entire PID namespace
                    self._spawn_worker()
                elif (
                    self._restart_every > 0
                    and self._tests_since_restart >= self._restart_every
                ):
                    print(
                        f"[crash-recovery] restarting worker after"
                        f" {self._tests_since_restart} tests (periodic cleanup)",
                        file=sys.stderr,
                        flush=True,
                    )
                    self._kill_worker()  # SIGKILL cleans up the entire PID namespace
                    self._spawn_worker()

        return True

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        if self._worker is not None:
            self._kill_worker()  # SIGKILL cleans up the entire PID namespace

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
        self._kill_worker()
        if self._crashes < self._max_crashes:
            self._spawn_worker()
        self._emit_reports(item, _crash_reports(item, "test runner crashed"))

    def _spawn_worker(self) -> None:
        self._tests_since_restart = 0
        worker_r, ctrl_w = os.pipe()
        ctrl_r, worker_w = os.pipe()
        self._rf = open(ctrl_r, "rb")
        self._wf = open(ctrl_w, "wb")
        if _unshare_mount_proc_available:
            unshare_prefix = ["unshare", "--user", "--pid", "--fork", "--mount-proc"]
            mode = "pid-namespace+mount-proc (full isolation + proc counting)"
        elif _unshare_pid_available:
            unshare_prefix = ["unshare", "--user", "--pid", "--fork"]
            mode = "pid-namespace (isolation + proc counting via ns/pid filter)"
        else:
            unshare_prefix = []
            mode = "no namespace (crash/timeout recovery only)"
        print(f"[crash-recovery] worker mode: {mode}", file=sys.stderr, flush=True)
        cmd = unshare_prefix + [sys.executable, __file__, str(worker_r), str(worker_w)]
        self._worker = subprocess.Popen(cmd, pass_fds=(worker_r, worker_w))
        os.close(worker_r)
        os.close(worker_w)
        _send(self._wf, list(self._config.invocation_params.args))
        match _recv(self._rf):
            case ReadyMsg():
                pass
            case WorkerCrashedMsg():
                raise RuntimeError("worker crashed before sending ready")
            case other:
                raise RuntimeError(
                    f"worker sent unexpected message during startup: {other!r}"
                )

    def _kill_worker(self) -> None:
        """SIGKILL unshare; prctl chain delivers SIGKILL to the Python worker.

        self._worker is the unshare wrapper process.  Killing it triggers
        PR_SET_PDEATHSIG in the Python worker (_worker_main set it), so the
        Python worker is killed too without needing process-group tricks.
        With both dead before we close pipes, the write buffer is guaranteed
        empty (flushed after the last _send) so wf.close() never raises.
        """
        try:
            self._worker.kill()
        except ProcessLookupError:
            pass  # already dead
        self._worker.wait()
        self._worker = None
        self._rf.close()
        self._rf = None
        self._wf.close()
        self._wf = None


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
    g.addoption(
        "--max-leaked-procs",
        type=int,
        default=100,
        metavar="N",
        help="Restart worker when its PID namespace contains more than N processes (default: 100).",
    )
    g.addoption(
        "--restart-every",
        type=int,
        default=0,
        metavar="N",
        help="Restart worker every N tests regardless of crashes or leaks (0 = disabled).",
    )


def pytest_configure(config: pytest.Config) -> None:
    if _in_worker:
        return  # running as worker; _WorkerPlugin is registered by _worker_main
    if config.getoption("--crash-recovery", default=False):
        max_c = config.getoption("--max-crashes", default=10)
        max_p = config.getoption("--max-leaked-procs", default=100)
        restart_every = config.getoption("--restart-every", default=0)
        config.pluginmanager.register(
            CrashRecoveryPlugin(max_c, max_p, restart_every, config), "crash-recovery"
        )


if __name__ == "__main__":
    # When run directly as the worker subprocess, register this module under
    # its importable name so that pickle uses "crash_recovery_plugin.Foo"
    # consistently on both sides rather than "__main__.Foo".
    sys.modules["crash_recovery_plugin"] = sys.modules["__main__"]
    for _cls in (
        RunMsg,
        StopMsg,
        ReadyMsg,
        NotFoundMsg,
        PickleReportsMsg,
        WorkerCrashedMsg,
    ):
        _cls.__module__ = "crash_recovery_plugin"
    _worker_main()
