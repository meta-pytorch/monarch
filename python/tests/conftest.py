# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import sys
import time

import pytest

# Propagate sys.path to PYTHONPATH so that worker subprocesses spawned by
# monarch (e.g. distributed_proc_mesh) see the same import paths as the
# pytest parent process. pytest's default "prepend" import mode modifies
# sys.path at the Python level, but child processes don't inherit that —
# they only see PYTHONPATH.
os.environ["PYTHONPATH"] = os.pathsep.join(sys.path)


def _total_threads() -> int:
    """Read total thread/process count from /proc/loadavg (4th field: running/total)."""
    try:
        with open("/proc/loadavg") as f:
            return int(f.read().split()[3].split("/")[1])
    except Exception:
        return -1


_last_start_time: dict[str, float] = {}
_last_start_threads: dict[str, int] = {}


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    _last_start_time[item.nodeid] = time.monotonic()
    _last_start_threads[item.nodeid] = _total_threads()


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if report.when != "teardown":
        return
    elapsed = time.monotonic() - _last_start_time.pop(report.nodeid, 0.0)
    before = _last_start_threads.pop(report.nodeid, 0)
    after = _total_threads()
    leaked = after - before if before >= 0 and after >= 0 else -1
    sys.stderr.write(
        f"\nMONARCH_TEST_DIAG: test={report.nodeid} "
        f"duration={elapsed:.2f}s "
        f"threads_before={before} "
        f"threads_after={after} "
        f"threads_leaked={leaked}\n"
    )
    sys.stderr.flush()
