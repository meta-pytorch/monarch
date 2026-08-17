# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import contextlib
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from typing import Callable, Dict, List, Optional, Union

from monarch._rust_bindings.monarch_hyperactor.config import get_propagatable_config_env
from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.actor.future import Future
from monarch._src.job.job import JobState, JobTrait, ProcessState

logger = logging.getLogger(__name__)

try:
    from __manifest__ import fbmake  # noqa

    _IN_PAR = bool(fbmake.get("par_style"))
except ImportError:
    _IN_PAR = False

_PROCESS_WORKER_MODULE = "monarch._src.job._process_worker"
_KILL_GRACE_SECONDS = 1.0


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except OSError:
        return False
    return True


def _live_worker_pids(session_ids: "set[int]") -> Optional[List[int]]:
    """Live (non-zombie) pids whose session id is one of ``session_ids``, or
    ``None`` when ``/proc`` cannot be enumerated.

    Full reaping is Linux-only. Workers launch with ``start_new_session=True``,
    so a worker's pid is its session id and the procs it spawns stay in that
    session while sitting in their own process groups -- ``killpg`` of the worker
    alone cannot reach them. Matching on session id (via ``/proc`` and
    ``os.getsid``) catches the worker and its whole subtree, including procs
    already reparented to init (session membership outlives the parent). Where
    ``/proc`` is missing or unreadable this returns ``None`` so callers fall back
    to best-effort signalling of the tracked worker pids, which does not reach
    the spawned procs.
    """
    own = os.getpid()
    try:
        entries = os.listdir("/proc")
    except OSError:
        return None
    pids: List[int] = []
    for entry in entries:
        if not entry.isdigit():
            continue
        pid = int(entry)
        if pid == own:
            continue
        try:
            if os.getsid(pid) not in session_ids:
                continue
            with open(f"/proc/{pid}/stat") as stat:
                state = stat.read().rsplit(")", 1)[1].split()[0]
        except OSError:
            continue
        if state not in ("Z", "X", "x"):  # skip procs that are already dead
            pids.append(pid)
    return pids


def _terminate_with_grace(
    live_pids: Callable[[], List[int]],
    signal_pids: Callable[[List[int], int], None],
    grace: float = _KILL_GRACE_SECONDS,
) -> None:
    """SIGTERM the live targets, wait up to ``grace`` for them to exit, then
    SIGKILL any stragglers.

    Factored out so the poll-with-sleep is not copied around, and so it can be
    swapped for an await-based wait later. ``live_pids`` is re-evaluated each
    round so procs spawned mid-teardown are still caught before the SIGKILL.
    """
    signal_pids(live_pids(), signal.SIGTERM)
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline and live_pids():
        time.sleep(0.05)
    signal_pids(live_pids(), signal.SIGKILL)


class ProcessJob(JobTrait):
    """Job where each host is a local subprocess communicating over IPC.

    Suitable for local testing of multi-host scenarios without SSH or a
    scheduler. Each host runs ``run_worker_loop_forever`` in a child
    process, listening on a Unix socket.

    Example::

        job = ProcessJob({"trainers": 2, "dataloaders": 1})
        state = job.state(cached_path=None)
        state.trainers    # HostMesh with 2 hosts
        state.dataloaders # HostMesh with 1 host
    """

    def __init__(
        self,
        meshes: Dict[str, int],
        env: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Args:
            meshes: Mapping from mesh name to number of hosts.
            env: Extra environment variables for worker subprocesses.
        """
        super().__init__()
        self._meshes = meshes
        self._env = env
        self._host_to_pid: Dict[str, ProcessState] = {}
        self._tmpdir: Optional[str] = None

    def _create(self, client_script: Optional[str]) -> None:
        if client_script is not None:
            raise RuntimeError("ProcessJob cannot run batch-mode scripts")

        self._tmpdir = tempfile.mkdtemp(prefix="monarch_process_job_")

        try:
            propagatable_config_env = get_propagatable_config_env()
            for mesh_name, count in self._meshes.items():
                for i in range(count):
                    host_key = f"{mesh_name}_{i}"
                    addr = f"ipc://{self._tmpdir}/{host_key}"
                    env = {
                        **propagatable_config_env,
                        **os.environ,
                        "HYPERACTOR_PROCESS_NAME": host_key,
                    }
                    if self._env is not None:
                        env.update(self._env)
                    if _IN_PAR:
                        # In PAR/XAR mode, sys.executable is the bare
                        # Python interpreter which cannot import modules
                        # from the archive.  Re-invoke the PAR binary
                        # (sys.argv[0]) with PAR_MAIN_OVERRIDE pointing
                        # to the worker module.
                        env["PAR_MAIN_OVERRIDE"] = _PROCESS_WORKER_MODULE
                        env["_MONARCH_WORKER_ADDR"] = addr
                        cmd = [sys.argv[0]]
                    else:
                        cmd = [
                            sys.executable,
                            "-c",
                            "from monarch.actor import run_worker_loop_forever; "
                            f'run_worker_loop_forever(address="{addr}", '
                            'ca="trust_all_connections")',
                        ]
                    proc = subprocess.Popen(
                        cmd,
                        env=env,
                        start_new_session=True,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    self._host_to_pid[host_key] = ProcessState(proc.pid, addr)
                    logger.info(
                        "ProcessJob: spawned worker pid=%d mesh=%s rank=%d addr=%s",
                        proc.pid,
                        mesh_name,
                        i,
                        addr,
                    )
                    self._watch_process(proc, mesh_name, i, addr)
        except BaseException:
            # BaseException, not Exception: a Ctrl-C / cancellation mid-startup
            # must still reap the workers already spawned above, or they leak.
            self._kill()
            raise

    @staticmethod
    def _watch_process(
        proc: subprocess.Popen,
        mesh_name: str,
        rank: int,
        addr: str,
    ) -> None:
        def _waiter() -> None:
            pid = proc.pid
            try:
                proc.wait()
                code = proc.returncode
            except Exception:
                logger.exception(
                    "ProcessJob: error waiting on pid=%d mesh=%s rank=%d addr=%s",
                    pid,
                    mesh_name,
                    rank,
                    addr,
                )
                return
            if code == 0 or code == -signal.SIGTERM:
                logger.info(
                    "ProcessJob: worker exited pid=%d exit_code=%d mesh=%s rank=%d addr=%s",
                    pid,
                    code,
                    mesh_name,
                    rank,
                    addr,
                )
            else:
                logger.warning(
                    "ProcessJob: worker died unexpectedly pid=%d exit_code=%d mesh=%s rank=%d addr=%s",
                    pid,
                    code,
                    mesh_name,
                    rank,
                    addr,
                )

        t = threading.Thread(
            target=_waiter, daemon=True, name=f"watch-{mesh_name}_{rank}"
        )
        t.start()

    def _state(self) -> JobState:
        if not self._pids_active():
            raise RuntimeError("lost connection to worker processes")

        host_meshes = {}
        for mesh_name, count in self._meshes.items():
            workers: List[Union[str, Future[str]]] = [
                self._host_to_pid[f"{mesh_name}_{i}"].channel for i in range(count)
            ]
            host_meshes[mesh_name] = attach_to_workers(
                name=mesh_name,
                ca="trust_all_connections",
                workers=workers,
            )

        return JobState(host_meshes)

    def _should_spawn_telemetry_worker_collector_actors(self) -> bool:
        return False

    def can_run(self, spec: "JobTrait") -> bool:
        return (
            isinstance(spec, ProcessJob)
            and spec._meshes == self._meshes
            and self._pids_active()
        )

    def _pids_active(self) -> bool:
        if not self.active:
            return False
        for p in self._host_to_pid.values():
            try:
                os.kill(p.pid, 0)
            except OSError:
                return False
        return True

    def _kill(self) -> None:
        # Reap each worker's whole session, not just its process group. Workers
        # run in detached sessions (start_new_session=True), and the procs they
        # spawn stay in that session but in their own process groups -- so
        # signalling only the worker's group would orphan them. Session reaping
        # needs /proc; without it we fall back to best-effort signalling of the
        # tracked worker pids, which does not reach the spawned procs.
        worker_pids = [p.pid for p in self._host_to_pid.values()]
        session_ids = set(worker_pids)

        def remaining() -> List[int]:
            live = _live_worker_pids(session_ids)
            if live is None:
                # /proc missing or unreadable: signal the worker pids we hold.
                return [pid for pid in worker_pids if _group_alive(pid)]
            return live

        def signal_all(pids: List[int], sig: int) -> None:
            # killpg reaps each enumerated group leader's whole group (catching
            # children forked after the scan); the os.kill fallback covers pids
            # that are not group leaders. A pid reaped and reused between scan and
            # signal is at worst a stray no-op via the OSError fallback.
            for pid in pids:
                try:
                    os.killpg(pid, sig)
                except OSError:
                    with contextlib.suppress(OSError):
                        os.kill(pid, sig)

        _terminate_with_grace(remaining, signal_all)

        self._host_to_pid.clear()
        if self._tmpdir is not None:
            shutil.rmtree(self._tmpdir, ignore_errors=True)
            self._tmpdir = None
