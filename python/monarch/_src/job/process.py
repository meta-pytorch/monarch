# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from typing import Dict, List, Optional, Union

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
        self, meshes: Dict[str, int], env: Optional[Dict[str, str]] = None
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
            for mesh_name, count in self._meshes.items():
                for i in range(count):
                    host_key = f"{mesh_name}_{i}"
                    addr = f"ipc://{self._tmpdir}/{host_key}"
                    env = {**os.environ}
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
                    )
                    self._host_to_pid[host_key] = ProcessState(proc.pid, addr)
        except Exception:
            self._kill()
            raise

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
        # Use SIGTERM to allow worker processes to shut down gracefully.
        # The HostMesh objects are not stored here (they're returned to callers),
        # so we rely on the worker processes handling SIGTERM for clean shutdown.
        for p in self._host_to_pid.values():
            try:
                os.kill(p.pid, signal.SIGTERM)
            except OSError:
                pass
        self._host_to_pid.clear()
        if self._tmpdir is not None:
            shutil.rmtree(self._tmpdir, ignore_errors=True)
            self._tmpdir = None
