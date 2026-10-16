# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import ipaddress
import json
import logging
import os
import shlex
import subprocess
import time
from typing import Any, Dict, FrozenSet, List, Optional, Sequence

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import configure
from monarch._rust_bindings.monarch_hyperactor.proc import ProcId
from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.job._batch_env import in_batch_job
from monarch._src.job._pbs_batch import read_nodefile
from monarch._src.job.job import BatchJob, DEFAULT_JOB_PATH, JobState, JobTrait
from monarch._src.job.service_identity import (
    allocate_service_proc_ids,
    deserialize_service_proc_ids,
    serialize_service_proc_ids,
    service_proc_addrs,
    SERVICE_PROC_IDS_ENV,
)

logger: logging.Logger = logging.getLogger(__name__)

# qstat job_state values after which the job will never run (F: finished, X: subjob done)
_PBS_TERMINAL_STATES: FrozenSet[str] = frozenset(["F", "X"])


def parse_exec_host(exec_host: str) -> List[str]:
    """Short hostnames from qstat's ``exec_host``, e.g. ``x1/0*64+x2/0*64``."""
    hosts = [chunk.split("/", 1)[0].split(".")[0] for chunk in exec_host.split("+")]
    return list(dict.fromkeys(host for host in hosts if host))


class PBSJob(JobTrait):
    """
    A job scheduler that uses PBS command line tools to schedule jobs.

    This implementation:
    1. Uses qsub to submit a PBS job that starts monarch workers with mpiexec
    2. Queries job status with qstat to get allocated hostnames
    3. Uses the hostnames to connect to the started workers

    Two launch modes are supported:
    - External controller (default): ``apply()`` submits the workers; a separate
      controller process gets their host meshes with ``state()`` (it connects to the
      workers with ``attach_to_workers``; this is not ``monarch.actor.attach``).
    - Batch (``apply(client_script=...)``): one allocation runs both the workers
      and the client, as ``SlurmJob`` does; ``apply`` caches a ``BatchJob`` wrapper
      for the in-allocation client, and the allocation is released when the client exits. A
      hand-written PBS script can run the same in-allocation runner
      (``python -m monarch._src.job._pbs_batch``); its client then gets the job
      with ``state()`` on a ``PBSJob`` with the same meshes.
    """

    def __init__(
        self,
        meshes: Dict[str, int],
        python_exe: str = "python",
        pbs_args: Sequence[str] = (),
        monarch_port: int = 22222,
        job_name: str = "monarch_job",
        time_limit: Optional[str] = None,
        queue: Optional[str] = None,
        account: Optional[str] = None,
        log_dir: Optional[str] = None,
        job_start_timeout: Optional[int] = None,
        mpiexec: str = "mpiexec",
        mpiexec_args: Sequence[str] = (),
        rank_env: str = "PMI_RANK",
        bind_to: Optional[str] = None,
    ) -> None:
        """
        Args:
            meshes: Dictionary mapping mesh names to number of nodes
            python_exe: Python executable to use for worker processes
            pbs_args: Additional qsub options, written as ``#PBS`` directives
                      (e.g. ``["-l filesystems=home", "-l place=scatter"]``)
            monarch_port: Port for TCP communication between workers
            job_name: Name for the PBS job
            time_limit: Maximum runtime in HH:MM:SS format. If None, uses the queue's default.
            queue: PBS queue to submit to (``#PBS -q``)
            account: PBS project to charge the job to (``#PBS -A``)
            log_dir: Directory for the PBS job output
            job_start_timeout: Maximum time in seconds to wait for the PBS job to start running.
                      This should account for potential queueing delays. If None (default), waits indefinitely.
            mpiexec: MPI launcher that starts one worker per node (``-n N --ppn 1 --hosts ...``)
            mpiexec_args: Extra launcher flags (e.g. ``["--cpu-bind=none"]`` for Cray PALS)
            rank_env: Variable holding each worker's rank in that launch
                      (``PMI_RANK`` for MPICH and Cray PALS)
            bind_to: Optional IP address on which workers listen, without a port.
                      When set, workers advertise their hostname while binding
                      this address through the ``dial_to@bind_to`` alias format.
        """
        configure(default_transport=ChannelTransport.TcpWithHostname)
        self._meshes = meshes
        self._python_exe = python_exe
        self._pbs_args = pbs_args
        self._port = monarch_port
        self._job_name = job_name
        self._time_limit = time_limit
        self._queue = queue
        self._account = account
        self._log_dir: str = log_dir if log_dir is not None else os.getcwd()
        self._job_start_timeout = job_start_timeout
        self._mpiexec = mpiexec
        self._mpiexec_args = mpiexec_args
        self._rank_env = rank_env
        try:
            self._bind_to = (
                str(ipaddress.ip_address(bind_to)) if bind_to is not None else None
            )
        except ValueError as error:
            raise ValueError(
                "bind_to must be an IPv4 or IPv6 address without a port"
            ) from error
        # Track the single PBS job ID and all allocated hostnames
        self._pbs_job_id: Optional[str] = None
        self._all_hostnames: List[str] = []
        self._service_proc_ids: list[ProcId] = []
        super().__init__()

    def __setstate__(self, state: Dict[str, Any]) -> None:
        # Rehydrated via pickle.load (job_load) bypasses __init__. Re-apply the
        # transport config so the client's host_agent serves on a TCP address
        # remote workers can dial back.
        self.__dict__.update(state)
        configure(default_transport=ChannelTransport.TcpWithHostname)

    def add_mesh(self, name: str, num_nodes: int) -> None:
        self._meshes[name] = num_nodes

    def _resolved_job_id(self) -> Optional[str]:
        """The submitted job id, falling back to ``$PBS_JOBID`` in batch mode."""
        if self._pbs_job_id is not None:
            return self._pbs_job_id
        return os.environ.get("PBS_JOBID") if in_batch_job() else None

    def _cleanup_log_context(self) -> dict[str, Any]:
        return {
            "job_type": type(self).__name__,
            "job_id": self._resolved_job_id(),
        }

    def _create(self, client_script: Optional[str]) -> None:
        """Submit a single PBS job for all meshes.

        In batch mode (``client_script`` set) the job runs the client inside the
        allocation alongside the workers, and a ``BatchJob`` wrapper is cached to
        ``.monarch/job_state.pkl`` so the client reconnects to this allocation
        instead of submitting a new one. Inside an allocation (``MONARCH_BATCH_JOB``
        set) there is nothing to submit: the surrounding job is adopted.
        """
        if in_batch_job():
            return
        total_nodes = sum(self._meshes.values())
        self._ensure_service_proc_ids(total_nodes)
        if client_script is not None:
            # Dumped before submit: the job id isn't known yet, and the client
            # resolves it from $PBS_JOBID anyway.
            BatchJob(self).dump(DEFAULT_JOB_PATH)
        self._pbs_job_id = self._submit_pbs_job(
            total_nodes, client_script=client_script
        )

    def _submit_pbs_job(
        self, num_nodes: int, client_script: Optional[str] = None
    ) -> str:
        """Submit a PBS job for all nodes."""
        self._ensure_service_proc_ids(num_nodes)
        os.makedirs(self._log_dir, exist_ok=True)

        pbs_directives = [
            "#!/bin/bash",
            f"#PBS -N {self._job_name}",
            f"#PBS -l select={num_nodes}",
            "#PBS -j oe",
            f"#PBS -o {self._log_dir}/",
        ]
        if self._time_limit is not None:
            pbs_directives.append(f"#PBS -l walltime={self._time_limit}")
        if self._queue is not None:
            pbs_directives.append(f"#PBS -q {self._queue}")
        if self._account is not None:
            pbs_directives.append(f"#PBS -A {self._account}")
        for arg in self._pbs_args:
            if arg.startswith("-"):
                pbs_directives.append(f"#PBS {arg}")

        runner = [
            self._python_exe,
            "-m",
            "monarch._src.job._pbs_batch",
            "--port",
            str(self._port),
            "--mpiexec",
            self._mpiexec,
            "--rank-env",
            self._rank_env,
        ]
        runner += [f"--mpiexec-arg={arg}" for arg in self._mpiexec_args]
        if self._bind_to is not None:
            runner += ["--bind-to", self._bind_to]
        if client_script is not None:
            # Batch mode: the runner seeds the workers, runs the client
            # (MONARCH_BATCH_JOB=1 so its cached BatchJob reconnects to this
            # allocation), and tears the workers down in a finally. Without a
            # client it runs the workers only, for an external controller.
            runner.append(client_script)

        batch_script = "\n".join(pbs_directives)
        batch_script += '\ncd "$PBS_O_WORKDIR"\n'
        batch_script += (
            f"export {SERVICE_PROC_IDS_ENV}="
            f"{shlex.quote(serialize_service_proc_ids(self._service_proc_ids))}\n"
        )
        batch_script += shlex.join(runner) + "\n"

        logger.info(f"Submitting PBS job with {num_nodes} nodes")
        try:
            result = subprocess.run(
                ["qsub"],
                input=batch_script,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to submit PBS job: {e.stderr}") from e

        # qsub prints the job id, e.g. "12345.pbs-server"
        job_id = result.stdout.strip().split()[0] if result.stdout.strip() else None
        if not job_id:
            raise RuntimeError(
                f"Failed to parse job ID from qsub output: {result.stdout}"
            )
        logger.info(
            f"PBS job {job_id} submitted. Logs will be written to: {self._log_dir}"
        )
        return job_id

    def _ensure_service_proc_ids(self, num_nodes: int) -> None:
        # One allocation pass, not a retry loop; callers pass the meshes' total.
        if len(self._service_proc_ids) != num_nodes:
            self._service_proc_ids = [
                proc_id
                for mesh_nodes in self._meshes.values()
                for proc_id in allocate_service_proc_ids(mesh_nodes)
            ]
        if len(self._service_proc_ids) != num_nodes:
            raise ValueError(
                f"meshes {self._meshes} need {len(self._service_proc_ids)} nodes, not {num_nodes}"
            )

    def _get_job_info_json(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job information using qstat -f -F json (-x includes finished jobs)."""
        try:
            result = subprocess.run(
                ["qstat", "-x", "-f", "-F", "json", job_id],
                capture_output=True,
                text=True,
                check=True,
            )
            if result.stdout.strip():
                jobs = json.loads(result.stdout).get("Jobs", {})
                return next(iter(jobs.values()), None)
            return None
        except subprocess.CalledProcessError as e:
            logger.warning(f"Error checking job {job_id} status: {e.stderr}")
            return None
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Error parsing JSON response for job {job_id}: {e}")
            return None

    def _wait_for_job_start(
        self, job_id: str, expected_nodes: int, timeout: Optional[int] = None
    ) -> List[str]:
        """Wait for the PBS job to start and return the allocated hostnames."""
        start_time = time.time()
        try:
            while timeout is None or time.time() - start_time < timeout:
                job_info = self._get_job_info_json(job_id)
                if not job_info:
                    raise RuntimeError(f"PBS job {job_id} not found")
                job_state = job_info.get("job_state")
                if job_state == "R" and job_info.get("exec_host"):
                    hostnames = parse_exec_host(job_info["exec_host"])
                    logger.info(
                        f"PBS job {job_id} is running on {len(hostnames)} nodes: {hostnames}"
                    )
                    if len(hostnames) != expected_nodes:
                        raise RuntimeError(
                            f"Expected {expected_nodes} nodes but got {len(hostnames)}. "
                            f"Partial allocation not supported."
                        )
                    return hostnames
                elif job_state in _PBS_TERMINAL_STATES:
                    raise RuntimeError(
                        f"PBS job {job_id} failed with state: {job_state}"
                    )
                logger.debug(f"PBS job {job_id} state: {job_state}, waiting...")
                time.sleep(2)  # Check every 2 seconds
            raise RuntimeError(f"Timeout waiting for PBS job {job_id} to start")
        except Exception:
            # Cleanup on failure - reuse _kill() logic
            logger.error(f"Failed to start PBS job {job_id}, deleting job")
            self._kill()
            raise

    def _state(self) -> JobState:
        if not self._jobs_active():
            raise RuntimeError("PBS job is no longer active")

        total_nodes = sum(self._meshes.values())
        if not self._all_hostnames:
            if in_batch_job():
                self._all_hostnames = read_nodefile()[:total_nodes]
            else:
                job_id = self._resolved_job_id()
                if job_id is None:
                    raise RuntimeError("PBS job ID is not set")
                self._all_hostnames = self._wait_for_job_start(
                    job_id, total_nodes, timeout=self._job_start_timeout
                )
        if len(self._all_hostnames) < total_nodes:
            raise RuntimeError(
                f"Meshes need {total_nodes} nodes but the job has {len(self._all_hostnames)}"
            )
        # Inside an allocation started by a hand-written script, the runner's
        # service ids are in the environment rather than in this object.
        if not self._service_proc_ids and in_batch_job():
            self._service_proc_ids = deserialize_service_proc_ids(
                os.environ[SERVICE_PROC_IDS_ENV]
            )

        # Distribute the allocated hostnames among meshes
        host_meshes = {}
        hostname_idx = 0
        for mesh_name, num_nodes in self._meshes.items():
            next_hostname_idx = hostname_idx + num_nodes
            mesh_hostnames = self._all_hostnames[hostname_idx:next_hostname_idx]
            service_proc_ids = self._service_proc_ids[hostname_idx:next_hostname_idx]
            hostname_idx = next_hostname_idx

            workers = [f"tcp://{hostname}:{self._port}" for hostname in mesh_hostnames]
            host_meshes[mesh_name] = attach_to_workers(
                name=mesh_name,
                ca="trust_all_connections",
                workers=service_proc_addrs(workers, service_proc_ids),
            )
        return JobState(host_meshes)

    def can_run(self, spec: "JobTrait") -> bool:
        """Check if this job can run the given spec."""
        return (
            isinstance(spec, PBSJob)
            and spec._meshes == self._meshes
            and spec._python_exe == self._python_exe
            and spec._port == self._port
            and spec._pbs_args == self._pbs_args
            and spec._job_name == self._job_name
            and spec._time_limit == self._time_limit
            and spec._queue == self._queue
            and spec._account == self._account
            and spec._job_start_timeout == self._job_start_timeout
            and spec._mpiexec == self._mpiexec
            and spec._mpiexec_args == self._mpiexec_args
            and spec._rank_env == self._rank_env
            and spec._bind_to == self._bind_to
            and self._jobs_active()
        )

    def _jobs_active(self) -> bool:
        """Check if the PBS job is still active by querying qstat."""
        # Inside the allocation the job is active by definition.
        if in_batch_job():
            return True
        if not self.active:
            return False
        job_id = self._resolved_job_id()
        if job_id is None:
            return False
        job_info = self._get_job_info_json(job_id)
        if not job_info:
            logger.warning(f"PBS job {job_id} not found")
            return False
        job_state = job_info.get("job_state")
        if job_state in _PBS_TERMINAL_STATES:
            logger.warning(f"PBS job {job_id} has state: {job_state}")
            return False
        return True

    def _kill(self) -> None:
        """Delete the PBS job.

        No-op in batch mode: ``BatchJob`` registers this as an ``atexit`` hook on
        the in-allocation client, but the PBS runner owns teardown there, so the
        client must not qdel its own allocation.
        """
        if in_batch_job():
            return
        job_id = self._resolved_job_id()
        if job_id is not None:
            try:
                subprocess.run(
                    ["qdel", job_id], capture_output=True, text=True, check=True
                )
                logger.info(f"Deleted PBS job {job_id}")
            except subprocess.CalledProcessError as e:
                logger.warning(f"Failed to delete PBS job {job_id}: {e.stderr}")
        self._pbs_job_id = None
        self._all_hostnames.clear()
