# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
import logging
import os
import subprocess
import sys
from typing import Any, Dict, FrozenSet, List, Optional, Sequence

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import configure
from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.job.job import JobState, JobTrait


logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stderr))
logger.propagate = False

# terminal states that indicate the job is no longer active
_SLURM_TERMINAL_STATES: FrozenSet[str] = frozenset(
    ["FAILED", "CANCELLED", "TIMEOUT", "PREEMPTED", "COMPLETED"]
)


class SlurmJob(JobTrait):
    """
    A job scheduler that uses SLURM command line tools to schedule jobs.

    This implementation:
    1. Uses sbatch to submit SLURM jobs that start monarch workers
    2. Queries job status with squeue to get allocated hostnames
    3. Uses the hostnames to connect to the started workers
    """

    def __init__(
        self,
        meshes: Dict[str, int],
        python_exe: str = "python",
        slurm_args: Sequence[str] = (),
        monarch_port: int = 22222,
        job_name: str = "monarch_job",
        ntasks_per_node: int = 1,
        time_limit: Optional[str] = None,
        partition: Optional[str] = None,
        log_dir: Optional[str] = None,
        exclusive: bool = True,
        gpus_per_node: Optional[int] = None,
        cpus_per_task: Optional[int] = None,
        mem: Optional[str] = None,
        job_start_timeout: Optional[int] = None,
    ) -> None:
        """
        Args:
            meshes: Dictionary mapping mesh names to number of nodes
            python_exe: Python executable to use for worker processes
            slurm_args: Additional SLURM arguments to pass to sbatch
            monarch_port: Port for TCP communication between workers
            job_name: Name for the SLURM job
            ntasks_per_node: Number of tasks per node
            time_limit: Maximum runtime in HH:MM:SS format. If None, uses SLURM's default time limit.
            partition: SLURM partition to submit to
            log_dir: Directory for SLURM log files
            exclusive: Whether to request exclusive node access (no other jobs can run on the nodes).
                      Defaults to True for predictable performance and resource isolation,
                      but may increase queue times and waste resources if nodes are underutilized.
            gpus_per_node: Number of GPUs to request per node. If None, no GPU resources are requested.
            job_start_timeout: Maximum time in seconds to wait for the SLURM job to start running.
                      This should account for potential queueing delays. If None (default), waits indefinitely.
        """
        configure(default_transport=ChannelTransport.TcpWithHostname)
        self._meshes = meshes
        self._python_exe = python_exe
        self._slurm_args = slurm_args
        self._port = monarch_port
        self._job_name = job_name
        self._ntasks_per_node = ntasks_per_node
        self._time_limit = time_limit
        self._partition = partition
        self._log_dir: str = log_dir if log_dir is not None else os.getcwd()
        self._exclusive = exclusive
        self._gpus_per_node = gpus_per_node
        self._cpus_per_task = cpus_per_task
        self._mem = mem
        self._job_start_timeout = job_start_timeout
        # Track the single SLURM job ID and all allocated hostnames
        self._slurm_job_id: Optional[str] = None
        self._all_hostnames: List[str] = []
        super().__init__()

    def add_mesh(self, name: str, num_nodes: int) -> None:
        self._meshes[name] = num_nodes

    def _create(self, client_script: Optional[str]) -> None:
        """Submit a single SLURM job for all meshes."""
        if client_script is not None:
            raise RuntimeError("SlurmJob cannot run batch-mode scripts")

        total_nodes = sum(self._meshes.values())
        self._slurm_job_id = self._submit_slurm_job(total_nodes)

    def _submit_slurm_job(self, num_nodes: int) -> str:
        """Submit a SLURM job for all nodes."""
        unique_job_name = f"{self._job_name}_{os.getpid()}"

        # Create log directory if it doesn't exist
        os.makedirs(self._log_dir, exist_ok=True)

        log_path_out = os.path.join(self._log_dir, f"slurm_%j_{unique_job_name}.out")
        log_path_err = os.path.join(self._log_dir, f"slurm_%j_{unique_job_name}.err")

        python_command = f'import socket; from monarch.actor import run_worker_loop_forever; hostname = socket.gethostname(); run_worker_loop_forever(address=f"tcp://{{hostname}}:{self._port}", ca="trust_all_connections")'

        # Build SBATCH directives
        sbatch_directives = [
            "#!/bin/bash",
            f"#SBATCH --job-name={unique_job_name}",
            f"#SBATCH --ntasks-per-node={self._ntasks_per_node}",
            f"#SBATCH --nodes={num_nodes}",
            f"#SBATCH --output={log_path_out}",
            f"#SBATCH --error={log_path_err}",
        ]

        if self._time_limit is not None:
            sbatch_directives.append(f"#SBATCH --time={self._time_limit}")

        if self._gpus_per_node is not None:
            sbatch_directives.append(f"#SBATCH --gpus-per-node={self._gpus_per_node}")

        if self._cpus_per_task is not None:
            sbatch_directives.append(f"#SBATCH --cpus-per-task={self._cpus_per_task}")

        if self._mem is not None:
            sbatch_directives.append(f"#SBATCH --mem={self._mem}")

        if self._exclusive:
            sbatch_directives.append("#SBATCH --exclusive")

        if self._partition is not None:
            sbatch_directives.append(f"#SBATCH --partition={self._partition}")

        if (
            not self._exclusive
            and self._partition is not None
            and self._gpus_per_node is not None
        ):
            gpus_per_task = self._gpus_per_node // self._ntasks_per_node
            assert self._partition, (
                "Slurm partition must be set for jobs that share nodes with other jobs"
            )
            self.share_node(
                tasks_per_node=self._ntasks_per_node,
                gpus_per_task=gpus_per_task,
                partition=self._partition,
            )

        # Add any additional slurm args as directives
        for arg in self._slurm_args:
            if arg.startswith("-"):
                sbatch_directives.append(f"#SBATCH {arg}")

        batch_script = "\n".join(sbatch_directives)
        batch_script += f"\nsrun {self._python_exe} -c '{python_command}'\n"

        logger.info(f"Submitting SLURM job with {num_nodes} nodes")

        try:
            result = subprocess.run(
                ["sbatch"],
                input=batch_script,
                capture_output=True,
                text=True,
                check=True,
            )

            # Parse the job ID from sbatch output (typically "Submitted batch job 12345")
            job_id = None
            for line in result.stdout.strip().split("\n"):
                if "Submitted batch job" in line:
                    job_id = line.split()[-1]
                    break

            if not job_id:
                raise RuntimeError(
                    f"Failed to parse job ID from sbatch output: {result.stdout}"
                )

            logger.info(
                f"SLURM job {job_id} submitted. Logs will be written to: {self._log_dir}/slurm_{job_id}_{unique_job_name}.out"
            )
            return job_id

        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to submit SLURM job: {e.stderr}") from e

    def _get_job_info_json(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job information using squeue --json."""
        try:
            result = subprocess.run(
                ["squeue", "--job", job_id, "--json"],
                capture_output=True,
                text=True,
                check=True,
            )

            if result.stdout.strip():
                data = json.loads(result.stdout)
                jobs = data.get("jobs", [])
                return jobs[0] if jobs else None
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
        """
        Wait for the SLURM job to start and return the allocated hostnames.
        Requires Slurm 20.02+ for squeue --json support.
        """
        import time

        start_time = time.time()

        try:
            while timeout is None or time.time() - start_time < timeout:
                job_info = self._get_job_info_json(job_id)

                if not job_info:
                    raise RuntimeError(f"SLURM job {job_id} not found in queue")

                job_state = job_info.get("job_state", [])

                if "RUNNING" in job_state:
                    # Extract hostnames from job_resources.nodes.allocation
                    job_resources = job_info.get("job_resources", {})
                    nodes_info = job_resources.get("nodes", {})
                    allocation = nodes_info.get("allocation", [])
                    hostnames = [node["name"] for node in allocation]

                    logger.info(
                        f"SLURM job {job_id} is running on {len(hostnames)} nodes: {hostnames}"
                    )

                    if len(hostnames) != expected_nodes:
                        raise RuntimeError(
                            f"Expected {expected_nodes} nodes but got {len(hostnames)}. "
                            f"Partial allocation not supported."
                        )

                    return hostnames
                elif any(state in job_state for state in _SLURM_TERMINAL_STATES):
                    raise RuntimeError(
                        f"SLURM job {job_id} failed with status: {job_state}"
                    )
                else:
                    logger.debug(f"SLURM job {job_id} status: {job_state}, waiting...")

                time.sleep(2)  # Check every 2 seconds

            raise RuntimeError(f"Timeout waiting for SLURM job {job_id} to start")

        except Exception:
            # Cleanup on failure - reuse _kill() logic
            logger.error(f"Failed to start SLURM job {job_id}, cancelling job")
            self._kill()
            raise

    def _state(self) -> JobState:
        if not self._jobs_active():
            raise RuntimeError("SLURM job is no longer active")

        # Wait for job to start and get hostnames if not already done
        if not self._all_hostnames:
            job_id = self._slurm_job_id
            if job_id is None:
                raise RuntimeError("SLURM job ID is not set")
            total_nodes = sum(self._meshes.values())
            self._all_hostnames = self._wait_for_job_start(
                job_id, total_nodes, timeout=self._job_start_timeout
            )

        # Distribute the allocated hostnames among meshes
        host_meshes = {}
        hostname_idx = 0

        for mesh_name, num_nodes in self._meshes.items():
            mesh_hostnames = self._all_hostnames[
                hostname_idx : hostname_idx + num_nodes
            ]
            hostname_idx += num_nodes

            workers = [f"tcp://{hostname}:{self._port}" for hostname in mesh_hostnames]
            host_mesh = attach_to_workers(
                name=mesh_name,
                ca="trust_all_connections",
                workers=workers,  # type: ignore[arg-type]
            )

            host_meshes[mesh_name] = host_mesh

        return JobState(host_meshes)

    def can_run(self, spec: "JobTrait") -> bool:
        """Check if this job can run the given spec."""
        return (
            isinstance(spec, SlurmJob)
            and spec._meshes == self._meshes
            and spec._python_exe == self._python_exe
            and spec._port == self._port
            and spec._slurm_args == self._slurm_args
            and spec._job_name == self._job_name
            and spec._ntasks_per_node == self._ntasks_per_node
            and spec._time_limit == self._time_limit
            and spec._partition == self._partition
            and spec._gpus_per_node == self._gpus_per_node
            and spec._cpus_per_task == self._cpus_per_task
            and spec._mem == self._mem
            and spec._job_start_timeout == self._job_start_timeout
            and self._jobs_active()
        )

    def _jobs_active(self) -> bool:
        """Check if SLURM job is still active by querying squeue."""
        if not self.active or self._slurm_job_id is None:
            return False

        job_info = self._get_job_info_json(self._slurm_job_id)

        if not job_info:
            logger.warning(f"SLURM job {self._slurm_job_id} not found in queue")
            return False

        job_state = job_info.get("job_state", [])
        if any(state in job_state for state in _SLURM_TERMINAL_STATES):
            logger.warning(f"SLURM job {self._slurm_job_id} has status: {job_state}")
            return False

        return True

    def share_node(
        self, tasks_per_node: int, gpus_per_task: int, partition: str
    ) -> None:
        """
        Share a node with other jobs.
        """
        try:
            import clusterscope
        except ImportError:
            raise RuntimeError(
                "please install clusterscope to use share_node. `pip install clusterscope`"
            )
        self._exclusive = False

        slurm_args = clusterscope.job_gen_task_slurm(
            partition=partition,
            gpus_per_task=gpus_per_task,
            tasks_per_node=tasks_per_node,
        )
        self._cpus_per_task = slurm_args["cpus_per_task"]
        self._mem = slurm_args["memory"]

    def _kill(self) -> None:
        """Cancel the SLURM job."""
        if self._slurm_job_id is not None:
            try:
                subprocess.run(
                    ["scancel", self._slurm_job_id],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                logger.info(f"Cancelled SLURM job {self._slurm_job_id}")
            except subprocess.CalledProcessError as e:
                logger.warning(
                    f"Failed to cancel SLURM job {self._slurm_job_id}: {e.stderr}"
                )

        self._slurm_job_id = None
        self._all_hostnames.clear()
