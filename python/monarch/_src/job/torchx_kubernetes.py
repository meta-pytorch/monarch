# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Monarch JobTrait implementation for TorchX Kubernetes scheduler.

TorchXKubernetesJob allows running Monarch workers on Kubernetes clusters
using TorchX with the Volcano scheduler backend.

Requirements:
    - pip install torchx[kubernetes]
    - Volcano scheduler installed on the Kubernetes cluster
    - kubectl configured with access to the target cluster

Note that Monarch containers come with torchx[kubernetes] and kubectl available for convenience.
"""

import logging
import time
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from torchx.specs import AppState

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import configure

from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.actor.future import Future
from monarch._src.job.job import JobState, JobTrait


logger: logging.Logger = logging.getLogger(__name__)


class TorchXKubernetesJob(JobTrait):
    """
    A job scheduler that uses TorchX with Kubernetes/Volcano to schedule jobs.

    This implementation:
    1. Uses TorchX Runner to submit Kubernetes jobs via Volcano scheduler
    2. Queries job status through TorchX to get pod hostnames/IPs
    3. Uses the pod addresses to connect to the started Monarch workers

    Prerequisites:
    - Volcano scheduler must be installed on the Kubernetes cluster.
    - A Volcano queue must exist (default queue is typically available)

    Tested with:
    - Kubernetes v1.33 (EKS)
    - Volcano v1.13.0 (Helm chart)

    See example usage in `monarch/examples/kubernetes/torchx_kubernetes_job.py`.
    """

    # Terminal state names - used for checking job completion status.
    # UNKNOWN is excluded because TorchX may return UNKNOWN briefly when a job
    # is first submitted before Kubernetes reports the real status.
    _TERMINAL_STATE_NAMES: FrozenSet[str] = frozenset(
        {"SUCCEEDED", "FAILED", "CANCELLED"}
    )

    @staticmethod
    def _is_terminal_state(state: "AppState") -> bool:
        """Check if an AppState represents a terminal (completed) state."""
        return state.name in TorchXKubernetesJob._TERMINAL_STATE_NAMES

    def __init__(
        self,
        meshes: Dict[str, int],
        namespace: str = "default",
        queue: str = "default",
        image: str = "ghcr.io/meta-pytorch/monarch:latest",  # placeholder; override until we start tagging latest.
        service_account: Optional[str] = None,
        monarch_port: int = 22222,
        job_name: str = "monarch-job",
        cpu: int = 1,
        memMB: int = 4096,
        gpu: int = 0,
        job_start_timeout: Optional[int] = 300,
        scheduler_args: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        Args:
            meshes: Dictionary mapping mesh names to number of replicas (pods).
            namespace: Kubernetes namespace to deploy jobs in.
            queue: Volcano queue name for job scheduling.
            image: Docker image containing Monarch installation.
            service_account: Kubernetes service account for pods.
            monarch_port: Port for TCP communication between workers.
            job_name: Base name for the Kubernetes job.
            cpu: Number of CPU cores per pod.
            memMB: Memory in MB per pod.
            gpu: Number of GPUs per pod.
            job_start_timeout: Maximum time in seconds to wait for job to start.
                If None, waits indefinitely.
            scheduler_args: Additional TorchX scheduler arguments to pass.
        """
        configure(default_transport=ChannelTransport.TcpWithHostname)

        self._meshes = meshes
        self._namespace = namespace
        self._queue = queue
        self._image = image
        self._service_account = service_account
        self._port = monarch_port
        self._job_name = job_name
        self._cpu = cpu
        self._memMB = memMB
        self._gpu = gpu
        self._job_start_timeout = job_start_timeout
        self._scheduler_args: Dict[str, Any] = (
            dict(scheduler_args) if scheduler_args else {}
        )

        # Runtime state
        self._job_handle: Optional[str] = None
        self._all_pod_ips: List[str] = []

        super().__init__()

    def add_mesh(self, name: str, num_replicas: int) -> None:
        self._meshes[name] = num_replicas

    def _get_scheduler_cfg(self) -> Dict[str, Any]:
        """Build TorchX scheduler configuration."""
        cfg: Dict[str, Any] = {
            "namespace": self._namespace,
            "queue": self._queue,
        }
        if self._service_account:
            cfg["service_account"] = self._service_account

        # Merge any additional scheduler args
        cfg.update(self._scheduler_args)
        return cfg

    def _create(self, client_script: Optional[str]) -> None:
        """Submit a TorchX Kubernetes job for all meshes."""
        if client_script is not None:
            raise RuntimeError("TorchXKubernetesJob cannot run batch-mode scripts")

        # Lazy import torchx to avoid import errors when not installed
        try:
            from torchx import specs
            from torchx.runner import get_runner
        except ImportError as e:
            raise RuntimeError(
                "TorchX is not installed. Install with: pip install torchx[kubernetes]"
            ) from e

        total_replicas = sum(self._meshes.values())

        # Build the worker startup command
        # Use pod hostname for the worker address since kubernetes provides
        # DNS resolution for pod hostnames within the cluster.
        bash_script = f"""
exec python3 -c '
import socket
hostname = socket.gethostname()
address = f"tcp://{{hostname}}:{self._port}"
print(f"Starting Monarch worker at {{address}}", flush=True)
from monarch.actor import run_worker_loop_forever
run_worker_loop_forever(address=address, ca="trust_all_connections")
'
"""
        entrypoint = "bash"
        entrypoint_args = ["-c", bash_script]

        # Create TorchX AppDef with a single role containing all replicas
        unique_job_name = f"{self._job_name}-{int(time.time())}"

        role = specs.Role(
            name="worker",
            image=self._image,
            entrypoint=entrypoint,
            args=entrypoint_args,
            num_replicas=total_replicas,
            resource=specs.Resource(
                cpu=self._cpu,
                memMB=self._memMB,
                gpu=self._gpu,
            ),
            port_map={"monarch": self._port},
        )

        appdef = specs.AppDef(
            name=unique_job_name,
            roles=[role],
        )

        logger.info(
            f"Submitting TorchX Kubernetes job '{unique_job_name}' "
            f"with {total_replicas} replicas to namespace '{self._namespace}'"
        )

        # Submit the job using TorchX Runner
        with get_runner() as runner:
            cfg = self._get_scheduler_cfg()
            self._job_handle = runner.run(appdef, scheduler="kubernetes", cfg=cfg)

        logger.info(f"TorchX job submitted: {self._job_handle}")

    def _wait_for_job_start(self, timeout: Optional[int] = None) -> List[str]:
        """
        Wait for the TorchX job to start and return pod IP addresses.

        Returns:
            List of pod IP addresses where workers are running.
        """
        try:
            from torchx.runner import get_runner
            from torchx.specs import AppState
        except ImportError as e:
            raise RuntimeError(
                "TorchX is not installed. Install with: pip install torchx[kubernetes]"
            ) from e

        job_handle = self._job_handle
        if job_handle is None:
            raise RuntimeError("Job has not been submitted yet")

        start_time = time.time()
        total_replicas = sum(self._meshes.values())

        try:
            while timeout is None or time.time() - start_time < timeout:
                with get_runner() as runner:
                    status = runner.status(job_handle)

                    if status is None:
                        raise RuntimeError(f"TorchX job {job_handle} not found")

                    if status.state == AppState.RUNNING:
                        # Get pod IPs from replica status
                        pod_ips: List[str] = []
                        for role_status in status.roles:
                            for replica in role_status.replicas:
                                # The hostname field contains the pod IP for k8s
                                if replica.hostname:
                                    pod_ips.append(replica.hostname)

                        if len(pod_ips) >= total_replicas:
                            logger.info(
                                f"TorchX job {job_handle} is running "
                                f"with {len(pod_ips)} pods: {pod_ips}"
                            )
                            return pod_ips

                        logger.debug(
                            f"Job running but only {len(pod_ips)}/{total_replicas} "
                            f"pods ready, waiting..."
                        )

                    elif self._is_terminal_state(status.state):
                        raise RuntimeError(
                            f"TorchX job {job_handle} failed with state: {status.state.name}"
                        )
                    else:
                        logger.debug(
                            f"TorchX job {job_handle} state: {status.state.name}, waiting..."
                        )

                time.sleep(5)  # Poll every 5 seconds

            raise RuntimeError(f"Timeout waiting for TorchX job {job_handle} to start")

        except Exception:
            logger.error(f"Failed waiting for TorchX job {job_handle}, cancelling")
            self._kill()
            raise

    def _state(self) -> JobState:
        """Get the current state with HostMesh objects for each mesh."""
        if not self._jobs_active():
            raise RuntimeError("TorchX Kubernetes job is no longer active")

        # Wait for job to start and get pod IPs if not already cached
        if not self._all_pod_ips:
            self._all_pod_ips = self._wait_for_job_start(
                timeout=self._job_start_timeout
            )

        # Distribute pod IPs among meshes
        host_meshes = {}
        ip_idx = 0

        for mesh_name, num_replicas in self._meshes.items():
            mesh_ips = self._all_pod_ips[ip_idx : ip_idx + num_replicas]
            ip_idx += num_replicas

            workers: List[str | Future[str]] = [
                f"tcp://{ip}:{self._port}" for ip in mesh_ips
            ]
            logger.info(f"Connecting to workers for mesh '{mesh_name}': {workers}")

            host_mesh = attach_to_workers(
                name=mesh_name,
                ca="trust_all_connections",
                workers=workers,
            )

            host_meshes[mesh_name] = host_mesh

        return JobState(host_meshes)

    def can_run(self, spec: "JobTrait") -> bool:
        """Check if this job can run the given spec."""
        return (
            isinstance(spec, TorchXKubernetesJob)
            and spec._meshes == self._meshes
            and spec._namespace == self._namespace
            and spec._queue == self._queue
            and spec._image == self._image
            and spec._port == self._port
            and spec._cpu == self._cpu
            and spec._memMB == self._memMB
            and spec._gpu == self._gpu
            and self._jobs_active()
        )

    def _jobs_active(self) -> bool:
        """Check if the TorchX job is still active."""
        job_handle = self._job_handle
        if not self.active or job_handle is None:
            return False

        try:
            from torchx.runner import get_runner

            with get_runner() as runner:
                status = runner.status(job_handle)

                if status is None:
                    logger.warning(f"TorchX job {job_handle} not found")
                    return False

                if self._is_terminal_state(status.state):
                    logger.warning(
                        f"TorchX job {job_handle} has state: {status.state.name}"
                    )
                    return False

                return True

        except ImportError:
            logger.warning("TorchX not installed, cannot check job status")
            return False
        except Exception as e:
            logger.warning(f"Error checking job status: {e}")
            return False

    def _kill(self) -> None:
        """Cancel the TorchX Kubernetes job."""
        job_handle = self._job_handle
        if job_handle is not None:
            try:
                from torchx.runner import get_runner

                with get_runner() as runner:
                    runner.cancel(job_handle)
                logger.info(f"Cancelled TorchX job {job_handle}")

            except ImportError:
                logger.warning("TorchX not installed, cannot cancel job")
            except Exception as e:
                logger.warning(f"Failed to cancel TorchX job {job_handle}: {e}")

        self._job_handle = None
        self._all_pod_ips.clear()
