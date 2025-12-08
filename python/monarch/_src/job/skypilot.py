# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import logging
import os
import sys
import time
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from monarch._src.job.job import JobState, JobTrait

# Defer imports that may not be available in all environments
if TYPE_CHECKING:
    import sky
    from sky.backends.cloud_vm_ray_backend import CloudVmRayResourceHandle

try:
    import sky
    from sky.backends.cloud_vm_ray_backend import CloudVmRayResourceHandle

    HAS_SKYPILOT = True
except ImportError:
    HAS_SKYPILOT = False
    sky = None  # type: ignore[assignment]
    CloudVmRayResourceHandle = None  # type: ignore[assignment, misc]


logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stderr))
logger.propagate = False

# Default port for Monarch TCP communication
DEFAULT_MONARCH_PORT = 22222

# Default setup commands to build Monarch from source on remote workers.
# NOTE: Cold start is slow (~7-10 minutes) because we need to compile Monarch
# each worker This is necessary to ensure client/worker version compatibility
# when using a development branch. For production use, consider
# using pre-built wheels from PyPI (pip install torchmonarch).
#
# For faster cold starts (<30s), use a custom Docker image with all dependencies
# pre-installed by setting image_id in sky.Resources:
#   resources = sky.Resources(image_id="docker:your-registry/monarch-image:tag", ...)
DEFAULT_SETUP_COMMANDS = """
set -ex

# Add PPA for newer toolchains
sudo apt-get update
sudo apt-get install -y software-properties-common
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt-get update

# Install system dependencies
sudo apt-get install -y \
  build-essential \
  ninja-build \
  g++-11 \
  rdma-core \
  libibverbs1 \
  libmlx5-1 \
  libibverbs-dev \
  curl \
  pkg-config \
  libssl-dev

# Install CUDA toolkit and NCCL
wget -q https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-keyring_1.1-1_all.deb
sudo dpkg -i cuda-keyring_1.1-1_all.deb
sudo apt-get update
sudo apt-get install -y cuda-toolkit-12-1
sudo apt-get install -y --allow-change-held-packages libnccl2=2.28.9-1+cuda12.9 libnccl-dev=2.28.9-1+cuda12.9

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
rustup default nightly

# Install Python dependencies and build Monarch from source
cd ~/sky_workdir
pip install setuptools-rust maturin
pip install -r torch-requirements.txt -r build-requirements.txt
CC=gcc-11 CXX=g++-11 USE_TENSOR_ENGINE=0 pip install --no-build-isolation .

echo "Done installing Monarch"
"""


def _configure_transport() -> None:
    """Configure the Monarch transport. Deferred import to avoid import errors."""
    from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
    from monarch._rust_bindings.monarch_hyperactor.config import configure

    configure(default_transport=ChannelTransport.TcpWithHostname)


def _attach_to_workers_wrapper(name: str, ca: str, workers: List[str]):
    """Wrapper around attach_to_workers with deferred import."""
    from monarch._src.actor.bootstrap import attach_to_workers

    return attach_to_workers(name=name, ca=ca, workers=workers)


class SkyPilotJob(JobTrait):
    
    """
    A job scheduler that uses SkyPilot to provision cloud instances.

    SkyPilot supports multiple cloud providers (AWS, GCP, Azure, Lambda, etc.)
    and can automatically select the cheapest available option.

    This implementation:
    1. Uses sky.launch() to provision cloud instances with specified resources
    2. Runs Monarch workers on each node via a startup script
    3. Connects to workers using their IP addresses from the cluster handle

    Example:
        >>> import sky
        >>> from monarch.job import SkyPilotJob
        >>>
        >>> job = SkyPilotJob(
        ...     meshes={"trainers": 2},
        ...     resources=sky.Resources(accelerators="A100:1"),
        ...     cluster_name="my-monarch-cluster",
        ... )
        >>> state = job.state()
        >>> trainers = state.trainers  # HostMesh with 2 nodes
    """

    def __init__(
        self,
        meshes: Dict[str, int],
        resources: Optional["sky.Resources"] = None,
        cluster_name: Optional[str] = None,
        monarch_port: int = DEFAULT_MONARCH_PORT,
        idle_minutes_to_autostop: Optional[int] = None,
        down_on_autostop: bool = False,
        python_exe: str = "python",
        setup_commands: Optional[str] = None,
        workdir: Optional[str] = None,
        file_mounts: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Args:
            meshes: Dictionary mapping mesh names to number of nodes.
                    e.g., {"trainers": 4, "dataloaders": 2}
            resources: SkyPilot Resources specification for the instances.
                       If None, uses SkyPilot defaults.
            cluster_name: Name for the SkyPilot cluster. If None, auto-generated.
            monarch_port: Port for TCP communication between Monarch workers.
            idle_minutes_to_autostop: If set, cluster will autostop after this
                                      many minutes of idleness.
            down_on_autostop: If True, tear down cluster on autostop instead of
                              just stopping it.
            python_exe: Python executable to use for worker processes.
            setup_commands: Optional setup commands to run before starting workers.
                           Use this to install dependencies including Monarch.
                           If None and workdir is provided, uses DEFAULT_SETUP_COMMANDS
                           which builds Monarch from source.
            workdir: Local directory to sync to the cluster. If provided, this
                    directory will be uploaded to ~/sky_workdir on each node.
                    When using workdir with the Monarch repo, DEFAULT_SETUP_COMMANDS
                    will build Monarch from source on each worker.
            file_mounts: Dictionary mapping remote paths to local paths for
                        additional file mounts.
        """
        if not HAS_SKYPILOT:
            raise ImportError(
                "SkyPilot is not installed. Install it with: pip install skypilot"
            )

        # Configure transport at runtime when Monarch is available
        try:
            _configure_transport()
        except ImportError:
            # Monarch bindings not available, will fail later when needed
            pass

        super().__init__()

        self._meshes = meshes
        self._resources = resources
        self._cluster_name = cluster_name
        self._port = monarch_port
        self._idle_minutes_to_autostop = idle_minutes_to_autostop
        self._down_on_autostop = down_on_autostop
        self._python_exe = python_exe
        self._setup_commands = setup_commands
        self._workdir = workdir
        self._file_mounts = file_mounts

        # Runtime state
        self._launched_cluster_name: Optional[str] = None
        self._node_ips: List[str] = []

    def _create(self, client_script: Optional[str]) -> None:
        """Launch a SkyPilot cluster and start Monarch workers."""
        if client_script is not None:
            raise RuntimeError("SkyPilotJob cannot run batch-mode scripts yet")

        total_nodes = sum(self._meshes.values())

        # Build the worker startup command
        worker_command = self._build_worker_command()

        # Create setup commands
        # If workdir is provided but no setup_commands, use defaults to build Monarch
        if self._setup_commands is not None:
            setup = self._setup_commands
        elif self._workdir is not None:
            setup = DEFAULT_SETUP_COMMANDS
        else:
            setup = ""
        if setup and not setup.endswith("\n"):
            setup += "\n"

        # Create the SkyPilot task
        task = sky.Task(
            name="monarch-workers",
            setup=setup if setup else None,
            run=worker_command,
            num_nodes=total_nodes,
            workdir=self._workdir,
        )

        # Add file mounts if provided
        if self._file_mounts:
            task.set_file_mounts(self._file_mounts)

        if self._resources is not None:
            task.set_resources(self._resources)

        # Generate cluster name if not provided
        cluster_name = self._cluster_name or f"monarch-{os.getpid()}"

        logger.info(f"Launching SkyPilot cluster '{cluster_name}' with {total_nodes} nodes")

        # Launch the cluster
        # Note: sky.launch returns a request ID in the SDK, we need to get the result
        try:
            request_id = sky.launch(
                task,
                cluster_name=cluster_name,
                idle_minutes_to_autostop=self._idle_minutes_to_autostop,
                down=self._down_on_autostop,
            )
            # Get the result from the request
            job_id, handle = sky.get(request_id)
        except Exception as e:
            logger.error(f"Failed to launch SkyPilot cluster: {e}")
            raise RuntimeError(f"Failed to launch SkyPilot cluster: {e}") from e

        self._launched_cluster_name = cluster_name
        logger.info(f"SkyPilot cluster '{cluster_name}' launched successfully")
        
        # Wait for the job to be RUNNING (setup complete, run started)
        self._wait_for_job_running(cluster_name, job_id, timeout=900)
    
    def _wait_for_job_running(self, cluster_name: str, job_id: int, timeout: int = 900) -> None:
        """Wait for the SkyPilot job to reach RUNNING status (setup complete)."""
        import time
        start_time = time.time()
        poll_interval = 10  # seconds
        
        logger.info(f"Waiting for job {job_id} setup to complete (timeout={timeout}s)...")
        
        while time.time() - start_time < timeout:
            try:
                # Get job queue for the cluster
                request_id = sky.queue(cluster_name)
                jobs = sky.get(request_id)
                
                # Find our job
                for job in jobs:
                    if job.get('id') == job_id or job.get('job_id') == job_id:
                        status = job.get('status', '')
                        status_str = str(status)
                        if 'RUNNING' in status_str:
                            logger.info(f"Job {job_id} is now RUNNING (setup complete)")
                            return
                        elif 'FAILED' in status_str or 'CANCELLED' in status_str:
                            raise RuntimeError(f"Job {job_id} failed with status: {status}. Check logs with: sky logs {cluster_name}")
                        else:
                            elapsed = int(time.time() - start_time)
                            logger.info(f"Job {job_id} status: {status} (waited {elapsed}s)")
                        break
                
            except Exception as e:
                logger.warning(f"Error checking job status: {e}")
            
            time.sleep(poll_interval)
        
        raise RuntimeError(f"Timeout waiting for job {job_id} to reach RUNNING status")

    def _build_worker_command(self) -> str:
        """Build the bash command to start Monarch workers on each node."""
        # This command will be run on each node via SkyPilot
        # SkyPilot expects a bash script, so we wrap Python code in python -c
        # Note: Use IP address (not hostname) for the worker address since
        # Kubernetes hostnames may not resolve across pods
        python_code = f'''
import socket
import logging
import sys

# Enable verbose logging
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

hostname = socket.gethostname()
ip_addr = socket.gethostbyname(hostname)
address = f"tcp://{{ip_addr}}:{self._port}"
print(f"Starting Monarch worker at {{address}} (hostname={{hostname}})", flush=True)
sys.stdout.flush()

try:
    from monarch.actor import run_worker_loop_forever
    print(f"Imported run_worker_loop_forever successfully", flush=True)
    print(f"Worker ready and listening...", flush=True)
    run_worker_loop_forever(address=address, ca="trust_all_connections")
except Exception as e:
    print(f"ERROR in worker: {{e}}", flush=True)
    import traceback
    traceback.print_exc()
    raise
'''
        # Escape single quotes in the Python code for bash
        escaped_code = python_code.replace("'", "'\"'\"'")
        # Set timeout env vars - setup takes time (building from source) so we need longer timeouts
        env_vars = " ".join([
            "export HYPERACTOR_HOST_SPAWN_READY_TIMEOUT=15m",
            "export HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT=15m",
            "export HYPERACTOR_MESH_PROC_SPAWN_MAX_IDLE=15m",
        ])
        return f"{env_vars} && {self._python_exe} -c '{escaped_code}'"

    def _get_node_ips(self) -> List[str]:
        """Get the IP addresses of all nodes in the cluster."""
        if not self._launched_cluster_name:
            raise RuntimeError("Cluster has not been launched yet")

        # Query cluster status to get handle with node IPs
        try:
            request_id = sky.status(cluster_names=[self._launched_cluster_name])
            statuses = sky.get(request_id)
        except Exception as e:
            raise RuntimeError(f"Failed to get cluster status: {e}") from e

        if not statuses:
            raise RuntimeError(
                f"Cluster '{self._launched_cluster_name}' not found"
            )

        status = statuses[0]
        handle = status.handle

        if handle is None:
            raise RuntimeError(
                f"Cluster '{self._launched_cluster_name}' has no handle"
            )

        if not isinstance(handle, CloudVmRayResourceHandle):
            raise RuntimeError(
                f"Unexpected handle type: {type(handle)}"
            )

        # Get the external IPs from the handle
        if handle.stable_internal_external_ips is None:
            raise RuntimeError("Cluster has no IP information")

        # stable_internal_external_ips is List[Tuple[internal_ip, external_ip]]
        # We use external IPs to connect
        ips = []
        for internal_ip, external_ip in handle.stable_internal_external_ips:
            # Prefer external IP, fall back to internal
            ip = external_ip if external_ip else internal_ip
            if ip:
                ips.append(ip)

        if not ips:
            raise RuntimeError("No IP addresses found for cluster nodes")

        return ips

    def _wait_for_workers_ready(
        self, expected_nodes: int, timeout: int = 300, poll_interval: int = 5
    ) -> List[str]:
        """Wait for workers to be ready and return their addresses."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                ips = self._get_node_ips()
                if len(ips) >= expected_nodes:
                    logger.info(f"Found {len(ips)} nodes ready")
                    return ips
            except Exception as e:
                logger.debug(f"Waiting for workers: {e}")

            time.sleep(poll_interval)

        raise RuntimeError(
            f"Timeout waiting for {expected_nodes} workers after {timeout}s"
        )

    def _state(self) -> JobState:
        """Get the current state with HostMesh objects for each mesh."""
        if not self._jobs_active():
            raise RuntimeError("SkyPilot cluster is not active")

        # Get node IPs if not cached
        if not self._node_ips:
            total_nodes = sum(self._meshes.values())
            self._node_ips = self._wait_for_workers_ready(total_nodes)

        # Distribute IPs among meshes
        host_meshes = {}
        ip_idx = 0

        for mesh_name, num_nodes in self._meshes.items():
            mesh_ips = self._node_ips[ip_idx : ip_idx + num_nodes]
            ip_idx += num_nodes

            workers = [f"tcp://{ip}:{self._port}" for ip in mesh_ips]
            logger.info(f"Connecting to workers for mesh '{mesh_name}': {workers}")

            host_mesh = _attach_to_workers_wrapper(
                name=mesh_name,
                ca="trust_all_connections",
                workers=workers,
            )
            
            # Wait for the host mesh to be initialized (connections established)
            logger.info(f"Waiting for host mesh '{mesh_name}' to initialize...")
            host_mesh.initialized.get()
            logger.info(f"Host mesh '{mesh_name}' initialized successfully")
            
            # Give connections a moment to fully stabilize
            time.sleep(5)
            logger.info(f"Host mesh '{mesh_name}' ready")
            
            host_meshes[mesh_name] = host_mesh

        return JobState(host_meshes)

    def can_run(self, spec: "JobTrait") -> bool:
        """Check if this job can run the given spec."""
        if not isinstance(spec, SkyPilotJob):
            return False

        return (
            spec._meshes == self._meshes
            and spec._resources == self._resources
            and spec._port == self._port
            and self._jobs_active()
        )

    def _jobs_active(self) -> bool:
        """Check if the SkyPilot cluster is still active."""
        if not self.active or not self._launched_cluster_name:
            return False

        try:
            request_id = sky.status(cluster_names=[self._launched_cluster_name])
            statuses = sky.get(request_id)

            if not statuses:
                return False

            status = statuses[0]
            # Check if cluster is UP
            return status.status == sky.ClusterStatus.UP
        except Exception as e:
            logger.warning(f"Error checking cluster status: {e}")
            return False

    def _kill(self) -> None:
        """Tear down the SkyPilot cluster."""
        if self._launched_cluster_name is not None:
            try:
                logger.info(f"Tearing down SkyPilot cluster '{self._launched_cluster_name}'")
                request_id = sky.down(self._launched_cluster_name)
                sky.get(request_id)
                logger.info(f"Cluster '{self._launched_cluster_name}' terminated")
            except Exception as e:
                logger.warning(f"Failed to tear down cluster: {e}")

        self._launched_cluster_name = None
        self._node_ips.clear()

