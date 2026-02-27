"""
Monarch KubernetesJob - Client for external Monarch access via Kubetorch.

This module provides the KubernetesJob class that allows users to create and
interact with Monarch meshes from outside the Kubernetes cluster, using
kubetorch's native RPC to communicate with a MonarchGateway deployed via kt.cls.
"""

import logging
import os
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    import kubetorch as kt

logger = logging.getLogger(__name__)

# Default Monarch port for worker communication
MONARCH_WORKER_PORT = 26600

# Default GPU image compatible with torchmonarch
DEFAULT_MONARCH_IMAGE_GPU = "pytorch/pytorch:2.9.1-cuda12.8-cudnn9-runtime"


class KubernetesJob:
    """
    Kubetorch's Monarch job for external cluster access.

    This class allows users to create and interact with Monarch meshes from
    outside the Kubernetes cluster. It:
    1. Deploys a MonarchGateway class to K8s pods via kt.cls
    2. Uses kubetorch's native RPC to call gateway methods
    3. Provides proxy objects that mirror Monarch's API

    Usage:
        job = KubernetesJob(compute=kt.Compute(cpu="4", gpu=8, replicas=4))
        state = job.state()
        proc_mesh = state.workers.spawn_procs(per_host={"gpus": 8})
        actors = proc_mesh.spawn("trainers", TrainerActor)
        result = actors.train.call(config).get()

    For pre-allocated compute:
        job = KubernetesJob(selector={"app": "my-monarch-workers"})
    """

    def __init__(
        self,
        compute: Optional["kt.Compute"] = None,
        selector: Optional[Dict[str, str]] = None,
        name: Optional[str] = None,
        namespace: str = "default",
        monarch_port: int = 26600,
        sync_dirs: Optional[List[str]] = None,
    ):
        """
        Initialize a KubernetesJob.

        Args:
            compute: kt.Compute object for fresh resource allocation.
                     Mutually exclusive with selector.
            selector: Label selector for pre-allocated pods.
                      Mutually exclusive with compute.
            name: Name for the job/service. Auto-generated if not provided.
            namespace: Kubernetes namespace.
            monarch_port: Port where Monarch workers listen (default 26600).
            sync_dirs: List of directories containing actor class definitions to sync.
                      Defaults to the git root of the current working directory.
        """
        if compute is not None and selector is not None:
            raise ValueError("Cannot specify both compute and selector")
        if compute is None and selector is None:
            raise ValueError("Must specify either compute or selector")

        self._compute = compute
        self._selector = selector
        self._name = name or f"monarch-{uuid4().hex[:8]}"
        self._namespace = namespace
        self._monarch_port = monarch_port
        self._sync_dirs = sync_dirs

        self._gateway: Optional[Any] = None  # kt.cls instance
        self._applied = False

    def _configure_monarch_image(
        self, image: Optional["kt.Image"], has_gpus: bool, sync_dirs: Optional[List[str]] = None
    ) -> "kt.Image":
        """
        Configure the image with Monarch dependencies and user code.

        Handles GPU/CPU image defaults and installs torchmonarch.
        Syncs user-specified directories (or cwd's git root) so actor classes
        are available on the pods.

        Args:
            image: Optional existing kt.Image to extend
            has_gpus: Whether the compute has GPU resources
            sync_dirs: Directories containing actor definitions to sync

        Returns:
            Configured kt.Image with Monarch support
        """
        import kubetorch as kt

        if image is None:
            if has_gpus:
                image = kt.Image(name="monarch-worker").from_docker(DEFAULT_MONARCH_IMAGE_GPU)
            else:
                image = kt.images.Debian()
        elif image.image_id is None:
            if has_gpus:
                image = image.from_docker(DEFAULT_MONARCH_IMAGE_GPU)
            else:
                image = kt.images.Debian()

        # For CPU-only, install PyTorch CPU version (smaller than CUDA version)
        if not has_gpus:
            image = image.pip_install(["torch --index-url https://download.pytorch.org/whl/cpu"])

        image = image.pip_install(["torchmonarch"])

        # Sync user's actor class directories to the pods
        if sync_dirs:
            dirs_to_sync = [os.path.abspath(d) for d in sync_dirs]
        else:
            from kubetorch.resources.callables.utils import locate_working_dir

            working_dir, _, _ = locate_working_dir(os.getcwd())
            dirs_to_sync = [working_dir]

        for sync_dir in dirs_to_sync:
            image = image.copy(sync_dir, ".", contents=True)

        return image

    def apply(self):
        """
        Deploy the gateway and prepare workers.

        This is idempotent - calling multiple times is a no-op.
        """
        if self._applied:
            return

        import kubetorch as kt
        from monarch_kubetorch.gateway import MonarchGateway

        logger.info(f"Applying KubernetesJob: {self._name}")

        if self._compute is not None:
            has_gpus = bool(getattr(self._compute, "gpus", None))

            # Configure image with Monarch dependencies and user code
            monarch_image = self._configure_monarch_image(self._compute.image, has_gpus, self._sync_dirs)
            self._compute.image = monarch_image

            replicas = getattr(self._compute, "replicas", None) or 1

            # Use local mode (gateway handles Monarch coordination itself)
            self._compute = self._compute.distribute(
                distribution_type="local",
                workers=replicas,
            )

            self._gateway = kt.cls(MonarchGateway, name=self._name).to(
                self._compute,
            )

        else:
            # Pre-allocated - use selector to find existing pods
            self._compute = kt.Compute(selector=self._selector)
            self._compute = self._compute.distribute(distribution_type="local")

            self._gateway = kt.cls(MonarchGateway, name=self._name).to(
                self._compute,
            )

        # Use persistent WebSocket connection (JSON serialization is the default;
        # actor args/returns are cloudpickled within the gateway layer)
        self._gateway.connection_mode = "websocket"

        # Suppress per-call "Calling remote cls ..." INFO logs from kubetorch,
        # since every proxy operation generates 1-2 RPC calls
        logging.getLogger("kubetorch.resources.callables.cls.cls").setLevel(logging.WARNING)

        logger.info(f"Gateway deployed: {self._name}")

        # Initialize the gateway (discovers workers and attaches to them)
        replicas = getattr(self._compute, "replicas", None) or 1
        init_result = self._gateway.initialize(
            num_workers=replicas,
            monarch_port=self._monarch_port,
        )
        logger.info(f"Gateway initialized: {init_result}")

        self._applied = True

    def state(self) -> "JobState":
        """
        Get the job state with HostMesh proxies.

        Returns:
            JobState with 'workers' attribute containing the HostMeshProxy
        """
        from monarch_kubetorch.proxy import HostMeshProxy, JobState

        self.apply()

        status = self._gateway.get_status()

        host_mesh = HostMeshProxy(
            host_mesh_id="hm_default",
            shape={"hosts": status.get("num_workers", 1)},
            gateway=self._gateway,
        )

        return JobState({"workers": host_mesh})

    def reset(self):
        """Reset Monarch state (actors, meshes, futures) but keep compute alive.

        After reset, call state() again to get fresh proxies and re-initialize.
        """
        if self._gateway:
            try:
                self._gateway.shutdown()
            except Exception as e:
                logger.warning(f"Error during gateway reset: {e}")

        self._applied = False
        logger.info(f"KubernetesJob {self._name} reset (compute still alive)")

    def kill(self):
        """Kill the job: clear Monarch state and free compute resources."""
        if self._gateway:
            try:
                self._gateway.shutdown()
            except Exception as e:
                logger.warning(f"Error during gateway shutdown: {e}")

            try:
                self._gateway.teardown()
            except Exception as e:
                logger.warning(f"Error during gateway teardown: {e}")

            self._gateway = None

        self._applied = False
        logger.info(f"KubernetesJob {self._name} killed")

    def __enter__(self):
        """Context manager entry."""
        self.apply()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.kill()
        return False

    @property
    def name(self) -> str:
        """Get the job name."""
        return self._name
