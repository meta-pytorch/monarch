"""
Monarch Gateway - Server-side component for external Monarch access.

This class is deployed to K8s pods via kt.cls and handles Monarch operations
proxied from the client via kubetorch's native RPC. It maintains references to
HostMesh, ProcMesh, ActorMesh objects and executes operations on behalf of
the client.
"""

import base64
import os
import socket
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional
from uuid import uuid4

import cloudpickle

# Set Monarch/Hyperactor timeouts BEFORE importing monarch
# These must be set before any monarch imports to take effect
os.environ.setdefault("HYPERACTOR_HOST_SPAWN_READY_TIMEOUT", "300s")
os.environ.setdefault("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT", "300s")
os.environ.setdefault("HYPERACTOR_MESH_PROC_SPAWN_MAX_IDLE", "300s")

# Default port for Monarch worker
DEFAULT_MONARCH_PORT = 26600


def _is_port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    """Check if a port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return False
        except OSError:
            return True


def _get_pod_ip() -> str:
    """Get the IP address of this pod."""
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)


class MonarchGateway:
    """
    Gateway for external Monarch access.

    Deployed as a kt.cls to K8s pods, this class:
    1. Starts the Monarch worker process if not already running
    2. Bootstraps a Monarch root client
    3. Attaches to worker pods discovered via kt.distributed.pod_ips()
    4. Creates and manages HostMesh, ProcMesh, ActorMesh objects
    5. Executes actor method calls on behalf of external clients

    The external client (KubernetesJob) communicates with this gateway via
    kubetorch's native RPC (kt.cls method calls).
    """

    def __init__(self, monarch_port: int = DEFAULT_MONARCH_PORT):
        self._initialized = False
        self._host_mesh = None
        self._proc_meshes: Dict[str, Any] = {}
        self._actor_meshes: Dict[str, Any] = {}
        self._futures: Dict[str, Any] = {}
        self._worker_process: Optional[subprocess.Popen] = None
        self._monarch_port = monarch_port

        # Start the Monarch worker if not already running
        self._ensure_worker_running()

    def _ensure_worker_running(self):
        """Start the Monarch worker process if not already running."""
        pod_ip = _get_pod_ip()

        if _is_port_in_use(self._monarch_port, pod_ip):
            if self._worker_process is not None:
                return
            else:
                time.sleep(3)
                if _is_port_in_use(self._monarch_port, pod_ip):
                    return

        print(f"Starting Monarch worker on {pod_ip}:{self._monarch_port}")

        worker_script = f"""
import os

os.environ.setdefault("HYPERACTOR_HOST_SPAWN_READY_TIMEOUT", "300s")
os.environ.setdefault("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT", "300s")
os.environ.setdefault("HYPERACTOR_MESH_PROC_SPAWN_MAX_IDLE", "300s")

from monarch.actor import run_worker_loop_forever

address = "tcp://{pod_ip}:{self._monarch_port}"
print(f"Monarch worker starting at {{address}}")
run_worker_loop_forever(address=address, ca="trust_all_connections")
"""

        try:
            # Inherit environment from gateway process (kubetorch sets PYTHONPATH)
            self._worker_process = subprocess.Popen(
                [sys.executable, "-c", worker_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            time.sleep(2)

            if self._worker_process.poll() is not None:
                _, stderr = self._worker_process.communicate()
                raise RuntimeError(f"Monarch worker failed to start: {stderr.decode('utf-8', errors='replace')}")

        except Exception as e:
            raise RuntimeError(f"Failed to start Monarch worker: {e}. Ensure torchmonarch is installed.")

    def _stop_worker(self):
        """Stop the Monarch worker subprocess."""
        if self._worker_process is not None:
            try:
                self._worker_process.terminate()
                self._worker_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._worker_process.kill()
                self._worker_process.wait()
            self._worker_process = None
            time.sleep(1)

    def initialize(
        self,
        num_workers: Optional[int] = None,
        monarch_port: int = 26600,
    ) -> Dict[str, Any]:
        """Initialize the gateway by discovering and attaching to worker pods."""
        if self._initialized:
            return {
                "status": "already_initialized",
                "host_mesh_id": "hm_default",
                "num_workers": len(self._host_mesh) if self._host_mesh else 0,
            }

        # Discover workers via kubetorch's headless service DNS
        from kubetorch.distributed import pod_ips

        discovered_ips = pod_ips(quorum_workers=num_workers, quorum_timeout=120)

        if not discovered_ips:
            raise RuntimeError("No worker IPs discovered")

        worker_addresses = [f"tcp://{ip}:{monarch_port}" for ip in discovered_ips]
        print(f"Attaching to {len(worker_addresses)} Monarch workers: {discovered_ips}")

        # Import Monarch and attach to workers
        from monarch.actor import attach_to_workers, enable_transport

        enable_transport("tcp")

        self._host_mesh = attach_to_workers(
            ca="trust_all_connections",
            workers=worker_addresses,
        )

        # Wait for initialization
        self._host_mesh.initialized.get()
        time.sleep(5)  # Stabilization delay

        self._initialized = True
        return {
            "status": "initialized",
            "host_mesh_id": "hm_default",
            "num_workers": len(worker_addresses),
            "worker_ips": discovered_ips,
        }

    def spawn_procs(
        self,
        per_host: Dict[str, int],
        name: str = "procs",
    ) -> Dict[str, Any]:
        """Spawn a ProcMesh on the HostMesh."""
        if not self._initialized:
            raise RuntimeError("Gateway not initialized. Call initialize() first.")

        proc_mesh = self._host_mesh.spawn_procs(per_host=per_host, name=name)
        proc_mesh_id = f"pm_{uuid4().hex[:8]}"
        self._proc_meshes[proc_mesh_id] = proc_mesh

        shape = dict(proc_mesh.sizes) if hasattr(proc_mesh, "sizes") else {}
        return {"proc_mesh_id": proc_mesh_id, "shape": shape}

    def spawn_actors(
        self,
        proc_mesh_id: str,
        name: str,
        module_name: str,
        class_name: str,
        module_path: str = "",
        encoded_args: str = "",
        encoded_kwargs: str = "",
    ) -> Dict[str, Any]:
        """Spawn an ActorMesh on a ProcMesh."""
        if proc_mesh_id not in self._proc_meshes:
            raise ValueError(f"Unknown proc_mesh_id: {proc_mesh_id}")

        proc_mesh = self._proc_meshes[proc_mesh_id]

        # Import the actor class
        import importlib

        cwd = os.getcwd()
        import_path = os.path.join(cwd, module_path) if module_path else cwd

        if import_path not in sys.path:
            sys.path.insert(0, import_path)

        module = importlib.import_module(module_name)
        actor_class = getattr(module, class_name)

        args = cloudpickle.loads(base64.b64decode(encoded_args)) if encoded_args else []
        kwargs = cloudpickle.loads(base64.b64decode(encoded_kwargs)) if encoded_kwargs else {}

        actor_mesh = proc_mesh.spawn(name, actor_class, *args, **kwargs)
        actor_mesh_id = f"am_{uuid4().hex[:8]}"
        self._actor_meshes[actor_mesh_id] = actor_mesh

        shape = dict(actor_mesh.sizes) if hasattr(actor_mesh, "sizes") else {}
        return {"actor_mesh_id": actor_mesh_id, "shape": shape}

    def call_endpoint(
        self,
        actor_mesh_id: str,
        endpoint_name: str,
        encoded_args: str = "",
        encoded_kwargs: str = "",
        selection: str = "all",
    ) -> Dict[str, Any]:
        """Call an endpoint on an ActorMesh."""
        if actor_mesh_id not in self._actor_meshes:
            raise ValueError(f"Unknown actor_mesh_id: {actor_mesh_id}")

        actor_mesh = self._actor_meshes[actor_mesh_id]
        args = cloudpickle.loads(base64.b64decode(encoded_args)) if encoded_args else []
        kwargs = cloudpickle.loads(base64.b64decode(encoded_kwargs)) if encoded_kwargs else {}

        endpoint = getattr(actor_mesh, endpoint_name)

        if selection == "one":
            future = endpoint.call_one(*args, **kwargs)
        else:
            future = endpoint.call(*args, **kwargs)

        future_id = f"fut_{uuid4().hex[:8]}"
        self._futures[future_id] = future
        return {"future_id": future_id}

    def broadcast_endpoint(
        self,
        actor_mesh_id: str,
        endpoint_name: str,
        encoded_args: str = "",
        encoded_kwargs: str = "",
    ) -> Dict[str, Any]:
        """Broadcast to an endpoint (fire-and-forget)."""
        if actor_mesh_id not in self._actor_meshes:
            raise ValueError(f"Unknown actor_mesh_id: {actor_mesh_id}")

        actor_mesh = self._actor_meshes[actor_mesh_id]
        args = cloudpickle.loads(base64.b64decode(encoded_args)) if encoded_args else []
        kwargs = cloudpickle.loads(base64.b64decode(encoded_kwargs)) if encoded_kwargs else {}

        endpoint = getattr(actor_mesh, endpoint_name)
        endpoint.broadcast(*args, **kwargs)
        return {"status": "ok"}

    def _convert_monarch_result(self, result: Any) -> Any:
        """Convert Monarch objects to plain Python objects for serialization."""
        import collections.abc

        result_module = getattr(type(result), "__module__", "")

        # Check if it's a ValueMesh (has items() and sizes property from MeshTrait)
        if hasattr(result, "items") and hasattr(result, "sizes"):
            try:
                data = {}
                for point, value in result.items():
                    # Point is a Mapping: keys are dim names, values are coordinates.
                    # Use values() to get coordinate ints, not keys() which gives names.
                    if isinstance(point, collections.abc.Mapping):
                        key = tuple(point.values())
                    elif hasattr(point, "_asdict"):
                        key = tuple(point._asdict().values())
                    else:
                        key = (point,)
                    data[key] = value

                # MeshTrait.sizes returns {label: size} dict
                shape = dict(result.sizes)

                return {"_type": "ValueMesh", "data": data, "shape": shape}
            except Exception:
                try:
                    return list(result.values())
                except Exception:
                    pass

        # For other Monarch objects
        if "monarch" in result_module:
            if hasattr(result, "values"):
                try:
                    return list(result.values())
                except Exception:
                    pass

        return result

    def get_future_result(
        self,
        future_id: str,
        timeout: Optional[float] = None,
    ) -> str:
        """Get the result of a future, returned as a base64-encoded cloudpickle string."""
        if future_id not in self._futures:
            raise ValueError(f"Unknown future_id: {future_id}")

        future = self._futures[future_id]
        result = future.get(timeout=timeout)
        converted_result = self._convert_monarch_result(result)
        del self._futures[future_id]

        return base64.b64encode(cloudpickle.dumps(converted_result)).decode()

    def check_future_ready(self, future_id: str) -> Dict[str, Any]:
        """Check if a future is ready without blocking."""
        if future_id not in self._futures:
            raise ValueError(f"Unknown future_id: {future_id}")

        future = self._futures[future_id]
        try:
            result = future.get(timeout=0)
            converted = self._convert_monarch_result(result)
            encoded = base64.b64encode(cloudpickle.dumps(converted)).decode()
            return {"ready": True, "encoded_result": encoded}
        except TimeoutError:
            return {"ready": False}
        except Exception as e:
            return {"ready": True, "error": str(e)}

    def stop_actor_mesh(self, actor_mesh_id: str) -> Dict[str, Any]:
        """Stop an actor mesh."""
        if actor_mesh_id not in self._actor_meshes:
            raise ValueError(f"Unknown actor_mesh_id: {actor_mesh_id}")

        actor_mesh = self._actor_meshes[actor_mesh_id]
        actor_mesh.stop().get()
        del self._actor_meshes[actor_mesh_id]
        return {"status": "stopped"}

    def stop_proc_mesh(self, proc_mesh_id: str) -> Dict[str, Any]:
        """Stop a proc mesh."""
        if proc_mesh_id not in self._proc_meshes:
            raise ValueError(f"Unknown proc_mesh_id: {proc_mesh_id}")

        proc_mesh = self._proc_meshes[proc_mesh_id]
        proc_mesh.stop().get()
        del self._proc_meshes[proc_mesh_id]
        return {"status": "stopped"}

    def shutdown(self) -> Dict[str, Any]:
        """Shutdown the gateway and all resources."""
        for actor_mesh_id in list(self._actor_meshes.keys()):
            try:
                self.stop_actor_mesh(actor_mesh_id)
            except Exception:
                pass

        for proc_mesh_id in list(self._proc_meshes.keys()):
            try:
                self.stop_proc_mesh(proc_mesh_id)
            except Exception:
                pass

        if self._host_mesh:
            try:
                self._host_mesh.shutdown().get()
            except Exception:
                pass

        self._initialized = False
        self._host_mesh = None
        self._proc_meshes.clear()
        self._actor_meshes.clear()
        self._futures.clear()

        return {"status": "shutdown"}

    def get_status(self) -> Dict[str, Any]:
        """Get current gateway status."""
        num_workers = 0
        if self._host_mesh is not None:
            try:
                num_workers = len(self._host_mesh) if hasattr(self._host_mesh, "__len__") else 1
            except Exception:
                num_workers = 1

        return {
            "initialized": self._initialized,
            "num_workers": num_workers,
            "num_proc_meshes": len(self._proc_meshes),
            "num_actor_meshes": len(self._actor_meshes),
            "num_pending_futures": len(self._futures),
        }
