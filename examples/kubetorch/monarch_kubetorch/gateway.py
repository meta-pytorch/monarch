"""
Monarch Gateway - Server-side component for external Monarch access.

This class is deployed to K8s pods via kt.cls and handles Monarch operations
proxied from external clients over WebSocket. It maintains references to
HostMesh, ProcMesh, ActorMesh objects and executes operations on behalf of
the client.
"""

import os
import pickle
import socket
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional
from uuid import uuid4

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
    3. Attaches to worker pods discovered via headless service DNS
    4. Creates and manages HostMesh, ProcMesh, ActorMesh objects
    5. Executes actor method calls on behalf of external clients

    The external client (KubernetesJob) communicates with this gateway over
    WebSocket, sending operation requests and receiving results.
    """

    def __init__(self, monarch_port: int = DEFAULT_MONARCH_PORT):
        self._initialized = False
        self._host_mesh = None
        self._proc_meshes: Dict[str, Any] = {}
        self._actor_meshes: Dict[str, Any] = {}
        self._futures: Dict[str, Any] = {}
        self._worker_process: Optional[subprocess.Popen] = None
        self._monarch_port = monarch_port
        self._worker_pythonpath: set = set()

        # Start the Monarch worker if not already running
        self._ensure_worker_running()

    def _ensure_worker_running(self, extra_paths: Optional[List[str]] = None):
        """Start the Monarch worker process if not already running."""
        pod_ip = _get_pod_ip()
        extra_paths = extra_paths or []

        # Check if we need to restart the worker for new paths
        new_paths = set(extra_paths) - self._worker_pythonpath
        if new_paths and self._worker_process is not None:
            self._stop_worker()

        if _is_port_in_use(self._monarch_port, pod_ip):
            if self._worker_process is not None:
                return
            else:
                time.sleep(3)
                if _is_port_in_use(self._monarch_port, pod_ip):
                    return

        print(f"Starting Monarch worker on {pod_ip}:{self._monarch_port}")

        # Build PYTHONPATH including cwd and all subdirectories
        cwd = os.getcwd()
        paths_to_add = {cwd}
        try:
            for entry in os.listdir(cwd):
                entry_path = os.path.join(cwd, entry)
                if os.path.isdir(entry_path) and not entry.startswith("."):
                    paths_to_add.add(entry_path)
        except Exception:
            pass

        paths_to_add.update(extra_paths)
        self._worker_pythonpath.update(paths_to_add)

        worker_script = f"""
import os
import sys

os.environ.setdefault("HYPERACTOR_HOST_SPAWN_READY_TIMEOUT", "300s")
os.environ.setdefault("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT", "300s")
os.environ.setdefault("HYPERACTOR_MESH_PROC_SPAWN_MAX_IDLE", "300s")

from monarch.actor import run_worker_loop_forever

address = "tcp://{pod_ip}:{self._monarch_port}"
print(f"Monarch worker starting at {{address}}")
run_worker_loop_forever(address=address, ca="trust_all_connections")
"""

        # Build environment with PYTHONPATH
        worker_env = os.environ.copy()
        current_pythonpath = worker_env.get("PYTHONPATH", "")
        pythonpath_parts = [p for p in current_pythonpath.split(":") if p]
        for path in self._worker_pythonpath:
            if path not in pythonpath_parts:
                pythonpath_parts.insert(0, path)
        worker_env["PYTHONPATH"] = ":".join(pythonpath_parts)

        try:
            self._worker_process = subprocess.Popen(
                [sys.executable, "-c", worker_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
                env=worker_env,
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

    def _discover_worker_ips(self, headless_service_dns: str) -> List[str]:
        """Discover all worker pod IPs via headless service DNS."""
        addr_info = socket.getaddrinfo(headless_service_dns, None, socket.AF_INET)
        pod_ips = sorted(list(set([addr[4][0] for addr in addr_info])))
        return pod_ips

    def initialize(
        self,
        headless_service_dns: Optional[str] = None,
        worker_ips: Optional[List[str]] = None,
        monarch_port: int = 26600,
    ) -> Dict[str, Any]:
        """Initialize the gateway by attaching to worker pods."""
        if self._initialized:
            return {
                "status": "already_initialized",
                "host_mesh_id": "hm_default",
                "num_workers": len(self._host_mesh) if self._host_mesh else 0,
            }

        # Discover workers
        if worker_ips:
            pod_ips = worker_ips
        else:
            if not headless_service_dns:
                service_name = os.environ.get("KT_SERVICE_NAME", "")
                namespace = os.environ.get("POD_NAMESPACE", "default")
                headless_service_dns = f"{service_name}-headless.{namespace}.svc.cluster.local"

            pod_ips = self._discover_worker_ips(headless_service_dns)

        if not pod_ips:
            raise RuntimeError("No worker IPs discovered")

        worker_addresses = [f"tcp://{ip}:{monarch_port}" for ip in pod_ips]
        print(f"Attaching to {len(worker_addresses)} Monarch workers: {pod_ips}")

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
            "worker_ips": pod_ips,
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
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
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

        args = args or []
        kwargs = kwargs or {}

        actor_mesh = proc_mesh.spawn(name, actor_class, *args, **kwargs)
        actor_mesh_id = f"am_{uuid4().hex[:8]}"
        self._actor_meshes[actor_mesh_id] = actor_mesh

        shape = dict(actor_mesh.sizes) if hasattr(actor_mesh, "sizes") else {}
        return {"actor_mesh_id": actor_mesh_id, "shape": shape}

    def call_endpoint(
        self,
        actor_mesh_id: str,
        endpoint_name: str,
        args_bytes: bytes,
        kwargs_bytes: bytes,
        selection: str = "all",
    ) -> Dict[str, Any]:
        """Call an endpoint on an ActorMesh."""
        if actor_mesh_id not in self._actor_meshes:
            raise ValueError(f"Unknown actor_mesh_id: {actor_mesh_id}")

        actor_mesh = self._actor_meshes[actor_mesh_id]
        args = pickle.loads(args_bytes) if args_bytes else []
        kwargs = pickle.loads(kwargs_bytes) if kwargs_bytes else {}

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
        args_bytes: bytes,
        kwargs_bytes: bytes,
    ) -> Dict[str, Any]:
        """Broadcast to an endpoint (fire-and-forget)."""
        if actor_mesh_id not in self._actor_meshes:
            raise ValueError(f"Unknown actor_mesh_id: {actor_mesh_id}")

        actor_mesh = self._actor_meshes[actor_mesh_id]
        args = pickle.loads(args_bytes) if args_bytes else []
        kwargs = pickle.loads(kwargs_bytes) if kwargs_bytes else {}

        endpoint = getattr(actor_mesh, endpoint_name)
        endpoint.broadcast(*args, **kwargs)
        return {"status": "ok"}

    def _convert_monarch_result(self, result: Any) -> Any:
        """Convert Monarch objects to plain Python objects for client unpickling."""
        result_module = getattr(type(result), "__module__", "")

        # Check if it's a ValueMesh
        if hasattr(result, "items") and hasattr(result, "_labels"):
            try:
                data = {}
                for point, value in result.items():
                    if hasattr(point, "_asdict"):
                        key = tuple(point._asdict().values())
                    elif hasattr(point, "__iter__"):
                        key = tuple(point)
                    else:
                        key = (point,)
                    data[key] = value

                shape = {}
                if hasattr(result, "_labels") and hasattr(result, "_shape"):
                    for label, size in zip(result._labels, result._shape):
                        shape[label] = size

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
    ) -> bytes:
        """Get the result of a future."""
        if future_id not in self._futures:
            raise ValueError(f"Unknown future_id: {future_id}")

        future = self._futures[future_id]
        result = future.get(timeout=timeout)
        converted_result = self._convert_monarch_result(result)
        del self._futures[future_id]

        return pickle.dumps(converted_result)

    def check_future_ready(self, future_id: str) -> Dict[str, Any]:
        """Check if a future is ready without blocking."""
        if future_id not in self._futures:
            raise ValueError(f"Unknown future_id: {future_id}")

        future = self._futures[future_id]
        try:
            result = future.get(timeout=0)
            return {"ready": True, "result": pickle.dumps(result)}
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
