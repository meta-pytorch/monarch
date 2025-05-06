# pyre-strict

import importlib.resources
import logging
import os
import random
import string
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from monarch._monarch.client import ClientActor

from monarch._monarch.hyperactor import ActorId, init_proc, Proc
from monarch._monarch.simulator_client import SimulatorClient
from monarch.common.client import Client
from monarch.common.constants import (
    SIM_MESH_CLIENT_SUPERVISION_UPDATE_INTERVAL,
    SIM_MESH_CLIENT_TIMEOUT,
)
from monarch.common.device_mesh import DeviceMesh
from monarch.common.fake import fake_call
from monarch.common.invocation import DeviceException, RemoteException
from monarch.common.shape import NDSlice
from monarch.controller.rust_backend.controller import RustController
from monarch.rust_backend_mesh import MeshWorld


logger: logging.Logger = logging.getLogger(__name__)


def sim_mesh(
    n_meshes: int, hosts: int, gpus_per_host: int, proxy_addr: Optional[str] = None
) -> List[DeviceMesh]:
    """
    Creates a single simulated device mesh with the given number of per host.

    Args:
        n_meshes            : number of device meshes to create.
        hosts               : number of hosts, primarily used for simulating multiple machines locally.
                              Default: 1
        gpus_per_host       : number of gpus per host.
                              Default: the number of GPUs this machine has.
    """
    mesh_world_state: Dict[MeshWorld, Optional[DeviceMesh]] = {}
    # TODO: pass hosts and gpus_per_host to Bootstrap to simulate the case of multiple workers per mesh.
    bootstrap: Bootstrap = Bootstrap(n_meshes, mesh_world_state, proxy_addr=proxy_addr)

    def create_exit(
        dm: DeviceMesh, bootstrap: Bootstrap
    ) -> Callable[[Optional[RemoteException | DeviceException | Exception]], None]:
        def exit(
            error: Optional[RemoteException | DeviceException | Exception] = None,
        ) -> None:
            # We only support one single client proc.
            if not bootstrap.has_shutdown:
                dm.client.shutdown(True, error)
                bootstrap.has_shutdown = True

        # We do not need to shutdown bootstrap and clean up the processes
        # as they will be cleaned up with the parent.
        return exit

    client_proc_id = "client[0]"
    client_proc: Proc = init_proc(
        proc_id=client_proc_id,
        bootstrap_addr=bootstrap.client_bootstrap_addr,
        timeout=SIM_MESH_CLIENT_TIMEOUT,  # unused
        supervision_update_interval=SIM_MESH_CLIENT_SUPERVISION_UPDATE_INTERVAL,
    )
    root_client_actor: ClientActor = ClientActor(
        proc=client_proc, actor_name="root_client"
    )

    dms = []
    for i in range(n_meshes):
        controller_id = ActorId(
            world_name=f"mesh_{i}_controller", rank=0, actor_name="root"
        )
        # Create a new device mesh
        backend_ctrl = RustController(
            proc=client_proc,
            client_actor=ClientActor.new_with_parent(
                client_proc, root_client_actor.actor_id
            ),
            controller_id=controller_id,
            worker_world_name=f"mesh_{i}_worker",
        )
        client = Client(backend_ctrl, hosts * gpus_per_host, gpus_per_host)
        dm = DeviceMesh(
            client,
            NDSlice(offset=0, sizes=[hosts, gpus_per_host], strides=[gpus_per_host, 1]),
            ("host", "gpu"),
            f"mesh_{i}_worker",
        )
        dm.exit = create_exit(dm, bootstrap)
        dms.append(dm)

    return dms


def _random_id(length: int = 14) -> str:
    """
    A simple random id generator.
    """
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))


class Bootstrap:
    def __init__(
        self,
        num_meshes: int,
        mesh_world_state: Dict[MeshWorld, Optional[DeviceMesh]],
        proxy_addr: Optional[str] = None,
    ) -> None:
        """
        Bootstraps a SimMesh.
        Args:
            num_meshes: int - number of meshes to create.
            proxy_addr: Option[str] - the proxy address of the simulation process
            mesh_world_state: a state of the meshes. Keys are the MeshWorld and values are boolean indicating if this mesh is active.
        """
        # do a fake call to instantiate ThreadPoolExecutor so we don't block GIL later
        fake_call(lambda: 0)
        with (
            importlib.resources.path("monarch", "monarch_simulator") as controller_main,
        ):
            if not controller_main.exists():
                raise ImportError(
                    "simulator main not found in monarch.monarch_simulator"
                )
            self.controller_main: Path = controller_main

        env = os.environ.copy()
        env["HYPERACTOR_MANAGED_SUBPROCESS"] = "1"
        self.env: dict[str, str] = env

        self.has_shutdown: bool = False
        self._mesh_world_state: Dict[MeshWorld, Optional[DeviceMesh]] = mesh_world_state

        proxy_addr = proxy_addr or f"unix!@{_random_id()}-proxy"
        self.bootstrap_addr: str = (
            f"sim!unix!@system,{proxy_addr},unix!@system,{proxy_addr}"
        )
        client_proxy_addr = f"unix!@{_random_id()}-proxy"
        # client lives on a different process so it uses a different proxy as system.
        # the src (unix!@client) is just a placeholder and will get changed by the client.
        self.client_bootstrap_addr: str = (
            f"sim!unix!@client,{client_proxy_addr},unix!@system,{proxy_addr}"
        )
        logging_dir = tempfile.mkdtemp(prefix="sim_mesh_")
        os.makedirs(logging_dir, exist_ok=True)
        print(f"\n*** Simulator backend logging to {logging_dir} ***\n")
        stdout_file = open(os.path.join(logging_dir, "log.stdout"), "w")
        stderr_file = open(os.path.join(logging_dir, "log.stderr"), "w")
        process = subprocess.Popen(
            [
                self.controller_main,
                "--system-addr",
                self.bootstrap_addr,
            ],
            stdout=stdout_file,
            stderr=stderr_file,
            env=self.env,
        )
        running = _validate_proccesses_end([process])
        if len(running) != 1:
            raise RuntimeError(
                f"System process did not end properly. Running processes: {running}"
            )

        self._simulator_client = SimulatorClient(proxy_addr)
        for i in range(num_meshes):
            mesh_name: str = f"mesh_{i}"
            controller_world: str = f"{mesh_name}_controller"
            worker_world: str = f"{mesh_name}_worker"
            controller_id: ActorId = ActorId(
                world_name=controller_world,
                rank=0,
                actor_name="root",
            )
            mesh_world = (worker_world, controller_id)
            self._mesh_world_state[mesh_world] = None
            self.spawn_mesh(mesh_world)
        # sleep for 10 sec for the worker and controller tasks to be spawned and ready.
        time.sleep(10)

    def get_mesh_worlds(self) -> List[MeshWorld]:
        return []

    def kill_mesh(self, mesh_world: MeshWorld) -> None:
        pass

    def spawn_mesh(self, mesh_world: MeshWorld) -> None:
        worker_world, controller_id = mesh_world
        controller_world = controller_id.world_name
        self._simulator_client.spawn_mesh(
            self.bootstrap_addr, f"{controller_world}[0].root", worker_world
        )


def _validate_proccesses_end(
    processes: Iterable[subprocess.Popen[bytes]],
    timeout_in_sec: int = 1,
    raise_on_abnormal_exit: bool = True,
) -> list[int]:
    """
    Check if processes have ended properly. Raise errors immediately
    if any process has ended with a non-zero return code.
    Return a list of process indices that have not ended yet.
    """
    running = []
    start_time = time.time()
    for i, process in enumerate(processes):
        try:
            current_time = time.time()
            elapsed_time = current_time - start_time
            # The processes are running in parallel. No need to wait for
            # `timeout_in_sec` for each process. Only count the remaining ones.
            wait_in_sec = max(0, timeout_in_sec - elapsed_time)
            return_code = process.wait(timeout=wait_in_sec)
            if return_code != 0:
                error_message: str = (
                    f"Process[{i}] {process.pid} exited with "
                    f"return code {return_code}. Command:\n "
                    f"{process.args!r}"
                )
                if raise_on_abnormal_exit:
                    raise RuntimeError(error_message)
                else:
                    logger.error(error_message)
        except subprocess.TimeoutExpired:
            running.append(i)

    return running


class PoolDeviceMeshProvider:
    def __init__(
        self,
        hosts_per_mesh: int,
        gpus_per_host: int,
        client_proc: Proc,
        mesh_world_state: Dict[MeshWorld, Optional[DeviceMesh]],
    ) -> None:
        self._hosts_per_mesh = hosts_per_mesh
        self._gpus_per_host = gpus_per_host
        self._client_proc = client_proc
        self._root_client_actor: ClientActor = ClientActor(
            proc=client_proc, actor_name="root_client"
        )
        self._mesh_world_state = mesh_world_state

    def new_mesh(self, timeout_in_sec: Optional[int] = None) -> DeviceMesh:
        def _create_exit(
            dm: DeviceMesh,
        ) -> Callable[[Optional[RemoteException | DeviceException | Exception]], None]:
            def _exit(
                error: Optional[RemoteException | DeviceException | Exception] = None,
            ) -> None:
                dm.client.shutdown(True, error)

            return _exit

            # We do not need to shutdown bootstrap and clean up the processes
            # as they will be cleaned up with the parent.
            return exit

        mesh_world_to_create = next(
            (
                mesh_world
                for mesh_world, is_created in self._mesh_world_state.items()
                if not is_created
            ),
            None,
        )
        assert mesh_world_to_create is not None, "No mesh world to create"

        worker_world, controller_id = mesh_world_to_create
        # Create a new device mesh
        backend_ctrl = RustController(
            proc=self._client_proc,
            client_actor=ClientActor.new_with_parent(
                self._client_proc, self._root_client_actor.actor_id
            ),
            controller_id=controller_id,
            worker_world_name=worker_world,
        )
        client = Client(
            backend_ctrl,
            self._hosts_per_mesh * self._gpus_per_host,
            self._gpus_per_host,
        )
        dm = DeviceMesh(
            client,
            NDSlice(
                offset=0,
                sizes=[self._hosts_per_mesh, self._gpus_per_host],
                strides=[self._gpus_per_host, 1],
            ),
            ("host", "gpu"),
            worker_world,
        )
        dm.exit = _create_exit(dm)
        self._mesh_world_state[mesh_world_to_create] = dm

        return dm


def sim_mesh_provider(
    num_meshes: int, hosts_per_mesh: int, gpus_per_host: int
) -> Tuple[PoolDeviceMeshProvider, Bootstrap]:
    mesh_world_state = {}
    bootstrap = Bootstrap(num_meshes, mesh_world_state)

    client_proc_id = "client[0]"
    client_proc: Proc = init_proc(
        proc_id=client_proc_id,
        bootstrap_addr=bootstrap.client_bootstrap_addr,
        timeout=SIM_MESH_CLIENT_TIMEOUT,  # unused
        supervision_update_interval=SIM_MESH_CLIENT_SUPERVISION_UPDATE_INTERVAL,
    )
    dm_provider = PoolDeviceMeshProvider(
        hosts_per_mesh, gpus_per_host, client_proc, mesh_world_state
    )
    return (dm_provider, bootstrap)
