# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import logging
import tempfile
import time
from contextlib import contextmanager, ExitStack
from typing import Any, Callable, Dict, Generator, Literal, Optional

from monarch._src.actor.endpoint import Extent
from monarch._src.actor.host_mesh import create_local_host_mesh
from monarch._src.actor.proc_mesh import ProcMesh
from monarch._src.actor.shape import NDSlice
from monarch.common.client import Client
from monarch.common.device_mesh import DeviceMesh
from monarch.common.invocation import DeviceException, RemoteException
from monarch.mesh_controller import spawn_tensor_engine
from monarch.simulator.mock_controller import MockController


class TestingContext:
    """
    Context manager for testing.
    Creates a local device mesh for a given number of hosts and gpus per host.
    Importantly, it also caches the worker processes so that tests can reuse them
    without having to reinitialize torch/NCCL.

    Example::
        with TestingContext() as c:
            local_mesh = c.local_device_mesh(2, 2)
            with local_mesh.activate():
                x = torch.rand(3, 4)
                local_tensor = fetch_shard(x).result()
    """

    __test__ = False

    def __init__(self):
        self.cleanup = ExitStack()
        self._py_process_cache = {}
        self._proc_mesh_cache: Dict[Any, ProcMesh] = {}

    @contextmanager
    def local_engine_on_proc_mesh(
        self,
        num_hosts,
        gpu_per_host,
    ) -> Generator[DeviceMesh, None, None]:
        key = (num_hosts, gpu_per_host)
        if key not in self._proc_mesh_cache:
            self._proc_mesh_cache[key] = create_local_host_mesh(
                Extent(["hosts"], [num_hosts])
            ).spawn_procs(per_host={"gpus": gpu_per_host})

        dm = spawn_tensor_engine(self._proc_mesh_cache[key])
        dm = dm.rename(hosts="host", gpus="gpu")
        try:
            yield dm
            dm.exit()
        except Exception:
            # abnormal exit, so we just make sure we do not try to communicate in destructors,
            # but we do notn wait for workers to exit since we do not know what state they are in.
            dm.client._shutdown = True
            raise

    @contextmanager
    def local_device_mesh(
        self,
        num_hosts,
        gpu_per_host,
        activate=True,
        backend: Literal["py", "rs", "mesh"] = "py",
        controller_params=None,
    ) -> Generator[DeviceMesh, None, None]:
        start = time.time()
        generator = self.local_engine_on_proc_mesh(num_hosts, gpu_per_host)
        with generator as dm:
            end = time.time()
            logging.info("initialized mesh in {:.2f}s".format(end - start))
            if activate:
                with dm.activate():
                    yield dm
            else:
                yield dm
            start = time.time()
        end = time.time()
        logging.info("shutdown mesh in {:.2f}s".format(end - start))

    def __enter__(self):
        start = time.time()
        self._log_dir = self.cleanup.enter_context(
            tempfile.TemporaryDirectory(prefix="rust_cached_workers.")
        )
        end = time.time()
        logging.info("started process caches in {:.2f}s".format(end - start))
        return self

    def __exit__(self, *args):
        start = time.time()
        self.cleanup.__exit__(*args)
        end = time.time()
        logging.info("shutdown process caches in {:.2f}s".format(end - start))


def mock_mesh(hosts: int, gpus: int):
    ctrl = MockController(hosts * gpus)
    client = Client(ctrl, ctrl.world_size, ctrl.gpu_per_host)
    dm = DeviceMesh(
        client,
        NDSlice(offset=0, sizes=[hosts, gpus], strides=[gpus, 1]),
        ("host", "gpu"),
    )

    def create_exit(
        client: Client,
    ) -> Callable[[Optional[RemoteException | DeviceException | Exception]], None]:
        def exit(
            error: Optional[RemoteException | DeviceException | Exception] = None,
        ) -> None:
            client.shutdown(True, error)

        return exit

    dm.exit = create_exit(client)
    return dm
