# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
import logging
from dataclasses import dataclass
from typing import Dict, final, Literal, Optional

from monarch._rust_bindings.monarch_hyperactor.alloc import (  # @manual=//monarch/monarch_extension:monarch_extension
    Alloc,
    AllocConstraints,
    AllocSpec,
    LocalAllocatorBase,
    ProcessAllocatorBase,
    RemoteAllocatorBase,
    SimAllocatorBase,
)
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._src.actor.future import Future


ALLOC_LABEL_PROC_MESH_NAME = "procmesh.monarch.meta.com/name"

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class AllocHandle:
    _hy_alloc: "Shared[Alloc]"
    _extent: Dict[str, int]
    _stream_logs: bool
    _allocator: "AllocateMixin"
    _constraints: AllocConstraints

    def reshape(self, extent: Dict[str, int]) -> "AllocHandle":
        async def task() -> Alloc:
            alloc = await self._hy_alloc
            return alloc.reshape(extent)

        return AllocHandle(
            PythonTask.from_coroutine(task()).spawn(),
            extent,
            self._stream_logs,
            self._allocator,
            self._constraints,
        )

    @property
    def initialized(self) -> Future[Literal[True]]:
        """
        Future completes with 'True' when the alloc has initialized.
        Because alloc are remote objects, there is no guarentee that the alloc is
        still usable after this completes, only that at some point in the past it was usable.
        """

        async def task() -> Literal[True]:
            await self._hy_alloc
            return True

        return Future(coro=task())

    @property
    def stream_logs(self) -> bool:
        """
        Whether to stream stdout/stderr logs from the allocated processes back to the client.
        The default behavior is determined by the underlying allocator.
        """
        return self._stream_logs


class AllocateMixin(abc.ABC):
    @abc.abstractmethod
    def allocate_nonblocking(self, spec: AllocSpec) -> "PythonTask[Alloc]": ...

    def allocate(self, spec: AllocSpec) -> "AllocHandle":
        """
        Allocate a process according to the provided spec.

        Arguments:
        - `spec`: The spec to allocate according to.

        Returns:
        - A future that will be fulfilled when the requested allocation is fulfilled.
        """
        return AllocHandle(
            self.allocate_nonblocking(spec).spawn(),
            spec.extent,
            self._stream_logs(),
            self,
            spec.constraints,
        )

    @abc.abstractmethod
    def _stream_logs(self) -> bool:
        """
        Whether to stream stdout/stderr logs from the allocated processes back to the client.
        A common pattern is if the processes are allocated on the same host as the client,
        then it is not necessary to stream logs back. But if the processes are remotely allocated,
        it is recommended to stream logs back. It is up to each allocator to decide the default behavior.

        Returns:
        - A boolean indicating whether to stream logs back to the client.
        """

        ...


class ProcessAllocator(ProcessAllocatorBase, AllocateMixin):
    """
    An allocator that allocates by spawning local processes.
    """

    def _stream_logs(self) -> bool:
        return False


@final
class LocalAllocator(LocalAllocatorBase, AllocateMixin):
    """
    An allocator that allocates by spawning actors into the current process.
    """

    def _stream_logs(self) -> bool:
        return False


@final
class SimAllocator(SimAllocatorBase, AllocateMixin):
    """
    An allocator that allocates by spawning actors into the current process using simulated channels for transport
    """

    def _stream_logs(self) -> bool:
        return False


class RemoteAllocInitializer(abc.ABC):
    """Subclass-able Python interface for `hyperactor_mesh::alloc::remoteprocess:RemoteProcessAllocInitializer`.

    NOTE: changes to method signatures of this class must be made to the call-site at
    `PyRemoteProcessAllocInitializer.py_initialize_alloc()` in `monarch/monarch_hyperactor/src/alloc.rs`
    """

    @abc.abstractmethod
    async def initialize_alloc(self, match_labels: dict[str, str]) -> list[str]:
        """
        Return the addresses of the servers that should be used to allocate processes
        for the proc mesh. The addresses should be running hyperactor's RemoteProcessAllocator.

        Each address is of the form `{transport}!{addr}(:{port})`.
        This is the string form of `hyperactor::channel::ChannelAddr` (Rust).
        For example, `tcp!127.0.0.1:1234`.

        NOTE: Currently, all the addresses must have the same transport type and port
        NOTE: Although this method is currently called once at the initialization of the Allocator,
            in the future this method can be called multiple times and should return the current set of
            addresses that are eligible to handle allocation requests.

        Arguments:
        - `match_labels`: The match labels specified in `AllocSpec.AllocConstraints`. Initializer implementations
            can read specific labels for matching a set of hosts that will service `allocate()` requests.

        """
        ...


class StaticRemoteAllocInitializer(RemoteAllocInitializer):
    """
    Returns the static list of server addresses that this initializer
    was constructed with on each `initialize_alloc()` call.
    """

    def __init__(self, *addrs: str) -> None:
        super().__init__()
        self.addrs: list[str] = list(addrs)

    async def initialize_alloc(self, match_labels: dict[str, str]) -> list[str]:
        _ = match_labels  # Suppress unused variable warning
        return list(self.addrs)


class TorchXRemoteAllocInitializer(RemoteAllocInitializer):
    """
    For monarch runtimes running as a job on a supported scheduler.
    Such runtimes are typically launched using the monarch CLI (e.g `monarch create --scheduler slurm ...`).

    Returns the server addresses of a specific monarch runtime by using TorchX's status API
    to get the hostnames of the nodes.
    """

    def __init__(
        self,
        server_handle: str,
        /,
        transport: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        """
        NOTE: If `transport` and `port` specified, they are used over the `transport` and `port`
          information that is tagged as metadata on the server's job. This is useful in two specific
          situations:
            1) The job was NOT created wit monarch CLI (hence no metadata tags exist)
            2) The scheduler does not support job metadata tagging

        Arguments:
        - `server_handle`: points to a monarch runtime. Of the form `{scheduler}://{namespace}/{job_id}`.
             the `{namespace}` can be empty if not configured (e.g. `slurm:///1234` - notice the triple slashes).
        - `transport`: the channel transport that should be used to connect to the remote process allocator address
        - `port`: the port that the remote process allocator is running on

        """
        self.server_handle = server_handle
        self.transport = transport
        self.port = port

    async def initialize_alloc(self, match_labels: dict[str, str]) -> list[str]:
        # lazy import since torchx-fb is not included in `fbcode//monarch/python/monarch:monarch.whl`
        # nor any of the base conda environments
        from monarch.tools.commands import server_ready

        mesh_name = match_labels.get(ALLOC_LABEL_PROC_MESH_NAME)

        server = await server_ready(self.server_handle)

        # job does not exist or it is in a terminal state (SUCCEEDED, FAILED, CANCELLED)
        if not (server and server.is_running):
            raise ValueError(
                f"{self.server_handle} does not exist or is in a terminal state"
            )

        if not mesh_name:
            logger.info(
                "no match label `%s` specified in alloc constraints",
                ALLOC_LABEL_PROC_MESH_NAME,
            )

            num_meshes = len(server.meshes)

            if num_meshes == 1:
                logger.info(
                    "found a single proc mesh `%s` in %s, will allocate on it",
                    server.meshes[0].name,
                    self.server_handle,
                )
            else:
                raise RuntimeError(
                    f"{num_meshes} proc meshes in {self.server_handle},"
                    f" please specify the mesh name as a match label `{ALLOC_LABEL_PROC_MESH_NAME}`"
                    f" in allocation constraints of the alloc spec"
                )
            mesh = server.meshes[0]
        else:
            mesh = server.get_mesh_spec(mesh_name)

        server_addrs = mesh.server_addrs(self.transport, self.port)

        logger.info(
            "initializing alloc on remote allocator addresses: %s", server_addrs
        )
        return server_addrs


@final
class RemoteAllocator(RemoteAllocatorBase, AllocateMixin):
    """
    An allocator that allocates by spawning actors on a remote host.
    The remote host must be running hyperactor's remote-process-allocator.
    """

    def _stream_logs(self) -> bool:
        return True
