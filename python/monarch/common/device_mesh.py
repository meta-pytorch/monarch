# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import logging
import warnings
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from enum import Enum
from logging import Logger
from typing import (
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
    Union,
)

import monarch.common.messages as messages
import torch
from monarch._src.actor.shape import MeshTrait, NDSlice, Shape
from torch.utils._python_dispatch import TorchDispatchMode
from torch.utils._pytree import tree_map
from torch.utils.weak import weakref

from ._tensor_to_table import tensor_to_table
from .context_manager import activate_first_context_manager
from .messages import Dims
from .reference import Referenceable
from .stream import Stream
from .tensor import MeshSliceTensor, Tensor

if TYPE_CHECKING:
    from monarch.common.client import Client

logger: Logger = logging.getLogger(__name__)


class RemoteProcessGroup(Referenceable):
    """
    Client's view of a process group.
    """

    def __init__(self, dims, device_mesh):
        logger.info(f"creating process group for {dims}")
        self.dims = dims
        self.device_mesh = device_mesh
        self.ref = self.device_mesh.client.new_ref()
        self._create_remotely()
        # A set of streams for which we've sent the split-comm message.
        self._split_comm_done = set()

    def _create_remotely(self):
        msg = messages.CreateRemoteProcessGroup(self, self.device_mesh, self.dims)
        self.device_mesh._send(msg)

    def ensure_split_comm_remotely(self, stream):
        """
        If we haven't already, send a message to the worker to split off a
        communicator for this PG on the given stream.
        """

        # Currently, the worker will error if we try to do the split-comm more
        # than once, so check for that here to allow this function to be called
        # lazily.
        if stream in self._split_comm_done:
            return
        self._split_comm_done.add(stream)

        msg = messages.SplitCommForProcessGroup(
            remote_process_group=self,
            stream=stream,
        )
        self.device_mesh.client.send_nocoalesce(
            self.device_mesh.client.all_ranks,
            msg,
        )

    def delete_ref(self, ref: int):
        if not self.device_mesh.client.has_shutdown:
            self.device_mesh.client.handle_deletes(self.device_mesh.processes, [ref])

    def drop(self):
        if self.ref is None:
            return
        self._drop_ref()

    def size(self):
        return self.device_mesh.size(self.dims)

    def _drop_ref(self):
        if self.ref is None:
            return
        self.delete_ref(self.ref)
        self.ref = None

    @property
    def dropped(self):
        return self.ref is None


class ActivateGuard:
    def __init__(self, iter):
        self.iter = iter
        next(iter)

    def __enter__(self):
        return

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            next(self.iter)
        except StopIteration:
            pass


class DeviceMeshStatus(Enum):
    """
    Enum representing the status of a device mesh.
    Attributes:
        LIVE (str): The mesh has enough processes than the world size specified and all of them are healthy.
        UNHEALTHY (str): Either the mesh does not have enough processes or some of the processes are unhealthy.
        AWAITING_CREATION (str): The mesh is still being created by the scheduler.
    """

    LIVE = "Live"
    UNHEALTHY = "Unhealthy"
    AWAITING_CREATION = "Awaiting Creation"


@dataclass
class DeviceMeshInfo:
    """
    Data class representing information about a device mesh.

    Attributes:
        mesh_labels (Dict[str, str]): Maps mesh labels to values.
        devices_labels (List[Dict[str, str]]): MAps  device labels to values.
    """

    mesh_labels: Dict[str, str]
    devices_labels: List[Dict[str, str]]


class DeviceMesh(Referenceable, MeshTrait):
    """A mesh of devices for distributed tensor operations.

    DeviceMesh represents a collection of devices arranged in a
    multidimensional grid for parallel computation. It manages
    communication between devices and enables distributed execution
    of operations across the mesh.

    Args:
        client (Client): The client connection to the mesh infrastructure
        processes (NDSlice): Multi-dimensional slice representing the process layout
        names (Dims): Names for each dimension of the mesh
        mesh_name (str, optional): Name identifier for the mesh. Default: "default"
    """

    def __init__(
        self,
        client: "Client",
        processes: "NDSlice",
        names: Dims,
        mesh_name: str = "default",
    ):
        assert isinstance(processes, NDSlice)
        self.client = client
        assert processes.ndim == len(names)
        self.names = names
        self.mesh_name = mesh_name
        # processes are a list of processes that participate in this device mesh, encoded as an NDSlice
        self.processes = processes
        self.exit = lambda: None
        self.ref = None
        self._active_mesh_context = None
        self._subset_of: Optional[weakref.ReferenceType["DeviceMesh"]] = None

    def define_remotely(self):
        if self.ref is None:
            self.ref = self.client.new_ref()
            msg = messages.CreateDeviceMesh(self, self.names, self.processes)
            self.client.send(self.processes, msg)

    def process_group(self, dims: str | Dims) -> RemoteProcessGroup:
        self.define_remotely()
        if isinstance(dims, str):
            dims = (dims,)
        return RemoteProcessGroup(dims, self)

    def to_tensor(self):
        with no_mesh.activate():
            vals = torch.tensor(list(self.processes), device="cpu", dtype=torch.int)
            return vals.view(self.processes.sizes)

    def to_table(self):
        with no_mesh.activate():
            tensor = self.to_tensor()
            names = list(self.names)
            labels = [list(str(i) for i in range(i)) for i in tensor.shape]
            gpus_per_host = self.client.gpu_per_host

            def format_data(x):
                return f"{x // gpus_per_host}.gpu[{x % gpus_per_host}]"

            return tensor_to_table(
                tensor, format_data=format_data, axis_names=names, axis_labels=labels
            )

    def __repr__(self):
        return f"<DeviceMesh(names({self.names}), processes({list(self.processes)})) at {hex(id(self))}>"

    def delete_ref(self, ref: int):
        if not self.client.has_shutdown:
            self.client.handle_deletes(self.processes, [ref])

    def _send(self, cmd: NamedTuple):
        self.client.flush_deletes()
        self.client.send(self.processes, cmd)

    def stack(self, **kwargs):
        raise NotImplementedError()

    @property
    def _ndslice(self) -> NDSlice:
        return self.processes

    @property
    def _labels(self) -> Tuple[str, ...]:
        return self.names

    def _new_with_shape(self, shape: Shape) -> "DeviceMesh":
        mesh = DeviceMesh(self.client, shape.ndslice, tuple(shape.labels))
        mesh.exit = self.exit
        mesh._subset_of = weakref.ref(self)
        return mesh

    def _is_subset_of(self, other: "DeviceMesh") -> bool:
        p = self
        while p is not None:
            if p is other:
                return True
            p = None if p._subset_of is None else p._subset_of()
        return False

    def __call__(self, **kwargs) -> "DeviceMesh":
        """
        device_mesh(batch=3) or device_mesh(batch=slice(3, None))
        """
        warnings.warn(
            "The use of this method is deprecated. Please use mesh.slice instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.slice(**kwargs)

    def rotate(self, **kwargs: Dict[str, int]):
        raise NotImplementedError()

    def rank(self, dims: Union[str, Sequence[str]]) -> torch.Tensor:
        self.define_remotely()
        if isinstance(dims, str):
            if dims not in self.names:
                raise KeyError(f"{self} does not have dimension {repr(dims)}")
            return _remote(
                _rank,
                propagate=lambda _self, _dims: torch.full((), 0, dtype=torch.long),
            )(self, dims)

        combined_rank: Any = 0
        for dim in dims:
            combined_rank *= self.size(dim)
            combined_rank += self.rank(dim)
        return combined_rank

    @property
    def ranks(self) -> dict[str, torch.Tensor]:
        return {dim: self.rank(dim) for dim in self.names}

    def process_idx(self):
        self.define_remotely()
        return _remote(
            "monarch.worker.worker._process_idx",
            propagate=lambda _self: torch.full((), 0, dtype=torch.long),
        )(self)

    def _process(self, coordinates: Optional[Dict[str, int]]) -> NDSlice:
        if coordinates is None:
            return NDSlice(offset=self.processes.offset, sizes=[1], strides=[1])
        if len(coordinates) > len(self.names):
            extra = set(coordinates.keys()) - set(self.names)
            raise KeyError(f"{list(extra)}")
        for name in self.names:
            if name not in coordinates:
                raise ValueError(
                    f"Missing key '{name}' in shard map. Need all of {self.names}"
                )
        flat = [coordinates[name] for name in self.names]
        return NDSlice(offset=self.processes.nditem(flat), sizes=[1], strides=[1])

    def activate(self) -> AbstractContextManager:
        self._active_mesh_context = _active_mesh(self)
        return self._active_mesh_context

    def deactivate(self):
        if self._active_mesh_context is not None:
            self._active_mesh_context.__exit__(None, None, None)
            self._active_mesh_context = None


_active: Optional[DeviceMesh] = None
_dispatch_enabled = False


def get_active_mesh():
    if _active is None:
        raise ValueError("no device mesh is active")
    return _active


class _ActiveMesh(TorchDispatchMode):
    ignore = ["profiler._record_function_exit._RecordFunction"]
    allowed_local_accessors = ["aten._local_scalar_dense.default"]

    def __torch_dispatch__(self, func, types, args=(), kwargs=None):
        if _active is None:
            return func(*args, **kwargs)
        fnstr = str(func)
        if fnstr in self.ignore:
            return func(*args, **kwargs)
        if fnstr in self.allowed_local_accessors and not isinstance(args[0], Tensor):
            return func(*args, **kwargs)
        return _remote(func, propagate=func)(*args, **kwargs)


def _rank(mesh, dim):
    return torch.full((), mesh.dims[dim].rank, dtype=torch.long)


@contextmanager
def _dispatch():
    global _dispatch_enabled
    if _dispatch_enabled:
        yield
    else:
        _dispatch_enabled = True
        try:
            with _ActiveMesh():
                yield
        finally:
            _dispatch_enabled = False


_on_change: List[Callable] = []


@activate_first_context_manager
def _active_mesh(mesh: Optional[DeviceMesh]):
    global _active
    for on_change in _on_change:
        on_change(_active, mesh)
    _active, old = mesh, _active
    try:
        with _dispatch():
            yield
    finally:
        for on_change in _on_change:
            on_change(_active, old)
        _active = old


class _NoMesh:
    def activate(self):
        return _active_mesh(None)


no_mesh = _NoMesh()


def _remote(*args, **kwargs):
    # device_mesh <-> tensor <-> remote are mututally recursive
    # we break the dependency to allow for separate files by
    # having device_mesh and tensor locally import the `remote`
    # entrypoint
    from monarch.common.remote import remote

    return remote(*args, **kwargs)


def to_mesh(
    tensors: Any,
    mesh: "DeviceMesh",
    stream: Optional[Stream] = None,
) -> Any:
    """
    Move all tensors in tensors to the given mesh.
    """

    def _to_mesh(tensor: Union["Tensor", "MeshSliceTensor"]) -> "Tensor":
        return tensor.to_mesh(mesh, stream)

    return tree_map(_to_mesh, tensors)


def slice_mesh(
    tensors: Any,
    **kwargs: Union[int, slice],
) -> Any:
    """
    Performs the slice_mesh operation for each tensor in tensors.
    """

    def _slice_mesh(tensor: "Tensor") -> "MeshSliceTensor":
        return tensor.slice_mesh(**kwargs)

    return tree_map(_slice_mesh, tensors)
