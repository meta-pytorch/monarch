# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import ctypes
import functools
import logging
import warnings
from typing import cast, Optional

import torch
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared

try:
    from monarch._rust_bindings.rdma import _RdmaBuffer, _RdmaManager
except ImportError as e:
    logging.error("RDMA is not available: {}".format(e))
    raise e
from typing import Dict

from monarch._src.actor.actor_mesh import Actor, context
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.future import Future
from monarch._src.actor.proc_mesh import get_or_spawn_controller, ProcMesh
from pyre_extensions import none_throws


# RDMARead/WriteTransferWarnings are warnings that are only printed once per process.
# Remove these once GPU support is added.
class RDMAReadTransferWarning(Warning):
    pass


class RDMAWriteTransferWarning(Warning):
    pass


warnings.simplefilter("once", RDMAReadTransferWarning)
warnings.simplefilter("once", RDMAWriteTransferWarning)


def is_rdma_available():
    return _RdmaBuffer.rdma_supported()


# Cached so that we don't have to call out to the root client every time,
# which may be on a different host.
@functools.cache
def _ensure_init_rdma_manager() -> Shared[None]:
    async def task() -> None:
        await (
            await get_or_spawn_controller("rdma_controller", RdmaController)
        ).init_rdma_on_mesh.call_one(
            # FIXME(slurye): Fix this once controller API is working properly
            # for v1.
            cast(ProcMesh, none_throws(context().actor_instance.proc_mesh))
        )

    return PythonTask.from_coroutine(task()).spawn()


def _get_error(buf) -> ValueError:
    return ValueError(
        "RDMABuffer only supports 1d contiguous torch.Tensor or 1d c-contiguous memoryview. Got: {}".format(
            buf
        )
    )


def _assert_1d_contiguous(buf: torch.Tensor | memoryview) -> None:
    if isinstance(buf, torch.Tensor):
        if buf.dim() != 1 or not buf.is_contiguous():
            raise _get_error(buf)
    elif isinstance(buf, memoryview):
        if buf.ndim != 1 or not buf.c_contiguous:
            raise _get_error(buf)
    else:
        raise _get_error(buf)


def _get_memoryview_addr_and_size(buf: memoryview) -> tuple[int, int]:
    addr = ctypes.addressof(ctypes.c_char.from_buffer(buf))
    size = buf.nbytes
    return addr, size


def _get_tensor_addr_and_size(tensor: torch.Tensor) -> tuple[int, int]:
    data_ptr: int = tensor.untyped_storage().data_ptr()
    # Calculate the actual starting address of the tensor data
    # storage_offset() can return either int or torch.SymInt in newer PyTorch versions
    try:
        storage_offset = int(tensor.storage_offset())
    except Exception as e:
        raise RuntimeError("Failed to convert tensor.storage_offset() to int.") from e
    offset: int = storage_offset * tensor.element_size()
    addr: int = data_ptr + offset
    size: int = tensor.element_size() * tensor.numel()
    return addr, size


def _get_addr_and_size(buf: torch.Tensor | memoryview) -> tuple[int, int]:
    _assert_1d_contiguous(buf)
    if isinstance(buf, memoryview):
        return _get_memoryview_addr_and_size(buf)
    elif isinstance(buf, torch.Tensor):
        return _get_tensor_addr_and_size(buf)
    # This shouldn't happen unless there is a bug, handle the type in caller.
    raise RuntimeError(
        "Trying to get address and size of unsupported type. Expected memoryview or torch.Tensor. Got: {}".format(
            type(buf)
        )
    )


class RdmaController(Actor):
    def __init__(self) -> None:
        self._manager_futures: Dict[ProcMesh, Future[_RdmaManager]] = {}

    @endpoint
    async def init_rdma_on_mesh(self, proc_mesh: ProcMesh) -> None:
        # Note: RdmaController acts as coordinator and can run on any node
        # The RDMA support check should happen on the target proc_mesh nodes, not on RdmaController's node

        if proc_mesh not in self._manager_futures:

            async def create_manager() -> _RdmaManager:
                proc_mesh_result = await Future(coro=proc_mesh._proc_mesh.task())
                return none_throws(
                    await _RdmaManager.create_rdma_manager_nonblocking(proc_mesh_result)
                )

            self._manager_futures[proc_mesh] = Future(coro=create_manager())

        await self._manager_futures[proc_mesh]


@functools.cache
def _check_cuda_expandable_segments_enabled() -> bool:
    """
    Check if PyTorch CUDA caching allocator is using expandable segments.

    Uses the Rust extension which calls the C++ implementation from rdmaxcel-sys
    that directly accesses the PyTorch C10 CUDA allocator configuration.

    Returns:
        bool: True if expandable segments are enabled, False otherwise
    """
    try:
        # Use the new Rust utility function that calls the C++ pt_cuda_allocator_compatibility()
        pt_cuda_compat = _RdmaBuffer.pt_cuda_allocator_compatibility()

        if not pt_cuda_compat:
            warnings.warn(
                "CUDA caching allocator is not using expandable segments.\n"
                "This is required to maximize RDMA performance with CUDA tensors.\n\n"
                "To fix this, set the environment variable BEFORE importing PyTorch:\n"
                "1. In shell:\n"
                '   export PYTORCH_CUDA_ALLOC_CONF="expandable_segments:True"\n'
                "2. Or in Python script (BEFORE any PyTorch imports):\n"
                "   import os\n"
                '   os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"\n'
                "   import torch  # Must come after setting the env var\n\n",
                UserWarning,
                stacklevel=2,
            )
            return False
        return True

    except Exception as e:
        warnings.warn(
            "Unable to verify CUDA allocator configuration.\n"
            "Please ensure expandable segments are enabled for best RDMA performance with CUDA tensors:\n"
            '   export PYTORCH_CUDA_ALLOC_CONF="expandable_segments:True"\n'
            "Set this environment variable before importing PyTorch.",
            UserWarning,
            stacklevel=2,
        )
        return False


class RDMABuffer:
    def __init__(
        self,
        data: torch.Tensor | memoryview,
    ) -> None:
        """
        RDMABuffer supports 1d contiguous tensors (including tensor views/slices) or 1d c-contiguous memoryviews.

        Args:
            data: torch.Tensor or memoryview to create the buffer from. Must be 1d and contiguous.
                  If provided, addr and size must not be specified.

        Raises:
            ValueError: If data is not 1d contiguous, if size is 0, or if data is a GPU tensor.
            RuntimeError: If RDMA is not available on this platform.

        Note:
            Currently only CPU tensors are supported. GPU tensor support will be added in the future.

        TODO: Create TensorBuffer, which will be main user API supporting non-contiguous tensors
        """
        if isinstance(data, torch.Tensor) and data.device.type == "cuda":
            # Check if CUDA caching allocator is using expandable segments
            _check_cuda_expandable_segments_enabled()

        assert (
            is_rdma_available()
        ), "Tried to create an RDMABuffer, but RDMA is not available on this platform."

        # We need to ensure that _RdmaManager is initialized at this point, because under the hood
        # _RdmaBuffer.create_rdma_buffer_blocking relies on this being the case.
        _ensure_init_rdma_manager().block_on()

        addr, size = _get_addr_and_size(data)

        try:
            if size == 0:
                raise ValueError("Cannot create RDMABuffer with size 0.")
            ctx = context()
            self._buffer: _RdmaBuffer = _RdmaBuffer.create_rdma_buffer_blocking(
                addr=addr,
                size=size,
                proc_id=ctx.actor_instance.proc_id,
                client=ctx.actor_instance,
            )
        # TODO - specific exception
        except Exception as e:
            logging.error("Failed to create buffer %s", e)
            raise e

    def size(self) -> int:
        return self._buffer.size()

    def read_into(
        self,
        dst: torch.Tensor | memoryview,
        *,
        timeout: int = 3,
    ) -> Future[Optional[int]]:
        """
        Read data from the RDMABuffer into a destination tensor.

        The destination tensor must be contiguous (including tensor views/slices).
        Args:
            dst: Destination tensor or memoryview to read into.
        Keyword Args:
            timeout (int, optional): Timeout in seconds for the operation. Defaults to 3s.
        Returns:
            Future[Optional[int]]: A Monarch Future that can be awaited or called with .get() for blocking operation.

        Raises:
            ValueError: If the destination tensor size is smaller than the RDMA buffer size.

        Note:
            Currently only CPU tensors are fully supported. GPU tensors will be temporarily
            copied to CPU, which may impact performance.
        """
        dst_addr, dst_size = _get_addr_and_size(dst)

        if self.size() > dst_size:
            raise ValueError(
                f"Destination tensor size ({dst_size}) must be >= RDMA buffer size ({self.size()})"
            )

        local_proc_id = context().actor_instance.proc_id
        client = context().actor_instance

        async def read_into_nonblocking() -> Optional[int]:
            await _ensure_init_rdma_manager()

            res = await self._buffer.read_into(
                addr=dst_addr,
                size=dst_size,
                local_proc_id=local_proc_id,
                client=client,
                timeout=timeout,
            )
            return res

        return Future(coro=read_into_nonblocking())

    def write_from(
        self,
        src: torch.Tensor | memoryview,
        *,
        timeout: int = 3,
    ) -> Future[None]:
        """
        Write data from a source tensor into the RDMABuffer.

        Args:
            src: Source tensor containing data to be written to the RDMA buffer.
                                Must be a contiguous tensor (including tensor views/slices).
                                Either src or addr/size must be provided.
        Keyword Args:
            timeout (int, optional): Timeout in seconds for the operation. Defaults to 3s.

        Returns:
            Future[None]: A Monarch Future object that can be awaited or called with .get()
                         for blocking operation. Returns None when completed successfully.

        Raises:
            ValueError: If the source tensor size exceeds the RDMA buffer size.

        Note:
            Currently only CPU tensors are fully supported. GPU tensors will be temporarily
            copied to CPU, which may impact performance.
        """

        src_addr, src_size = _get_addr_and_size(src)

        if src_size > self.size():
            raise ValueError(
                f"Source tensor size ({src_size}) must be <= RDMA buffer size ({self.size()})"
            )
        local_proc_id = context().actor_instance.proc_id
        client = context().actor_instance

        async def write_from_nonblocking() -> None:
            await _ensure_init_rdma_manager()

            res = await self._buffer.write_from(
                addr=src_addr,
                size=src_size,
                local_proc_id=local_proc_id,
                client=client,
                timeout=timeout,
            )
            return res

        return Future(coro=write_from_nonblocking())

    def drop(self) -> Future[None]:
        """
        Release the handle on the memory that the remote holds to this memory.
        """
        local_proc_id = context().actor_instance.proc_id
        client = context().actor_instance

        async def drop_nonblocking() -> None:
            await _ensure_init_rdma_manager()

            await self._buffer.drop(
                local_proc_id=local_proc_id,
                client=client,
            )

        return Future(coro=drop_nonblocking())

    @property
    def owner(self) -> ProcMesh:
        """
        The proc that owns this buffer
        """
        # FIXME(slurye): Fix this once controller API is working properly
        # for v1.
        return cast(ProcMesh, context().actor_instance.proc)
