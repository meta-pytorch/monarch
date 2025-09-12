# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import asyncio
import ctypes
import functools
import logging
import warnings
from typing import Optional

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


def is_available():
    return _RdmaBuffer.rdma_supported()


# Cached so that we don't have to call out to the root client every time,
# which may be on a different host.
@functools.cache
def _ensure_init_rdma_manager() -> Shared[None]:
    async def task() -> None:
        await (
            await get_or_spawn_controller("rdma_controller", RdmaController)
        ).init_rdma_on_mesh.call_one(none_throws(context().actor_instance.proc_mesh))

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
        self._managers: Dict[ProcMesh, _RdmaManager] = {}
        self._lock = asyncio.Lock()

    @endpoint
    async def init_rdma_on_mesh(self, proc_mesh: ProcMesh) -> None:
        if not _RdmaBuffer.rdma_supported():
            raise RuntimeError(
                "Cannot spawn _RdmaManager because RDMA is not supported on this machine"
            )

        if proc_mesh in self._managers:
            return

        async with self._lock:
            if proc_mesh not in self._managers:
                self._managers[proc_mesh] = none_throws(
                    await Future(
                        coro=_RdmaManager.create_rdma_manager_nonblocking(
                            await Future(coro=proc_mesh._proc_mesh.task())
                        )
                    )
                )


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
        if isinstance(data, torch.Tensor) and data.device.type != "cpu":
            # TODO - CUDA support for RDMABuffer exists at the Rust layer, but
            # runs into issues with MR creation. For now, only support CPU tensors.
            # Remove this once GPU support is added.
            raise ValueError(
                "RDMABuffer currently only supports CPU tensors (got device {})".format(
                    data.device
                )
            )

        assert (
            is_available()
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
                client=ctx.actor_instance._mailbox,
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
        dst_gpu = None
        if isinstance(dst, torch.Tensor) and dst.device.type != "cpu":
            warnings.warn(
                "note: read_into only supports CPU tensors, so `dst` is being copied to CPU.",
                RDMAReadTransferWarning,
                stacklevel=2,
            )
            dst_gpu = dst
            dst = dst.cpu()

        dst_addr, dst_size = _get_addr_and_size(dst)

        if self.size() > dst_size:
            raise ValueError(
                f"Destination tensor size ({dst_size}) must be >= RDMA buffer size ({self.size()})"
            )

        local_proc_id = context().actor_instance.proc_id
        client = context().actor_instance._mailbox

        async def read_into_nonblocking() -> Optional[int]:
            await _ensure_init_rdma_manager()

            res = await self._buffer.read_into(
                addr=dst_addr,
                size=dst_size,
                local_proc_id=local_proc_id,
                client=client,
                timeout=timeout,
            )
            # TODO - remove this once GPU support is added.
            if dst_gpu is not None:
                dst_gpu.copy_(dst)
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
        src_gpu = None
        if isinstance(src, torch.Tensor) and src.device.type != "cpu":
            # TODO - remove this once GPU support is added.
            warnings.warn(
                "note: write_from only supports CPU tensors, so we will write to CPU first, then transfer to `src` in place.",
                RDMAWriteTransferWarning,
                stacklevel=2,
            )
            src_gpu = src  # Save the original GPU tensor reference
            src = src.cpu()  # Convert to CPU for RDMA operation

        src_addr, src_size = _get_addr_and_size(src)

        if src_size > self.size():
            raise ValueError(
                f"Source tensor size ({src_size}) must be <= RDMA buffer size ({self.size()})"
            )
        local_proc_id = context().actor_instance.proc_id
        client = context().actor_instance._mailbox

        async def write_from_nonblocking() -> None:
            await _ensure_init_rdma_manager()

            res = await self._buffer.write_from(
                addr=src_addr,
                size=src_size,
                local_proc_id=local_proc_id,
                client=client,
                timeout=timeout,
            )
            # TODO - remove this once GPU support is added.
            if src_gpu is not None:
                src_gpu.copy_(src)
            return res

        return Future(coro=write_from_nonblocking())
