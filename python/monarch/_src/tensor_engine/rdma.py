# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import asyncio
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


# Cached so that we don't have to call out to the root client every time,
# which may be on a different host.
@functools.cache
def _ensure_init_rdma_manager() -> Shared[None]:
    async def task() -> None:
        await (
            await get_or_spawn_controller("rdma_controller", RdmaController)
        ).init_rdma_on_mesh.call_one(none_throws(context().actor_instance.proc_mesh))

    return PythonTask.from_coroutine(task()).spawn()


def _assert_tensor_is_1d_contiguous_uint8(t: torch.Tensor) -> None:
    if t.ndim != 1:
        raise ValueError(f"Tensor must be 1D, got {t.ndim}D")
    if t.dtype != torch.uint8:
        raise ValueError(f"Tensor must be uint8, got {t.dtype}")
    if not t.is_contiguous():
        raise ValueError("Tensor must be contiguous")


class RDMABuffer:
    def __init__(self, data: torch.Tensor) -> None:
        """
        RDMABuffer only supports 1D contiguous tensors that are 1 byte per item.

        To create a 1 byte, 1D view, use t.view(torch.uint8).flatten()

        TODO: Create TensorBuffer, which will be main user API supporting non-contiguous , multi-byte-per-elment tensors
        """
        assert (
            is_available()
        ), "Tried to create an RDMABuffer, but RDMA is not available on this platform."

        # We need to ensure that _RdmaManager is initialized at this point, because under the hood
        # _RdmaBuffer.create_rdma_buffer_blocking relies on this being the case.
        _ensure_init_rdma_manager().block_on()

        if data.device.type != "cpu":
            # TODO - CUDA support for RDMABuffer exists at the Rust layer, but
            # runs into issues with MR creation. For now, only support CPU tensors.
            # Remove this once GPU support is added.
            raise ValueError(
                "RDMABuffer currently only supports CPU tensors (got device {})".format(
                    data.device
                )
            )

        _assert_tensor_is_1d_contiguous_uint8(data)
        assert data.storage_offset() == 0

        try:
            storage = data.untyped_storage()
            addr: int = storage.data_ptr()
            size = storage.element_size() * data.numel()
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

    def read_into(
        self,
        dst: torch.Tensor,
        offset: int = 0,
        timeout: int = 3,
    ) -> Future[Optional[int]]:
        """
        Read data from the RDMABuffer into a destination tensor.

        The destination tensor must be contiguous and 1 byte per item.

        Returns an ActorFuture that can be awaited or called with .get() for blocking operation.
        """
        _assert_tensor_is_1d_contiguous_uint8(dst)
        dst_gpu = None
        if dst.device.type != "cpu":
            # TODO - remove this once GPU support is added.
            warnings.warn(
                "note: read_into only supports CPU tensors, so `dst` is being copied to CPU.",
                RDMAReadTransferWarning,
                stacklevel=2,
            )
            dst_gpu = dst
            dst = dst.cpu()
        storage = dst.untyped_storage()
        addr: int = storage.data_ptr() + offset
        size = storage.element_size() * dst.numel()
        if offset + size > dst.numel():
            raise ValueError(
                f"offset + size ({offset + size}) must be <= dst.numel() ({dst.numel()})"
            )

        local_proc_id = context().actor_instance.proc_id
        client = context().actor_instance._mailbox

        async def read_into_nonblocking() -> Optional[int]:
            await _ensure_init_rdma_manager()

            res = await self._buffer.read_into(
                addr=addr,
                size=size,
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
        self, src: torch.Tensor, offset: int = 0, timeout: int = 3
    ) -> Future[None]:
        """
        Write data from a source tensor into the RDMABuffer.

        The source tensor must be contiguous and 1 byte per item.

        Returns an ActorFuture that can be awaited or called with .get() for blocking operation.
        """
        _assert_tensor_is_1d_contiguous_uint8(src)
        src_gpu = None
        if src.device.type != "cpu":
            # TODO - remove this once GPU support is added.
            warnings.warn(
                "note: write_from only supports CPU tensors, so we will write to CPU first, then transfer to `src` in place.",
                RDMAWriteTransferWarning,
                stacklevel=2,
            )
            src_gpu = src  # Save the original GPU tensor reference
            src = src.cpu()  # Convert to CPU for RDMA operation
        storage = src.untyped_storage()
        addr: int = storage.data_ptr()
        size = storage.element_size() * src.numel()
        if size + offset > src.numel():
            raise ValueError(
                f"size + offset ({size + offset}) must be <= src.numel() ({src.numel()})"
            )

        local_proc_id = context().actor_instance.proc_id
        client = context().actor_instance._mailbox

        async def write_from_nonblocking() -> None:
            await _ensure_init_rdma_manager()

            res = await self._buffer.write_from(
                addr=addr,
                size=size,
                local_proc_id=local_proc_id,
                client=client,
                timeout=timeout,
            )
            # TODO - remove this once GPU support is added.
            if src_gpu is not None:
                src_gpu.copy_(src)
            return res

        return Future(coro=write_from_nonblocking())
