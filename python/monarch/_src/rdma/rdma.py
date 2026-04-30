# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import ctypes
import functools
import logging
import sys
import warnings
from collections import defaultdict
from enum import Enum
from typing import Any, cast, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import torch

from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._src.actor.actor_mesh import Actor, context
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.future import Future
from monarch._src.actor.proc_mesh import get_or_spawn_controller, ProcMesh
from pyre_extensions import none_throws
from typing_extensions import Self

_NATIVE_RDMA_IMPORT_ERROR: Optional[ImportError] = None

try:
    from monarch._rust_bindings.rdma import (
        _LocalMemoryHandle,
        _RdmaBuffer,
        _RdmaManager,
        _RdmaOpType,
        is_ibverbs_available as _is_ibverbs_available,
        rdma_supported as _rdma_supported,
    )
except ImportError as e:
    _NATIVE_RDMA_IMPORT_ERROR = e
    logging.warning("RDMA native bindings are not available: %s", e)

    class _UnavailableNativeBinding:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "RDMA native bindings are not available on this platform"
            ) from _NATIVE_RDMA_IMPORT_ERROR

    _LocalMemoryHandle = _UnavailableNativeBinding
    _RdmaBuffer = _UnavailableNativeBinding
    _RdmaManager = _UnavailableNativeBinding

    def _is_ibverbs_available() -> bool:
        return False

    def _rdma_supported() -> bool:
        return False


# RDMARead/WriteTransferWarnings are warnings that are only printed once per process.
# Remove these once GPU support is added.
class RDMAReadTransferWarning(Warning):
    pass


class RDMAWriteTransferWarning(Warning):
    pass


class RDMATcpFallbackWarning(Warning):
    pass


warnings.simplefilter("once", RDMAReadTransferWarning)
warnings.simplefilter("once", RDMAWriteTransferWarning)
warnings.simplefilter("once", RDMATcpFallbackWarning)


def is_ibverbs_available() -> bool:
    """Whether ibverbs RDMA hardware is available on this system."""
    return _is_ibverbs_available()


def is_rdma_available() -> bool:
    """Whether RDMA over ibverbs is available on this system.

    .. deprecated::
        Monarch now supports multiple RDMA backends, so `is_rdma_available`
        is ambiguous and will be removed in a future release. Use
        :func:`is_ibverbs_available` or :func:`get_rdma_backend` instead.
    """
    warnings.warn(
        "is_rdma_available is deprecated because Monarch now supports multiple "
        "RDMA backends, making this function ambiguous. For now it indicates "
        "whether RDMA over ibverbs is available. Use is_ibverbs_available() or "
        "get_rdma_backend() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return is_ibverbs_available()


def get_rdma_backend() -> str:
    """Return available RDMA backend.

    Returns:
        str: One of 'ibverbs', 'tcp', or 'none' indicating the available backend.
             Both Mellanox and EFA hardware are accessed through ibverbs.
             'tcp' indicates the TCP fallback transport is enabled.
    """
    if _is_ibverbs_available():
        return "ibverbs"

    if _rdma_supported():
        return "tcp"

    return "none"


# Cached so that we don't have to call out to the root client every time,
# which may be on a different host.
@functools.cache
def _ensure_init_rdma_manager() -> Shared[None]:
    """Initialize the RDMA manager for this node's backend (ibverbs or EFA)."""

    async def task() -> None:
        # Ensure the proc mesh is initialized before we can send it over the wire,
        # since pickling the proc mesh before it is initiliazed would block the
        # tokio runtime and cause a panic.
        await context().actor_instance.proc_mesh.initialized
        await (
            await get_or_spawn_controller("rdma_controller", RdmaController)
        ).init_rdma_on_mesh.call_one(none_throws(context().actor_instance.proc_mesh))

    return PythonTask.from_coroutine(task()).spawn()


def _get_error(buf: object) -> ValueError:
    return ValueError(
        "RDMABuffer only supports 1d contiguous torch.Tensor or 1d c-contiguous memoryview. Got: {}".format(
            buf
        )
    )


def _is_torch_tensor(obj: object) -> bool:
    """Check whether obj is a torch.Tensor without importing torch."""
    torch_mod = sys.modules.get("torch")
    if torch_mod is None:
        return False
    return isinstance(obj, torch_mod.Tensor)


def _assert_1d_contiguous(buf: "torch.Tensor | memoryview") -> None:
    if _is_torch_tensor(buf):
        if buf.dim() != 1 or not buf.is_contiguous():  # type: ignore[union-attr]
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


def _get_tensor_addr_and_size(tensor: "torch.Tensor") -> tuple[int, int]:
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


def _get_addr_and_size(buf: "torch.Tensor | memoryview") -> tuple[int, int]:
    _assert_1d_contiguous(buf)
    if isinstance(buf, memoryview):
        return _get_memoryview_addr_and_size(buf)
    elif _is_torch_tensor(buf):
        return _get_tensor_addr_and_size(buf)  # type: ignore[arg-type]
    # This shouldn't happen unless there is a bug, handle the type in caller.
    raise RuntimeError(
        "Trying to get address and size of unsupported type. Expected memoryview or torch.Tensor. Got: {}".format(
            type(buf)
        )
    )


def _make_local_memory_handle(
    data: "torch.Tensor | memoryview",
) -> _LocalMemoryHandle:
    addr, size = _get_addr_and_size(data)
    return _LocalMemoryHandle(obj=data, addr=addr, size=size)


class RdmaController(Actor):
    def __init__(self) -> None:
        self._manager_futures: Dict[ProcMesh, Future[_RdmaManager]] = {}

    @endpoint
    async def init_rdma_on_mesh(self, proc_mesh: ProcMesh) -> None:
        # Note: RdmaController acts as coordinator and can run on any node
        # The RDMA support check should happen on the target proc_mesh nodes, not on RdmaController's node

        if proc_mesh not in self._manager_futures:

            async def create_manager() -> _RdmaManager:
                proc_mesh_result = await Future(
                    coro=cast("PythonTask[Any]", proc_mesh._proc_mesh.task())
                )
                return none_throws(
                    await _RdmaManager.create_rdma_manager_nonblocking(
                        proc_mesh_result, context().actor_instance
                    )
                )

            self._manager_futures[proc_mesh] = Future(coro=create_manager())

        await self._manager_futures[proc_mesh]


def pt_cuda_allocator_compatibility() -> bool:
    """
    Check if PyTorch CUDA caching allocator is compatible with RDMA.

    This checks if both the CUDA caching allocator is enabled AND expandable
    segments are enabled, which is required for RDMA operations with CUDA tensors.

    Returns:
        bool: True if both conditions are met, False otherwise
    """
    import torch

    if not torch.cuda.is_available():
        return False

    # Get allocator snapshot which contains settings
    snapshot = torch.cuda.memory._snapshot()
    allocator_settings = snapshot.get("allocator_settings", {})

    # Check if expandable_segments is enabled
    return allocator_settings.get("expandable_segments", False)


@functools.cache
def _check_cuda_expandable_segments_enabled() -> bool:
    """
    Check if PyTorch CUDA caching allocator is using expandable segments.

    Returns:
        bool: True if expandable segments are enabled, False otherwise
    """
    try:
        # Call the Python implementation of pt_cuda_allocator_compatibility
        pt_cuda_compat = pt_cuda_allocator_compatibility()

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
        data: "torch.Tensor | memoryview",
    ) -> None:
        """
        RDMABuffer supports 1d contiguous tensors (including tensor views/slices) or 1d c-contiguous memoryviews.

        Args:
            data: torch.Tensor or memoryview to create the buffer from. Must be 1d and contiguous.
                  If provided, addr and size must not be specified.

        Raises:
            ValueError: If data is not 1d contiguous, if size is 0, or if data is a GPU tensor.
            RuntimeError: If no RDMA backend is available on this platform.

        Note:
            Currently only CPU tensors are supported. GPU tensor support will be added in the future.

        TODO: Create TensorBuffer, which will be main user API supporting non-contiguous tensors
        """
        if _is_torch_tensor(data) and data.device.type == "cuda":  # type: ignore[union-attr]
            # Check if CUDA caching allocator is using expandable segments
            _check_cuda_expandable_segments_enabled()

        backend = get_rdma_backend()
        assert backend != "none", (
            "Tried to create an RDMABuffer, but RDMA is not available on this platform. "
            "To enable TCP fallback transport, call "
            "monarch.configure(rdma_allow_tcp_fallback=True) before creating buffers."
        )
        if backend == "tcp":
            warnings.warn(
                "No ibverbs RDMA hardware detected. Falling back to TCP transport, "
                "which has significantly lower throughput and higher latency than "
                "native RDMA. To disable this fallback and fail explicitly, call "
                "monarch.configure(rdma_allow_tcp_fallback=False).",
                RDMATcpFallbackWarning,
                stacklevel=2,
            )
        # We need to ensure that _RdmaManager is initialized at this point, because under the hood
        # _RdmaBuffer.create_rdma_buffer_blocking relies on this being the case.
        _ensure_init_rdma_manager().block_on()

        handle = _make_local_memory_handle(data)

        try:
            if handle.size == 0:
                raise ValueError("Cannot create RDMABuffer with size 0.")
            ctx = context()
            self._buffer: _RdmaBuffer = _RdmaBuffer.create_rdma_buffer_blocking(
                local=handle,
                client=ctx.actor_instance,
            )
        # TODO - specific exception
        except Exception as e:
            logging.error("Failed to create buffer %s", e)
            raise e

    @property
    def backend(self) -> str:
        """Return the RDMA backend in use ('ibverbs')."""
        return get_rdma_backend()

    def size(self) -> int:
        return self._buffer.size()

    def _submit_impl(
        self,
        ops: "List[Tuple[_RdmaOpType, torch.Tensor | memoryview]]",
        timeout: int,
    ) -> "PythonTask[None]":
        """
        Build a PythonTask that submits a batch of ``(_RdmaOpType, local)``
        ops to the underlying backend. Size checks run eagerly so errors
        surface at task-construction time.

        Intended to be called only from ``RDMAAction.submit``, which runs
        intra-batch overlap detection; bypassing it cannot guarantee
        safety against intra-batch data races.
        """
        prepared: List[Tuple[_RdmaOpType, _LocalMemoryHandle]] = []
        for op_type, local in ops:
            handle = _make_local_memory_handle(local)
            if op_type == _RdmaOpType.ReadInto:
                if self.size() > handle.size:
                    raise ValueError(
                        f"Destination tensor size ({handle.size}) must be >= RDMA buffer size ({self.size()})"
                    )
            elif op_type == _RdmaOpType.WriteFrom:
                if handle.size > self.size():
                    raise ValueError(
                        f"Source tensor size ({handle.size}) must be <= RDMA buffer size ({self.size()})"
                    )
            else:
                raise NotImplementedError(f"Unsupported RDMA op type: {op_type}")
            prepared.append((op_type, handle))

        client = context().actor_instance

        async def submit_nonblocking() -> None:
            await _ensure_init_rdma_manager()
            await self._buffer.submit(
                ops=prepared,
                client=client,
                timeout=timeout,
            )

        return PythonTask.from_coroutine(submit_nonblocking())

    def read_into(
        self,
        dst: "torch.Tensor | memoryview",
        *,
        timeout: int = 60,
    ) -> Future[None]:
        """
        Read data from the RDMABuffer into a destination tensor.

        The destination tensor must be contiguous (including tensor views/slices).
        Args:
            dst: Destination tensor or memoryview to read into.
        Keyword Args:
            timeout (int, optional): Timeout in seconds for the operation. Defaults to 60s.
        Returns:
            Future[None]: A Monarch Future that can be awaited or called with .get() for blocking operation.

        Raises:
            ValueError: If the destination tensor size is smaller than the RDMA buffer size.
        """
        handle = _make_local_memory_handle(dst)

        if self.size() > handle.size:
            raise ValueError(
                f"Destination tensor size ({handle.size}) must be >= RDMA buffer size ({self.size()})"
            )

        client = context().actor_instance

        async def read_into_nonblocking() -> None:
            await _ensure_init_rdma_manager()
            await self._buffer.read_into(
                dst=handle,
                client=client,
                timeout=timeout,
            )

        return Future(coro=read_into_nonblocking())

    def write_from(
        self,
        src: "torch.Tensor | memoryview",
        *,
        timeout: int = 60,
    ) -> Future[None]:
        """
        Write data from a source tensor into the RDMABuffer.

        Args:
            src: Source tensor containing data to be written to the RDMA buffer.
                                Must be a contiguous tensor (including tensor views/slices).
                                Either src or addr/size must be provided.
        Keyword Args:
            timeout (int, optional): Timeout in seconds for the operation. Defaults to 60s.

        Returns:
            Future[None]: A Monarch Future object that can be awaited or called with .get()
                         for blocking operation. Returns None when completed successfully.

        Raises:
            ValueError: If the source tensor size exceeds the RDMA buffer size.
        """
        handle = _make_local_memory_handle(src)

        if handle.size > self.size():
            raise ValueError(
                f"Source tensor size ({handle.size}) must be <= RDMA buffer size ({self.size()})"
            )

        client = context().actor_instance

        async def write_from_nonblocking() -> None:
            await _ensure_init_rdma_manager()
            await self._buffer.write_from(
                src=handle,
                client=client,
                timeout=timeout,
            )

        return Future(coro=write_from_nonblocking())

    def drop(self) -> Future[None]:
        """
        Release the handle on the memory that the src holds to this memory.
        """
        client = context().actor_instance

        async def drop_nonblocking() -> None:
            await _ensure_init_rdma_manager()

            await self._buffer.drop(
                client=client,
            )

        return Future(coro=drop_nonblocking())

    @property
    def owner(self) -> str:
        """
        The owner reference (str)
        """
        return self._buffer.owner_actor_id()


if TYPE_CHECKING:
    LocalMemory = torch.Tensor | memoryview


class RDMAAction:
    """
    Schedule a bunch of actions at once. This provides an opportunity to
    optimize bulk RDMA transactions without exposing complexity to users.

    """

    class RDMAOp(Enum):
        """Enumeration of RDMA operation types."""

        READ_INTO = "read_into"
        WRITE_FROM = "write_from"
        FETCH_ADD = "fetch_add"
        COMPARE_AND_SWAP = "compare_and_swap"

    def __init__(self) -> None:
        self._instructs: "List[Tuple[RDMAAction.RDMAOp, RDMABuffer, LocalMemory]]" = []
        self._memory_dependencies: Dict[Tuple[int, int], RDMAAction.RDMAOp] = {}

    def _check_and_merge_overlapping_range(
        self, addr: int, size: int, op: "RDMAAction.RDMAOp"
    ) -> None:
        """
        Check for overlapping ranges and merge if found.

        Returns the final range to use (either new_range or expanded merged range).
        Updates self._memory_dependencies in place if merging occurs.
        """
        new_start, new_end = addr, addr + size

        # Find overlapping range
        overlapping_range = None
        for existing_start, existing_end in self._memory_dependencies:
            # Check if ranges overlap
            if not (new_end <= existing_start or existing_end <= new_start):
                overlapping_range = (existing_start, existing_end)
                break

        # No overlap found - good to go
        if overlapping_range is None:
            self._memory_dependencies[(new_start, new_end)] = op
            return

        # Overlap found - merge ranges
        existing_op = self._memory_dependencies[overlapping_range]

        # Merge ops, only safe if neither is write_from at the moment
        if existing_op == self.RDMAOp.WRITE_FROM or op == self.RDMAOp.WRITE_FROM:
            raise ValueError(
                f"Same data range already has a write_from within RDMAAction: {existing_op} vs {op}"
            )

        # Create expanded range that covers both
        expanded_range = (
            min(overlapping_range[0], new_start),
            max(overlapping_range[1], new_end),
        )

        # range is unchanged - no need to update
        if expanded_range == (new_start, new_end):
            return

        # Update dictionary: remove old range, add expanded range
        del self._memory_dependencies[overlapping_range]
        self._memory_dependencies[expanded_range] = op

        # now since merged, possible need to merge again
        return self._check_and_merge_overlapping_range(
            expanded_range[0], expanded_range[1] - expanded_range[0], op
        )

    def read_into(
        self, src: RDMABuffer, dst: "LocalMemory | List[LocalMemory]"
    ) -> Self:
        """
        Read from src RDMA buffer into dst memory.

        Args:
            src: Source RDMA buffer to read from
            dst: Destination local memory to read into
                   If dst is a list, it is the concatenation of the data in the list
        """
        # Throw NotImplementedError for lists to simplify logic
        if isinstance(dst, list):
            raise NotImplementedError("List destinations not yet supported")

        addr, size = _get_addr_and_size(dst)

        if size < src.size():
            raise ValueError(
                f"dst memory size ({size}) must be >= src buffer size ({src.size()})"
            )

        self._check_and_merge_overlapping_range(addr, size, self.RDMAOp.READ_INTO)

        self._instructs.append((self.RDMAOp.READ_INTO, src, dst))

        return self

    def write_from(
        self, src: RDMABuffer, dst: "LocalMemory | List[LocalMemory]"
    ) -> Self:
        """
        Write from dst memory to src RDMA buffer.

        Args:
            src: Destination RDMA buffer to write to
            dst: Source local memory to write from
                   If local is a list, it is the concatenation of the data in the list
        """
        # Throw NotImplementedError for lists to simplify logic
        if isinstance(dst, list):
            raise NotImplementedError("List sources not yet supported")

        addr, size = _get_addr_and_size(dst)

        if size > src.size():
            raise ValueError(
                f"Local memory size ({size}) must be <= src buffer size ({src.size()})"
            )

        self._check_and_merge_overlapping_range(addr, size, self.RDMAOp.WRITE_FROM)

        self._instructs.append((self.RDMAOp.WRITE_FROM, src, dst))

        return self

    def fetch_add(self, src: RDMABuffer, dst: "LocalMemory", add: int) -> Self:
        """
        Perform atomic fetch-and-add operation on src RDMA buffer.

        Args:
            src: src RDMA buffer to perform operation on
            dst: Local memory to store the original value
            add: Value to add to the src buffer

        Atomically:
            *dst = *src
            *src = *src + add

        Note: src/dst are 8 bytes
        """
        raise NotImplementedError("Not yet supported")

    def compare_and_swap(
        self, src: RDMABuffer, dst: "LocalMemory", compare: int, swap: int
    ) -> Self:
        """
        Perform atomic compare-and-swap operation on src RDMA buffer.

        Args:
            src: src RDMA buffer to perform operation on
            dst: Local memory to store the original value
            compare: Value to compare against
            swap: Value to swap in if comparison succeeds

        Atomically:
            *dst = *src;
            if (*src == compare) {
                *src = swap
            }

        Note: src/dst are 8 bytes
        """
        raise NotImplementedError("Not yet supported")

    def submit(self, *, timeout: int = 60) -> Future[None]:
        """
        Schedule the registered work.

        Ops are grouped by source ``RDMABuffer`` and submitted as one
        batch per buffer; submits across buffers are awaited concurrently.
        The Future resolves when all ops finish, or fails with the first
        error. Safe to call more than once.

        Keyword Args:
            timeout (int, optional): Per-buffer batch timeout in seconds.
                Defaults to 60s.
        """
        ops_by_buffer: Dict[RDMABuffer, List[Tuple[_RdmaOpType, "LocalMemory"]]] = (
            defaultdict(list)
        )
        for op, src, dst in self._instructs:
            if op == self.RDMAOp.READ_INTO:
                rdma_op = _RdmaOpType.ReadInto
            elif op == self.RDMAOp.WRITE_FROM:
                rdma_op = _RdmaOpType.WriteFrom
            else:
                raise NotImplementedError(f"Unknown RDMA operation: {op}")
            ops_by_buffer[src].append((rdma_op, dst))

        async def run() -> None:
            # Spawn before awaiting so per-buffer submits run concurrently.
            shareds = [
                buf._submit_impl(ops, timeout).spawn()
                for buf, ops in ops_by_buffer.items()
            ]
            results = await Shared.gather(*shareds)
            # Re-raise non-`Exception` `BaseException`s (e.g. `KeyboardInterrupt`,
            # `SystemExit`) directly: `ExceptionGroup` only accepts `Exception`
            # subclasses, so wrapping them would either drop them or fail.
            for r in results:
                if isinstance(r, BaseException) and not isinstance(r, Exception):
                    raise r
            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                raise ExceptionGroup("RDMAAction.submit failed", errors)

        return Future(coro=run())
