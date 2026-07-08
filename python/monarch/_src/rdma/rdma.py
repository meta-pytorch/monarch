# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import functools
import logging
import sys
import threading
import warnings
import weakref
from typing import Any, cast, Dict, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import torch

from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._src.actor.actor_mesh import Actor, context
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.future import Future
from monarch._src.actor.proc_mesh import get_or_spawn_controller, ProcMesh
from pyre_extensions import none_throws

_NATIVE_RDMA_IMPORT_ERROR: Optional[ImportError] = None

try:
    from monarch._rust_bindings.rdma import (
        _assert_1d_contiguous,
        _get_memoryview_addr_and_size,
        _get_tensor_addr_and_size,
        _LocalMemoryHandle,
        _make_local_memory_handle_from_memoryview,
        _make_local_memory_handle_from_tensor,
        _RdmaAction,
        _RdmaBuffer,
        _RdmaManager,
        _WeakLocalMemoryHandle,
        is_ibverbs_available as _is_ibverbs_available,
        rdma_supported as _rdma_supported,
    )
except ImportError as e:
    # These fallbacks let the module import on platforms without the native
    # RDMA bindings; every entry point raises on use. We hide them from the
    # type checker with `if not TYPE_CHECKING` so it resolves these names to
    # their real types from the `try` import (via the `.pyi` stub) rather
    # than the catch-all `_UnavailableNativeBinding`, which has none of the
    # real attributes. At runtime `TYPE_CHECKING` is `False`, so the
    # fallbacks below are the ones that take effect.
    if not TYPE_CHECKING:
        _NATIVE_RDMA_IMPORT_ERROR = e
        logging.warning("RDMA native bindings are not available: %s", e)

        class _UnavailableNativeBinding:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                raise ImportError(
                    "RDMA native bindings are not available on this platform"
                ) from _NATIVE_RDMA_IMPORT_ERROR

        _LocalMemoryHandle = _UnavailableNativeBinding
        _WeakLocalMemoryHandle = _UnavailableNativeBinding
        _RdmaAction = _UnavailableNativeBinding
        _RdmaBuffer = _UnavailableNativeBinding
        _RdmaManager = _UnavailableNativeBinding

        def _make_local_memory_handle_from_memoryview(mv: memoryview) -> Any:
            raise ImportError(
                "RDMA native bindings are not available on this platform"
            ) from _NATIVE_RDMA_IMPORT_ERROR

        def _make_local_memory_handle_from_tensor(tensor: Any) -> Any:
            raise ImportError(
                "RDMA native bindings are not available on this platform"
            ) from _NATIVE_RDMA_IMPORT_ERROR

        def _assert_1d_contiguous(buf: Any) -> None:
            raise ImportError(
                "RDMA native bindings are not available on this platform"
            ) from _NATIVE_RDMA_IMPORT_ERROR

        def _get_memoryview_addr_and_size(mv: memoryview) -> tuple[int, int]:
            raise ImportError(
                "RDMA native bindings are not available on this platform"
            ) from _NATIVE_RDMA_IMPORT_ERROR

        def _get_tensor_addr_and_size(tensor: Any) -> tuple[int, int]:
            raise ImportError(
                "RDMA native bindings are not available on this platform"
            ) from _NATIVE_RDMA_IMPORT_ERROR

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


def _is_torch_tensor(obj: object) -> bool:
    """Check whether obj is a torch.Tensor without importing torch."""
    torch_mod = sys.modules.get("torch")
    if torch_mod is None:
        return False
    return isinstance(obj, torch_mod.Tensor)


# Cache of weak handles to local memory regions, keyed by
# `(backing_id, addr, size)`. `backing_id` is `id(t.untyped_storage())`
# for tensors and `id(mv)` for memoryviews — the id of the stable
# backing object, not of any per-call view. Transient tensor views like
# `tensor.view(...).flatten()` share their storage's id and so reuse one
# entry; distinct memoryviews over the same buffer keep separate entries
# (their ids differ). `addr` and `size` further disambiguate slices of
# one backing.
#
# The cached `_WeakLocalMemoryHandle` does NOT pin the backing, so the
# cache cannot prolong an allocation's lifetime. Eviction is driven by
# a `weakref` to the backing — a tensor's `untyped_storage()` or the
# memoryview itself — whose callback drops the entry once the backing
# is garbage-collected.
_local_memory_cache: "Dict[Tuple[int, int, int], _WeakLocalMemoryHandle]" = {}
# Eviction weakrefs, keyed identically. Holding the weakref alive is
# what lets its callback fire; the callback removes the entry only when
# its own weakref is still the registered one, so a stale callback
# cannot evict a fresh registration whose backing reused a freed id.
_local_memory_cache_refs: "Dict[Tuple[int, int, int], weakref.ref]" = {}
# Serializes the get/upgrade/insert and eviction sequences; the GIL
# only makes the individual dict ops atomic.
_local_memory_cache_lock = threading.Lock()


def _evict_local_memory(key: Tuple[int, int, int], ref: "weakref.ref") -> None:
    """Drop the cache entry for ``key`` when its backing is collected.

    Guards against id reuse: a newer registration whose backing reused a
    freed object's id installs a fresh weakref under the same key, so
    this fires only when the registered weakref is still the one that
    scheduled the callback.
    """
    with _local_memory_cache_lock:
        if _local_memory_cache_refs.get(key) is ref:
            del _local_memory_cache_refs[key]
            _local_memory_cache.pop(key, None)


def _ensure_hip_runtime_ordering() -> None:
    """On ROCm, raise a clear error instead of a hard SIGABRT when monarch's
    native extension initialized the system HIP runtime before torch loaded its
    bundled copy.

    In that state the first HIP driver call (made by the local-memory probe)
    aborts inside rocprofiler-register with
    ``hip.cpp:512 "hipApiName has non-null function pointer ..."`` and takes the
    whole process down. We detect the condition here and turn it into an
    actionable exception. No-op on CUDA/CPU and whenever torch's runtime won.
    """
    monarch_pkg = sys.modules.get("monarch")
    if getattr(monarch_pkg, "_TORCH_PRELOADED_BEFORE_BINDINGS", True):
        return  # torch's HIP runtime loaded first (or preloaded): safe
    torch_mod = sys.modules.get("torch")
    if torch_mod is None:
        return  # no second HIP runtime loaded; the single system copy is safe
    if getattr(getattr(torch_mod, "version", None), "hip", None) is None:
        return  # CUDA / CPU torch: not the ROCm dual-runtime hazard
    # Only error if two distinct libamdhip64 copies are actually mapped.
    try:
        with open("/proc/self/maps") as f:
            maps = f.read()
    except OSError:
        return
    import re

    libs = set(re.findall(r"/\S*libamdhip64\.so[.0-9]*", maps))
    if len(libs) < 2:
        return  # single HIP runtime: safe
    raise RuntimeError(
        "monarch loaded the system ROCm HIP runtime before torch loaded its "
        "bundled copy; the HIP call needed for RDMA would abort the process "
        "(hip.cpp:512 'hipApiName has non-null function pointer ...'). "
        "On ROCm, import torch before importing monarch, or set the environment "
        "variable MONARCH_PRELOAD_TORCH=1 before importing monarch."
    )


def _make_local_memory_handle(
    data: "torch.Tensor | memoryview",
) -> _LocalMemoryHandle:
    _ensure_hip_runtime_ordering()
    _assert_1d_contiguous(data)
    if isinstance(data, memoryview):
        addr, size = _get_memoryview_addr_and_size(data)
        backing = data
    elif _is_torch_tensor(data):
        addr, size = _get_tensor_addr_and_size(data)
        backing = data.untyped_storage()  # type: ignore[union-attr]
    else:
        raise RuntimeError(
            "Trying to make a local memory handle for an unsupported type. "
            "Expected memoryview or torch.Tensor. Got: {}".format(type(data))
        )
    key = (id(backing), addr, size)
    with _local_memory_cache_lock:
        weak = _local_memory_cache.get(key)
        if weak is not None:
            cached = weak.upgrade()
            if cached is not None:
                return cached
            # The backing is gone but its eviction callback has not run
            # yet; drop the stale entry now.
            _local_memory_cache.pop(key, None)
            _local_memory_cache_refs.pop(key, None)
    if isinstance(data, memoryview):
        strong = _make_local_memory_handle_from_memoryview(data)
    else:
        strong = _make_local_memory_handle_from_tensor(data)
    weak = strong.downgrade()
    if weak is not None:
        ref = weakref.ref(backing, lambda _r, _k=key: _evict_local_memory(_k, _r))
        with _local_memory_cache_lock:
            _local_memory_cache[key] = weak
            _local_memory_cache_refs[key] = ref
    return strong


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
    # pyrefly: ignore [missing-attribute]
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

    except Exception:
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

    def read_into(
        self,
        dst: "torch.Tensor | memoryview",
        *,
        timeout: int = 60,
    ) -> Future[None]:
        """Read data from this RDMABuffer into ``dst``.

        ``dst`` must be a 1D contiguous tensor or c-contiguous memoryview
        whose byte-size is at least ``self.size()``.

        Args:
            dst: Destination tensor or memoryview to read into.
        Keyword Args:
            timeout (int, optional): Timeout in seconds. Defaults to 60s.
        Returns:
            Future[None]: A Monarch Future that resolves to ``None`` when
                the read completes.
        Raises:
            ValueError: If ``dst`` is smaller than the RDMA buffer.
        """
        return RDMAAction().read_remote(dst, self).submit(timeout=timeout)

    def write_from(
        self,
        src: "torch.Tensor | memoryview",
        *,
        timeout: int = 60,
    ) -> Future[None]:
        """Write data from ``src`` into this RDMABuffer.

        ``src`` must be a 1D contiguous tensor or c-contiguous memoryview
        whose byte-size is at most ``self.size()``.

        Args:
            src: Source tensor or memoryview containing the bytes to
                write to the RDMA buffer.
        Keyword Args:
            timeout (int, optional): Timeout in seconds. Defaults to 60s.
        Returns:
            Future[None]: A Monarch Future that resolves to ``None`` when
                the write completes.
        Raises:
            ValueError: If ``src`` exceeds the RDMA buffer size.
        """
        return RDMAAction().write_remote(self, src).submit(timeout=timeout)

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
    """Schedule a batch of RDMA operations and submit them as one unit.

    All bookkeeping (per-op validation, intra-batch local-memory race
    detection, backend grouping, parallel dispatch) lives in the Rust
    `_RdmaAction`; this class is a thin wrapper around it.
    """

    def __init__(self) -> None:
        self._inner: _RdmaAction = _RdmaAction()

    # pyrefly: ignore [not-a-type]
    def read_remote(self, dst: "LocalMemory", src: RDMABuffer) -> "RDMAAction":
        """Queue a read from RDMA buffer ``src`` into local memory ``dst``."""
        handle = _make_local_memory_handle(dst)
        self._inner.add_read_into_local(remote=src._buffer, local=handle)
        return self

    # pyrefly: ignore [not-a-type]
    def write_remote(self, dst: RDMABuffer, src: "LocalMemory") -> "RDMAAction":
        """Queue a write from local memory ``src`` into RDMA buffer ``dst``."""
        handle = _make_local_memory_handle(src)
        self._inner.add_write_from_local(remote=dst._buffer, local=handle)
        return self

    # pyrefly: ignore [not-a-type]
    def fetch_add(self, src: RDMABuffer, dst: "LocalMemory", add: int) -> "RDMAAction":
        raise NotImplementedError("Not yet supported")

    def compare_and_swap(
        self,
        src: RDMABuffer,
        dst: "LocalMemory",
        compare: int,
        swap: int,
        # pyrefly: ignore [not-a-type]
    ) -> "RDMAAction":
        raise NotImplementedError("Not yet supported")

    def submit(self, *, timeout: int = 60) -> Future[None]:
        """Schedule the queued ops. Safe to call multiple times.

        The returned Future does not resolve until every op in the batch
        completes, or until the timeout is reached. If any op fails, the
        Future resolves with an exception.
        """
        client = context().actor_instance
        inner = self._inner

        async def run() -> None:
            await _ensure_init_rdma_manager()
            await inner.submit(client=client, timeout=timeout)

        return Future(coro=run())
