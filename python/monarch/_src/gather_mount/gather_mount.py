# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
gather_mount – read-only FUSE mount of remote shard file systems.

Each host in the host mesh exposes its ``remote_mount_point`` as a
sub-directory of ``local_mount_point``, named after the host's mesh
coordinates (e.g. ``machine_3_gpu_2``).  A 0-dim mesh (single host)
mounts files directly in ``local_mount_point`` without any sub-directory.

**Cache invalidation** – push model, per file:

* When a file is first read into the client cache, the client fires a
  ``watch_path`` message to the remote actor.  The actor adds a single
  inotify watch for that specific file — no directory scans, no recursive
  walk.  Files that have never been cached need no watch because their
  data is always fetched fresh.

* The remote inotify watcher batches events and calls the
  ``GatherClientActor.invalidate`` endpoint directly, passing the shard
  key and the list of changed paths.  The client actor runs async so no
  thread blocks waiting for notifications.

* ``invalidate`` evicts the stat cache entry for each changed path.  The
  next ``getattr`` for the file goes to the remote and gets a fresh
  size/mtime.

Large transfers (≥ ``_RDMA_THRESHOLD``) are transferred via ``RDMABuffer``
directly within the async ``GatherClientActor.read_path`` endpoint.

``_GatherMountFS`` is a thin FUSE adapter with no state of its own; all
caching and coordination lives in ``GatherClientActor``.
"""

from __future__ import annotations

import asyncio
import atexit
import ctypes
import errno
import logging
import mmap as _mmap
import os
import stat as _stat
import struct
import time
from collections.abc import Mapping
from dataclasses import dataclass
from itertools import product
from typing import Callable, Union

from monarch._rust_bindings.monarch_extension.gather_fuse import mount_gather_fuse
from monarch.actor import Actor, context, endpoint, HostMesh, Point, this_proc

logger: logging.Logger = logging.getLogger(__name__)

# Seconds to batch remote inotify events before sending one notification.
_NOTIFY_BATCH_S: float = 0.05

# Byte threshold above which transfers use RDMABuffer instead of plain bytes.
_RDMA_THRESHOLD: int = 1 * 1024 * 1024  # 1 MiB

# inotify flags
_IN_MODIFY: int = 0x00000002
_IN_CLOSE_WRITE: int = 0x00000008
_IN_ATTRIB: int = 0x00000004  # catches chmod/chown/truncate
_IN_IGNORED: int = 0x00008000  # watch auto-removed (file deleted)
_IN_NONBLOCK: int = 0o00004000
_IN_CLOEXEC: int = 0o02000000
_WATCH_MASK: int = _IN_MODIFY | _IN_CLOSE_WRITE | _IN_ATTRIB
_INOTIFY_EVENT_FMT: str = "iIII"
_INOTIFY_EVENT_SIZE: int = struct.calcsize(_INOTIFY_EVENT_FMT)


# ── inotify wrapper ───────────────────────────────────────────────────────────


class _Inotify:
    """Manages an inotify fd and per-path change futures.

    ``add_watch`` is synchronous so callers can register a watch before
    reading a stat (avoiding a race where a change could go unnoticed).
    ``watch_path`` wraps it as a coroutine for use in tasks.
    """

    def __init__(self) -> None:
        libc: ctypes.CDLL = ctypes.CDLL("libc.so.6", use_errno=True)
        ifd: int = libc.inotify_init1(_IN_NONBLOCK | _IN_CLOEXEC)
        if ifd < 0:
            raise OSError(ctypes.get_errno(), os.strerror(ctypes.get_errno()))
        self._libc = libc
        self._ifd = ifd
        # wd → (rel_path, Future that resolves when the path changes)
        self._wd_to_entry: dict[int, tuple[str, asyncio.Future[None]]] = {}
        self._path_to_wd: dict[str, int] = {}
        asyncio.get_running_loop().add_reader(ifd, self._on_readable)

    def add_watch(self, path: str, rel_path: str) -> asyncio.Future[None]:
        """Register an inotify watch; returns a Future that resolves on change."""
        wd: int = self._libc.inotify_add_watch(self._ifd, path.encode(), _WATCH_MASK)
        if wd < 0:
            raise OSError(
                ctypes.get_errno(),
                f"inotify_add_watch failed for {path!r}: {os.strerror(ctypes.get_errno())}",
            )
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._wd_to_entry[wd] = (rel_path, future)
        self._path_to_wd[rel_path] = wd
        return future

    async def watch_path(self, path: str, rel_path: str) -> None:
        """Register a watch and suspend until *path* is modified."""
        await self.add_watch(path, rel_path)

    def _on_readable(self) -> None:
        """Read all pending inotify events and resolve the corresponding futures."""
        buf = ctypes.create_string_buffer(4096)
        nbytes: int = self._libc.read(self._ifd, buf, ctypes.sizeof(buf))
        if nbytes <= 0:
            return
        off = 0
        while off + _INOTIFY_EVENT_SIZE <= nbytes:
            wd, mask, _cookie, name_len = struct.unpack_from(
                _INOTIFY_EVENT_FMT, buf, off
            )
            off += _INOTIFY_EVENT_SIZE + name_len
            if mask & _IN_IGNORED:
                # Watch was removed (by us or because the file was deleted).
                entry = self._wd_to_entry.pop(wd, None)
                if entry is not None:
                    self._path_to_wd.pop(entry[0], None)
            elif wd in self._wd_to_entry:
                rel_path, future = self._wd_to_entry.pop(wd)
                self._path_to_wd.pop(rel_path, None)
                self._libc.inotify_rm_watch(self._ifd, wd)
                # IN_IGNORED will follow; already removed from _wd_to_entry.
                if not future.done():
                    future.set_result(None)


# ── Remote actor ──────────────────────────────────────────────────────────────


class GatherSourceActor(Actor):
    """Runs on each remote process; serves files from the remote mount path."""

    def __init__(self, remote_path: Union[str, Callable[[Point], str]]) -> None:
        rank: Point = context().actor_instance.rank
        if callable(remote_path):
            self._root: str = remote_path(rank)
        else:
            self._root = str(remote_path)

        self._shard_key: str = _point_to_key(dict(rank))
        self._pending_rdma: dict[int, tuple[_mmap.mmap, memoryview]] = {}
        self._next_token: int = 0
        self._inotify: _Inotify | None = None
        self._client_actor: object = None
        self._pending_paths: asyncio.Queue[str] = asyncio.Queue()

    def _full(self, rel_path: str) -> str:
        if rel_path:
            return os.path.join(self._root, rel_path.lstrip("/"))
        return self._root

    # ── inotify endpoints ──────────────────────────────────────────────────

    @endpoint
    async def init_watch(self, client_actor: object) -> None:
        """Set up inotify and start the flush task."""
        self._inotify = _Inotify()
        self._client_actor = client_actor
        asyncio.create_task(self._flush_task())

    async def _flush_task(self) -> None:
        """Collect changed paths and dispatch them to the client in batches."""
        while True:
            batch: list[str] = [await self._pending_paths.get()]
            deadline = asyncio.get_running_loop().time() + _NOTIFY_BATCH_S
            while True:
                try:
                    batch.append(
                        await asyncio.wait_for(
                            self._pending_paths.get(),
                            timeout=deadline - asyncio.get_running_loop().time(),
                        )
                    )
                except asyncio.TimeoutError:
                    # pyre-ignore[16]
                    self._client_actor.invalidate.broadcast(self._shard_key, batch)
                    break

    # ── File serving endpoints ─────────────────────────────────────────────

    @endpoint
    async def stat_and_watch(self, rel_path: str) -> tuple[int, int, int] | None:
        """Register an inotify watch then stat the path.

        The watch is registered synchronously before the stat so that any
        change occurring between registration and the stat still triggers an
        invalidation.  Returns ``(mtime_ns, size, mode)`` or ``None``.
        """
        if self._inotify is None:
            try:
                st = os.stat(self._full(rel_path))
                return (st.st_mtime_ns, st.st_size, st.st_mode)
            except OSError:
                return None
        try:
            future = self._inotify.add_watch(self._full(rel_path), rel_path)
            st = os.stat(self._full(rel_path))
        except OSError:
            return None

        async def _watch_and_enqueue() -> None:
            await future
            await self._pending_paths.put(rel_path)

        asyncio.create_task(_watch_and_enqueue())
        return (st.st_mtime_ns, st.st_size, st.st_mode)

    @endpoint
    async def listdir(self, rel_path: str) -> list[str]:
        return os.listdir(self._full(rel_path))

    @endpoint
    async def read_bytes(self, rel_path: str, offset: int, length: int) -> bytes:
        with open(self._full(rel_path), "rb") as f:
            f.seek(offset)
            return f.read(length)

    @endpoint
    async def prepare_rdma(
        self, rel_path: str, offset: int, length: int
    ) -> tuple[int, object, int]:
        """Pin a file range in an anonymous mmap and return an RDMABuffer."""
        from monarch.rdma import RDMABuffer

        with open(self._full(rel_path), "rb") as f:
            f.seek(offset)
            data = f.read(length)
        actual = len(data)
        if actual == 0:
            return (-1, None, 0)

        mm = _mmap.mmap(-1, actual, _mmap.MAP_PRIVATE | _mmap.MAP_ANONYMOUS)
        mv: memoryview = memoryview(mm)[:actual]
        mv[:] = data
        rdma_buf = RDMABuffer(mv)

        token = self._next_token
        self._next_token += 1
        self._pending_rdma[token] = (mm, mv)
        return (token, rdma_buf, actual)

    @endpoint
    async def release_rdma(self, token: int) -> None:
        entry = self._pending_rdma.pop(token, None)
        if entry is not None:
            mm, mv = entry
            mv.release()
            mm.close()


# ── Client actor ──────────────────────────────────────────────────────────────


@dataclass
class _CacheEntry:
    mtime_ns: int
    mode: int
    data: bytearray
    offset: int  # file position where data starts; covers [offset, offset + len(data)]

    @property
    def size(self) -> int:
        return self.offset + len(self.data)


class GatherClientActor(Actor):
    """Local actor: owns the stat/file caches and handles all client-side logic.

    All cache state lives here so that the actor's single-threaded event loop
    provides synchronisation for free — no locks needed.  ``_GatherMountFS``
    is a thin FUSE adapter that delegates every operation to this actor via
    synchronous ``.call_one(...).get()`` calls from the FUSE threads.
    """

    def __init__(self, actors: object) -> None:
        self._actors = actors
        extent: dict[str, int] = dict(actors.extent)  # pyre-ignore[16]
        if not extent:
            shard_points: list[dict[str, int]] = [{}]
        else:
            dims = list(extent.items())
            shard_points = [
                dict(zip([n for n, _ in dims], indices))
                for indices in product(*(range(s) for _, s in dims))
            ]
        self._key_to_point: dict[str, dict[str, int]] = {
            _point_to_key(p): p for p in shard_points
        }
        # (shard_key, rel_path) → _CacheEntry; evicted by invalidate()
        self._cache: dict[tuple[str, str], _CacheEntry] = {}

    @endpoint
    async def invalidate(self, shard_key: str, paths: list[str]) -> None:
        """Evict cache entries for changed paths.

        Called directly by :class:`GatherSourceActor` when inotify reports
        file changes.
        """
        for rel_path in paths:
            evicted = self._cache.pop((shard_key, rel_path), None)
            logger.debug(
                "INVALIDATE shard=%r path=%r (had_data=%s)",
                shard_key,
                rel_path,
                evicted is not None and len(evicted.data) > 0,
            )

    @endpoint
    async def getattr_path(self, path: str) -> dict[str, object] | int:
        """Return a stat dict, or an errno int on error."""
        try:
            point, rel_path = self._parse_path(path)
        except FileNotFoundError:
            return errno.ENOENT
        if point is None:
            return _synthetic_dir_stat()
        assert rel_path is not None
        cache_key = (_point_to_key(point), rel_path)
        cached = cache_key in self._cache
        entry = await self._ensure_cached(point, rel_path)
        if entry is None:
            return errno.ENOENT
        logger.debug(
            "STAT path=%r size=%d mtime=%d cache=%s",
            path,
            entry.size,
            entry.mtime_ns,
            "HIT" if cached else "MISS→rpc",
        )
        return _make_stat(entry.mode, entry.size, entry.mtime_ns)

    @endpoint
    async def readdir_path(self, path: str) -> list[str] | int:
        """Return directory entries (without ``.`` and ``..``), or an errno int on error."""
        try:
            point, rel_path = self._parse_path(path)
        except FileNotFoundError:
            return errno.ENOENT
        if point is None:
            return list(self._key_to_point.keys())
        logger.debug("LISTDIR path=%r", path)
        # pyre-ignore[16]
        return await self._actor_for(point).listdir.call_one(rel_path)

    @endpoint
    async def read_path(self, path: str, size: int, offset: int) -> bytes | int:
        """Return bytes, or an errno int on error."""
        try:
            point, rel_path = self._parse_path(path)
        except FileNotFoundError:
            return errno.ENOENT
        if point is None:
            return errno.EISDIR
        assert rel_path is not None

        cache_key = (_point_to_key(point), rel_path)
        stat_cached = cache_key in self._cache
        entry = await self._ensure_cached(point, rel_path)
        if entry is None:
            return errno.ENOENT
        if _stat.S_ISDIR(entry.mode):
            return errno.EISDIR

        # Fetch prefix if the read starts before the cached window.
        if offset < entry.offset:
            fetch_len = entry.offset - offset
            logger.debug(
                "READ path=%r offset=%d size=%d → fetch [%d, %d) (stat=%s)",
                path,
                offset,
                size,
                offset,
                entry.offset,
                "MISS→rpc" if not stat_cached else "HIT",
            )
            prefix = await self._fetch_range(point, rel_path, offset, fetch_len)
            entry.data = bytearray(prefix) + entry.data
            entry.offset = offset
        else:
            logger.debug(
                "READ path=%r offset=%d size=%d → cache HIT (stat=%s)",
                path,
                offset,
                size,
                "MISS→rpc" if not stat_cached else "HIT",
            )

        data_start = offset - entry.offset
        return bytes(entry.data[data_start : data_start + size])

    # ── Internal helpers (not endpoints) ──────────────────────────────────

    def _actor_for(self, point: dict[str, int]) -> object:
        # pyre-ignore[16]
        return self._actors.slice(**point)

    def _parse_path(self, path: str) -> tuple[dict[str, int] | None, str | None]:
        """Split *path* into ``(shard_point, rel_path)``.

        For a 0-dim mesh (empty extent) the point is ``{}`` and the rel_path
        is the full path stripped of its leading ``/``.

        For N-dim, the root path ``"/"`` returns ``(None, None)``.  Any other
        path has its first component parsed as the shard key; the remainder
        (which may be ``""``, meaning the shard root directory) is rel_path.

        Raises ``FileNotFoundError`` for paths with an invalid shard key.
        This is the only place that distinguishes 0-dim from N-dim.
        """
        rel = path.lstrip("/")
        if "" in self._key_to_point:  # 0-dim: no shard prefix
            return ({}, rel)
        if not rel:  # N-dim root
            return (None, None)
        head, _, rest = rel.partition("/")
        if head not in self._key_to_point:
            raise FileNotFoundError(path)
        return (self._key_to_point[head], rest)

    async def _ensure_cached(
        self, point: dict[str, int], rel_path: str
    ) -> _CacheEntry | None:
        """Return the cache entry for *(point, rel_path)*, fetching if absent.

        On a cache miss, calls ``stat_and_watch`` on the remote actor, which
        installs the inotify watch *before* statting to avoid a race, then
        creates the entry in the base state (no data cached, offset = size).
        """
        cache_key = (_point_to_key(point), rel_path)
        entry = self._cache.get(cache_key)
        if entry is not None:
            return entry
        # pyre-ignore[16]
        raw: tuple[int, int, int] | None = await self._actor_for(
            point
        ).stat_and_watch.call_one(rel_path)
        if raw is None:
            return None
        mtime_ns, size, mode = raw
        entry = _CacheEntry(mtime_ns=mtime_ns, mode=mode, data=bytearray(), offset=size)
        self._cache[cache_key] = entry
        return entry

    async def _fetch_range(
        self, point: dict[str, int], rel_path: str, offset: int, length: int
    ) -> bytes:
        if length <= 0:
            return b""
        actor = self._actor_for(point)
        if length < _RDMA_THRESHOLD:
            # pyre-ignore[16]
            return await actor.read_bytes.call_one(rel_path, offset, length)
        # RDMA path
        # pyre-ignore[16]
        token, rdma_buf, actual_len = await actor.prepare_rdma.call_one(
            rel_path, offset, length
        )
        if actual_len == 0 or rdma_buf is None:
            return b""
        mm = _mmap.mmap(-1, actual_len, _mmap.MAP_PRIVATE | _mmap.MAP_ANONYMOUS)
        try:
            mv: memoryview = memoryview(mm)[:actual_len]
            await rdma_buf.read_into(mv)
            data = bytes(mv)
            mv.release()
        finally:
            mm.close()
        # pyre-ignore[16]
        actor.release_rdma.broadcast(token)
        return data


# ── Helper functions ──────────────────────────────────────────────────────────


def _point_to_key(point: Mapping[str, int]) -> str:
    if not point:
        return ""
    return "_".join(f"{k}_{v}" for k, v in point.items())


def _synthetic_dir_stat() -> dict[str, object]:
    now = time.time()
    return {
        "st_mode": _stat.S_IFDIR | 0o555,
        "st_nlink": 2,
        "st_size": 0,
        "st_ctime": now,
        "st_mtime": now,
        "st_atime": now,
        "st_uid": os.getuid(),
        "st_gid": os.getgid(),
    }


def _make_stat(mode: int, size: int, mtime_ns: int) -> dict[str, object]:
    mtime = mtime_ns / 1e9
    ro_mode = mode & ~(_stat.S_IWUSR | _stat.S_IWGRP | _stat.S_IWOTH)
    return {
        "st_mode": ro_mode,
        "st_nlink": 1,
        "st_size": size,
        "st_ctime": mtime,
        "st_mtime": mtime,
        "st_atime": mtime,
        "st_uid": os.getuid(),
        "st_gid": os.getgid(),
    }


# ── Public API ────────────────────────────────────────────────────────────────


class GatherMount:
    """An active FUSE mount of remote shard file systems.

    Created and mounted immediately by :func:`gather_mount`.  Use as a context
    manager to have the mount closed automatically on exit, or call
    :meth:`close` explicitly.
    """

    def __init__(
        self,
        host_mesh: HostMesh,
        local_mount_point: str,
        remote_mount_point: Union[str, Callable[[Point], str]],
    ) -> None:
        self._local_mount_point = local_mount_point
        self._mounted: bool = False

        procs = host_mesh.spawn_procs(name="gather_mount")

        actors = procs.spawn(  # pyre-ignore[16]
            "GatherSourceActor", GatherSourceActor, remote_mount_point
        )
        client_actor = this_proc().spawn("GatherClientActor", GatherClientActor, actors)

        os.makedirs(local_mount_point, exist_ok=True)

        # Call init_watch on all source actors and wait for completion before
        # mounting — ensures inotify is ready before the first FUSE operation.
        # pyre-ignore[16]
        actors.init_watch.call(client_actor).get()

        # The Rust FUSE session runs on the shared Tokio runtime; it calls
        # back into client_actor for every filesystem operation.
        self._fuse_handle = mount_gather_fuse(client_actor, local_mount_point)
        self._mounted = True
        atexit.register(self.close)
        logger.info("gather_mount: mounted at %s", local_mount_point)

    def close(self) -> None:
        """Unmount the FUSE filesystem."""
        if not self._mounted:
            return
        self._mounted = False
        self._fuse_handle.unmount()
        logger.info("gather_mount: unmounted %s", self._local_mount_point)

    def __enter__(self) -> "GatherMount":
        return self

    def __exit__(self, *_: object) -> bool:
        self.close()
        return False


def gather_mount(
    host_mesh: HostMesh,
    local_mount_point: str,
    remote_mount_point: Union[str, Callable[[Point], str]],
) -> GatherMount:
    """Mount the file systems of hosts in a Monarch mesh as a local directory.

    The mount is live immediately on return.  Use the returned
    :class:`GatherMount` as a context manager to close it automatically, or
    call :meth:`~GatherMount.close` explicitly.

    Each host in *host_mesh* exposes its ``remote_mount_point`` as a
    read-only sub-directory of ``local_mount_point`` named after the host's
    mesh coordinates, e.g. ``machine_3_gpu_2``.  For a 0-dim mesh (single
    host) files are mounted directly inside ``local_mount_point`` with no
    sub-directory.

    Cached files are watched individually via inotify — no directory scans or
    recursive tree walks.  Stat entries are evicted and re-fetched only when
    inotify reports a change for that specific file.  Invalidation
    notifications are delivered directly to the local :class:`GatherClientActor`
    endpoint; no thread blocks waiting for events.

    Args:
        host_mesh: A :class:`~monarch.actor.HostMesh` providing the remote
            hosts.  One process per host is spawned internally.
        local_mount_point: Local directory path where the FUSE filesystem is
            mounted.  Created automatically if it does not exist.
        remote_mount_point: The absolute path (or a callable
            ``rank -> str``) that is served from each remote host.
            The callable receives the host's :class:`~monarch.actor.Point`:
            ``lambda rank: f"/tmp/data/gpu_{rank['gpu']}"``

    Returns:
        A :class:`GatherMount` that is already mounted.

    Example::

        with gather_mount(host_mesh, "/mnt/logs", "/var/log/training") as m:
            subprocess.run(["tail", "-f", "/mnt/logs/machine_0/train.log"])
    """
    return GatherMount(host_mesh, local_mount_point, remote_mount_point)
