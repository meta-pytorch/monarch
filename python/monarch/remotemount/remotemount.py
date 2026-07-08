# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-ignore-all-errors

from __future__ import annotations

import contextvars
import ctypes
import logging
import os
import subprocess
from typing import Any, Optional

from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionError
from monarch.actor import Actor, endpoint

logger: logging.Logger = logging.getLogger(__name__)

# Files >= this are "big" (libraries/data, on-demand); below are "code" (.py,
# configs) -- the small-first prefix that open() prefills.
BIG_FILE_THRESHOLD: int = 1 * 1024 * 1024

# The unit of on-demand delivery and block addressing: a file's bytes fall in
# block ``offset // BLOCK_SIZE``. Must match the Rust ``AVAILABILITY_BLOCK_SIZE``.
BLOCK_SIZE: int = 64 * 1024 * 1024


# ──────────────────────────────────────────────────────────────────────────
# Directory scan + persisted pack index (folded in from the former fast_pack
# module). ``build_index`` walks the source ONCE (without crossing FS
# boundaries) and turns it into the transfer layout and the FUSE meta in a
# single pass.
# ──────────────────────────────────────────────────────────────────────────

# Directory names never included in a mount: client-side state the workers do not
# need and that would churn the mount. ``.monarch`` is the job-state dir the CLI
# re-persists on every ``monarch exec``; mounting it makes every refresh observe
# a change and ship a block for nothing.
_MOUNT_EXCLUDED_DIRS: frozenset[str] = frozenset({".monarch"})


_ATTR_KEYS: tuple[str, ...] = (
    "st_atime",
    "st_ctime",
    "st_gid",
    "st_mode",
    "st_mtime",
    "st_nlink",
    "st_size",
    "st_uid",
)


# ──────────────────────────────────────────────────────────────────────────
# The v2 pure core: the per-file ``index`` dict doubles as the transfer layout
# (each regular-file node carries ``global_offset`` + ``file_len``), with
# stateless materialise and one FIFO block queue. All position-addressed,
# stat-only; the entire client state is O(files), never O(bytes) (it holds no
# file content).
# ──────────────────────────────────────────────────────────────────────────


def code_blocks(index: dict) -> set[int]:
    """The prefill set: every block backing at least one small (code) file,
    derived from ``index``'s file nodes (``global_offset`` + ``file_len``).
    Position-independent, so it stays correct under append-only repacking (new
    code appended at the tail is still prefilled). Big-lib-only blocks are left
    for on-demand delivery."""
    blocks: set[int] = set()
    for node in index.values():
        offset = node.get("global_offset")
        if offset is None:
            continue  # not a regular file (dir / symlink / the root total_size)
        size = node["file_len"]
        if size == 0 or size >= BIG_FILE_THRESHOLD:
            continue
        for blk in range(offset // BLOCK_SIZE, (offset + size - 1) // BLOCK_SIZE + 1):
            blocks.add(blk)
    return blocks


def build_index(source_path: str, previous: dict) -> dict:
    """Walk the source ONCE into a single ``index`` dict (vpath -> node) that is
    both the FUSE tree and the transfer layout: each file node carries ``attr``,
    ``file_len``, ``mtime_ns``, ``full_path`` and a ``global_offset``, and
    ``index["/"]["total_size"]`` is the packed size. ``materialise_block`` /
    ``code_blocks`` derive the block<->file map from it on demand.

    Pass ``previous={}`` for a cold pack, or the prior index to refresh. Two
    invariants the rest of the system relies on:
      - The walk does NOT cross filesystem boundaries (a foreign-``st_dev`` subdir
        becomes an empty leaf, not scanned) -- keeping an inner mount point out of
        the pack and avoiding a FUSE-in-FUSE deadlock.
      - Offsets are append-only vs ``previous``: an unchanged file keeps its
        ``global_offset`` so delivered blocks never move and a refresh invalidates
        nothing; new/changed files append block-aligned past the high-water mark
        (small files first, for prefill locality). A defrag is ``previous={}``.
    """
    # --- Walk the source tree (os.scandir + an explicit stack). A DirEntry types
    # each child without a stat, and recursion is opt-in -- we push only same-fs,
    # non-excluded dirs -- so an inner mount point (foreign st_dev) is recorded as
    # an empty leaf but never scanned (the FUSE-in-FUSE / huge-foreign-mount
    # guard). Each directory is stat'd once (the dev-probe stat is reused as its
    # attr when we descend). A regular file's offset is decided here: unchanged vs
    # ``previous`` -> keep it; otherwise queue it in ``appended`` for assignment
    # below. ---
    source_path = os.path.abspath(source_path)
    source_dev = os.stat(source_path).st_dev
    index: dict[str, Any] = {}
    appended: list[str] = []  # new/changed files -- offsets assigned after the walk
    stack: list[tuple[str, str, os.stat_result]] = [
        (source_path, "/", os.stat(source_path))
    ]
    while stack:
        abs_dir, vdir, dir_st = stack.pop()
        base = vdir.rstrip("/")  # "" at the root, else the dir's virtual path
        children: list[str] = []
        with os.scandir(abs_dir) as it:
            entries = sorted(it, key=lambda e: e.name)  # deterministic index
        for entry in entries:
            vpath = f"{base}/{entry.name}"
            if entry.is_symlink():
                children.append(entry.name)
                lst = entry.stat(follow_symlinks=False)
                index[vpath] = {
                    "attr": {key: getattr(lst, key) for key in _ATTR_KEYS},
                    "link_target": os.readlink(entry.path),
                }
            elif entry.is_dir():
                if entry.name in _MOUNT_EXCLUDED_DIRS:
                    continue  # client state (e.g. .monarch) -- never in the mount
                try:
                    st = entry.stat()  # one getattr; a mount point reports its dev
                except OSError:
                    logger.warning("build_index: cannot stat %r; skipping", entry.path)
                    continue
                children.append(entry.name)
                if st.st_dev == source_dev:
                    stack.append((entry.path, vpath, st))  # same fs -> descend
                else:  # inner mount point: empty leaf, contents NOT scanned
                    index[vpath] = {
                        "attr": {key: getattr(st, key) for key in _ATTR_KEYS},
                        "children": [],
                    }
                    logger.warning(
                        "build_index: skipping %r (st_dev=%d) -- different "
                        "filesystem than source %r (st_dev=%d); not packed.",
                        entry.path,
                        st.st_dev,
                        source_path,
                        source_dev,
                    )
            else:
                children.append(entry.name)
                lst = entry.stat(follow_symlinks=False)
                node: dict[str, Any] = {
                    "attr": {key: getattr(lst, key) for key in _ATTR_KEYS},
                    "file_len": lst.st_size,
                    "mtime_ns": lst.st_mtime_ns,
                    "full_path": entry.path,
                }
                pf = previous.get(vpath)
                if (
                    pf is not None
                    and pf.get("file_len") == lst.st_size
                    and pf.get("mtime_ns") == lst.st_mtime_ns
                ):
                    node["global_offset"] = pf[
                        "global_offset"
                    ]  # unchanged -> keep block
                else:
                    appended.append(vpath)  # new/changed -> offset assigned below
                index[vpath] = node
        index[vdir] = {
            "attr": {key: getattr(dir_st, key) for key in _ATTR_KEYS},
            "children": children,
        }

    # --- Assign new/changed files block-aligned offsets past the previous
    # high-water mark, small (code) files first for prefill locality -- the whole
    # of "append-only": kept files never move, so a worker's already-delivered
    # blocks stay valid across a refresh (cost: up to one block of dead space per
    # append generation, reclaimed by a fresh ``previous={}`` defrag). ---
    offset = previous.get("/", {}).get("total_size", 0)
    if appended and offset % BLOCK_SIZE != 0:
        offset = (offset // BLOCK_SIZE + 1) * BLOCK_SIZE  # block-align
    # Small (code) files take the front blocks (so they get prefilled); big files
    # follow. The walk already grouped each directory's files and is deterministic,
    # so this size split is all the ordering the prefill needs -- no sort.
    small = [v for v in appended if index[v]["file_len"] < BIG_FILE_THRESHOLD]
    big = [v for v in appended if index[v]["file_len"] >= BIG_FILE_THRESHOLD]
    for vp in small + big:
        index[vp]["global_offset"] = offset
        offset += index[vp]["file_len"]
    index["/"]["total_size"] = offset

    # st_atime reflects the source's last *read* time, which changes whenever the
    # client materialises a file -- making the index non-deterministic. The mount
    # is read-only (atime is cosmetic), so normalise it to st_mtime; the index is
    # then a pure function of the source's content + structure, which lets a
    # refresh detect "nothing changed" with a plain ``index == previous``.
    for node in index.values():
        attr = node.get("attr")
        if attr is not None:
            attr["st_atime"] = attr["st_mtime"]
    return index


def materialise_block(index: dict, block: int) -> tuple[bytes, list[str]]:
    """Re-read block ``block`` from the source into a fixed ``BLOCK_SIZE`` buffer
    (every block is the same length; the final block's tail past ``total_size``,
    and the gaps between files, stay zero). The fixed size lets a downstream
    transport move uniform chunks (e.g. a fixed-size RDMA buffer) at the cost of
    trailing zeros on the last block.

    Returns ``(bytes, diverged)``: the block buffer (``bytes``, not the working
    ``bytearray`` -- the actor message bus rejects ``bytearray`` with "cannot be
    converted to PyBytes", so that copy is load-bearing), and the list of vpaths
    whose source diverged under the fence. The freshness fence is PER FILE, not per
    block: a diverged file's bytes in the block are overwritten with random garbage
    (never its changed content) and its vpath returned, so the caller marks just that
    file stale (its reads then EIO) while co-located unchanged files keep their real
    bytes and serve normally. A ``block`` past the layout end raises ``ValueError``:
    ``total_size`` only grows within a mount, so an out-of-range id is a stale-index /
    wrong-id bug, not a benign no-op."""
    total_size = index["/"]["total_size"]
    block_start = block * BLOCK_SIZE
    if block_start >= total_size:
        raise ValueError(
            f"block {block} is past the layout end (offset {block_start} >= "
            f"total_size {total_size}); stale index or wrong block id"
        )
    block_end = block_start + BLOCK_SIZE
    buf = bytearray(BLOCK_SIZE)  # fixed size; gaps and the tail past total_size stay 0
    mv = memoryview(buf)
    diverged: list[str] = []
    for vpath, node in index.items():
        off = node.get("global_offset")
        if off is None:
            continue
        lo = max(off, block_start)
        hi = min(off + node["file_len"], block_end)
        if lo >= hi:
            continue  # this file does not touch the block
        dst = mv[lo - block_start : hi - block_start]
        full_path = node["full_path"]
        # Reproduce the file's fenced bytes, guarded by the size+mtime fence (anything
        # detectably != X cannot be served as X; a same-size+same-mtime content edit
        # is the accepted residual).
        ok = False
        try:
            st = os.stat(full_path, follow_symlinks=False)
            if st.st_size == node["file_len"] and st.st_mtime_ns == node["mtime_ns"]:
                with open(full_path, "rb", buffering=0) as fh:
                    fh.seek(lo - off)
                    ok = fh.readinto(dst) == hi - lo
        except OSError:
            ok = False
        if not ok:
            # Diverged (changed / vanished / short read): overwrite its range with
            # random garbage so no stale or torn bytes can leak, and report it so the
            # caller marks the file stale. The garbage is never served -- a read of a
            # stale file EIOs -- it just hardens against an EIO-check bypass.
            diverged.append(vpath)
            dst[:] = os.urandom(hi - lo)
    return bytes(buf), diverged


def _point_to_key(point: dict) -> str:
    if not point:
        return ""
    return "_".join(f"{k}_{v}" for k, v in point.items())


def prepare_mount_point(path: str) -> None:
    """Create the mount point directory, recovering from dead FUSE mounts.

    ``os.makedirs(exist_ok=True)`` raises ``FileExistsError`` when the path
    exists but is a stale FUSE mount (``os.path.isdir`` returns False on
    dead mounts).  Detect this case, unmount, and retry.
    """
    try:
        os.makedirs(path, exist_ok=True)
    except FileExistsError:
        # May be a dead FUSE mount — try to clean it up.
        result = subprocess.run(
            ["fusermount3", "-u", "-z", path], capture_output=True, text=True
        )
        if result.returncode != 0:
            # Not a FUSE mount or unmount failed — try plain umount.
            subprocess.run(["umount", "-l", path], capture_output=True)
        os.makedirs(path, exist_ok=True)


def _resolve_path(path: str) -> str:
    """Replace ``$SUBDIR`` with this actor's mesh-coordinate key, if present."""
    if "$SUBDIR" not in path:
        return path
    from monarch.actor import context

    rank = context().actor_instance.rank
    return path.replace("$SUBDIR", _point_to_key(dict(rank)))


class FUSEActor(Actor):
    """The worker side of a mount: it owns the FUSE handle, signals the blocks
    its FUSE reads are blocked on to the client, and delivers the bytes the client
    materialises into the mount's own block buffers.

    Delivery is a leader-mediated broadcast. A read fault fires the callback built in
    ``mount``, which asks the client's ``enqueue`` for the block; the client
    materialises it and sends it ONCE to the leader worker over the actor bus
    (``broadcast``). Each worker gets buffers from its Rust mount (``block_ptr``, which
    reserves them lazily) and wraps them as memoryviews; the RDMA write lands directly in
    that mount-owned memory, and ``receive_block`` freezes it into the served block with
    no copy. The leader stages the block into a buffer, commits it, and RDMA-fans that
    same buffer out to the peers over InfiniBand; each peer reads it into a buffer of its
    own (``receive_rdma``) and commits it. So the venv crosses the DC boundary once
    (client->leader, over the bus) and the bulk fan-out rides the fast intra-cluster
    fabric.
    """

    def __init__(self, handler):
        self._fuse_handle = None
        # The MountHandlerClient actor handle, passed in at spawn (by ``open``).
        # On a fault the Rust mount fires a callback (built in ``mount``) that calls
        # ``handler.enqueue`` to queue the faulted blocks for delivery.
        self._handler = handler
        # Delivery is keyed by block id: the mount owns each block's staging buffer
        # (``block_ptr(block_id)`` provisions it lazily), the RDMA write lands in it, and
        # ``receive_block(block_id, ...)`` freezes it into the served block with no copy.
        # On the leader, ``broadcast`` wraps a block's buffer in a per-call ``RDMABuffer``
        # fan-out source and drops it once the (synchronous) fan-out completes, so at most
        # one block's memory is RDMA-registered at a time -- not one MR per delivered block
        # held for the mount's life. ``_peers`` is the peer sub-mesh, set by
        # ``setup_leader`` (``None`` until then, and on non-leader workers).
        self._peers = None
        self._is_leader = False

    def _wrap(self, block_id: int) -> memoryview:
        """A writable memoryview over block ``block_id``'s mount-owned staging buffer (a
        ctypes view of the Rust pointer; ``block_ptr`` provisions the buffer lazily). RDMA
        / staging writes land directly in the mount's memory; ``receive_block`` then
        freezes it into the served block with no copy."""
        addr = self._fuse_handle.block_ptr(block_id)
        return memoryview((ctypes.c_char * BLOCK_SIZE).from_address(addr)).cast("B")

    @endpoint
    def mount(self, mount_point, meta):
        """Mount the FUSE filesystem from ``meta`` (the full directory tree).

        The Rust mount starts with no block data; every block faults in on
        demand (its bytes are delivered via ``receive_block`` and held in memory).
        This FUSEActor is freshly spawned per open (``close`` tears the previous
        mesh down), so it never holds a prior handle. The total size is read from
        ``meta["/"]["total_size"]``.
        """
        mount_point = _resolve_path(mount_point)
        from monarch._rust_bindings.monarch_extension.chunked_fuse import (
            mount_chunked_fuse,
        )

        assert self._fuse_handle is None, "FUSEActor already holds a fuse handle"
        # Build the fault callback the Rust mount fires (briefly under the GIL)
        # when a read faults a new block: it calls the MountHandlerClient ``enqueue``
        # endpoint with the single faulted block to queue for broadcast delivery.
        # Run in a copy of this endpoint's actor context so the endpoint call
        # routes correctly when fired off the Rust thread; ``broadcast`` is
        # fire-and-forget (no reply to await).
        handler = self._handler
        cb_ctx = contextvars.copy_context()

        def _fault_callback(block):
            if handler is not None:
                cb_ctx.run(lambda: handler.enqueue.broadcast(int(block)))

        self._fuse_handle = mount_chunked_fuse(
            meta,
            meta["/"]["total_size"],
            mount_point,
            _fault_callback,
        )

    @endpoint
    def refresh_mount(self, meta):
        """Refresh FUSE mount data without unmounting.

        Atomically swaps ``meta`` + size into the running FUSE filesystem.
        Open file handles remain valid and subsequent reads see the new data.
        Append-only: blocks already delivered (held in memory) stay valid, so
        this is just a metadata swap. The total size is read from
        ``meta["/"]["total_size"]``, which the layout build records there.
        """
        if self._fuse_handle is None:
            raise RuntimeError("no active mount to refresh")
        self._fuse_handle.refresh(meta, meta["/"]["total_size"])

    @endpoint
    def setup_leader(self, peers) -> None:
        """Promote this worker to the leader and store the peer sub-mesh (``None`` for a
        single-host mount with no peers). The client sends each block to the leader
        (``broadcast``); the leader wraps that block's buffer in an ``RDMABuffer`` (lazily,
        per block) and mesh-casts it to ``peers`` over RDMA. Peers need no ``RDMABuffer``
        -- ``read_into`` takes a plain memoryview as the local dest."""
        self._peers = peers
        self._is_leader = True

    @endpoint
    def broadcast(self, block_id, data, stale) -> None:
        """Leader entry point: the client sent this block's bytes over the actor bus
        (the one cross-DC copy). Stage them into block ``block_id``'s mount-owned buffer,
        commit it as the leader's own block via ``receive_block`` (zero-copy freeze), then
        wrap that (now committed) buffer as an RDMA fan-out source and mesh-cast
        ``receive_rdma`` to the peer sub-mesh so they RDMA-read it. Every block is a full
        BLOCK_SIZE buffer (the last block zero-padded past ``total_size``). ``stale`` (the
        vpaths that diverged under the fence) rides along so the mount marks them
        atomically. The cast's ``.get()`` means the block is delivered everywhere before
        this returns."""
        assert self._fuse_handle is not None, "broadcast on an unmounted FUSEActor"
        stale_s = [str(p) for p in stale]
        bid = int(block_id)
        mv = self._wrap(bid)
        mv[:] = data  # stage into the mount-owned buffer
        self._fuse_handle.receive_block(bid, stale_s)  # zero-copy freeze into blocks
        if self._peers is None:
            return  # single-host mount: block committed locally, no peers to fan out to
        from monarch.rdma import RDMABuffer

        # The freeze is zero-copy (same address), so wrap the now-committed block as the
        # RDMA fan-out source.
        rdma = RDMABuffer(mv)
        # One mesh-cast: monarch tree-fans ``receive_rdma`` to every peer over its comm
        # tree; ``.get()`` blocks until all peers have read the block. A failing cast is a
        # real delivery error -- let it propagate.
        self._peers.receive_rdma.call(rdma, block_id, stale_s).get()
        # Free the fan-out MR: RDMABuffer has no finalizer and the manager does no liveness
        # GC, so an explicit drop keeps the leader at one pinned MR at a time. (On the error
        # path above this is skipped; that MR is reclaimed when the mount tears down.)
        rdma.drop().get()

    @endpoint
    def receive_rdma(self, source, block_id, stale) -> None:
        """Peer entry point: RDMA-read the block from the leader's buffer (``source``)
        directly into block ``block_id``'s mount-owned buffer over InfiniBand, then commit
        it as this worker's block via ``receive_block`` (a zero-copy freeze -- no
        ``bytes()`` copy). Every block is a full BLOCK_SIZE buffer, delivered as-is."""
        assert self._fuse_handle is not None, "receive_rdma on an unmounted FUSEActor"
        bid = int(block_id)
        mv = self._wrap(bid)
        source.read_into(mv).get()
        self._fuse_handle.receive_block(bid, [str(p) for p in stale])

    @endpoint
    def mkdir(self, path):
        """Create a directory on the worker."""
        prepare_mount_point(_resolve_path(path))

    @endpoint
    def unmount(self, mount_point):
        """Unmount a FUSE filesystem.

        Returns (status, detail) where status is one of:
          "ok"          — unmounted successfully
          "not_mounted" — path was not a mountpoint (nothing to unmount)
          "busy"        — mountpoint is in use by another process
          "error"       — unexpected failure
        """
        mount_point = _resolve_path(mount_point)
        check = subprocess.run(
            ["mountpoint", "-q", mount_point],
            capture_output=True,
        )
        if check.returncode != 0:
            return "not_mounted", ""

        result = subprocess.run(
            ["fusermount3", "-u", mount_point], capture_output=True, text=True
        )
        if result.returncode == 0:
            return "ok", ""
        if "busy" in result.stderr.lower():
            return "busy", result.stderr.strip()
        return "error", result.stderr.strip()


class MountHandlerClient(Actor):
    def __init__(
        self,
        host_mesh,
        sourcepath: str,
        mntpoint: Optional[str] = None,
    ):
        self.sourcepath = os.path.abspath(sourcepath)
        if mntpoint is None:
            mntpoint = self.sourcepath
        self.mntpoint = os.path.abspath(mntpoint)
        self.fuse_actors = None
        self.host_mesh = host_mesh
        self.procs = None
        # The client state: the ``index`` dict that ``build_index`` produces
        # (file -> offset/size + tree). It holds no file content; blocks are
        # materialised on demand from the source by ``_deliver``, and it is the
        # append-only baseline for the next refresh.
        self.index: Optional[dict] = None
        # The block ids delivered to the current worker mesh, so a re-fault for a block
        # the workers already hold is a no-op instead of a redundant re-delivery.
        # ``open`` resets it when it spawns a fresh mesh (a re-open re-delivers); within
        # a mount it is never cleared. See ``_deliver`` for why this is safe.
        self._delivered: set[int] = set()
        # The leader FUSEActor slice, wired at open(). The client sends each block to
        # the leader over the actor bus; the leader RDMA-fans it out to the peers.
        self._leader = None

    def _deliver(self, block: int) -> None:
        """Materialise ``block`` once and deliver it to every worker, then remember it
        so a re-fault for it is a no-op.

        Delivery ships the block to the leader over the actor bus; the leader hands it
        to its own mount and RDMA-fans it out to the peers (``broadcast``). ``.get()``
        awaits the whole fan-out, so a returned call means every worker holds the block
        -- which is what makes the ``_delivered`` set safe: blocks live in worker memory
        for the life of the mount, and ``open`` clears the set when it (re-)spawns the
        mesh, so we never re-deliver a block the current workers already hold. Dedup is
        by block, so a cross-worker fault storm for one block collapses to one delivery.
        Delivery is synchronous (one block at a time); overlapping deliveries would be a
        follow-up.

        A file that diverged under the fence can't be reproduced: ``materialise_block``
        garbage-fills its bytes and returns its vpath, shipped in the SAME call so the
        mount marks it stale (its reads EIO) while co-located fresh files serve. An
        out-of-range block raises ``ValueError`` in ``materialise_block``; ``enqueue``
        logs it."""
        if block in self._delivered:
            return
        assert self.index is not None and self._leader is not None
        data, stale = materialise_block(self.index, block)
        if stale:
            logger.warning(
                "block %s: %d file(s) diverged under the fence, delivered stale: %s",
                block,
                len(stale),
                stale,
            )
        # Ship the block to the leader over the actor bus (the one cross-DC copy); the
        # leader delivers it locally and RDMA-fans it out to the peers. Record it
        # delivered only AFTER the call returns, so a failed delivery is not remembered.
        self._leader.broadcast.call_one(block, data, stale).get()
        self._delivered.add(block)

    @endpoint
    def enqueue(self, block) -> None:
        """Endpoint the workers call (fire-and-forget, from their fault callback) to
        deliver a single faulted block -- a FUSE read faults one block at a time (a
        straddling read re-faults the next on its following pass). Synchronous: the
        actor processes one delivery at a time, so a cross-worker fault storm for one
        block collapses against the permanent ``_delivered`` set -- the first delivery
        broadcasts the block to every worker and records it, the rest see it delivered
        and skip -- without needing the async interleaving the old in-flight dedup
        relied on. Same delivery path as the prefill (open) and refresh.

        Failures degrade the mount rather than abort this MountHandlerClient actor
        (an uncaught fault here kills the sidecar and wedges every other worker): a
        worker dying/preempted mid-delivery surfaces as ``SupervisionError`` and is
        logged; source divergence is handled in ``_deliver`` (it marks the diverged
        files stale so their reads get EIO); any other error is logged with its
        traceback."""
        try:
            self._deliver(int(block))
        except SupervisionError:
            logger.warning("delivery stopping: workers no longer reachable")
        except Exception:
            logger.exception("delivery failed for block %s", block)

    @endpoint
    def open(self, self_handle):
        """Spawn the workers, build the index, mount, wire the RDMA fan-out, and
        deliver the code prefix, then RETURN. The index (the whole tree) ships first
        as a 0-block ``find``; the small code blocks are delivered here; the big
        libraries stream in on demand when an import faults them.

        ``self_handle`` is this actor's own handle, passed by the caller (which
        holds it) so the workers can be spawned with it -- an actor can't obtain a
        callable handle to itself, so it is threaded in here.
        """
        # Spawn a fresh worker FUSEActor mesh for this mount; ``close`` tears the
        # previous one down, so each open starts clean (the workers hold blocks
        # only in memory, so a re-mount has nothing to reuse). The workers are
        # spawned with ``self_handle`` so their fault callbacks reach ``enqueue``.
        # The fresh mesh holds nothing, so reset the delivered set: a re-open must
        # re-deliver every block (the previous mesh's in-memory blocks are gone).
        self._delivered.clear()
        self.procs = self.host_mesh.spawn_procs()
        self.fuse_actors = self.procs.spawn("FUSEActor", FUSEActor, self_handle)
        self.fuse_actors.mkdir.call(self.mntpoint).get()
        # Build the index from a fresh walk of the source (a cold full pack: the
        # workers hold blocks only in memory, so there is no prior index to extend).
        self.index = build_index(self.sourcepath, {})

        # Mount with the full tree: a 0-block ``find`` works immediately; data faults in
        # afterwards.
        self.fuse_actors.mount.call(self.mntpoint, self.index).get()

        # Rank 0 is the leader; the rest are its RDMA fan-out peers. ``setup_leader`` hands
        # the leader the peer sub-mesh (ranks 1..n-1 as one mesh, so it can mesh-cast the
        # fan-out), or ``None`` for a single-host mount with no peers.
        flat = self.fuse_actors.flatten("rank")
        self._leader = flat.slice(rank=0)
        peers = flat.slice(rank=slice(1, None)) if flat.size("rank") > 1 else None
        self._leader.setup_leader.call_one(peers).get()

        # Deliver the code blocks (the small-file region), then return. Big files
        # (libraries, data) stream in on demand when a worker's read faults them
        # -> the fault callback -> ``enqueue`` -> the same ``_deliver``.
        for b in code_blocks(self.index):
            self._deliver(b)

    @endpoint
    def close(self) -> None:
        """Unmount the workers' FUSE mounts, then tear the worker procs down. A
        subsequent open() spawns a fresh FUSEActor mesh; nothing is reused across
        an open/close cycle (in-memory blocks do not survive a re-mount anyway).
        """
        if self.fuse_actors is not None:
            result = self.fuse_actors.unmount.call(self.mntpoint).get()
            for _point, (status, detail) in result:
                if status not in ("ok", "not_mounted"):
                    logger.warning(f"unmount failed ({status}): {detail}")
            # Stop the proc mesh -- this also stops the FUSEActors on it -- so the
            # workers are freed and the next open() spawns a clean, fresh mesh.
            self.procs.stop().get()
            self.fuse_actors = None
            self.procs = None
        self._leader = None

    @endpoint
    def refresh(self):
        """Re-sync a live mount to the current source, without unmounting.

        Rebuilds the index append-only (unchanged files keep their block ids;
        changed/new files are appended at fresh, block-aligned tail ids), ships
        the new tree, and atomically swaps the new tree + size into the running
        FUSE (``refresh_mount``). Open file handles stay valid; subsequent reads
        -- even through a handle opened before the refresh -- see the new data.

        Block-aligned appends mean an existing block id's content never changes,
        so the worker's already-delivered (in-memory) blocks stay valid and there
        is nothing to invalidate. The new tail blocks are NOT pushed here -- they
        fault in on demand on the next read, like every other block (open's code
        prefill is the only proactive delivery). So a refresh is just a metadata
        swap; even a big change costs nothing until something reads it.

        The actor already holds ``self.sourcepath`` from open(), so (unlike the
        plain-class version) the caller cannot pass a sourcepath to cross-check.
        """
        if self.fuse_actors is None:
            raise RuntimeError("no active mount to refresh; call open() first")

        new_index = build_index(self.sourcepath, self.index)

        # Guard the transport on the index: it is the single source of truth for
        # "did anything change". If it is identical to the last sync, the workers
        # are already current -- skip the ship + refresh_mount. Building the index
        # is cheap (in-memory); shipping it to every worker is the expensive part
        # this avoids on a no-op.
        if new_index == self.index:
            return
        self.index = new_index
        # Swap the new tree/size into the live FUSE. The new tail blocks are absent
        # from the workers' in-memory block maps, so the next read of a changed/new
        # file faults them in on demand (enqueue -> _deliver); refresh pushes
        # nothing.
        self.fuse_actors.refresh_mount.call(new_index).get()


class MountHandler:
    """Owner-facing handle returned by ``remotemount``: a thin wrapper that drives
    the spawned ``MountHandlerClient`` actor through plain method calls, so callers
    use the same ``handler.open()`` / ``close()`` / ``refresh()`` form as the other
    mount handlers instead of ``handler.open.call_one(handler).get()``. The actor
    is the remote surface (the worker FUSEActors call its ``enqueue`` on a fault);
    this wrapper is the local owner's control surface.
    """

    def __init__(
        self,
        host_mesh: object,
        sourcepath: str,
        mntpoint: Optional[str] = None,
    ) -> None:
        # MountHandlerClient is an Actor: it must be SPAWNED (so its endpoints get
        # an actor context, and the worker FUSEActors can call its ``enqueue`` on a
        # fault), not constructed. Spawn it on a 1-proc client-side mesh; the spawn
        # returns an actor-mesh handle, which this wrapper drives.
        from monarch.actor import this_host

        # Kept so the job-sidecar mount-config layer can identify the mount: it logs
        # ``handler.sourcepath`` / ``handler.mntpoint`` on a refresh/close error.
        self.sourcepath = sourcepath
        self.mntpoint = mntpoint
        client_procs = this_host().spawn_procs()
        self._client = client_procs.spawn(
            "MountHandlerClient",
            MountHandlerClient,
            host_mesh,
            sourcepath,
            mntpoint,
        )

    def open(self) -> None:
        """Spawn the workers, mount, wire the RDMA fan-out, and deliver the prefill."""
        # ``open`` takes the client's own handle (to spawn the FUSEActors with),
        # which an actor cannot obtain for itself -- so pass ``self._client`` in.
        self._client.open.call_one(self._client).get()

    def close(self) -> None:
        """Unmount the workers' FUSE mounts and tear the workers down."""
        self._client.close.call_one().get()

    def refresh(self, sourcepath: Optional[str] = None) -> None:
        """Re-sync the live mount to the current source, without unmounting.

        ``sourcepath`` is accepted for the mount-config interface (which calls
        ``handler.refresh(handler.sourcepath)``) but is unused -- the spawned actor
        already holds its own source path from ``open()``.
        """
        self._client.refresh.call_one().get()


def remotemount(
    host_mesh: object,
    sourcepath: str,
    mntpoint: Optional[str] = None,
) -> MountHandler:
    """Mount a local directory on remote hosts via FUSE, delivered on demand.

    The full directory tree (the FUSE meta) ships immediately, small "code"
    files are prefilled, and big libraries/data stream in when a read faults
    them. Each block is delivered as a leader-mediated broadcast: the client sends
    it to the leader worker over the actor bus, and the leader RDMA-fans it out to
    the peers over InfiniBand, so the venv crosses the DC boundary once.
    ``refresh()`` advances the freshness fence.
    """
    return MountHandler(host_mesh, sourcepath, mntpoint)
