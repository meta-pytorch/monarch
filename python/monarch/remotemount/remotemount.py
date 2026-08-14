# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-ignore-all-errors

from __future__ import annotations

import contextvars
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

# The chain-broadcast frame size: how much of a block each chunk carries down the
# pipeline. Smaller = finer within-stream pipelining (each stream keeps several
# chunks in flight, overlapping its send with the relay of the previous one);
# larger = fewer per-chunk messages (serialization + hyperactor routing + TLS
# records). At the default 128 streams, sweeping this over a 64 MiB block puts the
# knee at 64 KiB (1024 chunks, 8 per stream): the cold-import ship fell ~60 -> 41s
# (116 blocks) vs 512 KiB, and below 64 KiB per-chunk overhead (~2048 msgs/block at
# 32 KiB) overtakes the gain. Validated as the default across 1..1024 hosts.
CBC_CHUNK: int = 64 * 1024

# Number of parallel streams per chain hop. The client passes this to BOTH its head
# dial and every worker's ``forward`` (chosen once at ``open``), so one value tunes
# the whole chain; the Rust ``NUM_CBC_STREAMS`` pyo3 default is only the fallback for
# callers that omit the argument (this path always passes it explicitly). More
# streams fill a fatter cross-DC pipe (N congestion windows summing toward the
# bandwidth-delay product), fewer cut per-stream ramp overhead. 128 is the measured
# default: raising 32 -> 128 shaved >10s off the single-host deliver wall. 256 is a
# hard ceiling -- that many concurrent TLS handshakes overrun a single worker's
# accept path (connection resets -> block reassembly fails -> supervision death) --
# so 128 is the top of the safe range, validated across 1..1024 hosts.
CBC_STREAMS: int = 128


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


def materialise_block(
    index: dict, block: int, buf: bytearray
) -> tuple[bytes, list[str]]:
    """Re-read block ``block`` from the source into ``buf``, a caller-owned
    ``BLOCK_SIZE`` ``bytearray`` reused across calls (the client keeps one per mount
    instead of allocating + zeroing a fresh 64 MiB ``bytearray`` per delivery). Every
    block is the same length and only the ranges files occupy are (re)written, so
    ``buf`` may still carry the previous block's bytes in the inter-file gaps and the
    tail past ``total_size`` -- harmless, because those positions map to no file and a
    FUSE read is clamped to its file's size, so no read ever serves them. The fixed
    size lets a downstream transport move uniform chunks (e.g. a fixed-size receive
    buffer).

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
    """The worker side of a mount: it owns the FUSE handle, signals the blocks its FUSE
    reads are blocked on to the client, and receives the bytes the client materialises
    into its mount's own block buffers.

    A read fault fires the callback built in ``mount``, which calls ``enqueue`` on this
    worker's fault-sink handle for the faulted block. In production that handle is the
    ``RemoteMountLeader`` broker (one per mount, on the leader worker), not the client: it dedups
    the cross-worker fault storm and forwards each block to the client's own ``enqueue`` at
    most once. The client materialises the block and sources it ONCE straight down a
    pipelined broadcast chain it dials (client -> w[0] -> ... -> w[N-1]). Each worker gets
    a buffer from its Rust mount (``block_ptr``, which reserves it lazily), assembles the
    chain-delivered block straight into it, and ``receive_block`` freezes it into the
    served block with no copy. So the venv crosses the DC boundary once (client -> w[0])
    and the fan-out rides the chain, whose source egresses ~one copy regardless of worker
    count.
    """

    def __init__(self, handler: RemoteMountLeader | None):
        self._fuse_handle = None
        # The fault-sink handle this worker reports read faults to, via ``handler.enqueue``
        # in the callback built by ``mount``. In production it is the ``RemoteMountLeader`` (which
        # dedups and forwards to the client); because the broker exposes the same
        # ``enqueue`` as the client, this actor does not care which it is talking to.
        # ``None`` in tests that never fault.
        self._handler = handler
        # Chain-broadcast recv state, set at open: ``_cbc_server`` is this worker's
        # bound metatls listener (the predecessor dials it); ``_cbc_ctx`` is the
        # byte-counter recv-completion ctx the relay delivers each block into.
        self._cbc_server = None
        self._cbc_ctx = None

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
        # Build the fault callback the Rust mount fires (briefly under the GIL) when a
        # read faults a new block: it calls ``enqueue`` on this worker's fault-sink handle
        # -- the ``RemoteMountLeader`` in production, which dedups it and forwards it to the
        # client's ``enqueue`` at most once, so an N-worker fault storm for one block
        # collapses to a single cross-DC request. Run in a copy of this endpoint's actor
        # context so the call routes correctly when fired off the Rust thread;
        # ``broadcast`` is fire-and-forget (no reply to await). ``handler is None`` (tests
        # that never fault) makes the callback a no-op.
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
    def cbc_listen(self, bind_addr: str | None = None):
        """Bind this worker's chain listener (ephemeral port) and create its recv ctx.
        Returns ``(rank, addr)`` so ``open`` can order the workers into a chain. The
        rank is the actor's LINEAR rank, not a coordinate: it is dense over the mesh
        (so it indexes ``addrs`` directly) and it is defined even for a mesh with no
        named dimensions, which is what ``spawn_procs()`` yields without ``per_host``.
        ``bind_addr`` chooses the transport: ``None`` defers to monarch's process-wide
        default transport (the ``default_transport`` / ``HYPERACTOR_MESH_DEFAULT_TRANSPORT``
        knob), which in cluster is metatls -- reusing monarch's own x509 identity, no cert
        paths to read; an OSS / test caller flips that knob to e.g. tcp, or passes an
        explicit hyperactor channel address here (e.g. ``tcp![::]:0``)."""
        from monarch._rust_bindings.monarch_extension.chain_broadcast import (
            new_ctx,
            serve,
        )
        from monarch.actor import context

        rank = context().actor_instance.rank.rank
        self._cbc_server = serve(bind_addr)
        self._cbc_ctx = new_ctx(BLOCK_SIZE)
        return rank, self._cbc_server.addr

    @endpoint
    def cbc_start(self, addrs, num_streams: int = CBC_STREAMS) -> None:
        """Start this worker's chain relay. ``addrs`` is the full rank-ordered list of
        worker listener addresses; this worker forwards each chunk to its successor
        (the next rank, or ``None`` if it is the tail) the instant it lands, and
        delivers it into the recv ctx. ``num_streams`` is how many parallel streams
        this node opens to its successor (the client picks it at open, so the whole
        chain uses one tuned value). Runs on the tokio runtime; returns at once."""
        from monarch._rust_bindings.monarch_extension.chain_broadcast import forward
        from monarch.actor import context

        rank = context().actor_instance.rank.rank
        successor = addrs[rank + 1] if rank + 1 < len(addrs) else None
        forward(self._cbc_server, successor, self._cbc_ctx, num_streams)

    @endpoint
    def receive_via_cbcast(self, block_id, stale, nbytes) -> None:
        """Worker entry point: wait for block ``block_id``'s ``nbytes`` to arrive down the
        pipelined chain, assembling them straight into the block's mount-owned buffer, then
        commit via ``receive_block``. The bytes flow over the chain (leader -> w[0] -> ... ->
        w[N-1]) rather than N RDMA reads from one leader."""
        assert self._fuse_handle is not None, (
            "receive_via_cbcast on an unmounted FUSEActor"
        )
        from monarch._rust_bindings.monarch_extension.chain_broadcast import (
            ctx_wait_into,
        )

        bid = int(block_id)
        want = int(nbytes)
        addr = self._fuse_handle.block_ptr(bid)
        ctx_wait_into(self._cbc_ctx, addr, BLOCK_SIZE, want, 120000)
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


class RemoteMountLeader(Actor):
    """The single coordinator for a mount, spawned once on the leader worker (rank 0). It
    is the hub for both directions of client<->worker communication:

    Fault requests (workers -> client): it is handed to every FUSEActor as that actor's
    fault sink, in the client's place, and exposes the same ``enqueue`` interface as the
    ``MountHandlerClient`` -- so a worker's fault callback calls ``handler.enqueue`` without
    knowing whether it is the broker or the client. It forwards each block to the client's
    own ``enqueue`` at most once, collapsing an N-worker fault storm (worker -> broker is
    intra-cluster; broker -> client is the one cross-DC hop).

    Block delivery (client -> workers): the CLIENT is the chain source -- it
    ``send_block``s each block straight down the pipelined chain (client -> w[0] -> ... ->
    w[N-1]), each interior worker forwarding every chunk to its successor the instant it
    lands, so the cross-DC ship rides the multi-stream chain (not a single actor-bus hop)
    and the source egresses ~one copy regardless of N. The broker's only role in delivery
    is ``await_block``: an intra-cluster barrier that waits for every worker to receive +
    commit a block, so the N-worker gather never crosses the DC. The broker always has at
    least one FUSEActor; a mount with none would have no reason to exist.
    """

    def __init__(self, handler: MountHandlerClient | None):
        # The MountHandlerClient handle, to forward deduped requests to its ``enqueue``.
        self._handler = handler
        # Block ids already forwarded to the client. A re-fault for one (from any worker,
        # including a post-delivery re-read) is dropped, collapsing the storm to a single
        # cross-DC call. Fresh per open (the broker is respawned with the worker mesh).
        self._requested: set[int] = set()
        # The worker mesh, set by ``set_fuse_actors`` at open (the broker is spawned before
        # it). ``send`` sources each block into the chain and waits for every worker's
        # ``receive_via_cbcast``; there is always at least one (else there would be no mount).
        self._fuse_actors = None

    @endpoint
    def enqueue(self, block) -> None:
        """The workers' fault sink, with the same signature as the client's ``enqueue``.
        Forward the block to the client's ``enqueue`` at most once; a re-request for an
        already-forwarded block is dropped, collapsing the cross-worker storm to one
        cross-DC call. Fire-and-forget, like the fault callback that drives it. The
        client's ``_delivered`` set is the delivery dedup, and a failed delivery clears
        this marker (``clear_request``) so a re-fault re-requests -- mirroring the client's
        rule that a failed delivery is not remembered."""
        bid = int(block)
        if bid in self._requested:
            return
        self._requested.add(bid)
        self._handler.enqueue.broadcast(bid)

    @endpoint
    def clear_request(self, block) -> None:
        """Drop a forwarded-block marker so its block can be requested again. The client
        calls this when a delivery fails: without it the broker would suppress every
        re-fault for the block, leaving it undelivered for the mount's life."""
        self._requested.discard(int(block))

    @endpoint
    def set_fuse_actors(self, fuse_actors) -> None:
        """Store the worker mesh (the broker is spawned before it exists). ``send`` fans
        each delivered block out to all of these -- there is always at least one."""
        self._fuse_actors = fuse_actors

    @endpoint
    def await_block(self, block_id, stale, nbytes) -> None:
        """Barrier for one client-sourced block: wait until every worker has received it
        off the chain (the client is the source) and committed it via ``receive_via_cbcast``.
        ``.get()`` blocks until all workers hold it, so delivery stays synchronous (one
        block in flight) and the client may reuse its buffer. This gather runs
        intra-cluster (leader -> workers), so the N-worker barrier never crosses the DC.
        ``stale`` (the vpaths that diverged under the fence) rides along to the workers."""
        self._fuse_actors.receive_via_cbcast.call(
            int(block_id), stale, int(nbytes)
        ).get()


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
        # The RemoteMountLeader broker: a single actor spawned on the leader worker (rank 0),
        # the workers' fault sink (dedups + forwards to this client's ``enqueue``) and the
        # per-block delivery barrier (``await_block``). ``None`` until open() spawns it.
        self._leader = None
        # The persistent chain head (this client -> worker[0]), dialed once at open. The
        # client is the chain SOURCE: ``_deliver`` ``send_block``s each block straight down
        # the multi-stream chain (client -> w[0] -> ... -> w[N-1]), so the cross-DC ship
        # rides the tuned chain rather than a single actor-bus ``send(data)`` hop.
        self._cbc_head = None
        # Chunk size for striping each block across the chain's streams (the tuned
        # ``CBC_CHUNK``). Purely client-send-side: it sets how ``send_block`` fragments
        # the block, so it also sets the useful-stream ceiling (chunks/block =
        # ``BLOCK_SIZE // chunk``); workers/relays are agnostic.
        self._cbc_chunk: int = CBC_CHUNK
        # One reusable materialise buffer, filled by ``materialise_block`` per delivery
        # instead of allocating+zeroing a fresh 64 MiB ``bytearray`` each block. Safe to
        # reuse because delivery is one block in flight (``await_block`` barriers before
        # the next ``_deliver``), and the bytes a block does not overwrite (inter-file
        # gaps, the tail past ``total_size``) map to no file, so no FUSE read ever serves
        # them -- leftover content from the previous block is never observable. Only the
        # ``bytes(buf)`` snapshot handed to the transport is per-block; this backing
        # buffer is not.
        self._mat_buf: bytearray = bytearray(BLOCK_SIZE)

    def _deliver(self, block: int) -> None:
        """Materialise ``block`` once and deliver it to every worker, then remember it
        so a re-fault for it is a no-op.

        Delivery sources the block straight down the pipelined chain (client -> w[0] ->
        ... -> w[N-1]) via ``send_block``, then barriers on the leader's ``await_block``
        until every worker has received AND committed it (an intra-cluster gather), so a
        returned call means every worker holds the block
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
        data, stale = materialise_block(self.index, block, self._mat_buf)
        if stale:
            logger.warning(
                "block %s: %d file(s) diverged under the fence, delivered stale: %s",
                block,
                len(stale),
                stale,
            )
        # Source the block straight down the chain (client -> w[0] -> ... -> w[N-1]): the
        # one cross-DC copy rides the multi-stream chain, not a single actor-bus hop.
        # ``send_block`` returns once the chunks are queued; ``await_block`` then blocks on
        # the leader until every worker has received AND committed the block (an
        # intra-cluster gather, so the N-worker barrier never crosses the DC). We record it
        # only AFTER await returns, so a failed delivery is not remembered.
        from monarch._rust_bindings.monarch_extension.chain_broadcast import send_block

        send_block(self._cbc_head, data, self._cbc_chunk, int(block))
        self._leader.await_block.call_one(int(block), stale, len(data)).get()
        self._delivered.add(block)

    @endpoint
    def enqueue(self, block) -> None:
        """Deliver a single faulted block. The ``RemoteMountLeader`` broker calls this
        (fire-and-forget, forwarded from its own ``enqueue``) once per block; the workers
        call the broker, not this client, so the cross-worker fault storm is already
        collapsed to one request upstream. A FUSE read faults one block at a time (a straddling read re-faults the
        next on its following pass). Synchronous: the actor processes one delivery at a
        time, and the permanent ``_delivered`` set is the second dedup layer -- it also
        covers the prefill and refresh paths, which call ``_deliver`` directly -- so the
        first delivery broadcasts the block to every worker and records it, a later call
        sees it delivered and skips. Same delivery path as the prefill (open) and refresh.

        Failures degrade the mount rather than abort this MountHandlerClient actor
        (an uncaught fault here kills the sidecar and wedges every other worker): a
        worker dying/preempted mid-delivery surfaces as ``SupervisionError`` and is
        logged; source divergence is handled in ``_deliver`` (it marks the diverged
        files stale so their reads get EIO); any other error is logged with its
        traceback, and the RemoteMountLeader's request marker is cleared so a re-fault can retry."""
        try:
            self._deliver(int(block))
        except SupervisionError:
            logger.warning("delivery stopping: workers no longer reachable")
        except Exception:
            logger.exception("delivery failed for block %s", block)
            # The RemoteMountLeader deduped this block (its ``enqueue``); clear its marker so a
            # re-fault re-requests it instead of being silently suppressed. Not done on
            # the SupervisionError path above -- the workers are gone, so retry is futile.
            if self._leader is not None:
                self._leader.clear_request.broadcast(int(block))

    @endpoint
    def open(self, self_handle):
        """Spawn the workers, build the index, mount, and deliver the code prefix,
        then RETURN. The index (the whole tree) ships first
        as a 0-block ``find``; the small code blocks are delivered here; the big
        libraries stream in on demand when an import faults them.

        ``self_handle`` is this actor's own handle, passed by the caller (which
        holds it) so the ``RemoteMountLeader`` broker can be spawned with it (the broker
        forwards worker fault requests back to this client's ``enqueue``) -- an actor
        can't obtain a callable handle to itself, so it is threaded in here.
        """
        # Spawn a fresh worker FUSEActor mesh for this mount; ``close`` tears the
        # previous one down, so each open starts clean (the workers hold blocks only in
        # memory, so a re-mount has nothing to reuse). The fresh mesh holds nothing, so
        # reset the delivered set: a re-open must re-deliver every block (the previous
        # mesh's in-memory blocks are gone).
        self._delivered.clear()

        # The default cubic underfills a high-BDP WAN link, so tune the chain to bbr.
        # This runs BEFORE spawn_procs() on purpose: a configure() reaches procs spawned
        # after it (the Runtime layer is snapshotted into each child as ClientOverride) but
        # never retroactively, so setting it here is what gets bbr onto the workers' relay
        # hops as well as this proc's client -> w[0] ship. Setting it after the spawn would
        # tune only this proc, and would tune the workers on a RE-open (they would inherit
        # the previous open's setting), making the transport depend on open count.
        #
        # Congestion control is sender-side, applied by whichever proc dials, so it has to be
        # in the config of every proc that dials -- not the caller's proc, which cannot be
        # relied on to have configured anything before this one was spawned. Writing the
        # Runtime layer leaves an explicit HYPERACTOR_CHANNEL_TCP_CONGESTION env override
        # winning, since Env resolves above Runtime.
        #
        # Keep this an unscoped configure(), NOT a `with configured(...)`: connect() below
        # dials lazily -- it returns right after spawning the writer task, and the real
        # TcpStream connect + set_tcp_congestion run later on that background task, reading
        # this setting then. A scoped restore would clear it the instant connect() returns,
        # before the background dial reads it -- silently dropping bbr.
        from monarch.config import configure

        configure(channel_tcp_congestion="bbr")

        self.procs = self.host_mesh.spawn_procs()

        # Spawn the fault-request broker first, as a single actor on the leader worker
        # (rank 0's proc), with this client's handle. The workers are then spawned with
        # the broker as their fault sink, in the client's place: their fault callbacks
        # reach it intra-cluster and it forwards deduped requests to this client's
        # ``enqueue`` across the DC at most once per block. Because the broker exposes the
        # same ``enqueue`` as this client, the workers do not know the difference.
        self._leader = (
            self.procs.flatten("rank")
            .slice(rank=0)
            .spawn("RemoteMountLeader", RemoteMountLeader, self_handle)
        )
        self.fuse_actors = self.procs.spawn("FUSEActor", FUSEActor, self._leader)
        self.fuse_actors.mkdir.call(self.mntpoint).get()
        # Build the index from a fresh walk of the source (a cold full pack: the
        # workers hold blocks only in memory, so there is no prior index to extend).
        self.index = build_index(self.sourcepath, {})

        # Hand the broker the worker mesh; its ``send`` sources each delivered block into
        # the pipelined broadcast chain.
        self._leader.set_fuse_actors.call_one(self.fuse_actors).get()

        # Wire the broadcast chain: every worker binds a metatls listener + recv ctx
        # (``cbc_listen``); order them by rank into client -> w[0] -> ... -> w[N-1]; start
        # each worker's relay pointed at its successor (``cbc_start``); and dial the head
        # (w[0]) FROM THIS CLIENT, so the client is the chain source and the cross-DC ship
        # rides the multi-stream chain. Set up once and reused for every block -- metatls
        # reuses monarch's own identity, so there are no certs to distribute.
        from monarch._rust_bindings.monarch_extension.chain_broadcast import connect

        # One stream count for the whole chain (the tuned ``CBC_STREAMS``), so the
        # client's head dial and every worker's forward agree.
        num_streams = CBC_STREAMS
        listen = self.fuse_actors.cbc_listen.call().get()
        by_rank = sorted((value for _point, value in listen), key=lambda rv: rv[0])
        addrs = [addr for _rank, addr in by_rank]
        self.fuse_actors.cbc_start.call(addrs, num_streams).get()
        self._cbc_head = connect(addrs[0], num_streams)

        # Mount with the full tree: a 0-block ``find`` works immediately; data faults in
        # afterwards (the fault callback -> broker ``enqueue`` -> client ``enqueue``).
        self.fuse_actors.mount.call(self.mntpoint, self.index).get()

        # Deliver the code blocks (the small-file region), then return. Big files
        # (libraries, data) stream in on demand when a worker's read faults them
        # -> the fault callback -> broker ``enqueue`` -> client ``enqueue`` -> ``_deliver``.
        prefill = code_blocks(self.index)
        for b in prefill:
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
        """Spawn the workers, mount, wire the broadcast chain, and deliver the prefill."""
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
    them. Each block is delivered down a pipelined broadcast chain sourced by the
    client (client -> w[0] -> ... -> w[N-1]), so the venv crosses the DC boundary once
    and the source egresses ~one copy regardless of worker count.
    ``refresh()`` advances the freshness fence.
    """
    return MountHandler(host_mesh, sourcepath, mntpoint)
