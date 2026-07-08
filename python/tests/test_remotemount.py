# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from __future__ import annotations

import contextlib
import ctypes
import errno
import mmap
import os
import shutil
import stat
import tempfile
import threading
import time
from collections.abc import Generator

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_extension.chunked_fuse import (
    FuseMountHandle,
    mount_chunked_fuse,
)
from monarch.remotemount.remotemount import BLOCK_SIZE, build_index, materialise_block

# Freshness is driven by per-file (size, mtime_ns) recorded in the layout
# rather than xxhash block hashing. The FUSE tests pack a source dir into
# ``(fs_metadata, staging_mv, chunks)`` via the production layout primitives
# (``build_index`` + ``materialise_block``); see ``_pack_for_test``.


class TestMountExcludesMonarchState:
    """``.monarch`` is the CLI's job-state dir, re-persisted on every
    ``monarch exec``. Mounting it churns the source so every refresh ships a
    block for state the workers never need; ``build_index`` must drop it.
    Regression for a self-inflicted ~0.8s-per-exec ship.
    """

    def _make_tree(self, d: str) -> None:
        # A realistic mounted source: training code + a normal subdir, with the
        # client's .monarch job-state dir nested inside.
        with open(os.path.join(d, "train.py"), "w") as f:
            f.write("print('hi')\n")
        os.makedirs(os.path.join(d, "pkg"))
        with open(os.path.join(d, "pkg", "mod.py"), "w") as f:
            f.write("x = 1\n")
        os.makedirs(os.path.join(d, ".monarch", "default"))
        with open(os.path.join(d, ".monarch", "default", "state.pkl"), "wb") as f:
            f.write(b"\x00" * 64)

    def test_monarch_excluded_from_scan(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            self._make_tree(d)
            meta = build_index(d, {})

            # Nothing under .monarch reaches the FUSE meta or the root's child
            # listing.
            assert not any(".monarch" in vpath for vpath in meta)
            assert ".monarch" not in meta["/"]["children"]

            # Real content is present.
            assert "/train.py" in meta
            assert "/pkg" in meta
            assert "/pkg/mod.py" in meta

    def test_monarch_churn_keeps_meta_stable(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            self._make_tree(d)
            meta1 = build_index(d, {})

            # Re-persist job state the way ``monarch exec`` does: new size + mtime.
            # Without the exclusion this changes the meta and ships every refresh.
            sp = os.path.join(d, ".monarch", "default", "state.pkl")
            with open(sp, "wb") as f:
                f.write(b"\x01" * 4096)
            os.utime(sp, (time.time() + 5, time.time() + 5))

            meta2 = build_index(d, meta1)

            assert not any(".monarch" in v for v in meta1)
            assert meta1 == meta2


class TestShmRoundTrip:
    """Test /tmp/ create/write/collect round-trip (no actors)."""

    def test_create_write_collect(self) -> None:
        pid = os.getpid()
        data = os.urandom(1024)
        num_shards = 4
        shard_size = len(data) // num_shards
        paths = []

        # Create shm files.
        for i in range(num_shards):
            path = f"/tmp/monarch_test_{pid}_{i}"
            fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
            os.ftruncate(fd, shard_size)
            os.close(fd)
            paths.append(path)

        # Write shard data.
        for i, path in enumerate(paths):
            fd = os.open(path, os.O_RDWR)
            mm = mmap.mmap(fd, shard_size)
            start = i * shard_size
            mm[:] = data[start : start + shard_size]
            mm.close()
            os.close(fd)

        # Collect: mmap and unlink.
        collected = bytearray()
        for path in paths:
            fd = os.open(path, os.O_RDONLY)
            mm = mmap.mmap(fd, shard_size, mmap.MAP_PRIVATE, mmap.PROT_READ)
            os.close(fd)
            collected.extend(mm[:])
            mm.close()
            os.unlink(path)

        assert bytes(collected) == data

    def test_unlink_after_mmap(self) -> None:
        """Data persists after unlink as long as mmap is alive."""
        path = f"/tmp/monarch_test_{os.getpid()}_unlink"
        data = b"hello shm"
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        os.ftruncate(fd, len(data))
        mm = mmap.mmap(fd, len(data))
        mm[:] = data
        os.close(fd)

        # Unlink while mmap is still open.
        os.unlink(path)
        assert not os.path.exists(path)

        # Data still accessible via mmap.
        assert bytes(mm[:]) == data
        mm.close()


class TestBlockBoundary:
    """Test that a file straddling the 64MB block boundary round-trips through
    the production layout primitives (build_index + materialise_block)."""

    def test_pack_directory_large_file(self) -> None:
        """A file larger than the block size packs into multiple blocks and
        materialises back to the original bytes."""
        size = BLOCK_SIZE + 512
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "big.bin")
            data = os.urandom(size)
            with open(path, "wb") as f:
                f.write(data)

            meta, staging_mv, chunks = _pack_for_test(d)

            assert meta["/big.bin"]["file_len"] == size
            assert staging_mv is not None
            assert len(staging_mv) == size
            packed = b"".join(bytes(c) for c in chunks)
            assert packed == data

    def test_file_spanning_two_blocks(self) -> None:
        """A file larger than one block occupies two blocks; each holds the
        correct slice (the tail block zero-padded to BLOCK_SIZE)."""
        size = BLOCK_SIZE + 1024
        with tempfile.TemporaryDirectory() as d:
            data = os.urandom(size)
            with open(os.path.join(d, "big.bin"), "wb") as f:
                f.write(data)
            meta = build_index(d, {})
            assert meta["/big.bin"]["global_offset"] == 0
            assert meta["/"]["total_size"] == size
            assert (size + BLOCK_SIZE - 1) // BLOCK_SIZE == 2
            block0, _ = materialise_block(meta, 0)
            block1, _ = materialise_block(meta, 1)
            assert len(block0) == BLOCK_SIZE and len(block1) == BLOCK_SIZE
            assert block0 == data[:BLOCK_SIZE]
            assert block1[:1024] == data[BLOCK_SIZE:]
            assert block1[1024:] == bytes(BLOCK_SIZE - 1024)  # zero-padded tail

    def test_file_exactly_one_block(self) -> None:
        """A file exactly one block long occupies a single block; the next block
        is past the layout end and raises."""
        with tempfile.TemporaryDirectory() as d:
            data = os.urandom(BLOCK_SIZE)
            with open(os.path.join(d, "exact.bin"), "wb") as f:
                f.write(data)
            meta = build_index(d, {})
            assert meta["/"]["total_size"] == BLOCK_SIZE
            assert materialise_block(meta, 0)[0] == data
            with pytest.raises(ValueError):
                materialise_block(meta, 1)

    def test_multiple_files_across_block_boundary(self) -> None:
        """Files whose packed layout straddles a block boundary each reconstruct
        at their offsets. build_index packs the small (code) file first, so the
        big file starts after it and crosses into block 1."""
        small = b"s" * 200
        with tempfile.TemporaryDirectory() as d:
            big = os.urandom(BLOCK_SIZE)
            with open(os.path.join(d, "big.bin"), "wb") as f:
                f.write(big)
            with open(os.path.join(d, "small.txt"), "wb") as f:
                f.write(small)
            meta, _staging, chunks = _pack_for_test(d)
            assert meta["/small.txt"]["global_offset"] == 0
            assert meta["/big.bin"]["global_offset"] == len(small)
            total = meta["/"]["total_size"]
            assert (total + BLOCK_SIZE - 1) // BLOCK_SIZE == 2
            packed = b"".join(bytes(c) for c in chunks)
            assert packed[: len(small)] == small
            assert packed[len(small) : len(small) + len(big)] == big


class TestBuildIndex:
    """build_index meta structure: offsets, contiguity, symlinks, directories."""

    def test_empty_directory(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            meta = build_index(d, {})
            assert meta["/"]["children"] == []
            assert meta["/"]["total_size"] == 0

    def test_single_file(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            content = b"hello world"
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(content)
            meta, _staging, chunks = _pack_for_test(d)
            assert meta["/a.txt"]["global_offset"] == 0
            assert meta["/a.txt"]["file_len"] == len(content)
            assert meta["/"]["total_size"] == len(content)
            assert b"".join(bytes(c) for c in chunks)[: len(content)] == content

    def test_multiple_files_contiguous_offsets(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            files = {"a.txt": b"aaa", "b.txt": b"bbbbbb", "c.txt": b"c"}
            for name, content in files.items():
                with open(os.path.join(d, name), "wb") as f:
                    f.write(content)
            meta, _staging, chunks = _pack_for_test(d)
            packed = b"".join(bytes(c) for c in chunks)
            for name, content in files.items():
                node = meta[f"/{name}"]
                off, length = node["global_offset"], node["file_len"]
                assert length == len(content)
                assert packed[off : off + length] == content
            entries = sorted(
                (n["global_offset"], n["file_len"])
                for n in meta.values()
                if "global_offset" in n
            )
            for i in range(1, len(entries)):
                assert entries[i][0] == entries[i - 1][0] + entries[i - 1][1]

    def test_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            target = os.path.join(d, "target.txt")
            with open(target, "w") as f:
                f.write("target")
            os.symlink(target, os.path.join(d, "link.txt"))
            meta = build_index(d, {})
            assert meta["/link.txt"]["link_target"] == target
            assert "global_offset" not in meta["/link.txt"]

    def test_directory_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            os.makedirs(os.path.join(d, "sub"))
            with open(os.path.join(d, "sub", "f.txt"), "w") as f:
                f.write("x")
            meta = build_index(d, {})
            assert "sub" in meta["/"]["children"]
            assert "f.txt" in meta["/sub"]["children"]
            assert "st_mode" in meta["/"]["attr"]


class TestAppendOnlyOffsets:
    """build_index's append-only offset assignment against a previous index."""

    def test_no_previous_index_sequential(self) -> None:
        """Without a previous index, files get contiguous offsets from 0."""
        with tempfile.TemporaryDirectory() as d:
            for name in ("a.txt", "b.txt"):
                with open(os.path.join(d, name), "wb") as f:
                    f.write(name.encode() * 100)
            meta = build_index(d, {})
            nodes = sorted(
                (n["global_offset"], n["file_len"])
                for n in meta.values()
                if "global_offset" in n
            )
            assert nodes[0][0] == 0
            for i in range(1, len(nodes)):
                assert nodes[i][0] == nodes[i - 1][0] + nodes[i - 1][1]

    def test_unchanged_files_keep_offsets(self) -> None:
        """Files unchanged since the previous index keep their global_offset."""
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(b"hello" * 100)
            with open(os.path.join(d, "b.txt"), "wb") as f:
                f.write(b"world" * 100)
            meta1 = build_index(d, {})
            meta2 = build_index(d, meta1)
            for v in ("/a.txt", "/b.txt"):
                assert meta2[v]["global_offset"] == meta1[v]["global_offset"]

    def test_changed_file_appended(self) -> None:
        """A changed file is re-appended past the previous high-water mark while
        unchanged files keep their offsets."""
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "keep.txt"), "wb") as f:
                f.write(b"k" * 500)
            change = os.path.join(d, "change.txt")
            with open(change, "wb") as f:
                f.write(b"c" * 500)
            meta1 = build_index(d, {})
            old_total = meta1["/"]["total_size"]
            keep_off = meta1["/keep.txt"]["global_offset"]
            with open(change, "wb") as f:
                f.write(b"C" * 700)
            st = os.stat(change)
            os.utime(change, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))
            meta2 = build_index(d, meta1)
            assert meta2["/keep.txt"]["global_offset"] == keep_off
            assert meta2["/change.txt"]["global_offset"] >= old_total

    def test_new_file_appended(self) -> None:
        """A new file is appended past the previous high-water mark."""
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(b"a" * 300)
            meta1 = build_index(d, {})
            old_total = meta1["/"]["total_size"]
            with open(os.path.join(d, "b.txt"), "wb") as f:
                f.write(b"b" * 300)
            meta2 = build_index(d, meta1)
            assert meta2["/a.txt"]["global_offset"] == meta1["/a.txt"]["global_offset"]
            assert meta2["/b.txt"]["global_offset"] >= old_total

    def test_appended_region_is_block_aligned(self) -> None:
        """A non-block-aligned high-water mark (the normal case after contiguous
        packing) is rounded up so the appended file starts on a block boundary, in
        a strictly later block than the mark -- it never shares a 64 MiB block with
        already-delivered data."""
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(b"a" * 1000)  # sub-block file -> non-aligned total_size
            meta1 = build_index(d, {})
            old_total = meta1["/"]["total_size"]
            assert old_total % BLOCK_SIZE != 0  # precondition: mark is not aligned
            with open(os.path.join(d, "b.txt"), "wb") as f:
                f.write(b"b" * 10)
            meta2 = build_index(d, meta1)
            new_off = meta2["/b.txt"]["global_offset"]
            assert new_off == ((old_total // BLOCK_SIZE) + 1) * BLOCK_SIZE
            assert new_off % BLOCK_SIZE == 0
            assert new_off // BLOCK_SIZE > (old_total - 1) // BLOCK_SIZE

    def test_unchanged_files_stay_in_same_blocks(self) -> None:
        """At scale, modifying one file moves only that file; every other file
        keeps its offset, so already-delivered blocks stay valid."""
        with tempfile.TemporaryDirectory() as d:
            for i in range(100):
                with open(os.path.join(d, f"f_{i:03d}.bin"), "wb") as f:
                    f.write(os.urandom(1024))
            meta1 = build_index(d, {})
            p = os.path.join(d, "f_050.bin")
            with open(p, "wb") as f:
                f.write(os.urandom(1024))
            st = os.stat(p)
            os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))
            meta2 = build_index(d, meta1)
            moved = [
                v
                for v in meta1
                if "global_offset" in meta1[v]
                and meta2[v]["global_offset"] != meta1[v]["global_offset"]
            ]
            assert moved == ["/f_050.bin"]

    def test_previous_index_all_files_deleted(self) -> None:
        """build_index with a previous index but an emptied source does not crash;
        total_size stays at the high-water mark (append-only never shrinks)."""
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(b"hello")
            meta1 = build_index(d, {})
            os.remove(os.path.join(d, "a.txt"))
            meta2 = build_index(d, meta1)
            assert meta2["/"]["children"] == []
            assert not any("global_offset" in n for v, n in meta2.items() if v != "/")
            assert meta2["/"]["total_size"] == meta1["/"]["total_size"]


class TestMaterialiseSourceDiverged:
    """materialise_block enforces the freshness fence PER FILE: a file that changed
    since build_index is reported in the ``diverged`` list (its bytes garbage-filled,
    never served stale) instead of raising or serving stale/short bytes."""

    def test_truncated_file_diverges(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "f.bin")
            with open(path, "wb") as f:
                f.write(b"x" * 1000)
            meta = build_index(d, {})
            os.truncate(path, 500)
            data, diverged = materialise_block(meta, 0)
            assert diverged == ["/f.bin"]
            # The diverged file's bytes are garbage, NOT the (now-shorter) source.
            assert data[:1000] != b"x" * 1000

    def test_missing_file_diverges(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "f.bin")
            with open(path, "wb") as f:
                f.write(b"x" * 1000)
            meta = build_index(d, {})
            os.remove(path)
            _data, diverged = materialise_block(meta, 0)
            assert diverged == ["/f.bin"]

    def test_mtime_change_same_size_diverges(self) -> None:
        """A same-size in-place edit bumps mtime, tripping the fence via the
        mtime_ns guard even though the file length is unchanged."""
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "f.bin")
            with open(path, "wb") as f:
                f.write(b"x" * 1000)
            meta = build_index(d, {})
            st = os.stat(path)
            os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))
            _data, diverged = materialise_block(meta, 0)
            assert diverged == ["/f.bin"]

    def test_unchanged_files_not_diverged(self) -> None:
        """The happy path: every file matches the fence, so none is reported."""
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(b"a" * 100)
            with open(os.path.join(d, "b.txt"), "wb") as f:
                f.write(b"b" * 100)
            meta = build_index(d, {})
            _data, diverged = materialise_block(meta, 0)
            assert diverged == []


def _check_fuse_available() -> bool:
    """Check if FUSE mounts actually work, not just that the binary exists."""
    if shutil.which("fusermount3") is None or not os.path.exists("/dev/fuse"):
        return False
    # /dev/fuse may exist but be unusable (e.g. Docker without CAP_SYS_ADMIN).
    # Probe by checking if we can open it.
    try:
        fd = os.open("/dev/fuse", os.O_RDWR)
        os.close(fd)
        return True
    except OSError:
        return False


_fuse_available: bool = _check_fuse_available()


def _fuse_connection_abort(mount_point: str) -> None:
    """Best-effort: abort the FUSE connection backing ``mount_point`` via sysfs so an
    in-flight (possibly wedged) read returns EIO/ENOTCONN instead of hanging in
    uninterruptible sleep. Used only as a fallback in the parked-read tests: if a
    wake regresses, this frees the stuck reader thread so the test fails fast rather
    than stalling the whole suite. ``os.stat`` on the mount root is a getattr (it
    never parks on block data), so the device minor is readable while a read is
    wedged."""
    try:
        dev = os.stat(mount_point).st_dev
        with open(f"/sys/fs/fuse/connections/{os.minor(dev)}/abort", "w") as f:
            f.write("1")
    except OSError:
        pass  # sysfs absent / not permitted -- pytest-timeout is the backstop


def _make_attr(
    mode: int = 0o100644,
    size: int = 0,
    nlink: int = 1,
) -> dict[str, object]:
    now = time.time()
    return {
        "st_atime": now,
        "st_ctime": now,
        "st_gid": os.getgid(),
        "st_mode": mode,
        "st_mtime": now,
        "st_nlink": nlink,
        "st_size": size,
        "st_uid": os.getuid(),
    }


def _dir_attr(size: int = 4096, nlink: int = 2) -> dict[str, object]:
    return _make_attr(mode=0o40755, size=size, nlink=nlink)


def _file_attr(size: int) -> dict[str, object]:
    return _make_attr(mode=0o100644, size=size)


def _symlink_attr(target_len: int) -> dict[str, object]:
    return _make_attr(mode=0o120777, size=target_len)


def _publish_block(
    handle: FuseMountHandle, block_id: int, data: bytes, stale: list[str]
) -> None:
    """Deliver *data* as block *block_id* the way a worker does: write it into the mount's
    own buffer through ``block_ptr(block_id)`` (as an RDMA read would land it), then
    ``receive_block`` to freeze that buffer into the served block with no copy. ``data``
    may be shorter than ``BLOCK_SIZE``; the buffer's tail stays zeroed, exactly as
    production zero-pads the last block past ``total_size``."""
    mv = memoryview(
        (ctypes.c_char * BLOCK_SIZE).from_address(handle.block_ptr(block_id))
    ).cast("B")
    mv[: len(data)] = data
    handle.receive_block(block_id, stale)


def _deliver_blocks(handle: FuseMountHandle, data: bytes) -> None:
    """Hand *data* to the in-memory FUSE mount one block at a time via ``_publish_block``
    (one BLOCK_SIZE-or-shorter block per id) -- the delivery path the tests use in place
    of the worker's RDMA fan-out."""
    n_blocks = (len(data) + BLOCK_SIZE - 1) // BLOCK_SIZE
    for block_id in range(n_blocks):
        lo = block_id * BLOCK_SIZE
        _publish_block(handle, block_id, data[lo : lo + BLOCK_SIZE], [])


def _pack_for_test(
    src: str,
) -> tuple[dict, "memoryview | None", list[memoryview]]:
    """Pack a source dir into (fs_metadata, staging_mv, chunks) for the FUSE
    tests via the production layout primitives (build_index + materialise_block)
    -- replaces the removed pack_directory_chunked."""
    meta = build_index(src, {})
    total_size = meta["/"]["total_size"]
    if total_size == 0:
        return meta, None, []
    n_blocks = (total_size + BLOCK_SIZE - 1) // BLOCK_SIZE
    staging = bytearray(total_size)
    for b in range(n_blocks):
        # materialise_block returns a fixed BLOCK_SIZE buffer; clip the tail
        # block's zero pad so ``staging`` stays exactly total_size.
        block_bytes, _ = materialise_block(meta, b)
        start = b * BLOCK_SIZE
        valid = min(BLOCK_SIZE, total_size - start)
        staging[start : start + valid] = block_bytes[:valid]
    staging_mv = memoryview(staging)
    return meta, staging_mv, [staging_mv]


@contextlib.contextmanager
def _fuse_mount(
    metadata: dict[str, object],
    chunks: list[memoryview],
    mount_point: str,
) -> Generator[FuseMountHandle, None, None]:
    """Mount the FUSE filesystem (in-memory: it starts with no block data),
    deliver the packed chunks via ``_deliver_blocks``, and unmount on exit."""
    data = b"".join(bytes(c) for c in chunks)
    handle = mount_chunked_fuse(metadata, len(data), mount_point, None)
    _deliver_blocks(handle, data)
    try:
        yield handle
    finally:
        handle.unmount()


def _refresh(
    handle: FuseMountHandle,
    meta: dict[str, object],
    chunk_buf: memoryview,
    dirty_ranges: list[tuple[int, int]],
    total: int,
) -> None:
    """Adapt the old ``handle.refresh(meta, chunk_buf, dirty_ranges, total)``
    call to the in-memory delivery: hand each block's bytes to the mount via
    ``_deliver_blocks``, then swap the metadata. ``dirty_ranges`` is ignored -- every
    block is re-delivered, which is fine for these tests.
    """
    _deliver_blocks(handle, bytes(chunk_buf)[:total])
    handle.refresh(meta, total)


@pytest.mark.skipif(
    not _fuse_available,
    reason="FUSE not available (missing fusermount3, /dev/fuse, or permissions)",
)
class TestFuseMount:
    """Comprehensive tests for the chunked_fuse FUSE filesystem."""

    def test_single_file_read(self) -> None:
        """Mount a single file and read its content back."""
        content = b"hello fuse world"
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["a.txt"]},
            "/a.txt": {
                "attr": _file_attr(len(content)),
                "global_offset": 0,
                "file_len": len(content),
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(content)], mnt),
        ):
            with open(os.path.join(mnt, "a.txt"), "rb") as f:
                assert f.read() == content

    def test_zero_copy_block_ptr_receive_block(self) -> None:
        """The zero-copy delivery path at the binding level: get a mount-owned buffer's
        address via ``block_ptr`` (which reserves it lazily), write the block into it
        through a ctypes memoryview (as an RDMA read would), ``receive_block`` to freeze
        it into the served block with no copy, and read the file back."""
        content = b"zero copy delivery works"
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["z.txt"]},
            "/z.txt": {
                "attr": _file_attr(len(content)),
                "global_offset": 0,
                "file_len": len(content),
            },
        }
        with tempfile.TemporaryDirectory() as mnt:
            handle = mount_chunked_fuse(metadata, len(content), mnt, None)
            try:
                mv = memoryview(
                    (ctypes.c_char * BLOCK_SIZE).from_address(handle.block_ptr(0))
                ).cast("B")
                mv[: len(content)] = content  # write as RDMA would, into mount memory
                handle.receive_block(0, [])  # zero-copy freeze into the served block
                with open(os.path.join(mnt, "z.txt"), "rb") as f:
                    assert f.read() == content
            finally:
                handle.unmount()

    def test_multiple_files(self) -> None:
        """Mount multiple files and verify each has correct content."""
        files = {
            "a.txt": b"aaa",
            "b.txt": b"bbbbbb",
            "c.txt": b"c",
        }
        packed = b"".join(files.values())
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": list(files.keys())},
        }
        offset = 0
        for name, content in files.items():
            metadata[f"/{name}"] = {
                "attr": _file_attr(len(content)),
                "global_offset": offset,
                "file_len": len(content),
            }
            offset += len(content)

        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(packed)], mnt),
        ):
            for name, expected in files.items():
                with open(os.path.join(mnt, name), "rb") as f:
                    assert f.read() == expected

    def test_subdirectory(self) -> None:
        """Files in a subdirectory are accessible."""
        content = b"nested content"
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(nlink=3), "children": ["sub"]},
            "/sub": {"attr": _dir_attr(), "children": ["f.txt"]},
            "/sub/f.txt": {
                "attr": _file_attr(len(content)),
                "global_offset": 0,
                "file_len": len(content),
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(content)], mnt),
        ):
            with open(os.path.join(mnt, "sub", "f.txt"), "rb") as f:
                assert f.read() == content
            assert stat.S_ISDIR(os.stat(os.path.join(mnt, "sub")).st_mode)

    def test_listdir(self) -> None:
        """os.listdir returns the correct children."""
        metadata: dict[str, object] = {
            "/": {
                "attr": _dir_attr(),
                "children": ["x.bin", "y.bin", "sub"],
            },
            "/x.bin": {
                "attr": _file_attr(1),
                "global_offset": 0,
                "file_len": 1,
            },
            "/y.bin": {
                "attr": _file_attr(1),
                "global_offset": 1,
                "file_len": 1,
            },
            "/sub": {"attr": _dir_attr(), "children": []},
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(b"\x00\x00")], mnt),
        ):
            assert sorted(os.listdir(mnt)) == ["sub", "x.bin", "y.bin"]
            assert os.listdir(os.path.join(mnt, "sub")) == []

    def test_symlink(self) -> None:
        """Symlinks are readable via readlink."""
        target = "/some/target/path"
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["link"]},
            "/link": {
                "attr": _symlink_attr(len(target)),
                "link_target": target,
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(b"\x00")], mnt),
        ):
            assert os.readlink(os.path.join(mnt, "link")) == target
            assert stat.S_ISLNK(os.lstat(os.path.join(mnt, "link")).st_mode)

    def test_partial_read(self) -> None:
        """Reading a slice of a file returns the correct bytes."""
        content = b"0123456789abcdef"
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["f.bin"]},
            "/f.bin": {
                "attr": _file_attr(len(content)),
                "global_offset": 0,
                "file_len": len(content),
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(content)], mnt),
        ):
            with open(os.path.join(mnt, "f.bin"), "rb") as f:
                f.seek(4)
                assert f.read(4) == b"4567"
                f.seek(10)
                assert f.read() == b"abcdef"
                f.seek(100)
                assert f.read() == b""

    def test_dotdot_has_parent_attrs(self) -> None:
        """stat('sub/..') should return root's attrs, not sub's."""
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(size=4096, nlink=3), "children": ["sub"]},
            "/sub": {"attr": _dir_attr(size=80), "children": []},
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(b"\x00" * 64)], mnt),
        ):
            sub_stat = os.stat(os.path.join(mnt, "sub"))
            dotdot_stat = os.stat(os.path.join(mnt, "sub", ".."))
            root_stat = os.stat(mnt)

            assert dotdot_stat.st_size == root_stat.st_size
            assert dotdot_stat.st_nlink == root_stat.st_nlink
            assert stat.S_IMODE(dotdot_stat.st_mode) == stat.S_IMODE(root_stat.st_mode)
            assert sub_stat.st_size != root_stat.st_size

    def test_end_to_end_pack_mount_read(self) -> None:
        """Full round-trip: create files, pack, mount, read back."""
        with tempfile.TemporaryDirectory() as src:
            os.makedirs(os.path.join(src, "sub"))
            files = {
                "hello.txt": b"hello world",
                "binary.bin": os.urandom(2048),
                os.path.join("sub", "nested.txt"): b"nested content here",
            }
            for name, content in files.items():
                with open(os.path.join(src, name), "wb") as f:
                    f.write(content)

            meta, _staging_mv, chunks = _pack_for_test(src)

            with tempfile.TemporaryDirectory() as mnt:
                with _fuse_mount(meta, chunks, mnt):
                    for name, expected in files.items():
                        with open(os.path.join(mnt, name), "rb") as f:
                            assert f.read() == expected, f"content mismatch for {name}"

    def test_file_permissions(self) -> None:
        """File and directory permissions are preserved."""
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["rw.txt", "ro.txt"]},
            "/rw.txt": {
                "attr": _make_attr(mode=0o100666, size=1),
                "global_offset": 0,
                "file_len": 1,
            },
            "/ro.txt": {
                "attr": _make_attr(mode=0o100444, size=1),
                "global_offset": 1,
                "file_len": 1,
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(b"\x00\x00")], mnt),
        ):
            rw = os.stat(os.path.join(mnt, "rw.txt"))
            ro = os.stat(os.path.join(mnt, "ro.txt"))
            assert stat.S_IMODE(rw.st_mode) == 0o666
            assert stat.S_IMODE(ro.st_mode) == 0o444

    def test_nonexistent_file(self) -> None:
        """Accessing a nonexistent path raises FileNotFoundError."""
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": []},
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(b"\x00")], mnt),
        ):
            with pytest.raises(FileNotFoundError):
                open(os.path.join(mnt, "nope.txt"), "rb")
            with pytest.raises(FileNotFoundError):
                os.stat(os.path.join(mnt, "nope.txt"))

    def test_relative_mount_point_rejected(self) -> None:
        """Relative mount point should raise immediately."""
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": []},
        }
        with pytest.raises(RuntimeError, match="absolute path"):
            mount_chunked_fuse(metadata, 0, "relative/path", None)

    def test_mount_nonexistent_directory(self) -> None:
        """Mounting on a path that doesn't exist should fail."""
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": []},
        }
        with pytest.raises(RuntimeError):
            mount_chunked_fuse(metadata, 0, "/tmp/nonexistent_fuse_test_dir", None)

    def test_many_files(self) -> None:
        """Mount with many files and read all of them."""
        num_files = 200
        file_size = 128
        packed = os.urandom(num_files * file_size)
        children = [f"f_{i}.bin" for i in range(num_files)]
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": children},
        }
        for i in range(num_files):
            metadata[f"/f_{i}.bin"] = {
                "attr": _file_attr(file_size),
                "global_offset": i * file_size,
                "file_len": file_size,
            }

        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(packed)], mnt),
        ):
            for i in range(num_files):
                with open(os.path.join(mnt, f"f_{i}.bin"), "rb") as f:
                    expected = packed[i * file_size : (i + 1) * file_size]
                    assert f.read() == expected

    def test_repeated_reads(self) -> None:
        """Reading the same file repeatedly returns consistent data."""
        content = os.urandom(4096)
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["data.bin"]},
            "/data.bin": {
                "attr": _file_attr(len(content)),
                "global_offset": 0,
                "file_len": len(content),
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(content)], mnt),
        ):
            path = os.path.join(mnt, "data.bin")
            for _ in range(50):
                with open(path, "rb") as f:
                    assert f.read() == content

    def test_mount_unmount_remount(self) -> None:
        """Mount, unmount, then mount again on the same directory."""
        content = b"round-trip"
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["f.txt"]},
            "/f.txt": {
                "attr": _file_attr(len(content)),
                "global_offset": 0,
                "file_len": len(content),
            },
        }
        with tempfile.TemporaryDirectory() as mnt:
            # First mount/unmount cycle.
            with _fuse_mount(metadata, [memoryview(content)], mnt):
                with open(os.path.join(mnt, "f.txt"), "rb") as f:
                    assert f.read() == content

            # Second mount on the same directory.
            content2 = b"second time"
            metadata2: dict[str, object] = {
                "/": {"attr": _dir_attr(), "children": ["g.txt"]},
                "/g.txt": {
                    "attr": _file_attr(len(content2)),
                    "global_offset": 0,
                    "file_len": len(content2),
                },
            }
            with _fuse_mount(metadata2, [memoryview(content2)], mnt):
                with open(os.path.join(mnt, "g.txt"), "rb") as f:
                    assert f.read() == content2


@pytest.mark.skipif(
    not _fuse_available,
    reason="FUSE not available (missing fusermount3, /dev/fuse, or permissions)",
)
class TestFuseRefresh:
    """Tests for live FUSE refresh (atomic data swap without unmount)."""

    def test_unchanged_file(self) -> None:
        """File content identical before/after refresh — reads are correct."""
        content = b"unchanged data here"
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["f.txt"]},
            "/f.txt": {
                "attr": _file_attr(len(content)),
                "global_offset": 0,
                "file_len": len(content),
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(content)], mnt) as handle,
        ):
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == content
            # Refresh with identical data.
            _refresh(
                handle,
                metadata,
                memoryview(content),
                [(0, len(content))],
                len(content),
            )
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == content

    def test_changed_file(self) -> None:
        """File content changes — new reads see new data."""
        v1 = b"version one"
        meta1: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["f.txt"]},
            "/f.txt": {
                "attr": _file_attr(len(v1)),
                "global_offset": 0,
                "file_len": len(v1),
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(meta1, [memoryview(v1)], mnt) as handle,
        ):
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == v1
            v2 = b"version two!!"
            meta2: dict[str, object] = {
                "/": {"attr": _dir_attr(), "children": ["f.txt"]},
                "/f.txt": {
                    "attr": _file_attr(len(v2)),
                    "global_offset": 0,
                    "file_len": len(v2),
                },
            }
            _refresh(handle, meta2, memoryview(v2), [(0, len(v2))], len(v2))
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == v2

    def test_added_file(self) -> None:
        """New file appears after refresh."""
        v1 = b"aaa"
        meta1: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["a.txt"]},
            "/a.txt": {
                "attr": _file_attr(3),
                "global_offset": 0,
                "file_len": 3,
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(meta1, [memoryview(v1)], mnt) as handle,
        ):
            assert os.listdir(mnt) == ["a.txt"]
            v2 = b"aaabbb"
            meta2: dict[str, object] = {
                "/": {"attr": _dir_attr(), "children": ["a.txt", "b.txt"]},
                "/a.txt": {
                    "attr": _file_attr(3),
                    "global_offset": 0,
                    "file_len": 3,
                },
                "/b.txt": {
                    "attr": _file_attr(3),
                    "global_offset": 3,
                    "file_len": 3,
                },
            }
            _refresh(handle, meta2, memoryview(v2), [(0, len(v2))], len(v2))
            assert sorted(os.listdir(mnt)) == ["a.txt", "b.txt"]
            with open(os.path.join(mnt, "b.txt"), "rb") as f:
                assert f.read() == b"bbb"

    def test_deleted_file(self) -> None:
        """File disappears after refresh."""
        v1 = b"aaabbb"
        meta1: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["a.txt", "b.txt"]},
            "/a.txt": {
                "attr": _file_attr(3),
                "global_offset": 0,
                "file_len": 3,
            },
            "/b.txt": {
                "attr": _file_attr(3),
                "global_offset": 3,
                "file_len": 3,
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(meta1, [memoryview(v1)], mnt) as handle,
        ):
            assert sorted(os.listdir(mnt)) == ["a.txt", "b.txt"]
            v2 = b"aaa"
            meta2: dict[str, object] = {
                "/": {"attr": _dir_attr(), "children": ["a.txt"]},
                "/a.txt": {
                    "attr": _file_attr(3),
                    "global_offset": 0,
                    "file_len": 3,
                },
            }
            _refresh(handle, meta2, memoryview(v2), [(0, 3)], 3)
            assert os.listdir(mnt) == ["a.txt"]
            with pytest.raises(FileNotFoundError):
                open(os.path.join(mnt, "b.txt"), "rb")

    def test_file_size_grows(self) -> None:
        """File grows after refresh."""
        v1 = b"small"
        meta1: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["f.bin"]},
            "/f.bin": {
                "attr": _file_attr(len(v1)),
                "global_offset": 0,
                "file_len": len(v1),
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(meta1, [memoryview(v1)], mnt) as handle,
        ):
            assert os.stat(os.path.join(mnt, "f.bin")).st_size == 5
            v2 = b"much larger content now"
            meta2: dict[str, object] = {
                "/": {"attr": _dir_attr(), "children": ["f.bin"]},
                "/f.bin": {
                    "attr": _file_attr(len(v2)),
                    "global_offset": 0,
                    "file_len": len(v2),
                },
            }
            _refresh(handle, meta2, memoryview(v2), [(0, len(v2))], len(v2))
            with open(os.path.join(mnt, "f.bin"), "rb") as f:
                assert f.read() == v2
            assert os.stat(os.path.join(mnt, "f.bin")).st_size == len(v2)

    def test_file_size_shrinks(self) -> None:
        """File shrinks after refresh."""
        v1 = b"large content here!!"
        meta1: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["f.bin"]},
            "/f.bin": {
                "attr": _file_attr(len(v1)),
                "global_offset": 0,
                "file_len": len(v1),
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(meta1, [memoryview(v1)], mnt) as handle,
        ):
            assert os.stat(os.path.join(mnt, "f.bin")).st_size == len(v1)
            v2 = b"tiny"
            meta2: dict[str, object] = {
                "/": {"attr": _dir_attr(), "children": ["f.bin"]},
                "/f.bin": {
                    "attr": _file_attr(len(v2)),
                    "global_offset": 0,
                    "file_len": len(v2),
                },
            }
            _refresh(handle, meta2, memoryview(v2), [(0, len(v2))], len(v2))
            with open(os.path.join(mnt, "f.bin"), "rb") as f:
                assert f.read() == v2
            assert os.stat(os.path.join(mnt, "f.bin")).st_size == len(v2)

    def test_sequential_read_unchanged_file_across_refresh(self) -> None:
        """Read first half, refresh, read second half of an unchanged file."""
        content = b"A" * 4096 + b"B" * 4096
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["f.bin"]},
            "/f.bin": {
                "attr": _file_attr(len(content)),
                "global_offset": 0,
                "file_len": len(content),
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(content)], mnt) as handle,
        ):
            fh = open(os.path.join(mnt, "f.bin"), "rb")
            first_half = fh.read(4096)
            assert first_half == b"A" * 4096
            # Refresh with same content.
            _refresh(
                handle,
                metadata,
                memoryview(content),
                [(0, len(content))],
                len(content),
            )
            second_half = fh.read(4096)
            assert second_half == b"B" * 4096
            fh.close()

    def test_defrag_offset_shift_unchanged_content(self) -> None:
        """File's global_offset changes (defrag) but content is same.

        global_offset is a FUSE implementation detail — the kernel never
        sees it. Reads should return the correct file content regardless
        of where the file sits in the packed buffer.
        """
        file_data = b"the file content"
        # v1: file at offset 100 (preceded by 100 bytes of padding).
        buf1 = b"\x00" * 100 + file_data
        meta1: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": ["f.bin"]},
            "/f.bin": {
                "attr": _file_attr(len(file_data)),
                "global_offset": 100,
                "file_len": len(file_data),
            },
        }
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(meta1, [memoryview(buf1)], mnt) as handle,
        ):
            with open(os.path.join(mnt, "f.bin"), "rb") as f:
                assert f.read() == file_data
            # v2: file at offset 0 (defragged, no padding).
            buf2 = file_data
            meta2: dict[str, object] = {
                "/": {"attr": _dir_attr(), "children": ["f.bin"]},
                "/f.bin": {
                    "attr": _file_attr(len(file_data)),
                    "global_offset": 0,
                    "file_len": len(file_data),
                },
            }
            _refresh(handle, meta2, memoryview(buf2), [(0, len(buf2))], len(buf2))
            with open(os.path.join(mnt, "f.bin"), "rb") as f:
                assert f.read() == file_data

    def test_multiple_rapid_refreshes(self) -> None:
        """Refresh several times rapidly, reads always see latest data."""

        def meta_fn(n: str, sz: int) -> dict[str, object]:
            return {
                "/": {"attr": _dir_attr(), "children": [n]},
                f"/{n}": {
                    "attr": _file_attr(sz),
                    "global_offset": 0,
                    "file_len": sz,
                },
            }

        content = b"v00"
        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(meta_fn("f.txt", 3), [memoryview(content)], mnt) as handle,
        ):
            for i in range(20):
                new_content = f"v{i:02d}".encode()
                _refresh(
                    handle,
                    meta_fn("f.txt", len(new_content)),
                    memoryview(new_content),
                    [(0, len(new_content))],
                    len(new_content),
                )
                with open(os.path.join(mnt, "f.txt"), "rb") as f:
                    assert f.read() == new_content

    def test_many_files_one_changed(self) -> None:
        """Many files, only one changes — unchanged files unaffected."""
        num_files = 50
        file_size = 64
        data = os.urandom(num_files * file_size)
        children = [f"f_{i:03d}.bin" for i in range(num_files)]
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": children},
        }
        for i in range(num_files):
            metadata[f"/f_{i:03d}.bin"] = {
                "attr": _file_attr(file_size),
                "global_offset": i * file_size,
                "file_len": file_size,
            }

        with (
            tempfile.TemporaryDirectory() as mnt,
            _fuse_mount(metadata, [memoryview(data)], mnt) as handle,
        ):
            # Read all files.
            for i in range(num_files):
                with open(os.path.join(mnt, f"f_{i:03d}.bin"), "rb") as f:
                    expected = data[i * file_size : (i + 1) * file_size]
                    assert f.read() == expected

            # Change only file 25.
            new_data = bytearray(data)
            new_content = os.urandom(file_size)
            new_data[25 * file_size : 26 * file_size] = new_content
            new_data = bytes(new_data)

            _refresh(
                handle,
                metadata,
                memoryview(new_data),
                [(0, len(new_data))],
                len(new_data),
            )

            # All files should read correctly.
            for i in range(num_files):
                with open(os.path.join(mnt, f"f_{i:03d}.bin"), "rb") as f:
                    expected = new_data[i * file_size : (i + 1) * file_size]
                    assert f.read() == expected

    def test_end_to_end_pack_refresh_read(self) -> None:
        """Full round-trip: pack, mount, modify source, re-pack, refresh, read."""
        with tempfile.TemporaryDirectory() as src:
            with open(os.path.join(src, "data.txt"), "wb") as f:
                f.write(b"original content")

            meta, staging_mv, chunks = _pack_for_test(src)

            with (
                tempfile.TemporaryDirectory() as mnt,
                _fuse_mount(
                    meta,
                    chunks,
                    mnt,
                ) as handle,
            ):
                with open(os.path.join(mnt, "data.txt"), "rb") as f:
                    assert f.read() == b"original content"

                with open(os.path.join(src, "data.txt"), "wb") as f:
                    f.write(b"updated content!")
                meta2, staging_mv2, chunks2 = _pack_for_test(src)
                buf2 = staging_mv2 if staging_mv2 is not None else memoryview(b"")
                _refresh(
                    handle,
                    meta2,
                    buf2,
                    [(0, len(buf2))],
                    len(buf2),
                )

                with open(os.path.join(mnt, "data.txt"), "rb") as f:
                    assert f.read() == b"updated content!"


@pytest.mark.skipif(
    not _fuse_available,
    reason="FUSE not available (missing fusermount3, /dev/fuse, or permissions)",
)
@pytest.mark.timeout(60)
class TestFuseStaleFileEio:
    """The per-file stale -> EIO path through a real FUSE mount.

    When the client cannot reproduce a file (its source diverged under the fence), it
    garbage-fills that file's bytes in the delivered block and ships its vpath in the
    ``receive_block`` stale list. A read of a stale file must return EIO -- and a read
    already parked on its block must be WOKEN and EIO rather than hang forever in
    uninterruptible sleep (the bug this fixes) -- while co-located valid files sharing
    the block still serve. These mount the Rust FUSE filesystem directly (no actor
    layer) and drive ``receive_block`` the way the worker's endpoint does. Each
    parked-read test joins its reader with a wall-clock timeout and a FUSE-abort
    fallback, so a wake regression fails fast instead of wedging."""

    @staticmethod
    def _one_file_meta(file_len: int) -> dict[str, object]:
        """A single file occupying (part of) block 0, with nothing delivered, so the
        first read faults block 0."""
        return {
            "/": {"attr": _dir_attr(), "children": ["f.bin"]},
            "/f.bin": {
                "attr": _file_attr(file_len),
                "global_offset": 0,
                "file_len": file_len,
            },
        }

    @staticmethod
    def _two_file_meta(len_a: int, len_b: int) -> dict[str, object]:
        """Two files packed contiguously into block 0 (``a.bin`` at offset 0,
        ``b.bin`` right after), so they share a block -- the case where one can be
        stale while the other stays valid."""
        return {
            "/": {"attr": _dir_attr(), "children": ["a.bin", "b.bin"]},
            "/a.bin": {
                "attr": _file_attr(len_a),
                "global_offset": 0,
                "file_len": len_a,
            },
            "/b.bin": {
                "attr": _file_attr(len_b),
                "global_offset": len_a,
                "file_len": len_b,
            },
        }

    @staticmethod
    def _reader(path: str) -> tuple[threading.Thread, dict]:
        """A daemon thread that reads ``path`` once, recording either ``data`` (on
        success) or ``errno`` (on OSError). It is the only thread touching the mount's
        data, so the main thread can drive receive_block while it is parked in the
        read syscall (which releases the GIL)."""
        out: dict = {}

        def _run() -> None:
            try:
                with open(path, "rb") as f:
                    out["data"] = f.read()
            except OSError as e:
                out["errno"] = e.errno

        return threading.Thread(target=_run, daemon=True), out

    def test_stale_file_reads_eio(self) -> None:
        """A block delivered with its file in the stale set -> the read returns EIO
        (the per-file stale check fires before the gather)."""
        file_len = 32
        with tempfile.TemporaryDirectory() as mnt:
            handle = mount_chunked_fuse(
                self._one_file_meta(file_len), file_len, mnt, None
            )
            try:
                # Deliver the (garbage) block with f.bin flagged stale.
                _publish_block(handle, 0, os.urandom(file_len), ["/f.bin"])
                with pytest.raises(OSError) as ei:
                    with open(os.path.join(mnt, "f.bin"), "rb") as f:
                        f.read()
                assert ei.value.errno == errno.EIO
            finally:
                handle.unmount()

    def test_parked_read_woken_by_receive_block(self) -> None:
        """Control for the regression test below: a read parked on a missing block is
        woken by ``receive_block`` and returns the delivered bytes. Proves the read
        really parks and the wake machinery works, so the EIO in the stale test is
        specifically the stale bit's doing, not an unrelated error."""
        content = b"delivered-after-park" + bytes(12)  # 32 bytes == file_len
        with tempfile.TemporaryDirectory() as mnt:
            handle = mount_chunked_fuse(
                self._one_file_meta(len(content)), len(content), mnt, None
            )
            t, out = self._reader(os.path.join(mnt, "f.bin"))
            hung = False
            try:
                t.start()
                time.sleep(0.5)  # let the read fault and park on block 0
                assert t.is_alive(), "read should be parked on the missing block"
                _publish_block(handle, 0, content, [])
                t.join(timeout=10)
                hung = t.is_alive()
            finally:
                if hung:
                    _fuse_connection_abort(mnt)
                    t.join(timeout=10)
                handle.unmount()
            assert not hung, "receive_block did not wake the parked read"
            assert out.get("data") == content

    def test_parked_read_woken_by_stale_delivery_eios(self) -> None:
        """The regression guard: a read parked on a missing block, then delivered as a
        garbage block with its file in the stale set, must be woken and return EIO --
        not hang. One ``receive_block`` carries the bytes + the stale flag and wakes
        the parked read."""
        file_len = 32
        with tempfile.TemporaryDirectory() as mnt:
            handle = mount_chunked_fuse(
                self._one_file_meta(file_len), file_len, mnt, None
            )
            t, out = self._reader(os.path.join(mnt, "f.bin"))
            hung = False
            try:
                t.start()
                time.sleep(0.5)  # let the read fault and park on block 0
                assert t.is_alive(), "read should be parked on the missing block"
                # Deliver the garbage block with f.bin stale -- one atomic call.
                _publish_block(handle, 0, os.urandom(file_len), ["/f.bin"])
                t.join(timeout=10)
                hung = t.is_alive()
            finally:
                if hung:
                    _fuse_connection_abort(mnt)  # free the wedged read
                    t.join(timeout=10)
                handle.unmount()
            assert not hung, (
                "stale delivery did not wake the parked read (hang regression)"
            )
            assert out.get("errno") == errno.EIO, f"expected EIO, got {out!r}"

    def test_colocated_valid_file_serves_when_other_stale(self) -> None:
        """The point of per-file granularity: two files share block 0; one is stale,
        the other valid. The valid file reads its real bytes; only the stale file
        EIOs."""
        valid = b"valid-file-bytes"  # 16 bytes (a.bin)
        garbage = os.urandom(16)  # b.bin's range in the delivered (garbage) block
        with tempfile.TemporaryDirectory() as mnt:
            handle = mount_chunked_fuse(
                self._two_file_meta(len(valid), len(garbage)),
                len(valid) + len(garbage),
                mnt,
                None,
            )
            try:
                # Deliver block 0 = a's real bytes ++ b's garbage, with only b stale.
                _publish_block(handle, 0, valid + garbage, ["/b.bin"])
                with open(os.path.join(mnt, "a.bin"), "rb") as f:
                    assert f.read() == valid  # co-located valid file still serves
                with pytest.raises(OSError) as ei:
                    with open(os.path.join(mnt, "b.bin"), "rb") as f:
                        f.read()
                assert ei.value.errno == errno.EIO  # only the stale file EIOs
            finally:
                handle.unmount()

    def test_fault_callback_delivers_stale_eios(self) -> None:
        """Production shape end-to-end: the Rust read fault fires the callback, which
        delivers the garbage block with its file in the stale set (as ``_deliver``
        does). The read is woken and returns EIO -- fault -> callback ->
        receive_block(stale) -> wake -> EIO through real FUSE in one path."""
        file_len = 32
        holder: dict = {}
        faulted = threading.Event()

        def on_fault(block_id: int) -> None:
            faulted.set()
            handle = holder.get("handle")
            if handle is not None:
                _publish_block(handle, int(block_id), os.urandom(file_len), ["/f.bin"])

        with tempfile.TemporaryDirectory() as mnt:
            handle = mount_chunked_fuse(
                self._one_file_meta(file_len), file_len, mnt, on_fault
            )
            holder["handle"] = handle
            t, out = self._reader(os.path.join(mnt, "f.bin"))
            hung = False
            try:
                t.start()
                t.join(timeout=10)
                hung = t.is_alive()
            finally:
                if hung:
                    _fuse_connection_abort(mnt)
                    t.join(timeout=10)
                handle.unmount()
            assert not hung, (
                "fault-callback stale delivery did not wake the read (hang regression)"
            )
            assert faulted.is_set(), "the read should have faulted the block"
            assert out.get("errno") == errno.EIO, f"expected EIO, got {out!r}"


class TestDirtyBlockUnion:
    """Tests for the dirty-block union logic used in MountHandlerClient.open().

    When multiple workers have different dirty blocks, the transfer uses
    the union of all dirty blocks so a single fanout covers everyone.
    """

    @staticmethod
    def _compute_dirty_union(
        worker_dirty: dict[int, list[int] | None],
        num_blocks: int,
    ) -> list[int]:
        """Reproduce the dirty-block union logic from MountHandlerClient.open()."""
        all_blocks = list(range(num_blocks))
        dirty_blocks: set[int] = set()
        for _rank, d in worker_dirty.items():
            if d is None:
                dirty_blocks = set(all_blocks)
                break
            dirty_blocks.update(d)
        return sorted(dirty_blocks)

    def test_single_stale_worker(self) -> None:
        """One stale worker (None) → all blocks dirty."""
        dirty = self._compute_dirty_union({0: None}, num_blocks=5)
        assert dirty == [0, 1, 2, 3, 4]

    def test_single_partial_worker(self) -> None:
        """One partial worker → only its dirty blocks."""
        dirty = self._compute_dirty_union({0: [1, 3]}, num_blocks=5)
        assert dirty == [1, 3]

    def test_multiple_partial_workers_union(self) -> None:
        """Two partial workers with different dirty blocks → union."""
        dirty = self._compute_dirty_union({0: [1, 3], 1: [2, 3]}, num_blocks=5)
        assert dirty == [1, 2, 3]

    def test_stale_dominates_partial(self) -> None:
        """One stale + one partial → all blocks (stale forces full transfer)."""
        dirty = self._compute_dirty_union({0: [1], 1: None}, num_blocks=5)
        assert dirty == [0, 1, 2, 3, 4]

    def test_empty_dirty(self) -> None:
        """No dirty workers → empty list."""
        dirty = self._compute_dirty_union({}, num_blocks=5)
        assert dirty == []

    def test_overlapping_partial_deduped(self) -> None:
        """Overlapping dirty blocks from multiple workers are deduplicated."""
        dirty = self._compute_dirty_union(
            {0: [0, 1, 2], 1: [1, 2, 3], 2: [2, 3, 4]}, num_blocks=5
        )
        assert dirty == [0, 1, 2, 3, 4]


class TestEnsureStorageLogic:
    """Tests for the ensure_storage allocation logic.

    Since ensure_storage is an @endpoint (can't be called outside an actor
    runtime), we test the equivalent mmap allocation/resize logic directly.
    """

    def test_anonymous_mmap_allocation(self) -> None:
        """Anonymous mmap is allocated at the requested size."""
        import mmap as _mmap

        storage = _mmap.mmap(-1, 1024, _mmap.MAP_PRIVATE | _mmap.MAP_ANONYMOUS)
        mv = memoryview(storage)
        assert len(mv) == 1024
        mv[:4] = b"test"
        assert mv[:4] == b"test"

    def test_file_backed_preserves_on_resize(self) -> None:
        """File-backed mmap preserves data when resized (no O_TRUNC)."""
        import mmap as _mmap

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "cache.bin")
            # Create and write initial data.
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
            os.ftruncate(fd, 1024)
            m1 = _mmap.mmap(fd, 1024)
            os.close(fd)
            mv1 = memoryview(m1)
            mv1[:4] = b"keep"
            m1.flush()
            # Release before resize.
            del mv1
            m1.close()
            # Resize without O_TRUNC — data preserved.
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
            os.ftruncate(fd, 2048)
            m2 = _mmap.mmap(fd, 2048)
            os.close(fd)
            mv2 = memoryview(m2)
            assert mv2[:4] == b"keep"
            assert len(mv2) == 2048

    def test_chunk_building(self) -> None:
        """Chunks are built correctly from a buffer."""
        import mmap as _mmap

        total_size = 1000
        chunk_size = 400
        storage = _mmap.mmap(-1, total_size, _mmap.MAP_PRIVATE | _mmap.MAP_ANONYMOUS)
        mv = memoryview(storage)
        chunks = []
        remaining = total_size
        off = 0
        while remaining > 0:
            sz = min(remaining, chunk_size)
            chunks.append(mv[off : off + sz])
            off += sz
            remaining -= sz
        assert len(chunks) == 3
        assert len(chunks[0]) == 400
        assert len(chunks[1]) == 400
        assert len(chunks[2]) == 200


class TestReceiveBlockPreservesCache:
    """Tests that receive_block does not corrupt cached data on resize.

    The critical bug: receive_block used O_TRUNC when reallocating storage
    on size change, which wiped all existing cached blocks. Only dirty
    blocks are retransferred, so non-dirty blocks became zeros.
    """

    def test_resize_preserves_existing_blocks(self) -> None:
        """Simulates receive_block resize: existing data must survive."""
        import mmap as _mmap

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "cache.bin")

            # Initial allocation — write data to first 1024 bytes.
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
            os.ftruncate(fd, 1024)
            m1 = _mmap.mmap(fd, 1024)
            os.close(fd)
            mv1 = memoryview(m1)
            mv1[:4] = b"ABCD"
            mv1[512:516] = b"EFGH"
            m1.flush()
            del mv1
            m1.close()

            # Resize WITHOUT O_TRUNC (correct behavior).
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
            os.ftruncate(fd, 2048)
            m2 = _mmap.mmap(fd, 2048)
            os.close(fd)
            mv2 = memoryview(m2)
            assert mv2[:4] == b"ABCD", "First block data lost on resize"
            assert mv2[512:516] == b"EFGH", "Second block data lost on resize"
            assert len(mv2) == 2048

    def test_otrunc_would_corrupt(self) -> None:
        """Proves O_TRUNC destroys existing data — the bug we fixed."""
        import mmap as _mmap

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "cache.bin")

            # Write initial data.
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
            os.ftruncate(fd, 1024)
            m1 = _mmap.mmap(fd, 1024)
            os.close(fd)
            mv1 = memoryview(m1)
            mv1[:4] = b"KEEP"
            m1.flush()
            del mv1
            m1.close()

            # Resize WITH O_TRUNC — data is destroyed.
            fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
            os.ftruncate(fd, 2048)
            m2 = _mmap.mmap(fd, 2048)
            os.close(fd)
            mv2 = memoryview(m2)
            assert mv2[:4] == b"\x00\x00\x00\x00", "O_TRUNC should zero the file"


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_actor_rdma_fanout_to_all_peers() -> None:
    """The leader's ``broadcast`` fans a block out to every peer over RDMA, and each
    peer's mount then serves it -- exercising ``setup_leader`` + ``broadcast`` +
    ``receive_rdma`` + the mesh-cast across a real multi-proc mesh (RDMA over the TCP
    fallback locally). Reading through each worker's own mount confirms delivery."""
    from monarch.actor import this_host
    from monarch.remotemount.remotemount import FUSEActor

    # A host with no RDMA device (e.g. OSS CI) reports backend "none" and RDMABuffer()
    # raises. Allow the TCP fallback via the env var, which the spawned worker procs
    # inherit -- a parent-process monarch.configure() does NOT reach spawned procs. Must
    # be set before spawn_procs.
    os.environ["MONARCH_RDMA_ALLOW_TCP_FALLBACK"] = "1"

    content = b"rdma fan-out reaches every peer"
    # ``broadcast`` stages a full BLOCK_SIZE buffer (mv[:] = data), so pad the block.
    block = content + bytes(BLOCK_SIZE - len(content))
    meta: dict[str, object] = {
        "/": {"attr": _dir_attr(), "children": ["f.bin"], "total_size": len(content)},
        "/f.bin": {
            "attr": _file_attr(len(content)),
            "global_offset": 0,
            "file_len": len(content),
        },
    }

    n = 3  # rank 0 = leader, ranks 1..2 = its RDMA fan-out peers
    procs = this_host().spawn_procs(per_host={"cpus": n})
    # handler=None: no fault callback needed -- the block is broadcast before any read.
    fuse_actors = procs.spawn("FUSEActor", FUSEActor, None)
    flat = fuse_actors.flatten("rank")

    with tempfile.TemporaryDirectory() as base:
        mnts = [os.path.join(base, f"rank{r}") for r in range(n)]
        try:
            # Each worker mounts at its own path (all procs share this host, so one
            # shared mount point would collide).
            for r in range(n):
                actor = flat.slice(rank=r)
                actor.mkdir.call_one(mnts[r]).get()
                actor.mount.call_one(mnts[r], meta).get()

            # Rank 0 leads; ranks 1.. are its fan-out peers.
            leader = flat.slice(rank=0)
            leader.setup_leader.call_one(flat.slice(rank=slice(1, None))).get()

            # Deliver block 0 to the leader; broadcast commits it locally and mesh-casts
            # it to every peer over RDMA.
            leader.broadcast.call_one(0, block, []).get()

            # Every worker -- leader and peers -- now serves the block from its own mount.
            for r in range(n):
                with open(os.path.join(mnts[r], "f.bin"), "rb") as f:
                    assert f.read() == content, f"rank {r} did not receive the block"
        finally:
            for r in range(n):
                with contextlib.suppress(Exception):
                    flat.slice(rank=r).unmount.call_one(mnts[r]).get()
            procs.stop().get()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_actor_cold_transfer() -> None:
    """End-to-end: create files, transfer via actor mode, verify on FUSE mount."""
    from monarch.actor import this_host
    from monarch.remotemount import remotemount

    with tempfile.TemporaryDirectory() as src:
        os.makedirs(os.path.join(src, "sub"), exist_ok=True)
        files = {
            "hello.txt": b"hello world",
            "data.bin": os.urandom(2048),
            os.path.join("sub", "nested.txt"): b"nested content here",
        }
        for name, content in files.items():
            with open(os.path.join(src, name), "wb") as f:
                f.write(content)

        with tempfile.TemporaryDirectory() as mnt:
            host = this_host()
            rm = remotemount(host, src, mnt)
            rm.open()
            try:
                for name, expected in files.items():
                    with open(os.path.join(mnt, name), "rb") as f:
                        assert f.read() == expected, f"content mismatch for {name}"
            finally:
                rm.close()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_actor_incremental_no_change() -> None:
    """Open, close, re-open without changes: workers should be fresh (skip transfer)."""
    from monarch.actor import this_host
    from monarch.remotemount import remotemount

    with tempfile.TemporaryDirectory() as src:
        with open(os.path.join(src, "f.txt"), "wb") as f:
            f.write(b"unchanged content")

        with tempfile.TemporaryDirectory() as mnt:
            host = this_host()
            rm = remotemount(host, src, mnt)

            # First open: full transfer.
            rm.open()
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == b"unchanged content"
            rm.close()

            # Second open: no changes, should skip transfer.
            rm.open()
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == b"unchanged content"
            rm.close()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_actor_incremental_partial() -> None:
    """Open, close, modify a file, re-open: only dirty blocks transferred."""
    from monarch.actor import this_host
    from monarch.remotemount import remotemount

    with tempfile.TemporaryDirectory() as src:
        path = os.path.join(src, "config.json")
        with open(path, "w") as f:
            f.write('{"lr": 0.001}')

        with tempfile.TemporaryDirectory() as mnt:
            host = this_host()
            rm = remotemount(host, src, mnt)

            # First open: full transfer.
            rm.open()
            with open(os.path.join(mnt, "config.json")) as f:
                assert f.read() == '{"lr": 0.001}'
            rm.close()

            # Modify the source file.
            with open(path, "w") as f:
                f.write('{"lr": 0.01, "epochs": 20}')

            # Second open: should detect change and re-transfer.
            rm.open()
            with open(os.path.join(mnt, "config.json")) as f:
                assert f.read() == '{"lr": 0.01, "epochs": 20}'
            rm.close()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_unmount_not_mounted_returns_status() -> None:
    """Unmounting a path that isn't mounted returns ('not_mounted', '')."""
    from monarch.actor import this_host
    from monarch.remotemount.remotemount import FUSEActor

    host = this_host()
    procs = host.spawn_procs()
    actors = procs.spawn("FUSEActor", FUSEActor, "slurm")

    with tempfile.TemporaryDirectory() as d:
        result = actors.unmount.call(d).get()
        for _rank, (status, detail) in result:
            assert status == "not_mounted"
            assert detail == ""


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_unmount_after_mount_returns_ok() -> None:
    """Mount a FUSEActor, then unmount via its endpoint: first call returns 'ok',
    second 'not_mounted'. Spawns the FUSEActor directly -- the MountHandler handle
    does not expose its worker mesh."""
    from monarch.actor import this_host
    from monarch.remotemount.remotemount import build_index, FUSEActor

    host = this_host()
    procs = host.spawn_procs()
    actors = procs.spawn("FUSEActor", FUSEActor, "handler")

    with tempfile.TemporaryDirectory() as src, tempfile.TemporaryDirectory() as mnt:
        with open(os.path.join(src, "f.txt"), "wb") as f:
            f.write(b"test content")
        meta = build_index(src, {})
        actors.mount.call(mnt, meta).get()

        # First unmount -> ok.
        result = actors.unmount.call(mnt).get()
        for _rank, (status, detail) in result:
            assert status == "ok"
            assert detail == ""

        # Second unmount -> not_mounted.
        result = actors.unmount.call(mnt).get()
        for _rank, (status, _detail) in result:
            assert status == "not_mounted"


_tls_certs_available: bool = os.path.exists("/var/facebook/x509_identities/server.pem")


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.skipif(not _tls_certs_available, reason="TLS certs not available")
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_tls_cold_transfer() -> None:
    """End-to-end: create files, transfer via rust_tls mode, verify on FUSE mount."""
    from monarch.actor import this_host
    from monarch.remotemount import remotemount

    with tempfile.TemporaryDirectory() as src:
        os.makedirs(os.path.join(src, "sub"), exist_ok=True)
        files = {
            "hello.txt": b"hello world",
            "data.bin": os.urandom(2048),
            os.path.join("sub", "nested.txt"): b"nested content here",
        }
        for name, content in files.items():
            with open(os.path.join(src, name), "wb") as f:
                f.write(content)

        with tempfile.TemporaryDirectory() as mnt:
            host = this_host()
            rm = remotemount(host, src, mnt)
            rm.open()
            try:
                for name, expected in files.items():
                    with open(os.path.join(mnt, name), "rb") as f:
                        assert f.read() == expected, f"content mismatch for {name}"
            finally:
                rm.close()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.skipif(not _tls_certs_available, reason="TLS certs not available")
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_tls_incremental_no_change() -> None:
    """rust_tls: open, close, re-open without changes — workers should be fresh."""
    from monarch.actor import this_host
    from monarch.remotemount import remotemount

    with tempfile.TemporaryDirectory() as src:
        with open(os.path.join(src, "f.txt"), "wb") as f:
            f.write(b"unchanged content")

        with tempfile.TemporaryDirectory() as mnt:
            host = this_host()
            rm = remotemount(host, src, mnt)

            rm.open()
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == b"unchanged content"
            rm.close()

            rm.open()
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == b"unchanged content"
            rm.close()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.skipif(not _tls_certs_available, reason="TLS certs not available")
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_tls_incremental_partial() -> None:
    """rust_tls: open, close, modify a file, re-open — only dirty blocks transferred."""
    from monarch.actor import this_host
    from monarch.remotemount import remotemount

    with tempfile.TemporaryDirectory() as src:
        path = os.path.join(src, "config.json")
        with open(path, "w") as f:
            f.write('{"lr": 0.001}')

        with tempfile.TemporaryDirectory() as mnt:
            host = this_host()
            rm = remotemount(host, src, mnt)

            rm.open()
            with open(os.path.join(mnt, "config.json")) as f:
                assert f.read() == '{"lr": 0.001}'
            rm.close()

            with open(path, "w") as f:
                f.write('{"lr": 0.01, "epochs": 20}')

            rm.open()
            with open(os.path.join(mnt, "config.json")) as f:
                assert f.read() == '{"lr": 0.01, "epochs": 20}'
            rm.close()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_actor_refresh() -> None:
    """End-to-end: open with refreshable=True, modify source, refresh without unmounting."""
    from monarch.actor import this_host
    from monarch.remotemount import remotemount

    with tempfile.TemporaryDirectory() as src:
        path = os.path.join(src, "config.json")
        with open(path, "w") as f:
            f.write('{"lr": 0.001}')

        with tempfile.TemporaryDirectory() as mnt:
            host = this_host()
            rm = remotemount(host, src, mnt)

            rm.open()
            with open(os.path.join(mnt, "config.json")) as f:
                assert f.read() == '{"lr": 0.001}'

            # Modify the source file.
            with open(path, "w") as f:
                f.write('{"lr": 0.01, "epochs": 20}')

            # Refresh (no close/open cycle).
            rm.refresh()
            with open(os.path.join(mnt, "config.json")) as f:
                assert f.read() == '{"lr": 0.01, "epochs": 20}'

            rm.close()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_actor_refresh_with_open_handles() -> None:
    """Refresh while file handles are open — reads see updated data."""
    from monarch.actor import this_host
    from monarch.remotemount import remotemount

    with tempfile.TemporaryDirectory() as src:
        path = os.path.join(src, "data.bin")
        with open(path, "wb") as f:
            f.write(b"AAAA")

        with tempfile.TemporaryDirectory() as mnt:
            host = this_host()
            rm = remotemount(host, src, mnt)

            rm.open()

            # Open a file handle before refresh.
            fh = open(os.path.join(mnt, "data.bin"), "rb")
            assert fh.read() == b"AAAA"

            # Modify source and refresh.
            with open(path, "wb") as f:
                f.write(b"BBBB")
            rm.refresh()

            # Re-read from existing handle — should see new data.
            fh.seek(0)
            assert fh.read() == b"BBBB"
            fh.close()

            rm.close()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_actor_refresh_no_change() -> None:
    """Refresh with no source changes — should be a no-op."""
    from monarch.actor import this_host
    from monarch.remotemount import remotemount

    with tempfile.TemporaryDirectory() as src:
        with open(os.path.join(src, "f.txt"), "wb") as f:
            f.write(b"unchanged")

        with tempfile.TemporaryDirectory() as mnt:
            host = this_host()
            rm = remotemount(host, src, mnt)

            rm.open()
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == b"unchanged"

            # Refresh without changes.
            rm.refresh()
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == b"unchanged"

            rm.close()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_actor_refresh_add_file() -> None:
    """Refresh after adding a new file to the source directory."""
    from monarch.actor import this_host
    from monarch.remotemount import remotemount

    with tempfile.TemporaryDirectory() as src:
        with open(os.path.join(src, "a.txt"), "wb") as f:
            f.write(b"aaa")

        with tempfile.TemporaryDirectory() as mnt:
            host = this_host()
            rm = remotemount(host, src, mnt)

            rm.open()
            assert os.listdir(mnt) == ["a.txt"]

            # Add a new file and refresh.
            with open(os.path.join(src, "b.txt"), "wb") as f:
                f.write(b"bbb")
            rm.refresh()

            assert sorted(os.listdir(mnt)) == ["a.txt", "b.txt"]
            with open(os.path.join(mnt, "b.txt"), "rb") as f:
                assert f.read() == b"bbb"

            rm.close()
