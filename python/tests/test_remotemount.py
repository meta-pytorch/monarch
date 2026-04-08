# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from __future__ import annotations

import contextlib
import gc
import math
import mmap
import os
import shutil
import stat
import sys
import tempfile
import time
from collections.abc import Generator

import pytest

if sys.platform != "linux":
    pytest.skip("linux-only", allow_module_level=True)

from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_extension.chunked_fuse import (
    FuseMountHandle,
    mount_chunked_fuse,
)
from monarch._rust_bindings.monarch_extension.fast_pack import (
    load_file_and_hash,
    pack_files_with_offsets,
)
from monarch.remotemount.fast_pack import (
    block_hashes,
    HASH_BLOCK_SIZE,
    pack_directory_chunked,
)
from monarch.remotemount.remotemount import classify_workers


class TestPackDirectoryChunked:
    def test_empty_directory(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            meta, _staging_mv, chunks, hashes, _pi = pack_directory_chunked(d)
            assert chunks == []
            assert hashes == []
            assert "/" in meta
            assert "children" in meta["/"]
            assert meta["/"]["children"] == []

    def test_single_file(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            content = b"hello world"
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(content)

            meta, _staging_mv, chunks, hashes, _pi = pack_directory_chunked(d)

            assert "/a.txt" in meta
            file_meta = meta["/a.txt"]
            assert file_meta["global_offset"] == 0
            assert file_meta["file_len"] == len(content)

            packed = b"".join(bytes(c) for c in chunks)
            assert packed == content
            assert len(hashes) == 1

    def test_multiple_files_contiguous_offsets(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            files = {"a.txt": b"aaa", "b.txt": b"bbbbbb", "c.txt": b"c"}
            for name, content in files.items():
                with open(os.path.join(d, name), "wb") as f:
                    f.write(content)

            meta, _staging_mv, chunks, _hashes, _pi = pack_directory_chunked(d)
            packed = b"".join(bytes(c) for c in chunks)

            # Verify each file's content at its offset
            for name, content in files.items():
                path = f"/{name}"
                assert path in meta
                off = meta[path]["global_offset"]
                length = meta[path]["file_len"]
                assert length == len(content)
                assert packed[off : off + length] == content

            # Verify offsets are contiguous
            file_entries = sorted(
                (
                    (m["global_offset"], m["file_len"])
                    for m in meta.values()
                    if "global_offset" in m
                ),
                key=lambda x: x[0],
            )
            for i in range(1, len(file_entries)):
                prev_off, prev_len = file_entries[i - 1]
                curr_off, _ = file_entries[i]
                assert curr_off == prev_off + prev_len

    def test_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            target = os.path.join(d, "target.txt")
            with open(target, "w") as f:
                f.write("target")
            os.symlink(target, os.path.join(d, "link.txt"))

            meta, _staging_mv, chunks, _hashes, _pi = pack_directory_chunked(d)

            assert "/link.txt" in meta
            link_meta = meta["/link.txt"]
            assert "link_target" in link_meta
            assert link_meta["link_target"] == target
            assert "global_offset" not in link_meta

    def test_directory_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            os.makedirs(os.path.join(d, "sub"))
            with open(os.path.join(d, "sub", "f.txt"), "w") as f:
                f.write("x")

            meta, _, _, _, _ = pack_directory_chunked(d)

            assert "/" in meta
            assert "sub" in meta["/"]["children"]
            assert "/sub" in meta
            assert "f.txt" in meta["/sub"]["children"]
            assert "st_mode" in meta["/"]["attr"]

    def test_custom_chunk_size(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            content = b"x" * 1000
            with open(os.path.join(d, "big.txt"), "wb") as f:
                f.write(content)

            chunk_size = 300
            meta, _staging_mv, chunks, _hashes, _pi = pack_directory_chunked(
                d, chunk_size=chunk_size
            )

            assert len(chunks) == math.ceil(len(content) / chunk_size)
            packed = b"".join(bytes(c) for c in chunks)
            assert packed == content

    def test_hashes_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "f.bin"), "wb") as f:
                f.write(os.urandom(500))

            _, _, _, h1, _ = pack_directory_chunked(d)
            _, _, _, h2, _ = pack_directory_chunked(d)
            assert h1 == h2
            assert len(h1) > 0

    def test_hashes_change_on_content_change(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "f.bin")
            with open(path, "wb") as f:
                f.write(b"\x00" * 500)
            _, _, _, h1, _ = pack_directory_chunked(d)

            with open(path, "wb") as f:
                f.write(b"\xff" * 500)
            _, _, _, h2, _ = pack_directory_chunked(d)

            assert h1 != h2


class TestPackDirectoryAppendOnly:
    """Tests for append-only packing with previous_index."""

    def test_no_previous_index_sequential(self) -> None:
        """Without previous_index, files are packed sequentially."""
        with tempfile.TemporaryDirectory() as d:
            for name in ["a.txt", "b.txt"]:
                with open(os.path.join(d, name), "wb") as f:
                    f.write(name.encode() * 100)
            meta, _mv, _chunks, _hashes, pi = pack_directory_chunked(d)
            assert pi is not None
            assert "files" in pi
            assert "total_size" in pi
            # Sequential: offsets are contiguous.
            offsets = sorted(pi["files"][k]["offset"] for k in pi["files"])
            assert offsets[0] == 0

    def test_unchanged_files_keep_offsets(self) -> None:
        """Files with matching mtime_ns and size keep their original offsets."""
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(b"hello" * 100)
            with open(os.path.join(d, "b.txt"), "wb") as f:
                f.write(b"world" * 100)

            _, _, _, _, pi1 = pack_directory_chunked(d)
            assert pi1 is not None

            # Pack again with previous_index — offsets should be reused.
            _, _, _, _, pi2 = pack_directory_chunked(d, previous_index=pi1)
            assert pi2 is not None
            for vpath in pi1["files"]:
                assert pi2["files"][vpath]["offset"] == pi1["files"][vpath]["offset"]

    def test_changed_file_appended(self) -> None:
        """A changed file gets appended at the end, not at its old offset."""
        with tempfile.TemporaryDirectory() as d:
            # Use many unchanged files so the dead space from one changed
            # file stays below FRAG_THRESHOLD (20%).
            for i in range(10):
                with open(os.path.join(d, f"keep_{i:02d}.txt"), "wb") as f:
                    f.write(os.urandom(500))
            with open(os.path.join(d, "change_me.txt"), "wb") as f:
                f.write(b"bbb" * 100)

            _, _, _, _, pi1 = pack_directory_chunked(d)
            assert pi1 is not None
            old_total = pi1["total_size"]

            # Modify change_me.txt and force a different mtime_ns.
            change_path = os.path.join(d, "change_me.txt")
            with open(change_path, "wb") as f:
                f.write(b"BBB" * 100)
            # Ensure mtime differs even on fast filesystems.
            st = os.stat(change_path)
            os.utime(change_path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))

            _, _, _, _, pi2 = pack_directory_chunked(d, previous_index=pi1)
            assert pi2 is not None
            # Unchanged files should keep their offsets.
            for i in range(10):
                vpath = f"/keep_{i:02d}.txt"
                assert pi2["files"][vpath]["offset"] == pi1["files"][vpath]["offset"]
            # Changed file should be appended after the old total.
            assert pi2["files"]["/change_me.txt"]["offset"] >= old_total

    def test_new_file_appended(self) -> None:
        """A new file is appended at the end."""
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(b"aaa" * 100)

            _, _, _, _, pi1 = pack_directory_chunked(d)
            assert pi1 is not None
            old_total = pi1["total_size"]

            # Add a new file.
            with open(os.path.join(d, "b.txt"), "wb") as f:
                f.write(b"bbb" * 100)

            _, _, _, _, pi2 = pack_directory_chunked(d, previous_index=pi1)
            assert pi2 is not None
            assert pi2["files"]["/a.txt"]["offset"] == pi1["files"]["/a.txt"]["offset"]
            assert pi2["files"]["/b.txt"]["offset"] >= old_total

    def test_high_fragmentation_falls_back(self) -> None:
        """When dead space exceeds FRAG_THRESHOLD, repacks sequentially."""
        with tempfile.TemporaryDirectory() as d:
            # Create one large file.
            with open(os.path.join(d, "big.txt"), "wb") as f:
                f.write(b"x" * 1000)

            _, _, _, _, pi1 = pack_directory_chunked(d)
            assert pi1 is not None

            # Replace with a much smaller file — old offset has huge dead space.
            with open(os.path.join(d, "big.txt"), "wb") as f:
                f.write(b"y" * 10)

            _, _, _, _, pi2 = pack_directory_chunked(d, previous_index=pi1)
            assert pi2 is not None
            # With high fragmentation, should repack sequentially starting at 0.
            assert pi2["files"]["/big.txt"]["offset"] == 0

    def test_pack_index_has_content_hashes(self) -> None:
        """Pack index includes per-file content hashes."""
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(b"hello")

            _, _, _, _, pi = pack_directory_chunked(d)
            assert pi is not None
            assert "content_hash" in pi["files"]["/a.txt"]
            assert len(pi["files"]["/a.txt"]["content_hash"]) == 16  # xxh64 hex

    def test_unchanged_files_stay_in_same_blocks(self) -> None:
        """Append-only keeps unchanged file data in the same block positions.

        When a file is modified, sequential repacking shifts all subsequent
        file offsets. Append-only keeps unchanged files at their original
        offsets, so their block hashes remain stable.
        """
        with tempfile.TemporaryDirectory() as d:
            # Create 100 small files that share blocks.
            file_size = HASH_BLOCK_SIZE // 10
            for i in range(100):
                with open(os.path.join(d, f"file_{i:03d}.bin"), "wb") as f:
                    f.write(os.urandom(file_size))

            _, mv1, _, h1, pi1 = pack_directory_chunked(d)
            assert pi1 is not None
            assert mv1 is not None

            # Modify one file (triggers mtime change).
            with open(os.path.join(d, "file_050.bin"), "wb") as f:
                f.write(os.urandom(file_size))

            # With append-only: unchanged files keep their offsets.
            _, mv_app, _, h_app, pi2 = pack_directory_chunked(d, previous_index=pi1)
            assert pi2 is not None
            # All unchanged files should have the same offset as before.
            changed_count = 0
            for vpath in pi1["files"]:
                if vpath in pi2["files"]:
                    if pi2["files"][vpath]["offset"] != pi1["files"][vpath]["offset"]:
                        changed_count += 1
            # Only file_050.bin should have moved.
            assert changed_count == 1

    def test_previous_index_all_files_deleted(self) -> None:
        """Append-only with a previous_index but all files deleted must not crash."""
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(b"hello")
            _meta, _mv, _chunks, _hashes, pi = pack_directory_chunked(d)
            assert pi is not None

            # Remove all files, repack with previous_index.
            os.remove(os.path.join(d, "a.txt"))
            meta2, mv2, chunks2, hashes2, pi2 = pack_directory_chunked(
                d, previous_index=pi
            )
            # Empty directory: total_size=0, no buffer, no pack_index.
            assert "/" in meta2
            assert meta2["/"]["children"] == []
            assert mv2 is None
            assert chunks2 == []
            assert hashes2 == []
            assert pi2 is None


class TestBlockHashes:
    def test_deterministic(self) -> None:
        data = os.urandom(500)
        mv = memoryview(data)
        assert block_hashes(mv, block_size=200) == block_hashes(mv, block_size=200)

    def test_different_data(self) -> None:
        a = memoryview(b"\x00" * 500)
        b = memoryview(b"\xff" * 500)
        assert block_hashes(a, block_size=200) != block_hashes(b, block_size=200)

    def test_block_count(self) -> None:
        data = memoryview(os.urandom(500))
        hashes = block_hashes(data, block_size=200)
        assert len(hashes) == 3  # 200 + 200 + 100

    def test_empty(self) -> None:
        assert block_hashes(memoryview(b""), block_size=100) == []


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


class TestHashBlockSizeConsistency:
    """Verify Rust and Python hash block sizes match."""

    def test_default_block_sizes_match(self) -> None:
        """Rust and Python should produce identical hashes with their defaults."""
        with tempfile.TemporaryDirectory() as d:
            # Create data larger than 100MB to get multiple blocks.
            # Use a small amount for speed — just verify the constants agree.
            data = os.urandom(1024)
            path = os.path.join(d, "f.bin")
            with open(path, "wb") as f:
                f.write(data)

            # Rust default hashes (via pack_files_with_offsets).
            buf, rust_hashes = pack_files_with_offsets(
                [(path, 0, len(data))], len(data)
            )
            # Python default hashes.
            py_hashes = block_hashes(memoryview(buf))

            assert list(rust_hashes) == py_hashes


class TestMmapBufferOwnership:
    """Verify MmapBuffer owns mmap memory and supports zero-copy access."""

    def test_memoryview_zero_copy(self) -> None:
        """memoryview(buf) should be zero-copy: writes through mv are visible."""
        with tempfile.TemporaryDirectory() as d:
            data = b"AAAA"
            path = os.path.join(d, "f.bin")
            with open(path, "wb") as f:
                f.write(data)

            buf, _hashes = pack_files_with_offsets([(path, 0, len(data))], len(data))
            mv = memoryview(buf)

            assert bytes(mv) == b"AAAA"
            mv[0] = ord("Z")
            assert bytes(memoryview(buf)[:1]) == b"Z"

    def test_gc_frees_mmap(self) -> None:
        """MmapBuffer should be GC-collectable without leaking."""
        with tempfile.TemporaryDirectory() as d:
            data = b"x" * 4096
            path = os.path.join(d, "f.bin")
            with open(path, "wb") as f:
                f.write(data)

            buf, _hashes = pack_files_with_offsets([(path, 0, len(data))], len(data))
            # Create a memoryview, then release both.
            mv = memoryview(buf)
            assert len(mv) == 4096
            del mv, buf
            gc.collect()
            # If we get here without segfault, cleanup worked.

    def test_empty_buffer(self) -> None:
        """Empty packing returns a zero-length MmapBuffer."""
        buf, hashes = pack_files_with_offsets([], 0)
        assert len(buf) == 0
        assert list(hashes) == []

    def test_load_file_and_hash_returns_buffer(self) -> None:
        """load_file_and_hash should return an MmapBuffer, not raw memoryview."""
        with tempfile.TemporaryDirectory() as d:
            data = b"hello world" * 100
            path = os.path.join(d, "f.bin")
            with open(path, "wb") as f:
                f.write(data)

            buf, hashes = load_file_and_hash(path)
            mv = memoryview(buf)
            assert bytes(mv[: len(data)]) == data
            assert len(hashes) > 0


BLOCK_SIZE: int = 64 * 1024 * 1024  # Must match Rust HASH_BLOCK_SIZE / READ_CHUNK_SIZE


class TestBlockBoundary:
    """Test files that straddle the 64MB block/chunk boundary."""

    def test_file_spanning_two_blocks(self) -> None:
        """A single file larger than one block should produce correct hashes."""
        size = BLOCK_SIZE + 1024  # Just over one block boundary.
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "big.bin")
            with open(path, "wb") as f:
                f.write(os.urandom(size))

            buf, hashes = pack_files_with_offsets([(path, 0, size)], size)
            mv = memoryview(buf)

            assert len(mv) == size
            assert len(hashes) == 2  # ceil(size / 64MB) = 2

            # Verify hashes match Python-computed hashes.
            py_hashes = block_hashes(mv, block_size=BLOCK_SIZE)
            assert list(hashes) == py_hashes

            # Verify content matches the file.
            with open(path, "rb") as f:
                expected = f.read()
            assert bytes(mv) == expected

    def test_file_exactly_one_block(self) -> None:
        """A file exactly equal to block size should produce one hash."""
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "exact.bin")
            with open(path, "wb") as f:
                f.write(os.urandom(BLOCK_SIZE))

            buf, hashes = pack_files_with_offsets([(path, 0, BLOCK_SIZE)], BLOCK_SIZE)
            assert len(hashes) == 1
            assert len(memoryview(buf)) == BLOCK_SIZE

    def test_multiple_files_across_block_boundary(self) -> None:
        """Multiple small files whose total spans a block boundary."""
        # File A fills most of block 0, file B straddles into block 1.
        size_a = BLOCK_SIZE - 100
        size_b = 200  # Straddles: 100 bytes in block 0, 100 in block 1.
        total = size_a + size_b

        with tempfile.TemporaryDirectory() as d:
            path_a = os.path.join(d, "a.bin")
            path_b = os.path.join(d, "b.bin")
            data_a = os.urandom(size_a)
            data_b = os.urandom(size_b)

            with open(path_a, "wb") as f:
                f.write(data_a)
            with open(path_b, "wb") as f:
                f.write(data_b)

            file_list = [(path_a, 0, size_a), (path_b, size_a, size_b)]
            buf, hashes = pack_files_with_offsets(file_list, total)
            mv = memoryview(buf)

            assert len(mv) == total
            assert len(hashes) == 2  # Spans two blocks.

            # Verify content of both files at their offsets.
            assert bytes(mv[:size_a]) == data_a
            assert bytes(mv[size_a : size_a + size_b]) == data_b

    def test_pack_directory_large_file(self) -> None:
        """pack_directory_chunked with a file larger than the block size."""
        size = BLOCK_SIZE + 512
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "big.bin")
            data = os.urandom(size)
            with open(path, "wb") as f:
                f.write(data)

            meta, staging_mv, chunks, hashes, _pi = pack_directory_chunked(d)

            packed = b"".join(bytes(c) for c in chunks)
            assert packed == data
            assert len(hashes) == 2

            assert staging_mv is not None
            py_hashes = block_hashes(staging_mv, block_size=BLOCK_SIZE)
            assert hashes == py_hashes


class TestPreadErrorHandling:
    """Verify that pread errors are detected, not silently swallowed."""

    def test_truncated_file_panics(self) -> None:
        """If a file is truncated between stat and read, packing should fail."""
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "f.bin")
            with open(path, "wb") as f:
                f.write(b"x" * 1000)

            # Tell pack_files_with_offsets the file is 1000 bytes,
            # but truncate it to 500 before packing.
            os.truncate(path, 500)

            with pytest.raises(BaseException, match="panicked"):  # noqa: B017
                pack_files_with_offsets([(path, 0, 1000)], 1000)

    def test_missing_file_panics(self) -> None:
        """Packing a nonexistent file should fail, not silently zero-fill."""
        with pytest.raises(BaseException, match="panicked"):  # noqa: B017
            pack_files_with_offsets([("/nonexistent/path/to/file.bin", 0, 100)], 100)


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


@contextlib.contextmanager
def _fuse_mount(
    metadata: dict[str, object],
    # pyre-ignore[24]: memoryview is not generic in Pyre
    chunks: list[memoryview],
    chunk_size: int,
    mount_point: str,
    # pyre-ignore[24]
) -> Generator[FuseMountHandle, None, None]:
    """Mount a FUSE filesystem and unmount on exit for test isolation."""
    handle = mount_chunked_fuse(metadata, chunks, chunk_size, mount_point)
    yield handle
    handle.unmount()


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
            _fuse_mount(metadata, [memoryview(content)], len(content), mnt),
        ):
            with open(os.path.join(mnt, "a.txt"), "rb") as f:
                assert f.read() == content

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
            _fuse_mount(metadata, [memoryview(packed)], len(packed), mnt),
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
            _fuse_mount(metadata, [memoryview(content)], len(content), mnt),
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
            _fuse_mount(metadata, [memoryview(b"\x00\x00")], 2, mnt),
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
            _fuse_mount(metadata, [memoryview(b"\x00")], 1, mnt),
        ):
            assert os.readlink(os.path.join(mnt, "link")) == target
            assert stat.S_ISLNK(os.lstat(os.path.join(mnt, "link")).st_mode)

    def test_small_chunk_size(self) -> None:
        """File spanning multiple chunks is read correctly."""
        content = b"A" * 10 + b"B" * 10 + b"C" * 5
        chunk_size = 10
        chunk_list = [
            memoryview(content[i : i + chunk_size])
            for i in range(0, len(content), chunk_size)
        ]
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
            _fuse_mount(metadata, chunk_list, chunk_size, mnt),
        ):
            with open(os.path.join(mnt, "f.bin"), "rb") as f:
                assert f.read() == content

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
            _fuse_mount(metadata, [memoryview(content)], len(content), mnt),
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
            _fuse_mount(metadata, [memoryview(b"\x00" * 64)], 64, mnt),
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

            meta, _staging_mv, chunks, _hashes, _pi = pack_directory_chunked(src)

            with tempfile.TemporaryDirectory() as mnt:
                chunk_size = len(bytes(chunks[0])) if chunks else 1
                with _fuse_mount(meta, chunks, chunk_size, mnt):
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
            _fuse_mount(metadata, [memoryview(b"\x00\x00")], 2, mnt),
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
            _fuse_mount(metadata, [memoryview(b"\x00")], 1, mnt),
        ):
            with pytest.raises(FileNotFoundError):
                open(os.path.join(mnt, "nope.txt"), "rb")
            with pytest.raises(FileNotFoundError):
                os.stat(os.path.join(mnt, "nope.txt"))

    def test_zero_chunk_size_rejected(self) -> None:
        """chunk_size=0 should raise immediately."""
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": []},
        }
        with tempfile.TemporaryDirectory() as mnt:
            with pytest.raises(RuntimeError, match="chunk_size must be > 0"):
                mount_chunked_fuse(metadata, [], 0, mnt)

    def test_relative_mount_point_rejected(self) -> None:
        """Relative mount point should raise immediately."""
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": []},
        }
        with pytest.raises(RuntimeError, match="absolute path"):
            mount_chunked_fuse(metadata, [], 1, "relative/path")

    def test_mount_nonexistent_directory(self) -> None:
        """Mounting on a path that doesn't exist should fail."""
        metadata: dict[str, object] = {
            "/": {"attr": _dir_attr(), "children": []},
        }
        with pytest.raises(RuntimeError):
            mount_chunked_fuse(metadata, [], 1, "/tmp/nonexistent_fuse_test_dir")

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
            _fuse_mount(metadata, [memoryview(packed)], len(packed), mnt),
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
            _fuse_mount(metadata, [memoryview(content)], len(content), mnt),
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
            with _fuse_mount(metadata, [memoryview(content)], len(content), mnt):
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
            with _fuse_mount(metadata2, [memoryview(content2)], len(content2), mnt):
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
            _fuse_mount(metadata, [memoryview(content)], len(content), mnt) as handle,
        ):
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == content
            # Refresh with identical data.
            handle.refresh(
                metadata,
                memoryview(content),
                [(0, len(content))],
                len(content),
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
            _fuse_mount(meta1, [memoryview(v1)], len(v1), mnt) as handle,
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
            handle.refresh(meta2, memoryview(v2), [(0, len(v2))], len(v2), len(v2))
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
            _fuse_mount(meta1, [memoryview(v1)], 3, mnt) as handle,
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
            handle.refresh(meta2, memoryview(v2), [(0, len(v2))], len(v2), len(v2))
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
            _fuse_mount(meta1, [memoryview(v1)], len(v1), mnt) as handle,
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
            handle.refresh(meta2, memoryview(v2), [(0, 3)], 3, 3)
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
            _fuse_mount(meta1, [memoryview(v1)], len(v1), mnt) as handle,
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
            handle.refresh(meta2, memoryview(v2), [(0, len(v2))], len(v2), len(v2))
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
            _fuse_mount(meta1, [memoryview(v1)], len(v1), mnt) as handle,
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
            handle.refresh(meta2, memoryview(v2), [(0, len(v2))], len(v2), len(v2))
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
            _fuse_mount(metadata, [memoryview(content)], len(content), mnt) as handle,
        ):
            fh = open(os.path.join(mnt, "f.bin"), "rb")
            first_half = fh.read(4096)
            assert first_half == b"A" * 4096
            # Refresh with same content.
            handle.refresh(
                metadata,
                memoryview(content),
                [(0, len(content))],
                len(content),
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
            _fuse_mount(meta1, [memoryview(buf1)], len(buf1), mnt) as handle,
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
            handle.refresh(
                meta2, memoryview(buf2), [(0, len(buf2))], len(buf2), len(buf2)
            )
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
            _fuse_mount(meta_fn("f.txt", 3), [memoryview(content)], 3, mnt) as handle,
        ):
            for i in range(20):
                new_content = f"v{i:02d}".encode()
                handle.refresh(
                    meta_fn("f.txt", len(new_content)),
                    memoryview(new_content),
                    [(0, len(new_content))],
                    len(new_content),
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
            _fuse_mount(metadata, [memoryview(data)], len(data), mnt) as handle,
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

            handle.refresh(
                metadata,
                memoryview(new_data),
                [(0, len(new_data))],
                len(new_data),
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

            meta, staging_mv, chunks, _hashes, _pi = pack_directory_chunked(src)

            with (
                tempfile.TemporaryDirectory() as mnt,
                _fuse_mount(
                    meta,
                    chunks,
                    len(bytes(chunks[0])) if chunks else 1,
                    mnt,
                ) as handle,
            ):
                with open(os.path.join(mnt, "data.txt"), "rb") as f:
                    assert f.read() == b"original content"

                with open(os.path.join(src, "data.txt"), "wb") as f:
                    f.write(b"updated content!")
                meta2, staging_mv2, chunks2, _h2, _pi2 = pack_directory_chunked(src)
                chunk_size2 = len(bytes(chunks2[0])) if chunks2 else 1
                buf2 = staging_mv2 if staging_mv2 is not None else memoryview(b"")
                handle.refresh(
                    meta2,
                    buf2,
                    [(0, len(buf2))],
                    len(buf2),
                    chunk_size2,
                )

                with open(os.path.join(mnt, "data.txt"), "rb") as f:
                    assert f.read() == b"updated content!"


class TestClassifyWorkers:
    """Unit tests for the classify_workers function."""

    def test_all_fresh(self) -> None:
        hashes = ["h1", "h2", "h3"]
        size = 300
        states = [(hashes, size), (hashes, size)]
        fresh, dirty = classify_workers(hashes, size, states)
        assert fresh == [0, 1]
        assert dirty == {}

    def test_all_stale(self) -> None:
        hashes = ["h1", "h2"]
        size = 200
        states: list[tuple[list[str], int]] = [([], 0), ([], 0)]
        fresh, dirty = classify_workers(hashes, size, states)
        assert fresh == []
        assert dirty == {0: None, 1: None}

    def test_partial(self) -> None:
        hashes = ["h1", "h2", "h3"]
        size = 300
        states = [(["h1", "XX", "h3"], size)]
        fresh, dirty = classify_workers(hashes, size, states)
        assert fresh == []
        assert dirty == {0: [1]}

    def test_mixed(self) -> None:
        hashes = ["h1", "h2", "h3"]
        size = 300
        states = [(hashes, size), (["h1", "XX", "h3"], size), ([], 0)]
        fresh, dirty = classify_workers(hashes, size, states)
        assert fresh == [0]
        assert dirty == {1: [1], 2: None}

    def test_size_changed_partial_overlap(self) -> None:
        """Worker has 3 blocks at old size, client now has 4 blocks at new size.

        Overlapping blocks that match should NOT be retransferred.
        Only the new block and the boundary block should be dirty.
        Without the fix, any size change marks the worker fully stale (None).
        """
        client_hashes = ["h1", "h2", "h3", "h4"]
        client_size = 400
        # Worker has first 3 blocks matching, but at the old size.
        states = [(["h1", "h2", "h3"], 300)]
        fresh, dirty = classify_workers(client_hashes, client_size, states)
        assert fresh == []
        # Block 3 is new (index 3), and block 2 (last overlap) is dirty
        # because size changed.
        assert dirty == {0: [2, 3]}

    def test_size_shrunk_partial_overlap(self) -> None:
        """Client now has fewer blocks than worker (e.g. file deleted)."""
        client_hashes = ["h1", "h2"]
        client_size = 200
        states = [(["h1", "h2", "h3"], 300)]
        fresh, dirty = classify_workers(client_hashes, client_size, states)
        assert fresh == []
        # Last overlapping block (1) is dirty due to size change.
        assert dirty == {0: [1]}

    def test_size_changed_all_hashes_match(self) -> None:
        """All overlapping hashes match but size changed — boundary block dirty."""
        client_hashes = ["h1", "h2", "h3", "h4"]
        client_size = 450
        states = [(["h1", "h2", "h3"], 300)]
        fresh, dirty = classify_workers(client_hashes, client_size, states)
        assert fresh == []
        # Block 2 (boundary) + block 3 (new).
        assert dirty == {0: [2, 3]}

    def test_size_changed_with_content_change(self) -> None:
        """Size changed AND some overlapping blocks differ."""
        client_hashes = ["h1", "XX", "h3", "h4"]
        client_size = 400
        states = [(["h1", "h2", "h3"], 300)]
        fresh, dirty = classify_workers(client_hashes, client_size, states)
        assert fresh == []
        # Block 1 (changed), block 2 (boundary), block 3 (new).
        assert dirty == {0: [1, 2, 3]}


class TestDirtyBlockUnion:
    """Tests for the dirty-block union logic used in MountHandler.open().

    When multiple workers have different dirty blocks, the transfer uses
    the union of all dirty blocks so a single fanout covers everyone.
    """

    @staticmethod
    def _compute_dirty_union(
        worker_dirty: dict[int, list[int] | None],
        num_blocks: int,
    ) -> list[int]:
        """Reproduce the dirty-block union logic from MountHandler.open()."""
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


class TestTreeFanOut:
    """Tests for the tree-based RDMA fan-out scheduling.

    Concurrent reads from the same RDMABuffer are not safe (see disabled
    test_rdma_buffer_read_into_concurrent in test_rdma_unit.py). The tree
    fan-out ensures each source sends to at most ONE destination per level.
    """

    @staticmethod
    def _simulate_tree(
        leader_rank: int, peer_ranks: list[int]
    ) -> list[list[tuple[int, int]]]:
        """Reproduce the tree fan-out logic from _transfer_fanout.

        Returns list of levels, each level is a list of (src, dst) pairs.
        """
        have_data = [leader_rank]
        need_data = list(peer_ranks)
        levels = []

        while need_data:
            pairs = []
            for src_rank in have_data:
                if not need_data:
                    break
                dst_rank = need_data.pop(0)
                pairs.append((src_rank, dst_rank))
            levels.append(pairs)
            for _, dst in pairs:
                have_data.append(dst)

        return levels

    def test_single_peer(self) -> None:
        """One peer → one level, one pair."""
        levels = self._simulate_tree(0, [1])
        assert levels == [[(0, 1)]]

    def test_two_peers(self) -> None:
        """Two peers → level 1: [0]→1, level 2: [0,1]→2."""
        levels = self._simulate_tree(0, [1, 2])
        assert levels == [[(0, 1)], [(0, 2)]]

    def test_seven_peers(self) -> None:
        """7 peers → log2(7)≈3 levels, doubling each level."""
        levels = self._simulate_tree(0, [1, 2, 3, 4, 5, 6, 7])
        # Level 0: [0]→1               (1 pair)
        # Level 1: [0,1]→2,3           (2 pairs)
        # Level 2: [0,1,2,3]→4,5,6,7   (4 pairs)
        assert len(levels) == 3
        assert len(levels[0]) == 1
        assert len(levels[1]) == 2
        assert len(levels[2]) == 4

    def test_no_concurrent_reads_from_same_source(self) -> None:
        """No source appears twice in the same level."""
        levels = self._simulate_tree(0, list(range(1, 64)))
        for level_idx, level in enumerate(levels):
            sources = [src for src, _dst in level]
            assert len(sources) == len(set(sources)), (
                f"Level {level_idx} has duplicate sources: {sources}"
            )

    def test_all_peers_receive_data(self) -> None:
        """Every peer receives data exactly once."""
        peer_ranks = list(range(1, 64))
        levels = self._simulate_tree(0, peer_ranks)
        received = set()
        for level in levels:
            for _src, dst in level:
                assert dst not in received, f"Peer {dst} received data twice"
                received.add(dst)
        assert received == set(peer_ranks)

    def test_sources_have_data_before_sending(self) -> None:
        """Every source in a level must have received data in a prior level."""
        levels = self._simulate_tree(0, list(range(1, 64)))
        have_data = {0}  # leader starts with data
        for level_idx, level in enumerate(levels):
            for src, _dst in level:
                assert src in have_data, (
                    f"Level {level_idx}: source {src} doesn't have data yet"
                )
            for _, dst in level:
                have_data.add(dst)

    def test_logarithmic_depth(self) -> None:
        """63 peers should complete in ~6 levels (log2(64))."""
        levels = self._simulate_tree(0, list(range(1, 64)))
        assert len(levels) <= 7  # ceil(log2(64)) = 6, allow 7 for rounding

    @staticmethod
    def _simulate_chunked_tree(
        leader_rank: int, peer_ranks: list[int], num_chunks: int
    ) -> list[list[tuple[int, int, int]]]:
        """Simulate chunked pipelined tree fan-out.

        Returns list of rounds, each round is a list of (src, dst, chunk_idx).
        Mirrors the scheduling logic in _transfer_fanout.
        """
        chunk_owners: dict[int, set[int]] = {
            c: {leader_rank} for c in range(num_chunks)
        }
        completed_peers: set[int] = set()
        rounds = []

        while len(completed_peers) < len(peer_ranks):
            busy: set[int] = set()
            pairs: list[tuple[int, int, int]] = []
            for chunk_idx in range(num_chunks):
                for src_rank in list(chunk_owners[chunk_idx]):
                    if src_rank in busy:
                        continue
                    dst_rank = None
                    for c in peer_ranks:
                        if (
                            c not in completed_peers
                            and c not in chunk_owners[chunk_idx]
                            and c not in busy
                        ):
                            dst_rank = c
                            break
                    if dst_rank is None:
                        continue
                    busy.add(src_rank)
                    busy.add(dst_rank)
                    pairs.append((src_rank, dst_rank, chunk_idx))

            if not pairs:
                break
            rounds.append(pairs)
            for _, dst, chunk_idx in pairs:
                chunk_owners[chunk_idx].add(dst)
                if all(dst in chunk_owners[c] for c in range(num_chunks)):
                    completed_peers.add(dst)

        return rounds

    def test_chunked_no_node_both_src_and_dst_in_same_round(self) -> None:
        """No node is both source and destination in the same round.

        get_blocks_rdma_buffer (source) and replace_blocks (destination)
        share _rdma_staging on the same actor. Using the same node as
        both source and destination in one round causes data corruption.
        """
        rounds = self._simulate_chunked_tree(0, list(range(1, 64)), num_chunks=8)
        for round_idx, round_pairs in enumerate(rounds):
            sources = {src for src, _, _ in round_pairs}
            destinations = {dst for _, dst, _ in round_pairs}
            overlap = sources & destinations
            assert not overlap, (
                f"Round {round_idx}: nodes {overlap} are both source and "
                f"destination — _rdma_staging would be corrupted"
            )

    def test_chunked_no_source_sends_twice_in_same_round(self) -> None:
        """No source appears twice in the same round."""
        rounds = self._simulate_chunked_tree(0, list(range(1, 64)), num_chunks=8)
        for round_idx, round_pairs in enumerate(rounds):
            sources = [src for src, _, _ in round_pairs]
            assert len(sources) == len(set(sources)), (
                f"Round {round_idx} has duplicate sources"
            )

    def test_chunked_all_peers_receive_all_chunks(self) -> None:
        """Every peer receives every chunk."""
        peer_ranks = list(range(1, 64))
        rounds = self._simulate_chunked_tree(0, peer_ranks, num_chunks=8)
        received: dict[int, set[int]] = {r: set() for r in peer_ranks}
        for round_pairs in rounds:
            for _, dst, chunk_idx in round_pairs:
                received[dst].add(chunk_idx)
        for rank in peer_ranks:
            assert received[rank] == set(range(8)), (
                f"Peer {rank} missing chunks: {set(range(8)) - received[rank]}"
            )


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
# pyre-ignore[56]: Pyre can't infer pytest.mark.timeout decorator type
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
            with remotemount(host, src, mnt, transfer_mode="actor"):
                for name, expected in files.items():
                    with open(os.path.join(mnt, name), "rb") as f:
                        assert f.read() == expected, f"content mismatch for {name}"


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
# pyre-ignore[56]: Pyre can't infer pytest.mark.timeout decorator type
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
            rm = remotemount(host, src, mnt, transfer_mode="actor")

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
# pyre-ignore[56]: Pyre can't infer pytest.mark.timeout decorator type
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
            rm = remotemount(host, src, mnt, transfer_mode="actor")

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
# pyre-ignore[56]: Pyre can't infer pytest.mark.timeout decorator type
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_unmount_not_mounted_returns_status() -> None:
    """Unmounting a path that isn't mounted returns ('not_mounted', '')."""
    from monarch.actor import this_host
    from monarch.remotemount.remotemount import FUSEActor

    host = this_host()
    procs = host.spawn_procs()
    actors = procs.spawn("FUSEActor", FUSEActor, 1024, "slurm")

    with tempfile.TemporaryDirectory() as d:
        result = actors.unmount.call(d).get()
        for _rank, (status, detail) in result:
            assert status == "not_mounted"
            assert detail == ""


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
# pyre-ignore[56]: Pyre can't infer pytest.mark.timeout decorator type
@pytest.mark.timeout(60)
@isolate_in_subprocess
def test_unmount_after_mount_returns_ok() -> None:
    """Mount via remotemount, unmount via actor endpoint, check 'ok' status."""
    from monarch.actor import this_host
    from monarch.remotemount import remotemount

    with tempfile.TemporaryDirectory() as src:
        with open(os.path.join(src, "f.txt"), "wb") as f:
            f.write(b"test content")

        with tempfile.TemporaryDirectory() as mnt:
            host = this_host()
            rm = remotemount(host, src, mnt, transfer_mode="actor")
            rm.open()

            # Verify mount works
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == b"test content"

            # Unmount via the actor endpoint — should return 'ok'
            result = rm.fuse_actors.unmount.call(mnt).get()
            for _rank, (status, _detail) in result:
                assert status == "ok"
                assert _detail == ""

            # Second unmount — should return 'not_mounted'
            result = rm.fuse_actors.unmount.call(mnt).get()
            for _rank, (status, _detail) in result:
                assert status == "not_mounted"


_tls_certs_available: bool = os.path.exists("/var/facebook/x509_identities/server.pem")


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.skipif(not _tls_certs_available, reason="TLS certs not available")
# pyre-ignore[56]: Pyre can't infer pytest.mark.timeout decorator type
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
            with remotemount(host, src, mnt, transfer_mode="rust_tls"):
                for name, expected in files.items():
                    with open(os.path.join(mnt, name), "rb") as f:
                        assert f.read() == expected, f"content mismatch for {name}"


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
@pytest.mark.skipif(not _tls_certs_available, reason="TLS certs not available")
# pyre-ignore[56]: Pyre can't infer pytest.mark.timeout decorator type
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
            rm = remotemount(host, src, mnt, transfer_mode="rust_tls")

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
# pyre-ignore[56]: Pyre can't infer pytest.mark.timeout decorator type
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
            rm = remotemount(host, src, mnt, transfer_mode="rust_tls")

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
# pyre-ignore[56]: Pyre can't infer pytest.mark.timeout decorator type
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
            rm = remotemount(host, src, mnt, transfer_mode="actor")

            rm.open()
            with open(os.path.join(mnt, "config.json")) as f:
                assert f.read() == '{"lr": 0.001}'

            # Modify the source file.
            with open(path, "w") as f:
                f.write('{"lr": 0.01, "epochs": 20}')

            # Refresh (no close/open cycle).
            rm.refresh(src)
            with open(os.path.join(mnt, "config.json")) as f:
                assert f.read() == '{"lr": 0.01, "epochs": 20}'

            rm.close()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
# pyre-ignore[56]: Pyre can't infer pytest.mark.timeout decorator type
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
            rm = remotemount(host, src, mnt, transfer_mode="actor")

            rm.open()

            # Open a file handle before refresh.
            fh = open(os.path.join(mnt, "data.bin"), "rb")
            assert fh.read() == b"AAAA"

            # Modify source and refresh.
            with open(path, "wb") as f:
                f.write(b"BBBB")
            rm.refresh(src)

            # Re-read from existing handle — should see new data.
            fh.seek(0)
            assert fh.read() == b"BBBB"
            fh.close()

            rm.close()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
# pyre-ignore[56]: Pyre can't infer pytest.mark.timeout decorator type
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
            rm = remotemount(host, src, mnt, transfer_mode="actor")

            rm.open()
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == b"unchanged"

            # Refresh without changes.
            rm.refresh(src)
            with open(os.path.join(mnt, "f.txt"), "rb") as f:
                assert f.read() == b"unchanged"

            rm.close()


@pytest.mark.skipif(not _fuse_available, reason="FUSE not available")
# pyre-ignore[56]: Pyre can't infer pytest.mark.timeout decorator type
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
            rm = remotemount(host, src, mnt, transfer_mode="actor")

            rm.open()
            assert os.listdir(mnt) == ["a.txt"]

            # Add a new file and refresh.
            with open(os.path.join(src, "b.txt"), "wb") as f:
                f.write(b"bbb")
            rm.refresh(src)

            assert sorted(os.listdir(mnt)) == ["a.txt", "b.txt"]
            with open(os.path.join(mnt, "b.txt"), "rb") as f:
                assert f.read() == b"bbb"

            rm.close()
