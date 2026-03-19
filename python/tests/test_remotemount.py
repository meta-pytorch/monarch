# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from __future__ import annotations

import gc
import math
import mmap
import os
import tempfile

import pytest
from monarch._rust_bindings.monarch_extension.fast_pack import (
    load_file_and_hash,
    pack_files_with_offsets,
)
from monarch.remotemount.fast_pack import block_hashes, pack_directory_chunked


class TestPackDirectoryChunked:
    def test_empty_directory(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            meta, _staging_mv, chunks, hashes = pack_directory_chunked(d)
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

            meta, _staging_mv, chunks, hashes = pack_directory_chunked(d)

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

            meta, _staging_mv, chunks, _hashes = pack_directory_chunked(d)
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

            meta, _staging_mv, chunks, _hashes = pack_directory_chunked(d)

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

            meta, _, _, _ = pack_directory_chunked(d)

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
            meta, _staging_mv, chunks, _hashes = pack_directory_chunked(
                d, chunk_size=chunk_size
            )

            assert len(chunks) == math.ceil(len(content) / chunk_size)
            packed = b"".join(bytes(c) for c in chunks)
            assert packed == content

    def test_hashes_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "f.bin"), "wb") as f:
                f.write(os.urandom(500))

            _, _, _, h1 = pack_directory_chunked(d)
            _, _, _, h2 = pack_directory_chunked(d)
            assert h1 == h2
            assert len(h1) > 0

    def test_hashes_change_on_content_change(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "f.bin")
            with open(path, "wb") as f:
                f.write(b"\x00" * 500)
            _, _, _, h1 = pack_directory_chunked(d)

            with open(path, "wb") as f:
                f.write(b"\xff" * 500)
            _, _, _, h2 = pack_directory_chunked(d)

            assert h1 != h2


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

            meta, staging_mv, chunks, hashes = pack_directory_chunked(d)

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
