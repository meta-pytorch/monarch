# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import math
import mmap
import os
import tempfile

from monarch.remotemount.remotemount import block_hashes, pack_directory_chunked


class TestPackDirectoryChunked:
    def test_empty_directory(self):
        with tempfile.TemporaryDirectory() as d:
            meta, _staging_mv, chunks, _shm_path = pack_directory_chunked(d)
            assert chunks == []
            assert "/" in meta
            assert "children" in meta["/"]
            assert meta["/"]["children"] == []

    def test_single_file(self):
        with tempfile.TemporaryDirectory() as d:
            content = b"hello world"
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(content)

            meta, _staging_mv, chunks, _shm_path = pack_directory_chunked(d)

            assert "/a.txt" in meta
            file_meta = meta["/a.txt"]
            assert file_meta["global_offset"] == 0
            assert file_meta["file_len"] == len(content)

            packed = b"".join(bytes(c) for c in chunks)
            assert packed == content

    def test_multiple_files_contiguous_offsets(self):
        with tempfile.TemporaryDirectory() as d:
            files = {"a.txt": b"aaa", "b.txt": b"bbbbbb", "c.txt": b"c"}
            for name, content in files.items():
                with open(os.path.join(d, name), "wb") as f:
                    f.write(content)

            meta, _staging_mv, chunks, _shm_path = pack_directory_chunked(d)
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

    def test_symlink(self):
        with tempfile.TemporaryDirectory() as d:
            target = os.path.join(d, "target.txt")
            with open(target, "w") as f:
                f.write("target")
            os.symlink(target, os.path.join(d, "link.txt"))

            meta, _staging_mv, chunks, _shm_path = pack_directory_chunked(d)

            assert "/link.txt" in meta
            link_meta = meta["/link.txt"]
            assert "link_target" in link_meta
            assert link_meta["link_target"] == target
            assert "global_offset" not in link_meta

    def test_directory_metadata(self):
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

    def test_custom_chunk_size(self):
        with tempfile.TemporaryDirectory() as d:
            content = b"x" * 1000
            with open(os.path.join(d, "big.txt"), "wb") as f:
                f.write(content)

            chunk_size = 300
            meta, _staging_mv, chunks, _shm_path = pack_directory_chunked(
                d, chunk_size=chunk_size
            )

            assert len(chunks) == math.ceil(len(content) / chunk_size)
            packed = b"".join(bytes(c) for c in chunks)
            assert packed == content


class TestBlockHashes:
    def test_deterministic(self):
        data = os.urandom(500)
        mv = memoryview(data)
        assert block_hashes(mv, block_size=200) == block_hashes(mv, block_size=200)

    def test_different_data(self):
        a = memoryview(b"\x00" * 500)
        b = memoryview(b"\xff" * 500)
        assert block_hashes(a, block_size=200) != block_hashes(b, block_size=200)

    def test_block_count(self):
        data = memoryview(os.urandom(500))
        hashes = block_hashes(data, block_size=200)
        assert len(hashes) == 3  # 200 + 200 + 100

    def test_empty(self):
        assert block_hashes(memoryview(b""), block_size=100) == []


class TestShmRoundTrip:
    """Test /tmp/ create/write/collect round-trip (no actors)."""

    def test_create_write_collect(self):
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

    def test_unlink_after_mmap(self):
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
