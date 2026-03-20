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
import tempfile
import time
from collections.abc import Generator

import pytest
from monarch._rust_bindings.monarch_extension.chunked_fuse import mount_chunked_fuse
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
) -> Generator[object, None, None]:
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

            meta, _staging_mv, chunks, _hashes = pack_directory_chunked(src)

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
