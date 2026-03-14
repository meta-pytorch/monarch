# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import contextlib
import json
import math
import mmap
import os
import subprocess
import tempfile
import threading

import pytest
from monarch._rust_bindings.monarch_extension.chunked_fuse import mount_chunked_fuse
from monarch.remotemount.remotemount import (
    block_hashes,
    classify_workers,
    pack_directory_chunked,
)


@contextlib.contextmanager
def fuse_mount(source_dir, chunk_size=None):
    """Pack a directory and mount it as a FUSE filesystem. Yields the mount path."""
    meta, _staging_mv, chunks, _shm_path, _hashes = pack_directory_chunked(
        source_dir, chunk_size=chunk_size
    )
    cs = chunk_size if chunk_size is not None else (1024 * 1024 * 1024) * 8
    with tempfile.TemporaryDirectory() as mnt:
        handle = mount_chunked_fuse(json.dumps(meta), chunks, cs, mnt)
        try:
            yield mnt
        finally:
            handle.unmount()
            subprocess.run(["fusermount3", "-u", mnt], capture_output=True, timeout=10)


class TestPackDirectoryChunked:
    def test_empty_directory(self):
        with tempfile.TemporaryDirectory() as d:
            meta, _staging_mv, chunks, _shm_path, _hashes = pack_directory_chunked(d)
            assert chunks == []
            assert "/" in meta
            assert "children" in meta["/"]
            assert meta["/"]["children"] == []

    def test_single_file(self):
        with tempfile.TemporaryDirectory() as d:
            content = b"hello world"
            with open(os.path.join(d, "a.txt"), "wb") as f:
                f.write(content)

            meta, _staging_mv, chunks, _shm_path, _hashes = pack_directory_chunked(d)

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

            meta, _staging_mv, chunks, _shm_path, _hashes = pack_directory_chunked(d)
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

            meta, _staging_mv, chunks, _shm_path, _hashes = pack_directory_chunked(d)

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

            meta, _, _, _, _ = pack_directory_chunked(d)

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
            meta, _staging_mv, chunks, _shm_path, _hashes = pack_directory_chunked(
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


class TestPackAndHash:
    """Verify that Rust-computed block hashes match Python-computed ones."""

    def test_hashes_match_python(self):
        with tempfile.TemporaryDirectory() as d:
            content = os.urandom(1000)
            with open(os.path.join(d, "data.bin"), "wb") as f:
                f.write(content)

            _, staging_mv, _, _, rust_hashes = pack_directory_chunked(d)
            python_hashes = block_hashes(staging_mv, block_size=64 * 1024 * 1024)
            assert rust_hashes == python_hashes

    def test_empty_returns_no_hashes(self):
        with tempfile.TemporaryDirectory() as d:
            _, _, _, _, hashes = pack_directory_chunked(d)
            assert hashes == []


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


class TestFuseMount:
    """Test the Rust FUSE filesystem by mounting and reading files."""

    def test_single_file_read(self):
        with tempfile.TemporaryDirectory() as src:
            content = b"hello from fuse"
            with open(os.path.join(src, "test.txt"), "wb") as f:
                f.write(content)

            with fuse_mount(src) as mnt:
                with open(os.path.join(mnt, "test.txt"), "rb") as f:
                    assert f.read() == content

    def test_multiple_files(self):
        with tempfile.TemporaryDirectory() as src:
            files = {"a.txt": b"aaa", "b.txt": b"bbbbbb", "c.txt": b"c"}
            for name, data in files.items():
                with open(os.path.join(src, name), "wb") as f:
                    f.write(data)

            with fuse_mount(src) as mnt:
                for name, expected in files.items():
                    with open(os.path.join(mnt, name), "rb") as f:
                        assert f.read() == expected, f"mismatch for {name}"

    def test_subdirectory(self):
        with tempfile.TemporaryDirectory() as src:
            os.makedirs(os.path.join(src, "sub"))
            content = b"nested file"
            with open(os.path.join(src, "sub", "deep.txt"), "wb") as f:
                f.write(content)

            with fuse_mount(src) as mnt:
                assert os.path.isdir(os.path.join(mnt, "sub"))
                with open(os.path.join(mnt, "sub", "deep.txt"), "rb") as f:
                    assert f.read() == content

    def test_listdir(self):
        with tempfile.TemporaryDirectory() as src:
            for name in ["x.txt", "y.txt", "z.txt"]:
                with open(os.path.join(src, name), "w") as f:
                    f.write(name)

            with fuse_mount(src) as mnt:
                entries = sorted(os.listdir(mnt))
                assert entries == ["x.txt", "y.txt", "z.txt"]

    def test_symlink(self):
        with tempfile.TemporaryDirectory() as src:
            with open(os.path.join(src, "target.txt"), "w") as f:
                f.write("target")
            target_path = os.path.join(src, "target.txt")
            os.symlink(target_path, os.path.join(src, "link.txt"))

            with fuse_mount(src) as mnt:
                assert os.path.islink(os.path.join(mnt, "link.txt"))
                assert os.readlink(os.path.join(mnt, "link.txt")) == target_path

    def test_small_chunk_size(self):
        """Files larger than chunk_size are split across chunks."""
        with tempfile.TemporaryDirectory() as src:
            content = b"x" * 1000
            with open(os.path.join(src, "big.txt"), "wb") as f:
                f.write(content)

            with fuse_mount(src, chunk_size=300) as mnt:
                with open(os.path.join(mnt, "big.txt"), "rb") as f:
                    assert f.read() == content

    def test_partial_read(self):
        with tempfile.TemporaryDirectory() as src:
            content = b"hello world"
            with open(os.path.join(src, "f.txt"), "wb") as f:
                f.write(content)

            with fuse_mount(src) as mnt:
                with open(os.path.join(mnt, "f.txt"), "rb") as f:
                    assert f.read(5) == b"hello"
                    f.seek(6)
                    assert f.read(5) == b"world"


class TestIncrementalHashing:
    """Test block hash comparison logic used for incremental updates."""

    def test_classify_fresh(self):
        """Two identical memoryviews produce matching block hashes."""
        data = os.urandom(500)
        mv1 = memoryview(bytearray(data))
        mv2 = memoryview(bytearray(data))
        h1 = block_hashes(mv1, block_size=200)
        h2 = block_hashes(mv2, block_size=200)
        assert h1 == h2

    def test_classify_partial(self):
        """Memoryviews differing in one block produce one dirty index."""
        data = bytearray(600)
        mv1 = memoryview(data)
        h1 = block_hashes(mv1, block_size=200)

        modified = bytearray(data)
        modified[400] = 0xFF  # Modify only the third block
        mv2 = memoryview(modified)
        h2 = block_hashes(mv2, block_size=200)

        # Find dirty blocks
        dirty = [i for i, (a, b) in enumerate(zip(h1, h2)) if a != b]
        assert dirty == [2]

    def test_classify_stale(self):
        """Different total sizes produce different-length hash lists."""
        mv_small = memoryview(bytearray(300))
        mv_large = memoryview(bytearray(600))
        h_small = block_hashes(mv_small, block_size=200)
        h_large = block_hashes(mv_large, block_size=200)
        # Different sizes → different number of blocks → "stale"
        assert len(h_small) != len(h_large)


class TestClassifyWorkers:
    """Test classify_workers() pure function."""

    def test_all_fresh(self):
        hashes = ["aaa", "bbb", "ccc"]
        states = [(hashes, 300), (hashes, 300)]
        fresh, dirty = classify_workers(hashes, 300, states)
        assert fresh == [0, 1]
        assert dirty == {}

    def test_all_stale(self):
        client_hashes = ["aaa", "bbb"]
        states = [(["aaa"], 100), (["xxx", "yyy", "zzz"], 900)]
        fresh, dirty = classify_workers(client_hashes, 200, states)
        assert fresh == []
        assert dirty == {0: None, 1: None}

    def test_partial(self):
        client_hashes = ["aaa", "bbb", "ccc"]
        remote_hashes = ["aaa", "XXX", "ccc"]
        states = [(remote_hashes, 300)]
        fresh, dirty = classify_workers(client_hashes, 300, states)
        assert fresh == []
        assert dirty == {0: [1]}

    def test_mixed(self):
        client_hashes = ["aaa", "bbb"]
        states = [
            (["aaa", "bbb"], 200),  # fresh
            (["aaa", "XXX"], 200),  # partial — block 1 dirty
            (["zzz"], 100),  # stale — different size
        ]
        fresh, dirty = classify_workers(client_hashes, 200, states)
        assert fresh == [0]
        assert dirty == {1: [1], 2: None}

    def test_empty(self):
        fresh, dirty = classify_workers([], 0, [([], 0), ([], 0)])
        assert fresh == [0, 1]
        assert dirty == {}


@pytest.mark.skipif(
    not os.path.exists("/var/facebook/x509_identities/server.pem")
    or not os.path.exists("/var/facebook/rootcanal/ca.pem"),
    reason="TLS certificates not available",
)
class TestRustTlsTransfer:
    """Test Rust TLS receiver + sender round-trip (no actors needed).

    Each test packs source data with ``pack_and_hash``, starts a
    ``TlsReceiver`` writing into an anonymous mmap, then sends blocks
    via ``PackedBuffer.send_blocks`` and verifies the result.

    Note: ``receiver.addr`` must be read *before* starting the receiver
    thread because ``wait()`` takes ``&mut self`` (PyO3 mutable borrow),
    and accessing any attribute from another thread while that borrow is
    active raises ``RuntimeError: Already mutably borrowed``.
    """

    def test_single_stream_round_trip(self):
        """Send all blocks over one TLS stream; verify data arrives intact."""
        from monarch._rust_bindings.monarch_extension.tls_receiver import TlsReceiver
        from monarch._rust_bindings.monarch_extension.tls_sender import pack_and_hash

        total_size = 4096
        block_size = 2048
        data = os.urandom(total_size)

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(data)
            src_path = f.name

        try:
            packed = pack_and_hash(
                [(src_path, 0, total_size)],
                total_size,
                hash_block_size=block_size,
            )
            assert len(packed.hashes) == 2

            receiver = TlsReceiver(num_streams=1)
            recv_addr = receiver.addr
            dest = mmap.mmap(-1, total_size)
            dest_mv = memoryview(dest)

            errors = []

            def run_receiver():
                try:
                    receiver.wait(dest_mv)  # noqa: F821
                except Exception as e:
                    errors.append(e)

            t = threading.Thread(target=run_receiver)
            t.start()

            num_blocks = (total_size + block_size - 1) // block_size
            packed.send_blocks(
                dirty_blocks=list(range(num_blocks)),
                addresses=[recv_addr],
                cache_path="",
                hash_block_size=block_size,
            )

            t.join(timeout=30)
            assert not t.is_alive(), "Receiver thread timed out"
            assert not errors, f"Receiver errors: {errors}"
            assert bytes(dest[:]) == data
        finally:
            os.unlink(src_path)
            del dest_mv
            dest.close()

    def test_multiple_streams_round_trip(self):
        """Send blocks over 3 parallel TLS streams."""
        from monarch._rust_bindings.monarch_extension.tls_receiver import TlsReceiver
        from monarch._rust_bindings.monarch_extension.tls_sender import pack_and_hash

        num_streams = 3
        total_size = 8192
        block_size = 2048
        data = os.urandom(total_size)

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(data)
            src_path = f.name

        try:
            packed = pack_and_hash(
                [(src_path, 0, total_size)],
                total_size,
                hash_block_size=block_size,
            )
            assert len(packed.hashes) == 4

            receiver = TlsReceiver(num_streams=num_streams)
            recv_addr = receiver.addr
            dest = mmap.mmap(-1, total_size)
            dest_mv = memoryview(dest)

            errors = []

            def run_receiver():
                try:
                    receiver.wait(dest_mv)  # noqa: F821
                except Exception as e:
                    errors.append(e)

            t = threading.Thread(target=run_receiver)
            t.start()

            num_blocks = (total_size + block_size - 1) // block_size
            packed.send_blocks(
                dirty_blocks=list(range(num_blocks)),
                addresses=[recv_addr] * num_streams,
                cache_path="",
                hash_block_size=block_size,
            )

            t.join(timeout=30)
            assert not t.is_alive(), "Receiver thread timed out"
            assert not errors, f"Receiver errors: {errors}"
            assert bytes(dest[:]) == data
        finally:
            os.unlink(src_path)
            del dest_mv
            dest.close()

    def test_partial_blocks(self):
        """Only dirty blocks are transferred; clean blocks remain zeroed."""
        from monarch._rust_bindings.monarch_extension.tls_receiver import TlsReceiver
        from monarch._rust_bindings.monarch_extension.tls_sender import pack_and_hash

        total_size = 4096
        block_size = 1024
        data = os.urandom(total_size)

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(data)
            src_path = f.name

        try:
            packed = pack_and_hash(
                [(src_path, 0, total_size)],
                total_size,
                hash_block_size=block_size,
            )
            assert len(packed.hashes) == 4

            receiver = TlsReceiver(num_streams=1)
            recv_addr = receiver.addr
            dest = mmap.mmap(-1, total_size)
            dest_mv = memoryview(dest)

            errors = []

            def run_receiver():
                try:
                    receiver.wait(dest_mv)  # noqa: F821
                except Exception as e:
                    errors.append(e)

            t = threading.Thread(target=run_receiver)
            t.start()

            # Only send blocks 1 and 3 (skip 0 and 2).
            packed.send_blocks(
                dirty_blocks=[1, 3],
                addresses=[recv_addr],
                cache_path="",
                hash_block_size=block_size,
            )

            t.join(timeout=30)
            assert not t.is_alive(), "Receiver thread timed out"
            assert not errors, f"Receiver errors: {errors}"

            # Blocks 0 and 2 should still be zeroed.
            assert bytes(dest[0:1024]) == b"\x00" * 1024
            assert bytes(dest[2048:3072]) == b"\x00" * 1024
            # Blocks 1 and 3 should match source data.
            assert bytes(dest[1024:2048]) == data[1024:2048]
            assert bytes(dest[3072:4096]) == data[3072:4096]
        finally:
            os.unlink(src_path)
            del dest_mv
            dest.close()

    def test_large_transfer(self):
        """Transfer 10 MB to verify multi-block handling at scale."""
        from monarch._rust_bindings.monarch_extension.tls_receiver import TlsReceiver
        from monarch._rust_bindings.monarch_extension.tls_sender import pack_and_hash

        total_size = 10 * 1024 * 1024  # 10 MB
        block_size = 1024 * 1024  # 1 MB blocks
        data = os.urandom(total_size)

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(data)
            src_path = f.name

        try:
            packed = pack_and_hash(
                [(src_path, 0, total_size)],
                total_size,
                hash_block_size=block_size,
            )
            assert len(packed.hashes) == 10

            receiver = TlsReceiver(num_streams=2)
            recv_addr = receiver.addr
            dest = mmap.mmap(-1, total_size)
            dest_mv = memoryview(dest)

            errors = []

            def run_receiver():
                try:
                    receiver.wait(dest_mv)  # noqa: F821
                except Exception as e:
                    errors.append(e)

            t = threading.Thread(target=run_receiver)
            t.start()

            num_blocks = (total_size + block_size - 1) // block_size
            packed.send_blocks(
                dirty_blocks=list(range(num_blocks)),
                addresses=[recv_addr] * 2,
                cache_path="",
                hash_block_size=block_size,
            )

            t.join(timeout=60)
            assert not t.is_alive(), "Receiver thread timed out"
            assert not errors, f"Receiver errors: {errors}"
            assert bytes(dest[:]) == data
        finally:
            os.unlink(src_path)
            del dest_mv
            dest.close()
