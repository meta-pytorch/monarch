# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""NFS keeps the FUSE gather-cache algorithm, substituting bytes for RDMA."""

import stat
import unittest
from unittest.mock import Mock, patch

from monarch._src.gather_mount.gather_mount import (
    _BYTE_READ_CHUNK_SIZE,
    _RDMA_THRESHOLD,
    GatherClientActor,
    GatherMount,
)

_TEST_BYTE_CHUNK_SIZE = 64


class GatherMountCloseTest(unittest.TestCase):
    def test_failed_unmount_can_be_retried(self) -> None:
        mount = object.__new__(GatherMount)
        mount._mounted = True
        mount._local_mount_point = "/unused"
        handle = Mock()
        handle.unmount.side_effect = [OSError("mount busy"), None]
        mount._mount_handle = handle

        with self.assertRaisesRegex(OSError, "mount busy"):
            mount.close()
        self.assertTrue(mount._mounted)
        mount.close()
        self.assertFalse(mount._mounted)
        self.assertEqual(handle.unmount.call_count, 2)


class _RemoteFiles:
    extent = {}

    def __init__(self, file_size: int = 2 * _RDMA_THRESHOLD + 12) -> None:
        self.stat_and_watch = self
        self.read_bytes = self
        self.requests: list[tuple[int, int]] = []
        self.file_size = file_size

    def slice(self, **kwargs: int) -> "_RemoteFiles":
        assert not kwargs
        return self

    async def call_one(self, *args: object) -> tuple[int, int, int] | bytes:
        if len(args) == 1:
            return (1_000_000_000, self.file_size, stat.S_IFREG | 0o644)
        _path, offset, size = args
        assert isinstance(offset, int) and isinstance(size, int)
        self.requests.append((offset, size))
        return b"x" * size


class GatherNfsReadTest(unittest.IsolatedAsyncioTestCase):
    def client(self, remote: _RemoteFiles) -> GatherClientActor:
        client = object.__new__(GatherClientActor)
        client._actors = remote
        client._use_rdma = False
        client._key_to_point = {"": {}}
        client._cache = {}
        return client

    async def test_first_read_fetches_same_prefix_as_fuse(self) -> None:
        self.assertEqual(_BYTE_READ_CHUNK_SIZE, 32 * 1024 * 1024)
        remote = _RemoteFiles()
        client = self.client(remote)

        # Call the endpoint implementation directly without starting an actor.
        read = GatherClientActor.read_path._method
        getattr = GatherClientActor.getattr_path._method

        self.assertEqual(await read(client, "/log", 12, 0), b"x" * 12)
        whole_file_requests = [(0, remote.file_size)]
        self.assertEqual(remote.requests, whole_file_requests)
        self.assertEqual(await read(client, "/log", 4, 4), b"x" * 4)
        self.assertEqual(remote.requests, whole_file_requests)
        self.assertEqual(await read(client, "/log", 5, 1024), b"x" * 5)
        self.assertEqual(remote.requests, whole_file_requests)
        attributes = await getattr(client, "/log")
        assert isinstance(attributes, dict)
        self.assertEqual(attributes["st_size"], remote.file_size)

    async def test_byte_fallback_splits_large_ranges(self) -> None:
        remote = _RemoteFiles(file_size=2 * _TEST_BYTE_CHUNK_SIZE + 12)
        client = self.client(remote)
        with patch(
            "monarch._src.gather_mount.gather_mount._BYTE_READ_CHUNK_SIZE",
            _TEST_BYTE_CHUNK_SIZE,
        ):
            read = GatherClientActor.read_path._method
            self.assertEqual(await read(client, "/log", 12, 0), b"x" * 12)
        self.assertEqual(
            remote.requests,
            [
                (0, _TEST_BYTE_CHUNK_SIZE),
                (_TEST_BYTE_CHUNK_SIZE, _TEST_BYTE_CHUNK_SIZE),
                (2 * _TEST_BYTE_CHUNK_SIZE, 12),
            ],
        )
