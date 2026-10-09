# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""NFS keeps the FUSE gather-cache algorithm, substituting bytes for RDMA."""

import stat
import unittest
from unittest.mock import Mock, mock_open, patch

from monarch._src.gather_mount.gather_mount import (
    _BYTE_READ_CHUNK_SIZE,
    _CLOSE_ATTEMPTS,
    _CLOSE_RETRY_DELAY_S,
    _is_mounted,
    _MOUNT_STATUS_TIMEOUT_S,
    _RDMA_THRESHOLD,
    GatherClientActor,
    GatherMount,
)

_TEST_BYTE_CHUNK_SIZE = 64


class GatherMountCloseTest(unittest.TestCase):
    @patch(
        "monarch._src.gather_mount.gather_mount._is_mounted",
        side_effect=[True, False],
    )
    @patch("monarch._src.gather_mount.gather_mount.time.sleep")
    def test_transient_unmount_failure_is_retried(
        self, sleep: Mock, _is_mounted: Mock
    ) -> None:
        mount = object.__new__(GatherMount)
        mount._mounted = True
        mount._local_mount_point = "/unused"
        handle = Mock()
        handle.unmount.side_effect = [OSError("mount busy"), None]
        mount._mount_handle = handle

        mount.close()
        self.assertFalse(mount._mounted)
        self.assertEqual(handle.unmount.call_count, 2)
        sleep.assert_called_once()

    @patch(
        "monarch._src.gather_mount.gather_mount._is_mounted",
        side_effect=[True, False],
    )
    @patch("monarch._src.gather_mount.gather_mount.time.sleep")
    def test_runtime_error_from_unmount_is_retried(
        self, sleep: Mock, _is_mounted: Mock
    ) -> None:
        mount = object.__new__(GatherMount)
        mount._mounted = True
        mount._local_mount_point = "/unused"
        handle = Mock()
        handle.unmount.side_effect = [RuntimeError("mount busy"), None]
        mount._mount_handle = handle

        mount.close()

        self.assertFalse(mount._mounted)
        self.assertEqual(handle.unmount.call_count, 2)
        sleep.assert_called_once()

    @patch("monarch._src.gather_mount.gather_mount.time.sleep")
    @patch(
        "monarch._src.gather_mount.gather_mount._is_mounted",
        return_value=True,
    )
    def test_failed_unmount_keeps_handle_retryable(
        self, _is_mounted: Mock, sleep: Mock
    ) -> None:
        mount = object.__new__(GatherMount)
        mount._mounted = True
        mount._local_mount_point = "/unused"
        handle = Mock()
        handle.unmount.side_effect = OSError("mount busy")
        mount._mount_handle = handle

        with self.assertRaisesRegex(OSError, "mount busy"):
            mount.close()
        self.assertTrue(mount._mounted)
        self.assertEqual(handle.unmount.call_count, _CLOSE_ATTEMPTS)
        self.assertEqual(sleep.call_count, _CLOSE_ATTEMPTS - 1)

    @patch(
        "monarch._src.gather_mount.gather_mount._is_mounted",
        return_value=False,
    )
    def test_successful_unmount_is_verified(self, ismount: Mock) -> None:
        mount = object.__new__(GatherMount)
        mount._mounted = True
        mount._local_mount_point = "/unused"
        mount._mount_handle = Mock()

        mount.close()

        self.assertFalse(mount._mounted)
        mount._mount_handle.unmount.assert_called_once()
        ismount.assert_called_once_with("/unused", timeout=_MOUNT_STATUS_TIMEOUT_S)

    @patch(
        "monarch._src.gather_mount.gather_mount._is_mounted",
        return_value=False,
    )
    @patch("monarch._src.gather_mount.gather_mount.logger")
    def test_unmount_error_is_logged_when_mount_is_gone(
        self, logger: Mock, _ismount: Mock
    ) -> None:
        mount = object.__new__(GatherMount)
        mount._mounted = True
        mount._local_mount_point = "/unused"
        mount._mount_handle = Mock()
        mount._mount_handle.unmount.side_effect = OSError("not mounted")

        mount.close()

        self.assertFalse(mount._mounted)
        logger.warning.assert_called_once()

    @patch("monarch._src.gather_mount.gather_mount.time.sleep")
    @patch(
        "monarch._src.gather_mount.gather_mount._is_mounted",
        side_effect=[RuntimeError("mount table unavailable"), False],
    )
    def test_mount_inspection_failure_is_retried(
        self, _is_mounted: Mock, sleep: Mock
    ) -> None:
        mount = object.__new__(GatherMount)
        mount._mounted = True
        mount._local_mount_point = "/unused"
        mount._mount_handle = Mock()

        mount.close()

        self.assertFalse(mount._mounted)
        self.assertEqual(mount._mount_handle.unmount.call_count, 2)
        sleep.assert_called_once_with(_CLOSE_RETRY_DELAY_S)

    @patch("monarch._src.gather_mount.gather_mount.time.sleep")
    @patch(
        "monarch._src.gather_mount.gather_mount._is_mounted",
        side_effect=RuntimeError("mount table unavailable"),
    )
    def test_successful_unmount_with_failed_inspection_is_inconclusive(
        self, _is_mounted: Mock, sleep: Mock
    ) -> None:
        mount = object.__new__(GatherMount)
        mount._mounted = True
        mount._local_mount_point = "/unused"
        mount._mount_handle = Mock()

        with self.assertRaisesRegex(
            RuntimeError,
            "unmount returned successfully, but the mount state .* could not be verified",
        ) as raised:
            mount.close()

        self.assertTrue(mount._mounted)
        self.assertIsInstance(raised.exception.__cause__, RuntimeError)
        self.assertIn("mount table unavailable", str(raised.exception.__cause__))
        self.assertEqual(mount._mount_handle.unmount.call_count, _CLOSE_ATTEMPTS)
        self.assertEqual(sleep.call_count, _CLOSE_ATTEMPTS - 1)

    @patch(
        "monarch._src.gather_mount.gather_mount._is_mounted",
        return_value=True,
    )
    def test_existing_mountpoint_is_rejected_before_spawning(
        self, _ismount: Mock
    ) -> None:
        host_mesh = Mock()

        with self.assertRaisesRegex(RuntimeError, "already mounted"):
            GatherMount(host_mesh, "/remote", "/already-mounted")

        host_mesh.spawn_procs.assert_not_called()

    @patch(
        "monarch._src.gather_mount.gather_mount._is_mounted",
        side_effect=RuntimeError("mount table unavailable"),
    )
    def test_mount_inspection_failure_stops_before_spawning(
        self, _ismount: Mock
    ) -> None:
        host_mesh = Mock()

        with self.assertRaisesRegex(RuntimeError, "mount table unavailable"):
            GatherMount(host_mesh, "/remote", "/unknown-mount-state")

        host_mesh.spawn_procs.assert_not_called()

    @patch("monarch._src.gather_mount.gather_mount.sys.platform", "linux")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="36 25 0:32 / /tmp/a\\040b rw - tmpfs tmpfs rw\n",
    )
    def test_linux_mount_table_decodes_mountpoint(self, _open: Mock) -> None:
        self.assertTrue(_is_mounted("/tmp/a b"))

    @patch("monarch._src.gather_mount.gather_mount.sys.platform", "darwin")
    @patch("monarch._src.gather_mount.gather_mount.os.path.exists", return_value=True)
    @patch("monarch._src.gather_mount.gather_mount.subprocess.run")
    def test_macos_mount_table_finds_mountpoint(self, run: Mock, _exists: Mock) -> None:
        run.return_value.stdout = (
            "127.0.0.1:/ on /tmp/gather (data)/mnt (nfs, read-only)\n"
        )

        self.assertTrue(_is_mounted("/tmp/gather (data)/mnt"))

        run.assert_called_once_with(
            ["/sbin/mount"],
            check=True,
            capture_output=True,
            text=True,
            timeout=_MOUNT_STATUS_TIMEOUT_S,
        )


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
        # pyrefly: ignore [missing-attribute]
        read = GatherClientActor.read_path._method
        # pyrefly: ignore [missing-attribute]
        getattr_path = GatherClientActor.getattr_path._method

        self.assertEqual(await read(client, "/log", 12, 0), b"x" * 12)
        whole_file_requests = [(0, remote.file_size)]
        self.assertEqual(remote.requests, whole_file_requests)
        self.assertEqual(await read(client, "/log", 4, 4), b"x" * 4)
        self.assertEqual(remote.requests, whole_file_requests)
        self.assertEqual(await read(client, "/log", 5, 1024), b"x" * 5)
        self.assertEqual(remote.requests, whole_file_requests)
        attributes = await getattr_path(client, "/log")
        assert isinstance(attributes, dict)
        self.assertEqual(attributes["st_size"], remote.file_size)

    async def test_byte_fallback_splits_large_ranges(self) -> None:
        remote = _RemoteFiles(file_size=2 * _TEST_BYTE_CHUNK_SIZE + 12)
        client = self.client(remote)
        with patch(
            "monarch._src.gather_mount.gather_mount._BYTE_READ_CHUNK_SIZE",
            _TEST_BYTE_CHUNK_SIZE,
        ):
            # pyrefly: ignore [missing-attribute]
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
