# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Tests for monarch.gather_mount.

Run with:
    uv run pytest python/tests/test_gather_mount.py -v
"""

from __future__ import annotations

import os
import shutil
import tempfile
import time
import unittest


def _skip_if_no_fuse() -> None:
    """Skip the test if FUSE is unavailable in the current environment."""
    try:
        import fuse  # noqa: F401
    except ImportError:
        raise unittest.SkipTest("fusepy not installed")

    # fusermount must be accessible for mounting/unmounting.
    import shutil as _shutil

    if _shutil.which("fusermount3") is None and _shutil.which("fusermount") is None:
        raise unittest.SkipTest("fusermount / fusermount3 not found on PATH")


class GatherMountBasicTest(unittest.TestCase):
    """Basic correctness tests for gather_mount running entirely on localhost."""

    def setUp(self) -> None:
        _skip_if_no_fuse()
        self._cleanup: list[str] = []

    def tearDown(self) -> None:
        for path in self._cleanup:
            shutil.rmtree(path, ignore_errors=True)

    def _tmpdir(self) -> str:
        d = tempfile.mkdtemp(prefix="sm_test_")
        self._cleanup.append(d)
        return d

    # ── 0-dim mesh ────────────────────────────────────────────────────────

    def test_0dim_read_file(self) -> None:
        """Files in the remote root are directly visible in the mount root."""
        from monarch.actor import this_host
        from monarch.gather_mount import gather_mount

        src = self._tmpdir()
        mnt = self._tmpdir()
        shutil.rmtree(mnt)  # gather_mount creates it

        with open(os.path.join(src, "hello.txt"), "w") as f:
            f.write("hello world\n")

        with gather_mount(this_host(), mnt, src):
            self.assertEqual(
                open(os.path.join(mnt, "hello.txt")).read(),
                "hello world\n",
            )

    def test_0dim_nested_directory(self) -> None:
        """Sub-directories inside the remote root are traversable."""
        from monarch.actor import this_host
        from monarch.gather_mount import gather_mount

        src = self._tmpdir()
        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        os.makedirs(os.path.join(src, "subdir"))
        with open(os.path.join(src, "subdir", "nested.txt"), "w") as f:
            f.write("nested content\n")

        with gather_mount(this_host(), mnt, src):
            nested = os.path.join(mnt, "subdir", "nested.txt")
            self.assertEqual(open(nested).read(), "nested content\n")

    def test_0dim_listdir(self) -> None:
        """os.listdir on the mount root returns the remote files."""
        from monarch.actor import this_host
        from monarch.gather_mount import gather_mount

        src = self._tmpdir()
        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        for name in ("a.txt", "b.txt", "c.txt"):
            open(os.path.join(src, name), "w").close()

        with gather_mount(this_host(), mnt, src):
            self.assertCountEqual(os.listdir(mnt), ["a.txt", "b.txt", "c.txt"])

    def test_0dim_read_only(self) -> None:
        """Writing into the mount raises an OS error (EACCES or EROFS)."""
        from monarch.actor import this_host
        from monarch.gather_mount import gather_mount

        src = self._tmpdir()
        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        with open(os.path.join(src, "x.txt"), "w") as f:
            f.write("x")

        with gather_mount(this_host(), mnt, src):
            with self.assertRaises(OSError):
                open(os.path.join(mnt, "new.txt"), "w").write("oops")

    # ── N-dim mesh ────────────────────────────────────────────────────────

    def test_ndim_subfolders_exist(self) -> None:
        """Each shard gets its own sub-directory named by its mesh point."""
        from monarch._src.job.process import ProcessJob
        from monarch.gather_mount import gather_mount

        src_dirs = [self._tmpdir() for _ in range(2)]
        for i, d in enumerate(src_dirs):
            with open(os.path.join(d, "shard.txt"), "w") as f:
                f.write(f"shard {i}\n")

        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        host_mesh = ProcessJob({"hosts": 2}).state(cached_path=None).hosts
        with gather_mount(host_mesh, mnt, lambda rank: src_dirs[rank["hosts"]]):
            subdirs = sorted(os.listdir(mnt))
            self.assertEqual(subdirs, ["hosts_0", "hosts_1"])

    def test_ndim_per_shard_content(self) -> None:
        """Each shard sub-directory contains the correct remote files."""
        from monarch._src.job.process import ProcessJob
        from monarch.gather_mount import gather_mount

        src_dirs = [self._tmpdir() for _ in range(2)]
        for i, d in enumerate(src_dirs):
            with open(os.path.join(d, "data.txt"), "w") as f:
                f.write(f"shard={i}\n")

        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        host_mesh = ProcessJob({"hosts": 2}).state(cached_path=None).hosts
        with gather_mount(host_mesh, mnt, lambda rank: src_dirs[rank["hosts"]]):
            for i in range(2):
                content = open(os.path.join(mnt, f"hosts_{i}", "data.txt")).read()
                self.assertEqual(content, f"shard={i}\n")

    # ── Caching and tail -f ───────────────────────────────────────────────

    def test_cache_hit(self) -> None:
        """A second read of the same file is served from cache (no extra RPC)."""
        from monarch.actor import this_host
        from monarch.gather_mount import gather_mount

        src = self._tmpdir()
        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        with open(os.path.join(src, "data.txt"), "w") as f:
            f.write("cached content\n")

        with gather_mount(this_host(), mnt, src):
            path = os.path.join(mnt, "data.txt")
            first = open(path).read()
            second = open(path).read()
            self.assertEqual(first, second)
            self.assertEqual(first, "cached content\n")

    def test_tail_f_append(self) -> None:
        """Appended bytes become visible after inotify invalidation."""
        from monarch._src.gather_mount.gather_mount import _NOTIFY_BATCH_S
        from monarch.actor import this_host
        from monarch.gather_mount import gather_mount

        src = self._tmpdir()
        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        log = os.path.join(src, "log.txt")
        with open(log, "w") as f:
            f.write("line1\n")

        with gather_mount(this_host(), mnt, src):
            mnt_log = os.path.join(mnt, "log.txt")

            # Initial read.
            self.assertEqual(open(mnt_log).read(), "line1\n")

            # Append to remote file (small sleep ensures mtime change).
            time.sleep(0.01)
            with open(log, "a") as f:
                f.write("line2\n")

            # Wait for inotify batch window + notification round-trip.
            time.sleep(_NOTIFY_BATCH_S * 3)

            content = open(mnt_log).read()
            self.assertIn("line1", content)
            self.assertIn("line2", content)

    def test_file_replaced(self) -> None:
        """When a file is replaced (not just appended), the new content appears."""
        from monarch._src.gather_mount.gather_mount import _NOTIFY_BATCH_S
        from monarch.actor import this_host
        from monarch.gather_mount import gather_mount

        src = self._tmpdir()
        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        path = os.path.join(src, "data.txt")
        with open(path, "w") as f:
            f.write("version1\n")

        with gather_mount(this_host(), mnt, src):
            mnt_path = os.path.join(mnt, "data.txt")
            self.assertEqual(open(mnt_path).read(), "version1\n")

            time.sleep(0.01)
            with open(path, "w") as f:  # overwrite (not append)
                f.write("v2\n")  # shorter than v1

            time.sleep(_NOTIFY_BATCH_S * 3)
            self.assertEqual(open(mnt_path).read(), "v2\n")

    # ── Callable remote path ──────────────────────────────────────────────

    def test_callable_remote_path(self) -> None:
        """A callable remote_mount_point receives the rank Point and returns a path."""
        from monarch.actor import this_host
        from monarch.gather_mount import gather_mount

        src = self._tmpdir()
        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        with open(os.path.join(src, "info.txt"), "w") as f:
            f.write("rank-based path\n")

        # Callable ignores rank and always returns src; valid for 0-dim.
        with gather_mount(this_host(), mnt, lambda rank: src):
            self.assertEqual(
                open(os.path.join(mnt, "info.txt")).read(),
                "rank-based path\n",
            )

    # ── Context manager ───────────────────────────────────────────────────

    def test_context_manager_unmounts(self) -> None:
        """The filesystem is mounted immediately and unmounted on __exit__."""
        from monarch.actor import this_host
        from monarch.gather_mount import gather_mount

        src = self._tmpdir()
        mnt = self._tmpdir()
        shutil.rmtree(mnt)
        with open(os.path.join(src, "f.txt"), "w") as f:
            f.write("x")

        m = gather_mount(this_host(), mnt, src)
        self.assertTrue(os.path.ismount(mnt))
        with m:
            pass
        self.assertFalse(os.path.ismount(mnt))


# ── ProcessJob multi-host tests ───────────────────────────────────────────────
# These tests use ProcessJob to create multiple fake local "hosts" (separate
# OS processes), each with its own temp directory, and verify that gather_mount
# correctly exposes per-host sub-directories with the right content.


class GatherMountProcessJobTest(unittest.TestCase):
    """Multi-host gather_mount tests using ProcessJob for fake local hosts."""

    def setUp(self) -> None:
        _skip_if_no_fuse()
        self._cleanup: list[str] = []

    def tearDown(self) -> None:
        for path in self._cleanup:
            shutil.rmtree(path, ignore_errors=True)

    def _tmpdir(self) -> str:
        d = tempfile.mkdtemp(prefix="sm_pj_")
        self._cleanup.append(d)
        return d

    def test_multi_host_read(self) -> None:
        """Each ProcessJob host gets its own dir; gather_mount exposes sub-dirs."""
        from monarch._src.job.process import ProcessJob
        from monarch.gather_mount import gather_mount

        num_hosts = 3
        src_dirs = [self._tmpdir() for _ in range(num_hosts)]
        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        for i, d in enumerate(src_dirs):
            with open(os.path.join(d, "info.txt"), "w") as f:
                f.write(f"host={i}\n")
            with open(os.path.join(d, "data.bin"), "wb") as f:
                f.write(bytes(range(i * 10, i * 10 + 10)))

        host_mesh = ProcessJob({"hosts": num_hosts}).state(cached_path=None).hosts

        with gather_mount(host_mesh, mnt, lambda rank: src_dirs[rank["hosts"]]):
            subdirs = sorted(os.listdir(mnt))
            self.assertEqual(subdirs, [f"hosts_{i}" for i in range(num_hosts)])

            for i in range(num_hosts):
                txt = open(os.path.join(mnt, f"hosts_{i}", "info.txt")).read()
                self.assertEqual(txt, f"host={i}\n")

                bin_data = open(
                    os.path.join(mnt, f"hosts_{i}", "data.bin"), "rb"
                ).read()
                self.assertEqual(bin_data, bytes(range(i * 10, i * 10 + 10)))

    def test_multi_host_listdir(self) -> None:
        """os.listdir on each per-host sub-dir returns only that host's files."""
        from monarch._src.job.process import ProcessJob
        from monarch.gather_mount import gather_mount

        src_dirs = [self._tmpdir(), self._tmpdir()]
        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        for fname in ("a.txt", "b.txt"):
            open(os.path.join(src_dirs[0], fname), "w").close()
        for fname in ("c.txt", "d.txt"):
            open(os.path.join(src_dirs[1], fname), "w").close()

        host_mesh = ProcessJob({"hosts": 2}).state(cached_path=None).hosts

        with gather_mount(host_mesh, mnt, lambda rank: src_dirs[rank["hosts"]]):
            self.assertEqual(
                sorted(os.listdir(os.path.join(mnt, "hosts_0"))), ["a.txt", "b.txt"]
            )
            self.assertEqual(
                sorted(os.listdir(os.path.join(mnt, "hosts_1"))), ["c.txt", "d.txt"]
            )

    def test_multi_host_cache_invalidation_append(self) -> None:
        """Appending to a remote file is visible after inotify invalidation fires."""
        from monarch._src.gather_mount.gather_mount import _NOTIFY_BATCH_S
        from monarch._src.job.process import ProcessJob
        from monarch.gather_mount import gather_mount

        num_hosts = 2
        src_dirs = [self._tmpdir() for _ in range(num_hosts)]
        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        log_paths = [os.path.join(d, "log.txt") for d in src_dirs]
        for i, p in enumerate(log_paths):
            with open(p, "w") as f:
                f.write(f"initial line host={i}\n")

        host_mesh = ProcessJob({"hosts": num_hosts}).state(cached_path=None).hosts

        with gather_mount(host_mesh, mnt, lambda rank: src_dirs[rank["hosts"]]):
            # Prime the cache.
            for i in range(num_hosts):
                content = open(os.path.join(mnt, f"hosts_{i}", "log.txt")).read()
                self.assertIn(f"initial line host={i}", content)

            # Append to each host's log file.
            time.sleep(0.05)  # ensure mtime advances
            for i, p in enumerate(log_paths):
                with open(p, "a") as f:
                    f.write(f"appended line host={i}\n")

            # Wait for inotify batch window + notification round-trip.
            time.sleep(_NOTIFY_BATCH_S * 4)

            for i in range(num_hosts):
                content = open(os.path.join(mnt, f"hosts_{i}", "log.txt")).read()
                self.assertIn(f"initial line host={i}", content)
                self.assertIn(f"appended line host={i}", content)

    def test_multi_host_cache_invalidation_replace(self) -> None:
        """Replacing a file is detected and the new (shorter) content is served."""
        from monarch._src.gather_mount.gather_mount import _NOTIFY_BATCH_S
        from monarch._src.job.process import ProcessJob
        from monarch.gather_mount import gather_mount

        num_hosts = 2
        src_dirs = [self._tmpdir() for _ in range(num_hosts)]
        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        file_paths = [os.path.join(d, "state.txt") for d in src_dirs]
        for i, p in enumerate(file_paths):
            with open(p, "w") as f:
                f.write(f"version1 host={i}\n")

        host_mesh = ProcessJob({"hosts": num_hosts}).state(cached_path=None).hosts

        with gather_mount(host_mesh, mnt, lambda rank: src_dirs[rank["hosts"]]):
            # Prime cache.
            for i in range(num_hosts):
                content = open(os.path.join(mnt, f"hosts_{i}", "state.txt")).read()
                self.assertIn("version1", content)

            # Overwrite with a shorter string.
            time.sleep(0.05)
            for i, p in enumerate(file_paths):
                with open(p, "w") as f:
                    f.write(f"v2 h{i}\n")

            time.sleep(_NOTIFY_BATCH_S * 4)

            for i in range(num_hosts):
                content = open(os.path.join(mnt, f"hosts_{i}", "state.txt")).read()
                self.assertEqual(content, f"v2 h{i}\n")

    def test_multi_host_nested_dirs(self) -> None:
        """Deep sub-directories inside each host's root are traversable."""
        from monarch._src.job.process import ProcessJob
        from monarch.gather_mount import gather_mount

        num_hosts = 2
        src_dirs = [self._tmpdir() for _ in range(num_hosts)]
        mnt = self._tmpdir()
        shutil.rmtree(mnt)

        for i, d in enumerate(src_dirs):
            sub = os.path.join(d, "checkpoints", "step_100")
            os.makedirs(sub)
            with open(os.path.join(sub, "model.pt"), "w") as f:
                f.write(f"weights_host_{i}\n")

        host_mesh = ProcessJob({"hosts": num_hosts}).state(cached_path=None).hosts

        with gather_mount(host_mesh, mnt, lambda rank: src_dirs[rank["hosts"]]):
            for i in range(num_hosts):
                path = os.path.join(
                    mnt, f"hosts_{i}", "checkpoints", "step_100", "model.pt"
                )
                self.assertEqual(open(path).read(), f"weights_host_{i}\n")
