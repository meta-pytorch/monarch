# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import asyncio
import threading
import time
import unittest

from monarch.actor import Actor, endpoint, this_proc
from monarch.experimental.thread_timeout import ThreadTimeout, ThreadTimeoutError


def _busy_wait(seconds: float = 2) -> None:
    """Run Python code for a bounded time so a regression cannot hang CI."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        pass


class _ThreadTimeoutActor(Actor):
    @endpoint
    async def time_out(self) -> bool:
        """Return whether the timeout interrupted this endpoint."""
        try:
            with ThreadTimeout(0.01):
                _busy_wait()
        except ThreadTimeoutError:
            return True
        return False

    @endpoint
    async def ping(self) -> int:
        """Return a value that confirms the endpoint loop is still healthy."""
        return 42


class ThreadTimeoutTest(unittest.TestCase):
    """Tests the public thread-timeout behavior."""

    def test_completes_before_deadline(self) -> None:
        with ThreadTimeout(1):
            result = 42

        self.assertEqual(42, result)

    def test_propagates_unrelated_exception(self) -> None:
        with self.assertRaisesRegex(ValueError, "failure"):
            with ThreadTimeout(1):
                raise ValueError("failure")

    def test_preserves_exception_raised_after_timeout(self) -> None:
        with self.assertRaisesRegex(ValueError, "after timeout"):
            with ThreadTimeout(0.01):
                try:
                    _busy_wait()
                except ThreadTimeoutError:
                    raise ValueError("after timeout")

    def test_timeout_bypasses_exception_handlers(self) -> None:
        caught = False
        deadline = time.monotonic() + 2

        with self.assertRaises(ThreadTimeoutError):
            with ThreadTimeout(0.01):
                while time.monotonic() < deadline:
                    try:
                        for _ in range(100):
                            pass
                    except Exception:
                        caught = True

        self.assertFalse(caught)

    def test_base_exception_handler_cannot_suppress_timeout(self) -> None:
        caught: BaseException | None = None

        with self.assertRaises(ThreadTimeoutError):
            with ThreadTimeout(0.01):
                try:
                    _busy_wait()
                except BaseException as error:  # noqa: B036
                    caught = error

        # The context raises again after the handler catches the injection.
        self.assertIsInstance(caught, ThreadTimeoutError)

    def test_rejects_nonpositive_deadline(self) -> None:
        with self.assertRaises(ValueError):
            ThreadTimeout(0)

    def test_instance_is_single_use(self) -> None:
        timeout = ThreadTimeout(1)
        with timeout:
            pass

        with self.assertRaises(RuntimeError):
            with timeout:
                pass

    def test_rejects_nested_contexts(self) -> None:
        with ThreadTimeout(1):
            with self.assertRaisesRegex(RuntimeError, "cannot be nested"):
                with ThreadTimeout(1):
                    pass

        with ThreadTimeout(1):
            pass

    def test_works_inside_monarch_endpoint(self) -> None:
        actor = this_proc().spawn("thread_timeout_test", _ThreadTimeoutActor)
        try:
            self.assertTrue(actor.time_out.call_one().get(timeout=5))
            self.assertEqual(42, actor.ping.call_one().get(timeout=5))
        finally:
            actor.stop().get(timeout=5)


class ThreadTimeoutAsyncTest(unittest.IsolatedAsyncioTestCase):
    """Tests thread timeouts used from asyncio."""

    async def test_works_with_to_thread(self) -> None:
        worker_thread_id: int | None = None

        def work() -> None:
            nonlocal worker_thread_id
            worker_thread_id = threading.get_ident()
            with ThreadTimeout(0.01):
                _busy_wait()

        with self.assertRaises(ThreadTimeoutError):
            await asyncio.to_thread(work)

        self.assertIsNotNone(worker_thread_id)
        self.assertNotEqual(threading.get_ident(), worker_thread_id)
