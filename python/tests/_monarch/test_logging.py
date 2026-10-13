# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
import threading
from typing import Any, Callable
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import call, Mock, patch

import pytest
from monarch._rust_bindings.monarch_hyperactor.handle import _new_handle_pair, Handle
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh as HyProcMesh
from monarch._src.actor.future import _ready_handle
from monarch._src.actor.logging import flush_all_proc_mesh_logs, LoggingManager


class _AwaitTracker:
    def __init__(self, awaitable: Any) -> None:
        self._awaitable = awaitable
        self.awaited = False
        self.get_called = False

    def __await__(self) -> Any:
        self.awaited = True
        return self._awaitable.__await__()

    def get(self, *args: object, **kwargs: object) -> None:
        self.get_called = True


class _FlushBaseException(BaseException):
    pass


_FLUSH_BASE_EXCEPTION_MESSAGE = "test flush base exception"


def _failed_handle(error: BaseException) -> Handle[None]:
    """A Handle already settled with `error`."""
    handle, completer = _new_handle_pair()
    completer.set_exception(error)
    return handle


def _completed_handle() -> Handle[None]:
    """A Handle already settled with `None`."""
    handle, completer = _new_handle_pair()
    completer.set_result(None)
    return handle


class LoggingManagerTest(TestCase):
    def setUp(self) -> None:
        self.logging_manager = LoggingManager()

    def test_init_initializes_logging_mesh_client_to_none(self) -> None:
        # Setup: create a new LoggingManager instance
        manager = LoggingManager()

        # Execute: check initial state
        # Assert: confirm that _logging_mesh_client is initialized to None
        self.assertIsNone(manager._logging_mesh_client)

    @pytest.mark.oss_skip  # type: ignore: IPython.get_ipython doesn't exist in OSS CI
    @patch("monarch._src.actor.logging.IN_IPYTHON", True)
    @patch("IPython.get_ipython")
    @patch("monarch._src.actor.logging._global_flush_registered", False)
    def test_register_flusher_if_in_ipython_registers_event(
        self, mock_get_ipython: Mock
    ) -> None:
        # Setup: mock IPython environment
        mock_ipython = Mock()
        mock_get_ipython.return_value = mock_ipython

        # Execute: register flusher
        self.logging_manager.register_flusher_if_in_ipython()

        # Assert: post_run_cell event was registered
        mock_ipython.events.register.assert_called_once()
        args = mock_ipython.events.register.call_args[0]
        self.assertEqual(args[0], "post_run_cell")
        # Check that the callback is callable
        self.assertTrue(callable(args[1]))

    @pytest.mark.oss_skip  # type: ignore: IPython.get_ipython doesn't exist in OSS CI
    @patch("monarch._src.actor.logging.IN_IPYTHON", True)
    @patch("IPython.get_ipython")
    @patch("monarch._src.actor.logging._global_flush_registered", False)
    @patch("monarch._src.actor.logging._flush_all_proc_mesh_logs_async")
    @patch("monarch._src.actor.logging.flush_all_proc_mesh_logs")
    def test_post_run_cell_callback_in_sync_context_calls_sync_flush(
        self,
        mock_sync_flush: Mock,
        mock_async_flush: Mock,
        mock_get_ipython: Mock,
    ) -> None:
        # Setup: register the post_run_cell callback and capture it.
        mock_ipython = Mock()
        mock_get_ipython.return_value = mock_ipython
        self.logging_manager.register_flusher_if_in_ipython()
        callback = mock_ipython.events.register.call_args[0][1]

        # Execute: fire the callback in a sync context (no running loop).
        callback(None)

        # Assert: sync flush ran; async helper was not scheduled.
        mock_sync_flush.assert_called_once()
        mock_async_flush.assert_not_called()

    @pytest.mark.oss_skip  # type: ignore: IPython.get_ipython doesn't exist in OSS CI
    @patch("monarch._src.actor.logging.IN_IPYTHON", True)
    @patch("IPython.get_ipython")
    @patch("monarch._src.actor.logging._global_flush_registered", False)
    @patch("monarch._src.actor.logging._flush_all_proc_mesh_logs_async")
    @patch("monarch._src.actor.logging.flush_all_proc_mesh_logs")
    def test_post_run_cell_callback_in_async_context_schedules_async_flush(
        self,
        mock_sync_flush: Mock,
        mock_async_flush: Mock,
        mock_get_ipython: Mock,
    ) -> None:
        # Setup: register the post_run_cell callback and capture it.
        mock_ipython = Mock()
        mock_get_ipython.return_value = mock_ipython

        # The patched async helper must still produce an awaitable so that
        # loop.create_task does not complain.
        async def _coro() -> None:
            return None

        mock_async_flush.return_value = _coro()

        self.logging_manager.register_flusher_if_in_ipython()
        callback: Callable[[object], None] = mock_ipython.events.register.call_args[0][
            1
        ]

        # Execute: fire the callback from within a running asyncio loop. This
        # is the path that would raise the RuntimeError landed in D104448236
        # if the implementation called Future.get() directly.
        async def driver() -> None:
            callback(None)
            # Drain pending tasks so the scheduled coroutine actually runs.
            await asyncio.sleep(0)

        asyncio.run(driver())

        # Assert: async helper was scheduled; sync flush was not called.
        mock_async_flush.assert_called_once()
        mock_sync_flush.assert_not_called()

    @patch("monarch._src.actor.logging.IN_IPYTHON", False)
    def test_enable_fd_capture_if_not_in_ipython_returns_none(self) -> None:
        # Execute: try to enable FD capture when not in IPython
        result = self.logging_manager.enable_fd_capture_if_in_ipython()

        # Assert: None is returned
        self.assertIsNone(result)

    @patch("monarch._src.actor.logging.context")
    def test_flush_calls_mesh_client_flush(self, mock_context: Mock) -> None:
        # Setup: mock context, client, and the Handle the client returns
        mock_instance = Mock()
        mock_context.return_value.actor_instance._as_rust.return_value = mock_instance
        mock_client = Mock()
        mock_handle = Mock()
        mock_client.flush.return_value = mock_handle
        self.logging_manager._logging_mesh_client_handle = _ready_handle(mock_client)

        # Execute: flush logs
        result = self.logging_manager.flush()

        # Assert: the returned Handle was observed directly with the timeout.
        self.assertIsNone(result)
        mock_client.flush.assert_called_once_with(mock_instance)
        mock_handle.get.assert_called_once_with(timeout=3)
        mock_handle.spawn_handle.assert_not_called()

    @patch("monarch._src.actor.logging.context")
    def test_flush_handles_exception_gracefully(self, mock_context: Mock) -> None:
        # Setup: make Handle observation raise an ordinary exception.
        mock_instance = Mock()
        mock_context.return_value.actor_instance._as_rust.return_value = mock_instance
        mock_client = Mock()
        mock_handle = Mock()
        mock_client.flush.return_value = mock_handle
        mock_handle.get.side_effect = Exception("Test exception")
        self.logging_manager._logging_mesh_client_handle = _ready_handle(mock_client)

        # Execute: flush logs (should not raise exception)
        result = self.logging_manager.flush()

        # Assert: the operation was started and the observer error was suppressed.
        self.assertIsNone(result)
        mock_client.flush.assert_called_once_with(mock_instance)
        mock_handle.get.assert_called_once_with(timeout=3)

    def test_flush_propagates_base_exception(self) -> None:
        with patch.object(
            self.logging_manager, "_new_flush_handle"
        ) as no_client_handle:
            with self.assertRaises(AssertionError) as no_client_error:
                self.logging_manager.flush()
        self.assertIs(type(no_client_error.exception), AssertionError)
        no_client_handle.assert_not_called()

        self.logging_manager._logging_mesh_client_handle = _ready_handle(Mock())
        with patch.object(
            self.logging_manager,
            "_new_flush_handle",
            return_value=_failed_handle(
                _FlushBaseException(_FLUSH_BASE_EXCEPTION_MESSAGE)
            ),
        ) as mock_new_flush_handle:
            with self.assertRaises(_FlushBaseException) as raised:
                self.logging_manager.flush()

        self.assertIs(type(raised.exception), _FlushBaseException)
        self.assertEqual(str(raised.exception), _FLUSH_BASE_EXCEPTION_MESSAGE)
        mock_new_flush_handle.assert_called_once_with()


class FlushAllProcMeshLogsTest(TestCase):
    @patch("monarch._src.actor.proc_mesh.get_active_proc_meshes")
    def test_flush_all_proc_mesh_logs_calls_flush_on_all_meshes(
        self, mock_get_active: Mock
    ) -> None:
        # Setup: create mock proc meshes
        mock_mesh1 = Mock()
        mock_mesh2 = Mock()
        mock_get_active.return_value = [mock_mesh1, mock_mesh2]

        # Execute: flush all proc mesh logs
        flush_all_proc_mesh_logs()

        # Assert: flush was called on all meshes
        mock_mesh1._logging_manager.flush.assert_called_once()
        mock_mesh2._logging_manager.flush.assert_called_once()

    @patch("monarch._src.actor.proc_mesh.get_active_proc_meshes")
    def test_flush_all_proc_mesh_logs_handles_empty_list(
        self, mock_get_active: Mock
    ) -> None:
        # Setup: no active proc meshes
        mock_get_active.return_value = []

        # Execute: flush all proc mesh logs (should not raise exception)
        flush_all_proc_mesh_logs()

        # Assert: method completes without error


class LoggingManagerAsyncTest(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.logging_manager = LoggingManager()

    async def test_flush_async_awaits_the_handle(self) -> None:
        ready = asyncio.get_running_loop().create_future()
        ready.set_result(None)
        handle = _AwaitTracker(ready)
        self.logging_manager._logging_mesh_client_handle = _ready_handle(Mock())

        with patch.object(
            self.logging_manager, "_new_flush_handle", return_value=handle
        ) as mock_new_flush_handle:
            result = await self.logging_manager.flush_async()

        self.assertIsNone(result)
        mock_new_flush_handle.assert_called_once_with()
        self.assertTrue(handle.awaited)
        self.assertFalse(handle.get_called)

    async def test_flush_async_suppresses_ordinary_error(self) -> None:
        self.logging_manager._logging_mesh_client_handle = _ready_handle(Mock())
        with patch.object(
            self.logging_manager,
            "_new_flush_handle",
            return_value=_failed_handle(RuntimeError("test flush error")),
        ) as mock_new_flush_handle:
            result = await asyncio.wait_for(
                self.logging_manager.flush_async(), timeout=10
            )

        self.assertIsNone(result)
        mock_new_flush_handle.assert_called_once_with()

    async def test_flush_async_propagates_base_exception(self) -> None:
        with patch.object(
            self.logging_manager, "_new_flush_handle"
        ) as no_client_handle:
            result = await asyncio.wait_for(
                self.logging_manager.flush_async(), timeout=10
            )
        self.assertIsNone(result)
        no_client_handle.assert_not_called()

        self.logging_manager._logging_mesh_client_handle = _ready_handle(Mock())
        with patch.object(
            self.logging_manager,
            "_new_flush_handle",
            return_value=_failed_handle(
                _FlushBaseException(_FLUSH_BASE_EXCEPTION_MESSAGE)
            ),
        ) as mock_new_flush_handle:
            with self.assertRaises(_FlushBaseException) as raised:
                await asyncio.wait_for(self.logging_manager.flush_async(), timeout=10)

        self.assertIs(type(raised.exception), _FlushBaseException)
        self.assertEqual(str(raised.exception), _FLUSH_BASE_EXCEPTION_MESSAGE)
        mock_new_flush_handle.assert_called_once_with()

    async def test_flush_async_cancellation_leaves_producer_running(self) -> None:
        # Adapter-level seam: a controlled Handle, not a native flush. An
        # independent thread settles it only after the asyncio observer has
        # been cancelled, as the native producer would keep running.
        handle, completer = _new_handle_pair()
        requested = asyncio.Event()
        release = threading.Event()
        completed = threading.Event()

        def request() -> Handle[None]:
            requested.set()
            return handle

        def produce() -> None:
            if release.wait(timeout=10):
                completer.set_result(None)
                completed.set()

        producer = threading.Thread(target=produce, name="controlled-flush")
        producer.start()
        observer: asyncio.Task[None] | None = None
        completed_after_cancel = False
        self.logging_manager._logging_mesh_client_handle = _ready_handle(Mock())
        try:
            with patch.object(
                self.logging_manager, "_new_flush_handle", side_effect=request
            ):
                observer = asyncio.create_task(self.logging_manager.flush_async())
                # Once requested, flush_async is suspended awaiting the Handle.
                await asyncio.wait_for(requested.wait(), timeout=10)

                self.assertTrue(observer.cancel())
                with self.assertRaises(asyncio.CancelledError):
                    await observer
        finally:
            release.set()
            completed_after_cancel = await asyncio.wait_for(
                asyncio.to_thread(completed.wait, 5), timeout=10
            )
            producer.join(timeout=5)
            if observer is not None and not observer.done():
                observer.cancel()
                try:
                    await observer
                except asyncio.CancelledError:
                    pass

        self.assertTrue(
            completed_after_cancel,
            "cancelling the observer must not stop the producer",
        )

    async def test_two_manager_flushes_create_two_operations(self) -> None:
        # Adapter-level seam: the native client supplies distinct controlled
        # Handles, while the real manager helper must request and observe each
        # one independently. Native sync-flush barriers are covered natively.
        handles: list[Handle[None]] = []

        def controlled_handle(*_: object) -> Handle[None]:
            handle = _completed_handle()
            handles.append(handle)
            return handle

        mock_client = Mock()
        mock_client.flush.side_effect = controlled_handle
        mock_instance = Mock()
        self.logging_manager._logging_mesh_client_handle = _ready_handle(mock_client)
        with patch("monarch._src.actor.logging.context") as mock_context:
            mock_context.return_value.actor_instance._as_rust.return_value = (
                mock_instance
            )
            first_result = await asyncio.wait_for(
                self.logging_manager.flush_async(), timeout=10
            )
            second_result = await asyncio.wait_for(
                self.logging_manager.flush_async(), timeout=10
            )

        self.assertIsNone(first_result)
        self.assertIsNone(second_result)
        self.assertEqual(len(handles), 2)
        self.assertIsNot(handles[0], handles[1])
        self.assertEqual(
            mock_client.flush.call_args_list,
            [call(mock_instance), call(mock_instance)],
        )

    @patch("monarch._src.actor.logging.LoggingMeshClient")
    async def test_start_captures_inputs_and_publishes_the_client(
        self, mock_logging_client: Mock
    ) -> None:
        mock_instance = Mock()
        raw = _ready_handle(Mock(spec=HyProcMesh))
        mock_client = Mock()
        handle = _ready_handle(mock_client)
        mock_logging_client.spawn.return_value = handle

        self.assertIs(self.logging_manager.start(mock_instance, raw, True), handle)
        mock_logging_client.spawn.assert_called_once_with(
            mock_instance,
            raw,
            stream_to_client=True,
            aggregate_window_sec=3,
            level=logging.INFO,
        )
        self.assertIs(self.logging_manager._logging_mesh_client, mock_client)

    async def test_start_returns_the_existing_logging_handle(self) -> None:
        handle = _ready_handle(Mock())
        self.logging_manager._logging_mesh_client_handle = handle
        with patch("monarch._src.actor.logging.LoggingMeshClient") as mock_client:
            self.assertIs(
                self.logging_manager.start(Mock(), _ready_handle(Mock()), True), handle
            )
            mock_client.spawn.assert_not_called()

    @patch("monarch._src.actor.logging.context")
    async def test_logging_option_sets_mode_with_valid_parameters(
        self, mock_context: Mock
    ) -> None:
        # Setup: mock context and client
        mock_instance = Mock()
        mock_context.return_value.actor_instance._as_rust.return_value = mock_instance
        mock_client = Mock()
        self.logging_manager._logging_mesh_client_handle = _ready_handle(mock_client)

        with (
            patch.object(
                self.logging_manager, "register_flusher_if_in_ipython"
            ) as mock_register,
            patch.object(
                self.logging_manager, "enable_fd_capture_if_in_ipython"
            ) as mock_enable,
        ):
            # Execute: call logging_option with valid parameters
            await self.logging_manager.logging_option(
                stream_to_client=False,
                aggregate_window_sec=5,
                level=logging.WARNING,
            )

            # Assert: set_mode was called with correct parameters
            mock_client.set_mode.assert_called_once_with(
                mock_instance,
                stream_to_client=False,
                aggregate_window_sec=5,
                level=logging.WARNING,
            )
            # Assert: helper methods were called
            mock_register.assert_called_once()
            mock_enable.assert_called_once()
