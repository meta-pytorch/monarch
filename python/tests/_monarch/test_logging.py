# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from typing import Any
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import Mock, patch

import pytest
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh as HyProcMesh
from monarch._src.actor.logging import flush_all_proc_mesh_logs, LoggingManager


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

    @patch("monarch._src.actor.logging.IN_IPYTHON", False)
    def test_enable_fd_capture_if_not_in_ipython_returns_none(self) -> None:
        # Execute: try to enable FD capture when not in IPython
        result = self.logging_manager.enable_fd_capture_if_in_ipython()

        # Assert: None is returned
        self.assertIsNone(result)

    @patch("monarch._src.actor.logging.Future")
    @patch("monarch._src.actor.logging.context")
    def test_flush_calls_mesh_client_flush(
        self, mock_context: Mock, mock_future: Mock
    ) -> None:
        # Setup: mock context, client, and Future
        mock_instance = Mock()
        mock_context.return_value.actor_instance._as_rust.return_value = mock_instance
        mock_client = Mock()
        mock_task = Mock()
        mock_client.flush.return_value.spawn.return_value.task.return_value = mock_task
        self.logging_manager._logging_mesh_client = mock_client

        mock_future_instance = Mock()
        mock_future.return_value = mock_future_instance

        # Execute: flush logs
        self.logging_manager.flush()

        # Assert: mesh client flush was called
        mock_client.flush.assert_called_once_with(mock_instance)
        # Assert: Future was created and get was called with timeout
        mock_future.assert_called_once_with(coro=mock_task)
        mock_future_instance.get.assert_called_once_with(timeout=3)

    @patch("monarch._src.actor.logging.Future")
    @patch("monarch._src.actor.logging.context")
    def test_flush_handles_exception_gracefully(
        self, mock_context: Mock, mock_future: Mock
    ) -> None:
        # Setup: mock context, client, and Future that raises exception
        mock_instance = Mock()
        mock_context.return_value.actor_instance._as_rust.return_value = mock_instance
        mock_client = Mock()
        self.logging_manager._logging_mesh_client = mock_client

        mock_future_instance = Mock()
        mock_future_instance.get.side_effect = Exception("Test exception")
        mock_future.return_value = mock_future_instance

        # Execute: flush logs (should not raise exception)
        self.logging_manager.flush()

        # Assert: no exception is raised and method completes gracefully


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

    @patch("monarch._src.actor.logging.LoggingMeshClient")
    @patch("monarch._src.actor.logging.context")
    async def test_init_with_hyprocmesh_creates_logging_mesh_client(
        self, mock_context: Mock, mock_logging_client: Mock
    ) -> None:
        # Setup: mock the context and LoggingMeshClient
        mock_instance = Mock()
        mock_context.return_value.actor_instance._as_rust.return_value = mock_instance
        mock_proc_mesh = Mock(spec=HyProcMesh)

        mock_client: Mock = Mock()

        # Make spawn return a coroutine that resolves to mock_client
        async def mock_spawn(*args: Any, **kwargs: Any) -> Mock:
            return mock_client

        mock_logging_client.spawn = mock_spawn

        # Execute: initialize the logging manager with HyProcMesh
        await self.logging_manager.init(mock_proc_mesh, stream_to_client=True)

        # Assert: set_mode was called with correct parameters
        mock_client.set_mode.assert_called_once_with(
            mock_instance,
            stream_to_client=True,
            aggregate_window_sec=3,
            level=logging.INFO,
        )
        self.assertEqual(self.logging_manager._logging_mesh_client, mock_client)

    async def test_init_returns_early_if_already_initialized(self) -> None:
        # Setup: set _logging_mesh_client to a mock value
        mock_client = Mock()
        self.logging_manager._logging_mesh_client = mock_client

        with patch(
            "monarch._src.actor.logging.LoggingMeshClient"
        ) as mock_logging_client:
            # Execute: try to initialize again
            await self.logging_manager.init(Mock(), stream_to_client=True)

            # Assert: LoggingMeshClient.spawn was not called
            mock_logging_client.spawn.assert_not_called()

    @patch("monarch._src.actor.logging.context")
    async def test_logging_option_sets_mode_with_valid_parameters(
        self, mock_context: Mock
    ) -> None:
        # Setup: mock context and client
        mock_instance = Mock()
        mock_context.return_value.actor_instance._as_rust.return_value = mock_instance
        mock_client = Mock()
        self.logging_manager._logging_mesh_client = mock_client

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
