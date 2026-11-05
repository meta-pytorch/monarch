# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""CLI cleanup stops owned work on normal and exceptional exits."""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest
from monarch.tools import cli


@pytest.mark.parametrize(
    "error",
    [
        None,
        KeyboardInterrupt(),
        RuntimeError("command failed"),
        SystemExit(0),
        SystemExit(7),
    ],
)
def test_shutdown_runs_on_every_command_exit(error):
    events = []

    def command(args):
        events.append("command")
        if error is not None:
            raise error

    def wait():
        events.append("shutdown completed")

    args = SimpleNamespace(func=command)
    parser = Mock()
    parser.parse_args.return_value = args
    shutdown = Mock(return_value=SimpleNamespace(get=wait))
    with (
        patch.object(cli, "get_parser", return_value=parser),
        patch.object(cli, "shutdown_context", shutdown),
    ):
        if error is None:
            cli.main(["test"])
        else:
            with pytest.raises(type(error)) as raised:
                cli.main(["test"])
            assert raised.value is error
    assert events == ["command", "shutdown completed"]
    shutdown.assert_called_once_with()


def test_help_without_command_does_not_bootstrap_shutdown():
    parser = Mock()
    parser.parse_args.return_value = SimpleNamespace()
    with (
        patch.object(cli, "get_parser", return_value=parser),
        patch.object(cli, "shutdown_context") as shutdown,
    ):
        with pytest.raises(SystemExit) as raised:
            cli.main([])
    assert raised.value.code == 1
    parser.print_help.assert_called_once_with()
    shutdown.assert_not_called()


@pytest.mark.parametrize("outcome", [KeyboardInterrupt(), RuntimeError("failed"), 0, 7])
def test_exec_always_shuts_down_client(outcome):
    # Use the real parser and bound ExecCmd.run so this exercises the CLI's
    # exceptional-exit path, which direct exec_on_job calls do not enter.
    execute = (
        Mock(side_effect=outcome)
        if isinstance(outcome, BaseException)
        else Mock(return_value=outcome)
    )
    with (
        patch.object(cli, "exec_on_job", execute),
        patch.object(cli, "shutdown_context") as shutdown,
    ):
        if outcome == 0:
            cli.main(["exec", "true"])
        else:
            error_type = (
                type(outcome) if isinstance(outcome, BaseException) else SystemExit
            )
            with pytest.raises(error_type):
                cli.main(["exec", "true"])
        shutdown.assert_called_once_with()
        shutdown.return_value.get.assert_called_once_with()


def test_interrupted_cli_does_not_start_remaining_meshes(monkeypatch):
    from monarch._src.job import job
    from monarch._src.tools import commands

    spec = MagicMock()
    spec.state.return_value._hosts = {"first": MagicMock(), "second": MagicMock()}
    spec._components.mounts.python_executable_for_mesh.return_value = None
    monkeypatch.setattr(commands, "load_current_job", lambda: spec)
    calls, observers = [], []

    def execute(mesh, cmd, **kwargs):
        calls.append(mesh)
        result = MagicMock()

        def wait_for_command():
            observers.append(threading.current_thread())
            raise KeyboardInterrupt

        result.get.side_effect = wait_for_command
        return result

    monkeypatch.setattr(job, "exec_command", execute)
    with patch.object(cli, "shutdown_context") as shutdown:
        with pytest.raises(KeyboardInterrupt):
            cli.main(["exec", "--all", "--", "true"])
        shutdown.assert_called_once_with()
        shutdown.return_value.get.assert_called_once_with()
    assert calls == [spec.state.return_value._hosts["first"]]
    assert observers == [threading.main_thread()]
