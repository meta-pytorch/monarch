# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""CLI shutdown must run before exceptional exit starts Python thread joining."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

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
