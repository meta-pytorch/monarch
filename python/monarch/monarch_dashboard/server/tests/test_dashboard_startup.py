# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Unit tests for start_dashboard() thread mode.

Each test is isolated in a subprocess so the daemon Flask thread is
cleaned up when the subprocess exits.
"""

import os
import socket
import tempfile
import threading

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch.monarch_dashboard.fake_data.generate import generate
from monarch.monarch_dashboard.server.app import start_dashboard
from monarch.monarch_dashboard.server.db import SQLiteAdapter


def _make_adapter() -> SQLiteAdapter:
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test.db")
    generate(db_path)
    return SQLiteAdapter(db_path)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


@pytest.mark.timeout(30)
@isolate_in_subprocess
def test_returns_dict_with_expected_keys() -> None:
    adapter = _make_adapter()
    port = _free_port()
    info = start_dashboard(adapter=adapter, port=port, host="127.0.0.1")
    assert "url" in info
    assert "port" in info
    assert "handle" in info
    assert info["port"] == port
    assert info["pid"] is None
    assert isinstance(info["handle"], threading.Thread)
    assert info["handle"].daemon
    assert info["handle"].is_alive()


@pytest.mark.timeout(30)
@isolate_in_subprocess
def test_occupied_port_raises() -> None:
    adapter = _make_adapter()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        occupied = s.getsockname()[1]
        with pytest.raises(OSError):
            start_dashboard(adapter=adapter, port=occupied, host="127.0.0.1")
