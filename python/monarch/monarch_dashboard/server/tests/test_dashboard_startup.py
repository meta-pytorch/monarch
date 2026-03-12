# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Unit tests for start_dashboard() thread mode."""

import os
import socket
import tempfile
import threading
import unittest

from monarch.monarch_dashboard.fake_data.generate import generate
from monarch.monarch_dashboard.server.app import start_dashboard
from monarch.monarch_dashboard.server.db import SQLiteAdapter


class StartDashboardThreadTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "test.db")
        generate(self._db_path)
        self._adapter = SQLiteAdapter(self._db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def _free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("localhost", 0))
            return s.getsockname()[1]

    def test_returns_dict_with_expected_keys(self):
        port = self._free_port()
        info = start_dashboard(
            adapter=self._adapter,
            port=port,
            host="127.0.0.1",
        )
        self.assertIn("url", info)
        self.assertIn("port", info)
        self.assertIn("handle", info)
        self.assertEqual(info["port"], port)
        self.assertIsNone(info["pid"])
        self.assertIsInstance(info["handle"], threading.Thread)
        self.assertTrue(info["handle"].daemon)
        self.assertTrue(info["handle"].is_alive())

    def test_occupied_port_raises(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("127.0.0.1", 0))
            occupied = s.getsockname()[1]
            with self.assertRaises(OSError):
                start_dashboard(
                    adapter=self._adapter,
                    port=occupied,
                    host="127.0.0.1",
                )


if __name__ == "__main__":
    unittest.main()
