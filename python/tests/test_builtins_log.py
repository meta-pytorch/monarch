# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import logging
from unittest.mock import patch

import pytest
from monarch._testing import TestingContext
from monarch.builtins.log import log_remote, set_logging_level_remote


@pytest.fixture(scope="module", autouse=True)
def testing_context():
    global local
    with TestingContext() as local:
        yield


@pytest.mark.timeout(120)
class TestLogFunctions:
    @classmethod
    def local_device_mesh(cls, num_hosts, gpu_per_host, activate=True):
        return local.local_device_mesh(
            num_hosts,
            gpu_per_host,
            activate,
        )

    @patch("monarch.builtins.log.logger")
    def test_log_remote_default_level(self, mock_log):
        with self.local_device_mesh(1, 1):
            log_remote("test warning message")

    @patch("monarch.builtins.log.logger")
    def test_log_remote_with_args(self, mock_log):
        with self.local_device_mesh(1, 1):
            log_remote("test message with %s and %d", "str", 42)

    @patch("monarch.builtins.log.logger")
    def test_set_logging_level_remote(self, mock_logger):
        with self.local_device_mesh(1, 1):
            set_logging_level_remote(logging.DEBUG)

    @patch("monarch.builtins.log.logger")
    def test_log_remote_custom_level(self, mock_log):
        with self.local_device_mesh(1, 1):
            set_logging_level_remote(logging.ERROR)
            log_remote("ignored info message", level=logging.INFO)
            log_remote("seen error message", level=logging.ERROR)

    @patch("monarch.builtins.log.logger")
    def test_log_remote_multiple_calls(self, mock_log):
        with self.local_device_mesh(1, 1):
            log_remote("First message")
            log_remote("Second message", level=logging.INFO)
            log_remote("Third message", level=logging.ERROR)
