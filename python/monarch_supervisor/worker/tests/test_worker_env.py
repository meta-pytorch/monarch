# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import sys
import unittest
from unittest.mock import patch

from monarch_supervisor.worker import worker_env


class WorkerEnvTest(unittest.TestCase):
    def test_main_runs_requested_module_with_forwarded_arguments(self) -> None:
        with (
            patch.object(
                sys,
                "argv",
                ["worker_env", "-m", "example.worker", "--rank", "3"],
            ),
            patch.object(
                worker_env.runpy,
                "_run_module_as_main",
            ) as run_module_as_main,
        ):
            worker_env.main()

            self.assertEqual(sys.argv, ["worker_env", "--rank", "3"])

        run_module_as_main.assert_called_once_with("example.worker", False)
