# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import unittest

from monarch.tools.config import (  # @manual=//monarch/python/monarch/tools/config/meta:defaults
    defaults,
)


class TestDefaults(unittest.TestCase):
    def test_default_scheduler_factories(self) -> None:
        # just make sure the common schedulers are present
        self.assertIn("local_cwd", defaults.scheduler_factories())
        self.assertIn("slurm", defaults.scheduler_factories())
