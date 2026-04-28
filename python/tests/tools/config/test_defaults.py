# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import unittest

from monarch.tools.config import (  # @manual=//monarch/python/monarch/tools/config/meta:defaults
    defaults,
    NOT_SET,
)
from torchx.specs import AppDef


class TestDefaults(unittest.TestCase):
    def test_default_config(self) -> None:
        for scheduler in defaults.scheduler_factories():
            with self.subTest(scheduler=scheduler):
                config = defaults.config(scheduler)

                # make sure that we've set the scheduler name when returning the config
                self.assertEqual(scheduler, config.scheduler)

                # make sure a new Config is returned each time
                # by modifying the returned config
                #   -> re-getting the default configs for the same scheduler
                #   -> validating the changes are not persisted in the new config
                self.assertNotIn("foo", config.scheduler_args)
                config.scheduler_args["foo"] = "bar"
                self.assertNotIn("foo", defaults.config(scheduler).scheduler_args)

    def test_default_config_appdef(self) -> None:
        for scheduler, _ in {
            "mast": {"image": "_DUMMY_FBPKG_:0"},
            "whatever": {"image": "_DUMMY_FBPKG_:0"},
            "mast_conda": {},
        }.items():
            config = defaults.config(
                scheduler,
            )
            self.assertEqual(AppDef(name=NOT_SET), config.appdef)

    def test_default_scheduler_factories(self) -> None:
        # just make sure the common schedulers are present
        self.assertIn("local_cwd", defaults.scheduler_factories())
        self.assertIn("slurm", defaults.scheduler_factories())
