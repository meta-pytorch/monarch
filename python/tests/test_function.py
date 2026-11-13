# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import unittest

from monarch.common.function import function_to_import_path, resolvable_function


class FunctionResolutionTest(unittest.TestCase):
    def test_unresolvable_functions_round_trip_by_value(self) -> None:
        for module, qualname in [
            ("builtins", "missing_function"),
            ("builtins", "missing_owner.function"),
            ("builtins", "dict.missing_function"),
            ("__main__", "function"),
        ]:
            with self.subTest(module=module, qualname=qualname):

                def function(value: int) -> int:
                    return value + 1

                function.__module__ = module
                function.__qualname__ = qualname

                self.assertIsNone(function_to_import_path(function))
                self.assertEqual(resolvable_function(function).resolve()(41), 42)
