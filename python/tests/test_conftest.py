# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import pytest
from conftest import _is_disabled_test


@pytest.mark.parametrize(
    ("node_id", "disabled_entry", "expected"),
    [
        (
            "python/tests/test_example.py::test_one",
            "python/tests/test_example.py::test_one",
            True,
        ),
        (
            "python/tests/test_example.py::TestExample::test_one",
            "python/tests/test_example.py",
            True,
        ),
        (
            "python/tests/test_example.py::TestExample::test_one",
            "python/tests/test_example.py::TestExample",
            True,
        ),
        (
            "python/tests/test_example.py::test_one[value]",
            "python/tests/test_example.py::test_one",
            True,
        ),
        (
            "python/tests/test_example.py::test_one[value]",
            "test_one",
            True,
        ),
        (
            "python/tests/test_example.py::test_one_extra",
            "python/tests/test_example.py::test_one",
            False,
        ),
        (
            "python/tests/test_example.py.bak::test_one",
            "python/tests/test_example.py",
            False,
        ),
    ],
)
def test_is_disabled_test(node_id: str, disabled_entry: str, expected: bool) -> None:
    assert _is_disabled_test(node_id, frozenset({disabled_entry})) is expected
