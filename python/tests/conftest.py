# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Propagate sys.path to PYTHONPATH so that worker subprocesses spawned by
# monarch (e.g. distributed_proc_mesh) see the same import paths as the
# pytest parent process. pytest's default "prepend" import mode modifies
# sys.path at the Python level, but child processes don't inherit that —
# they only see PYTHONPATH.
os.environ["PYTHONPATH"] = os.pathsep.join(sys.path)

# disabled_tests.txt lives at the project root (three levels up from here:
# python/tests/conftest.py -> python/tests -> python -> project root).
_DISABLED_TESTS_FILE = Path(__file__).parent.parent.parent / "disabled_tests.txt"


def _load_disabled_tests() -> frozenset[str]:
    if not _DISABLED_TESTS_FILE.exists():
        return frozenset()
    return frozenset(
        line.strip()
        for line in _DISABLED_TESTS_FILE.read_text().splitlines()
        if line.strip()
    )


def pytest_collection_modifyitems(
    items: list[pytest.Item],
    config: pytest.Config,
) -> None:
    """Skip any test whose name or node ID appears in disabled_tests.txt."""
    disabled = _load_disabled_tests()
    if not disabled:
        return

    for item in items:
        node_id = item.nodeid
        # Match on the full node ID (e.g. "python/tests/test_foo.py::test_bar")
        # or just the test name (the part after the last "::").
        test_name = node_id.split("::")[-1]
        if node_id in disabled or test_name in disabled:
            item.add_marker(
                pytest.mark.skip(
                    reason=f"Disabled via GitHub issue: DISABLED {test_name}"
                )
            )
