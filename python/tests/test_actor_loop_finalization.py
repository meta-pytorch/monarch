# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Interpreter-exit actor-loop regression test.

Two arms, gated separately:

``unmitigated``
    the `SIGABRT` oracle. CPython 3.13 changed daemon-thread finalization from
    forced exit to a hang (gh-87135), so this arm only runs below 3.13.

``fixed``
    "zero actor-loop threads survive shutdown". Version-independent, and most
    informative on 3.14 where the unfixed symptom would be a hang rather than
    an abort. Runs on every supported version.
"""

from __future__ import annotations

import importlib.resources
import signal
import subprocess
import sys
import unittest

import pytest

try:
    # Buck synthesizes `monarch.python.tests` from the target path.
    from monarch.python.tests import actor_loop_finalization_timeouts as timeouts
except ImportError:
    # OSS: there is no `__init__.py` under python/tests, so pytest's prepend
    # import mode puts this directory on sys.path and the module is top-level.
    # Without this fallback the module raises during collection, which yields a
    # JUnit <error> shape and crashes pytest-results-action.
    import actor_loop_finalization_timeouts as timeouts  # type: ignore[no-redef]


_SUBPROCESS_TIMEOUT_S = timeouts.SUBPROCESS_TIMEOUT_S


class ActorLoopFinalizationTest(unittest.TestCase):
    def test_subprocess_timeout_exceeds_every_child_deadline(self) -> None:
        """A later edit to one constant must not silently re-invert the nesting."""
        self.assertGreater(
            _SUBPROCESS_TIMEOUT_S,
            timeouts.CHILD_WORST_CASE_S,
            "the parent timeout must be a backstop, not the usual failure mode",
        )

    def run_model(self, mode: str) -> subprocess.CompletedProcess[str]:
        test_bin = importlib.resources.files("monarch.python.tests").joinpath(
            "test_bin"
        )
        completed = subprocess.run(
            [str(test_bin), mode],
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT_S,
            check=False,
        )
        self.assert_child_interpreter_matches(completed)
        return completed

    def assert_child_interpreter_matches(
        self, completed: subprocess.CompletedProcess[str]
    ) -> None:
        """The model resource must be built at this target's `py_version`.

        Without this a 3.14 test target whose resource was built at the default
        version would report false 3.14 coverage.
        """
        expected = (
            f"python {sys.version_info.major}.{sys.version_info.minor} gil=enabled"
        )
        self.assertIn(
            expected,
            completed.stderr,
            f"child interpreter does not match the parent, stderr:\n{completed.stderr}",
        )

    def assert_survivors(
        self, completed: subprocess.CompletedProcess[str], mode: str, count: int
    ) -> None:
        self.assertIn(
            f"{mode} observed {count} actor loops after runtime shutdown",
            completed.stderr,
        )

    # oss_skip: drives the Buck-provided `test_bin` resource, which the OSS
    # layout does not have; same reason as test_actor_error.py's resource tests.
    @pytest.mark.oss_skip
    @unittest.skipUnless(
        sys.version_info < (3, 13),
        "CPython 3.13 changed daemon-thread finalization from forced exit to hang",
    )
    def test_unmitigated_actor_loop_aborts_during_finalization(self) -> None:
        completed = self.run_model("unmitigated")
        self.assertEqual(
            -signal.SIGABRT,
            completed.returncode,
            f"expected SIGABRT, stderr:\n{completed.stderr}",
        )
        self.assertIn("FATAL: exception not rethrown", completed.stderr)
        self.assert_survivors(completed, "unmitigated", 2)

    # oss_skip: drives the Buck-provided `test_bin` resource.
    @pytest.mark.oss_skip
    def test_actor_loop_reaper_prevents_finalization_abort(self) -> None:
        completed = self.run_model("fixed")
        self.assertEqual(
            0,
            completed.returncode,
            f"mitigated process did not exit normally:\n{completed.stderr}",
        )
        self.assertNotIn("FATAL: exception not rethrown", completed.stderr)
        # The reaper ran and emptied the set. Exit status alone would also be 0
        # if no actor loop had ever been created.
        self.assert_survivors(completed, "fixed", 0)
