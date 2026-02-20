# Copyright (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

import os
import subprocess
import sys

import pytest


@pytest.fixture
def isolated(request):
    """Re-run this single test in an isolated subprocess.

    Use this fixture on tests that mutate global state (e.g.
    monarch.actor.unhandled_fault_hook) without restoring it, so the
    mutation cannot leak into other tests that share the process.

    Skipped under fbcode/XAR builds where subprocess re-invocation
    does not work.
    """
    # In fbcode (XAR) builds, subprocess pytest doesn't work — just run
    # the test in-process as before.
    if "FB_XAR_INVOKED_NAME" in os.environ:
        yield
        return

    # If we're already inside an isolated subprocess, run the test normally.
    if os.environ.get("_MONARCH_ISOLATED_TEST"):
        yield
        return

    # Otherwise, re-invoke pytest for just this one test in a subprocess.
    env = {**os.environ, "_MONARCH_ISOLATED_TEST": "1"}
    node_id = request.node.nodeid
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-xvs", node_id],
        env=env,
        cwd=str(request.config.rootdir),
        timeout=120,
    )
    if result.returncode != 0:
        pytest.fail(f"Isolated subprocess failed (rc={result.returncode}): {node_id}")
    pytest.skip("passed in isolated subprocess")
