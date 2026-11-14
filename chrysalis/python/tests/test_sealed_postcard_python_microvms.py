# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs the Python sealed-postcard with Alice and Bob in Firecracker microVMs.

The orchestration lives in `sealed_postcard_microvms`, which runs the Rust and
the Python roles. Here Alice and Bob boot from a `python_guest_rootfs` image of
the Python program, and the Python Relay runs on the host: the run
`python/firecracker.md` walks through.
"""

import asyncio
import tempfile
from pathlib import Path

import microvm
import pytest
import sealed_postcard_microvms as microvms


@pytest.mark.timeout(900)
# oss_skip: needs KVM and Buck-built Firecracker inputs.
@pytest.mark.oss_skip
def test_sealed_postcard_python_across_microvms() -> None:
    """Python Alice and Bob exchange the postcard and 32 streams across two
    microVMs, through the Python Relay on the host."""
    if (reason := microvm.kvm_unavailable()) is not None:
        pytest.skip(reason)
    inputs = microvms.Inputs.from_env("python")
    run_dir = Path(tempfile.mkdtemp(dir="/tmp", prefix="chr-fc-"))
    asyncio.run(microvms.run_scenario(run_dir, inputs))
    assert not run_dir.exists(), "a fully released run should remove its directory"
