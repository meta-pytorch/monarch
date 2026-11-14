# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs `hello_world.py`, unchanged, in a Firecracker microVM.

`hello_world_guest_rootfs` is a `python_guest_rootfs` image of the hello-world
program. The test runs it through `python_guest.main`, with the command line
`python/firecracker.md` gives for running your own program in a VM.
"""

import os

import microvm
import pytest
import python_guest


@pytest.mark.timeout(600)
# oss_skip: needs KVM and Buck-built Firecracker inputs.
@pytest.mark.oss_skip
def test_hello_world_in_a_microvm(capsys: pytest.CaptureFixture[str]) -> None:
    """Hello world's three nodes exchange their messages over the guest's
    loopback. `main` returns the program's exit status and prints the guest
    console."""
    if (reason := microvm.kvm_unavailable()) is not None:
        pytest.skip(reason)
    status = python_guest.main([os.environ["CHR_FC_ROOTFS"], "hello_world"])
    console = capsys.readouterr().out
    assert status == 0, console
    lines = console.splitlines()
    for expected in (
        "bob received b'wish you were here' from alice",
        "alice received b\"we're just two lost souls swimming in a fish bowl,"
        ' year after year" from bob',
    ):
        assert expected in lines, f"{expected!r} is not a console line:\n{console}"
