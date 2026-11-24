# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Checkpoint fidelity and subprocess-level controller subtree recovery tests."""

import os
import pickle
import random
import re
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import fault_tolerant_controller as example
import pytest


def test_complete_checkpoint_replays_gaussian_cache(tmp_path: Path) -> None:
    """Recovery must preserve the cached sample as well as the base RNG state."""
    checkpoint = example.reference_checkpoint(3)
    assert all(state[2] is not None for state in checkpoint.rng_states)
    path = tmp_path / "checkpoint.pkl"
    example.atomic_write(path, checkpoint)
    restored = example.read_checkpoint(path)
    assert restored == checkpoint
    rngs = [random.Random() for _ in restored.rng_states]
    for rng, state in zip(rngs, restored.rng_states, strict=True):
        rng.setstate(state)
    weight = restored.weight
    for _ in range(restored.step, 10):
        weight = min(
            [rng.gauss(weight, 3.0) for rng in rngs],
            key=lambda sample: abs(sample - example.TARGET),
        )
    assert example.Checkpoint(
        10, weight, tuple(rng.getstate() for rng in rngs)
    ) == example.reference_checkpoint(10)


def test_failed_atomic_write_preserves_previous_checkpoint(tmp_path: Path) -> None:
    """A failed write may leave a pending file but cannot replace committed state."""
    path = tmp_path / "checkpoint.pkl"
    checkpoint = example.reference_checkpoint(2)
    example.atomic_write(path, checkpoint)
    with (
        patch.object(example.pickle, "dump", side_effect=OSError("disk full")),
        pytest.raises(OSError, match="disk full"),
    ):
        example.atomic_write(path, example.reference_checkpoint(3))
    assert example.read_checkpoint(path) == checkpoint


def test_rejects_partial_checkpoint(tmp_path: Path) -> None:
    """A checkpoint without all generator states is not resumable."""
    path = tmp_path / "checkpoint.pkl"
    example.atomic_write(path, example.Checkpoint(2, 1.0, ()))
    with pytest.raises(ValueError, match="incomplete checkpoint"):
        example.read_checkpoint(path)
    path.write_bytes(b"truncated pickle")
    with pytest.raises(pickle.UnpicklingError):
        example.read_checkpoint(path)


def test_process_identity_rejects_pid_reuse() -> None:
    """A reused pid must not be mistaken for the old worker during cleanup."""
    identity = example.ProcessIdentity(123, 456)
    with patch.object(
        example, "process_identity", return_value=example.ProcessIdentity(123, 789)
    ):
        assert not example.is_alive(identity)


def test_wait_for_exit_never_kills_workers() -> None:
    """Failure to observe natural exit must fail, without forcing success."""
    with (
        patch.object(example, "is_alive", return_value=True),
        patch.object(example.os, "kill") as kill,
    ):
        with pytest.raises(TimeoutError, match="workers survived"):
            example.wait_for_exit([example.ProcessIdentity(123, 456)], timeout=0)
        kill.assert_not_called()


@pytest.mark.parametrize(
    ("arguments", "success", "expected"),
    [
        (["--restarts", "0", "--steps", "3"], True, "PASS: 0 recoveries"),
        (["--restarts", "2", "--steps", "8"], True, "PASS: 2 recoveries"),
        (
            ["--restarts", "2", "--steps", "10", "--random-kills", "--seed", "7"],
            True,
            "PASS: 2 recoveries",
        ),
        (
            ["--restarts", "2", "--steps", "10", "--random-kills", "--seed", "19"],
            True,
            "PASS: 2 recoveries",
        ),
        (
            ["--inject", "checkpoint"],
            False,
            "final state differs from uninterrupted reference",
        ),
        (
            ["--restarts", "0", "--inject", "hang", "--controller-timeout", "10"],
            False,
            "exceeded its deadline",
        ),
    ],
)
def test_controller_recovery(
    arguments: list[str], success: bool, expected: str
) -> None:
    """Check real SIGKILL recovery, timeout, checkpoint rejection, and cleanup."""
    script = Path(example.__file__).resolve()
    environment = os.environ.copy()
    result = subprocess.run(
        [sys.executable, str(script), *arguments],
        capture_output=True,
        check=False,
        text=True,
        env=environment,
        timeout=180,
    )
    output = result.stdout + result.stderr
    assert (result.returncode == 0) == success, output
    assert expected in output, output
    marker = re.search(r"\[keeper\] pid \d+, run ([a-f0-9]+)", output)
    assert marker is not None, output
    assert example.run_processes(marker.group(1)) == [], output
    if success:
        assert "emergency cleanup" not in output, output
        if (
            "--restarts" in arguments
            and arguments[arguments.index("--restarts") + 1] == "2"
        ):
            for generation in (1, 2):
                assert output.index(
                    f"all generation {generation} workers exited naturally"
                ) < output.index(f"joined generation {generation + 1}"), output
