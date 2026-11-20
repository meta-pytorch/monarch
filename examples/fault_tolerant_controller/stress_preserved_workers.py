# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Repeat the real-process preservation example and optional negative probes."""

import argparse
import os
import signal
import subprocess
import sys
from pathlib import Path


def run(seed: int, injection: str = "") -> None:
    """Check exit status, result evidence, and cleanup within a hard deadline."""
    command = [
        sys.executable,
        str(Path(__file__).with_name("preserved_workers.py")),
        "--seed",
        str(seed),
        "--timeout",
        "90",
    ]
    if injection:
        command += ["--inject", injection]
    if injection == "hang":
        command += ["--generation-timeout", "1"]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        output, _ = process.communicate(timeout=150)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        output, _ = process.communicate()
        print(output, end="", flush=True)
        raise TimeoutError(f"seed {seed}: hard integration deadline") from None
    print(output, end="", flush=True)
    expected_success = not injection
    if (process.returncode == 0) != expected_success:
        raise AssertionError(
            f"seed {seed}, injection {injection!r}: exit {process.returncode}"
        )
    if ('"result": "PASS"' in output) != expected_success:
        raise AssertionError("incorrect result marker")
    reasons = {
        "hang": "TimeoutError: final controller deadline exceeded",
        "skip-worker-fault": "TimeoutError: missing worker supervision",
    }
    if injection and reasons[injection] not in output:
        raise AssertionError("negative probe failed for an unintended reason")
    if "cleanup complete" not in output:
        raise AssertionError("cleanup was not verified")


def main() -> None:
    """Run several deterministic kill schedules and optional failure-path checks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", type=int, nargs="+", default=[7, 11, 29])
    parser.add_argument("--negative", action="store_true")
    args = parser.parse_args()
    for seed in args.seeds:
        run(seed)
    if args.negative:
        run(args.seeds[0], "hang")
        run(args.seeds[0], "skip-worker-fault")
    print("stress checks passed", flush=True)


if __name__ == "__main__":
    main()
