# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Run a Python program in one Firecracker microVM.

`python_guest_rootfs` builds the VM's read-only root image. The image contains
the program at `GUEST_PROGRAM` and starts `fc_agent.py` as PID 1. This module
boots the VM as one `MicroVm` from `microvm.py`, tells `fc_agent.py` to run the
program, waits for its exit report and releases everything it started.

Run this file directly, as `python/firecracker.md` does:

    python_guest.py <rootfs-image-path> <program-name> [argument ...]

`rootfs-image-path` is the host path to the ext3 image. Firecracker attaches it
as the VM's read-only root disk.

The command reads two host-tool paths from environment variables:
`CHR_FC_FIRECRACKER` names the Firecracker executable, and `CHR_FC_KERNEL`
names the Linux kernel image. The rootfs is an argument because it changes
with the program; these two inputs are normally reused across runs.

Inside the VM, the program receives the supplied arguments, an empty
environment and `/` as its working directory. The command prints the guest
console and exits with the program's status.

SIGINT, SIGTERM and SIGHUP stop the run, and the VM is released before the
command exits.
"""

import argparse
import asyncio
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

from microvm import (
    built,
    cleanup,
    GUEST_PROGRAM,
    kvm_unavailable,
    MicroVm,
    MicroVmError,
    Owner,
    run_cli,
)

# Seconds for the whole run: the agent connecting, the program's exit report
# and Firecracker's exit.
RUN_TIMEOUT = 360


def _say(message: str) -> None:
    """Print a status message with the command's prefix to stderr."""
    print(f"python_guest: {message}", file=sys.stderr, flush=True)


def _console(vm: MicroVm) -> str:
    """The guest console so far, or nothing if Firecracker never wrote one."""
    if not vm.console.exists():
        return ""
    return vm.console.read_text(errors="replace")


async def _run(
    firecracker: Path, kernel: Path, rootfs: Path, argv: list[str]
) -> tuple[dict[str, object], str]:
    """Boot one microVM from `rootfs`, run its program with `argv`, and return
    the agent's exit report and the guest's console text.

    `RUN_TIMEOUT` bounds the run. Whatever happens, `cleanup` releases the VM
    and removes the run directory, after the console is read; a failed run
    prints the console first.
    """
    # Created before the first await, so a signal can't cancel the run between
    # creating the directory and the `try` that releases it.
    run_dir = Path(tempfile.mkdtemp(prefix="pyguest-", dir="/tmp"))
    vm = MicroVm(run_dir)
    owners: list[Owner] = [vm]
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + RUN_TIMEOUT
        command: dict[str, object] = {
            "executable": GUEST_PROGRAM,
            "argv": argv,
            "env": {},
            "cwd": "/",
        }
        await vm.boot(firecracker, kernel, rootfs, command)
        try:
            exit_event = await vm.exit_event(deadline - loop.time())
            await vm.wait_exit(deadline - loop.time())
        except TimeoutError:
            raise MicroVmError(
                f"the guest did not finish within {RUN_TIMEOUT} s"
            ) from None
        return exit_event, _console(vm)
    except BaseException:
        print(f"guest console:\n{_console(vm)}", flush=True)
        raise
    finally:
        await cleanup(owners, run_dir)


def main(argv: Sequence[str] | None = None) -> int:
    """Run a program in a VM built by `python_guest_rootfs`, print the guest
    console and return the program's exit status.

    Return 1 if the program was ended by a signal, the run or its cleanup
    failed, or a handled signal interrupted it.
    """
    parser = argparse.ArgumentParser(
        description="Run a Python program in a Firecracker microVM built by "
        "python_guest_rootfs."
    )
    parser.add_argument("rootfs", type=Path, help="the guest image")
    parser.add_argument(
        "argv", nargs=argparse.REMAINDER, help="the program's argv, from its name"
    )
    args = parser.parse_args(argv)
    if not args.argv:
        parser.error("give the program's argv, starting with its name")
    try:
        firecracker = built("CHR_FC_FIRECRACKER")
        kernel = built("CHR_FC_KERNEL")
        rootfs = args.rootfs.resolve()
        if not rootfs.is_file():
            raise MicroVmError(f"{rootfs} is not a built file")
        if (reason := kvm_unavailable()) is not None:
            raise MicroVmError(reason)
    except MicroVmError as exc:
        _say(str(exc))
        return 1
    result: list[tuple[dict[str, object], str]] = []

    async def scenario() -> None:
        result.append(await _run(firecracker, kernel, rootfs, args.argv))

    if run_cli(scenario, _say) != 0:
        return 1
    exit_event, console = result[0]
    print(console, end="", flush=True)
    code = exit_event.get("exit_code")
    if exit_event.get("signal") is not None or not isinstance(code, int):
        return 1
    return code


if __name__ == "__main__":
    sys.exit(main())
