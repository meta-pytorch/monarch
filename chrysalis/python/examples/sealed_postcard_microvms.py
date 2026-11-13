# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Runs Alice, Relay and Bob with Alice and Bob in Firecracker microVMs.

Postcard connections
--------------------

Alice, Bob and Relay are roles of the same Rust `sealed-postcard` binary.
Alice and Bob each run in their own microVM. The Relay role runs on the
host, is the namespace root, and forwards their encrypted datagrams. Alice
and Bob each dial host vsock port 5001, and Firecracker delivers each
connection to Relay's Unix listener for that VM:

    VM A                          host                           VM B
    Alice                         Relay                          Bob
    dials native:2:5001  --> listens on unix:<uds_A>_5001
                             listens on unix:<uds_B>_5001 <-- dials native:2:5001

Build inputs
------------

Before running this Python program, set four environment variables to the
paths of the Buck-built files it needs:

- `CHR_FC_FIRECRACKER`: the Firecracker executable run on the host.
- `CHR_FC_KERNEL`: the Linux kernel booted inside each VM.
- `CHR_FC_ROOTFS`: the guest root filesystem containing `fc_agent.py` and
  the Rust binary.
- `SEALED_POSTCARD`: the host copy of the Rust binary, used to generate
  identities and run Relay.

These variables supply file paths to this Python program. The separate
`SEALED_POSTCARD_IDENTITY` variable supplies an identity bundle to each
Rust role, as described below.

Preparing identities
--------------------

Before starting any role, this Python program runs the Rust binary on the
host with `identities --out <run_dir>/ids`. The Rust binary creates
`alice.pem`, `relay.pem` and `bob.pem` in that directory. Each bundle contains
the role's certificate and private key, plus the shared certificate they
trust, so the roles can authenticate one another.

The Rust binary reads its identity bundle from the
`SEALED_POSTCARD_IDENTITY` environment variable.

For Relay, this Python program sets the variable when starting the binary
on the host. For Alice and Bob, it puts the variable's name and value in
the JSON startup command. `fc_agent.py` sets that environment variable
when starting the binary inside the VM.

Starting the roles
------------------

This Python program starts Relay directly on the host, then boots VM A
with Alice and VM B with Bob. Each VM starts `fc_agent.py` as its first
process.

This Python program sends `fc_agent.py` a JSON command over the control
connection described below. The command names the binary and its arguments,
including `alice` or `bob`. `fc_agent.py` starts that command inside the VM.

VM control
----------

This Python program opens one Unix-socket control listener for each VM.
Each VM's `fc_agent.py` connects to host vsock port 9000. Firecracker
bridges that connection to the corresponding host listener:

    VM A: fc_agent.py -- native:2:9000 --> Firecracker A --> <uds_A>_9000
    VM B: fc_agent.py -- native:2:9000 --> Firecracker B --> <uds_B>_9000

Over these connections, the host sends commands to start Alice or Bob
and to signal the running process. When the role exits, `fc_agent.py`
reports its exit status and reboots the VM, causing Firecracker to exit.

These control connections are separate from the postcard traffic above.
The host's side of each VM, booting, signalling and releasing it, is `MicroVm`
in `firecracker/microvm.py`.

Running the example
-------------------

The pytest suite imports this module. Built by Buck as
`//monarch/chrysalis/python/examples:sealed_postcard_microvms`, it is the
standalone demo.
The guides under `crates/chrysalis/examples/sealed-postcard/` explain:

- `firecracker.md`: prerequisites, build commands, environment variables and
  running this VM demo in tmux.
- `separate-processes.md`: running the same three roles as host processes,
  without VMs.
- `README.md`: what the postcard exchange demonstrates.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import re
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from microvm import (
    built,
    cleanup,
    CONTROL_PORT,
    GUEST_PROGRAM,
    kvm_unavailable,
    MicroVm,
    MicroVmError,
    Owner,
    run_cli,
)

# Host vsock port Alice and Bob dial; each Firecracker bridges it to Relay's
# Unix listener for that VM.
RELAY_PORT = 5001

# Maximum Unix socket pathname length in bytes, excluding the terminating NUL.
SUN_PATH_MAX = 107

# Expected Bob report, checked in VM B's console log after Firecracker exits.
BOB_REPORT = "bob served 33 streams, all from alice: true"
# Require Relay's report to say it observed at least one datagram,
# saw no postcard plaintext, and its bounded observation log dropped no datagrams.
RELAY_REPORT = (
    r"relay observed [1-9][0-9]* datagrams, "
    r"postcard plaintext seen: false, log complete: true"
)

# All timeouts below are in seconds.

# Wait for Alice to finish the exchange and report its exit.
ALICE_TIMEOUT = 300
# Wait for Bob's exit report after asking fc_agent.py to send SIGTERM.
ROLE_EXIT_TIMEOUT = 30
# Wait for Firecracker to exit after the guest role has reported its exit.
VMM_EXIT_TIMEOUT = 30
# Allow Relay to exit after SIGTERM before escalating to SIGKILL.
RELAY_STOP_TIMEOUT = 30
# After SIGKILL, wait for Relay's exit to be collected.
RELAY_KILL_TIMEOUT = 5
# Wait for the identity generator to produce all three identity bundles.
IDENTITIES_TIMEOUT = 30
# During cleanup, kill a short-lived host process and collect its exit.
CHILD_REAP_TIMEOUT = 5


class ScenarioError(Exception):
    """The run cannot start or did not complete as expected.

    The message identifies the failed check, such as a missing build input,
    a lost VM control connection, or a missing role report.
    """


@dataclass(frozen=True)
class Inputs:
    """The Buck-built programs and image one run needs."""

    # Firecracker executable started on the host to run each VM.
    firecracker: Path
    # Linux kernel booted inside each VM.
    kernel: Path
    # Guest root filesystem containing fc_agent.py and the Rust binary.
    rootfs: Path
    # The same Rust binary implements Alice, Bob and Relay. This host copy
    # generates identities and runs Relay; Alice and Bob use the guest copy.
    sealed_postcard: Path

    @classmethod
    def from_env(cls) -> Inputs:
        """Read the four build paths from this Python program's environment.

        Resolve each path and require it to name an existing file. Raise
        `ScenarioError` if a variable is unset or its path is not a file.
        """
        return cls(
            firecracker=built("CHR_FC_FIRECRACKER"),
            kernel=built("CHR_FC_KERNEL"),
            rootfs=built("CHR_FC_ROOTFS"),
            sealed_postcard=built("SEALED_POSTCARD"),
        )


def _say(message: str) -> None:
    """Print a status message with the demo's prefix and flush stdout."""
    print(f"sealed-postcard microVMs: {message}", flush=True)


class _Relay:
    """Host-side handle for the Rust Relay role, which runs directly on the host.

    A process group is a set of processes that can be signaled together.
    `start` gives Relay its own session and process group, separate from
    this Python program's group. Shutdown sends signals to Relay's group.

    Its stdout and stderr are captured in `log` for checking the Relay report
    and diagnosing failures.
    """

    def __init__(self, process: asyncio.subprocess.Process, log: Path) -> None:
        """Record the running Relay process and the file capturing its output."""
        # Handle used to signal Relay's process group and wait for its exit.
        self.process = process
        # File containing Relay's stdout and stderr, including its final report.
        self.log = log

    @classmethod
    async def start(
        cls, binary: Path, args: list[str], identity: str, log: Path
    ) -> _Relay:
        """Start the Rust binary's Relay role as a new process on the host.

        Run `binary relay` with `args`. Supply the identity bundle's contents
        through `SEALED_POSTCARD_IDENTITY`, preserving the host environment.
        Capture stdout and stderr in `log`, and give Relay its own session
        and process group for shutdown.

        Return its process handle and log path wrapped in `_Relay`. This does
        not wait for Relay to connect to Alice or exit.
        """
        with open(log, "w") as output:
            process = await asyncio.create_subprocess_exec(
                str(binary),
                "relay",
                *args,
                env={**os.environ, "SEALED_POSTCARD_IDENTITY": identity},
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        return cls(process, log)

    def _signal_group(self, sig: signal.Signals) -> None:
        """Send `sig` to Relay's process group on the host.

        `start` gives Relay a new session, making its PID also the process
        group ID. Ignore `ProcessLookupError` if that group has already exited.
        """
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.process.pid, sig)

    async def stop(self, timeout: float) -> int:
        """Ask Relay to exit, then force it to stop if it takes too long.

        Send SIGTERM to its process group and wait up to `timeout` seconds.
        If that wait expires, send SIGKILL and wait up to `RELAY_KILL_TIMEOUT`
        seconds to collect its exit. Return Relay's process exit code,
        including if it had already exited before this call.

        Raise `TimeoutError` if its exit cannot be collected after SIGKILL.
        """
        if self.process.returncode is not None:
            return self.process.returncode
        self._signal_group(signal.SIGTERM)
        try:
            return await asyncio.wait_for(self.process.wait(), timeout)
        except TimeoutError:
            pass
        self._signal_group(signal.SIGKILL)
        try:
            return await asyncio.wait_for(self.process.wait(), RELAY_KILL_TIMEOUT)
        except TimeoutError:
            raise TimeoutError(f"Relay {self.process.pid} was not reaped") from None

    async def release(self) -> None:
        """Stop Relay during cleanup, allowing `RELAY_STOP_TIMEOUT` seconds
        for SIGTERM before `stop` escalates to SIGKILL. Propagate any failure
        so `cleanup` knows Relay's exit could not be collected.
        """
        await self.stop(RELAY_STOP_TIMEOUT)


class _Child:
    """A short-lived host process, such as the identity generator."""

    def __init__(self, process: asyncio.subprocess.Process) -> None:
        """Keep an already-started host process's handle for cleanup."""
        self.process = process

    async def release(self) -> None:
        """Stop this host process and collect its exit during cleanup.

        If it has already exited, return. Otherwise send SIGKILL and wait
        up to `CHILD_REAP_TIMEOUT` seconds to collect its exit. Raise
        `TimeoutError` if that wait expires so `cleanup` preserves the run
        directory rather than treating the process as gone.
        """
        if self.process.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            self.process.kill()
        try:
            await asyncio.wait_for(self.process.wait(), CHILD_REAP_TIMEOUT)
        except TimeoutError:
            raise TimeoutError(f"process {self.process.pid} was not reaped") from None


def _guest_command(role_args: list[str], identity: str) -> dict[str, object]:
    """Build the request telling fc_agent.py to start Alice or Bob inside its VM.

    Name the guest's Rust binary, its arguments, identity bundle environment
    variable and working directory. `identity` contains the bundle's text,
    not a file path. `MicroVm.boot` sends this dictionary as JSON over the control
    connection; fc_agent.py receives it and starts the process.
    """
    return {
        "executable": GUEST_PROGRAM,
        "argv": ["sealed-postcard", *role_args],
        "env": {"SEALED_POSTCARD_IDENTITY": identity},
        "cwd": "/",
    }


async def _identities(
    inputs: Inputs, run_dir: Path, owners: list[Owner]
) -> dict[str, str]:
    """Generate Alice, Relay and Bob's identity bundles on the host.

    The Rust binary writes `alice.pem`, `relay.pem` and `bob.pem` under
    `<run_dir>/ids`. Return a dictionary mapping each role name to its
    certificate-derived Chrysalis PID: the identity peers authenticate,
    not an OS process ID. `_start_roles` supplies these IDs and bundles
    when starting the roles.
    """
    # Run the Rust binary's `identities` command as a short-lived host process.
    # It writes the bundles to disk and prints their role names and PIDs.
    process = await asyncio.create_subprocess_exec(
        str(inputs.sealed_postcard),
        "identities",
        "--out",
        str(run_dir / "ids"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Register the process before awaiting its result. If that wait times out
    # or is cancelled, `cleanup` still has its handle and can kill and reap it.
    owners.append(_Child(process))
    # Collect stdout and stderr and wait for exit, with one timeout for all three.
    out, err = await asyncio.wait_for(process.communicate(), IDENTITIES_TIMEOUT)
    if process.returncode != 0:
        raise ScenarioError(f"identities failed: {err.decode(errors='replace')}")

    # Stdout has one `<role> <pid>` line per bundle. The PEM text stays in
    # the files on disk; this dictionary contains only the role names and IDs.
    pids = dict(line.split() for line in out.decode().splitlines())
    # Require all three role names and the PID's 32-lowercase-hex-digit format.
    if set(pids) != {"alice", "relay", "bob"} or not all(
        re.fullmatch(r"[0-9a-f]{32}", pid) for pid in pids.values()
    ):
        raise ScenarioError(f"unexpected identities output: {out.decode()!r}")
    return pids


async def _start_roles(
    run_dir: Path, inputs: Inputs, owners: list[Owner]
) -> tuple[MicroVm, _Relay, MicroVm]:
    """Start Relay, then boot VM A with Alice and VM B with Bob, not waiting
    between them: children retry their parent, and Alice waits for Bob."""
    alice_vm, bob_vm = MicroVm(run_dir / "a"), MicroVm(run_dir / "b")
    # Relay's Unix listeners use `<run_dir>/a/v.sock_5001` and
    # `<run_dir>/b/v.sock_5001` on the host. Firecracker A and B connect
    # Alice's and Bob's traffic to them. Create both host directories before
    # starting Relay, rather than waiting until the VMs boot.
    alice_vm.workdir.mkdir()
    bob_vm.workdir.mkdir()
    _say("generating one identity bundle per role")
    pids = await _identities(inputs, run_dir, owners)
    ids = run_dir / "ids"

    _say("starting Relay on the host")
    # Relay is the namespace root. Listen where Firecracker A and B will
    # connect Alice's and Bob's guest-to-host traffic.
    relay_args = ["--listen", f"unix:{alice_vm.uds}_{RELAY_PORT}"]
    relay_args += ["--listen", f"unix:{bob_vm.uds}_{RELAY_PORT}"]
    relay = await _Relay.start(
        inputs.sealed_postcard,
        relay_args,
        (ids / "relay.pem").read_text(),
        run_dir / "relay.log",
    )
    owners.append(relay)

    _say(f"booting VM A with Alice, dialing the host on vsock port {RELAY_PORT}")
    owners.append(alice_vm)
    # CID 2 means the host. Firecracker A bridges this connection to Relay's
    # Unix listener above; Alice requires that peer to authenticate as Relay.
    alice = [
        "alice",
        "--parent-pid",
        pids["relay"],
        "--parent",
        f"native:2:{RELAY_PORT}",
    ]
    # Tell Alice which Chrysalis identity belongs to Bob.
    alice += ["--bob-pid", pids["bob"]]
    await alice_vm.boot(
        inputs.firecracker,
        inputs.kernel,
        inputs.rootfs,
        _guest_command(alice, (ids / "alice.pem").read_text()),
    )

    _say(f"booting VM B with Bob, dialing the host on vsock port {RELAY_PORT}")
    owners.append(bob_vm)
    # CID 2 means the host. Firecracker B bridges this connection to Relay's
    # Unix listener above; Bob requires that peer to authenticate as Relay.
    bob = ["bob", "--parent-pid", pids["relay"], "--parent", f"native:2:{RELAY_PORT}"]
    # Alice is the end-to-end peer; Relay forwards their encrypted traffic.
    bob += ["--alice-pid", pids["alice"]]
    await bob_vm.boot(
        inputs.firecracker,
        inputs.kernel,
        inputs.rootfs,
        _guest_command(bob, (ids / "bob.pem").read_text()),
    )
    return alice_vm, relay, bob_vm


async def _check_outcomes(
    run_dir: Path, inputs: Inputs, alice_vm: MicroVm, relay: _Relay, bob_vm: MicroVm
) -> None:
    """Confirm the exchange succeeded and stop the remaining roles.

    Alice exits when her exchange completes. Stop Bob and Relay afterwards
    and require successful exits and their expected reports. Wait for each
    VM's Firecracker process to exit, and reject any still running from this run.
    """
    # Alice exits on her own after the postcard and numbered streams complete.
    alice = await alice_vm.exit_event(ALICE_TIMEOUT)
    _say(f"Alice exited: {alice}")
    if alice.get("exit_code") != 0:
        raise ScenarioError(f"Alice failed: {alice}")
    # fc_agent.py reports Alice's exit, then reboots VM A so Firecracker exits.
    await alice_vm.wait_exit(VMM_EXIT_TIMEOUT)

    # Bob keeps serving until asked to stop; SIGTERM lets him print his report.
    _say("stopping Bob with SIGTERM through his guest agent")
    bob_vm.send_signal(signal.SIGTERM)
    bob = await bob_vm.exit_event(ROLE_EXIT_TIMEOUT)
    if bob.get("exit_code") != 0:
        raise ScenarioError(f"Bob failed: {bob}")
    # Bob's exit status arrives over the control connection, but his printed
    # report arrives through Firecracker's console. Wait for Firecracker B
    # to exit before reading the completed console log.
    await bob_vm.wait_exit(VMM_EXIT_TIMEOUT)
    if BOB_REPORT not in bob_vm.console.read_text(errors="replace"):
        raise ScenarioError(f"Bob's report is missing from {bob_vm.console}")
    _say(BOB_REPORT)

    # Relay runs on the host: stop and await its process before reading its log.
    _say("stopping Relay with SIGTERM")
    status = await relay.stop(RELAY_STOP_TIMEOUT)
    report = re.search(RELAY_REPORT, relay.log.read_text(errors="replace"))
    if status != 0 or report is None:
        raise ScenarioError(f"Relay exited with {status}; see {relay.log}")
    _say(report.group(0))

    # Check for any Firecracker process from this run that survived its role.
    if left := _vmms_in(run_dir, inputs.firecracker):
        raise ScenarioError(f"VMMs outlived their roles: {left}")


def _vmms_in(run_dir: Path, firecracker: Path) -> list[int]:
    """Find Firecracker processes from this run that are still present.

    `_check_outcomes` uses this as its final check for leftover VMs and fails
    the run if the returned list is nonempty. Return OS process IDs; this
    function does not signal or stop those processes.

    Inspect Linux's `/proc` for this user's processes whose executable is
    `firecracker` and whose command line names a path under `run_dir`.
    Match the executable, not a mention elsewhere in the command line: a
    tmux command can also contain the Firecracker path and run directory.
    """
    found = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            argv = [
                os.fsdecode(arg)
                for arg in (entry / "cmdline").read_bytes().split(b"\0")
            ]
            owner = entry.stat().st_uid
        except OSError:  # the process exited while being read
            continue
        if owner != os.getuid() or argv[0] != str(firecracker):
            continue
        if any(arg.startswith(f"{run_dir}/") for arg in argv[1:]):
            found.append(int(entry.name))
    return found


def _print_diagnostics(run_dir: Path) -> None:
    """Show available logs when `run_scenario` fails or is cancelled.

    `run_scenario` calls this before `cleanup` can remove the run directory.
    Print VM A's console, Relay's log and VM B's console wherever they exist;
    a partial boot may not have created all three. Ignore `OSError` from
    reading or printing a log so diagnostics do not interrupt cleanup.
    """
    for path in (
        run_dir / "a" / "console.log",
        run_dir / "relay.log",
        run_dir / "b" / "console.log",
    ):
        try:
            if path.exists():
                print(f"----- {path} -----\n{path.read_text(errors='replace')}")
        except OSError:  # an unreadable log or a closed terminal
            pass


async def run_scenario(
    run_dir: Path, inputs: Inputs, *, keep_run_dir: bool = False
) -> None:
    """Start Alice, Relay and Bob, check their outcomes, then clean up.

    `main` calls this through `run_cli` to run the exchange. The pytest test
    calls the same function directly.

    Cleanup runs after success, failure or cancellation. If the exchange did
    not succeed, print the available logs before cleanup can remove them.
    Remove `run_dir` only after every resource was released, unless
    `keep_run_dir` is set. The console demo sets it to keep logs for inspection.
    """
    owners: list[Owner] = []
    succeeded = False
    try:
        alice_vm, relay, bob_vm = await _start_roles(run_dir, inputs, owners)
        await _check_outcomes(run_dir, inputs, alice_vm, relay, bob_vm)
        succeeded = True
    finally:
        try:
            if not succeeded:
                _print_diagnostics(run_dir)
        finally:
            await cleanup(owners, run_dir, keep_run_dir=keep_run_dir)


def _check_run_dir(run_dir: Path) -> None:
    """Check `main`'s run directory before any processes are started.

    Require an existing, empty directory. The longest Unix socket path this
    run will create must fit within `SUN_PATH_MAX` bytes, including the
    directory and socket filename. Raise `ScenarioError` if a check fails.
    """
    if not run_dir.is_dir():
        raise ScenarioError(f"{run_dir} is not a directory")
    if any(run_dir.iterdir()):
        raise ScenarioError(f"{run_dir} is not empty")
    # Every socket path is `<run>/{a,b}/v.sock_<four-digit port>`, as long as this.
    longest = run_dir / "b" / f"v.sock_{CONTROL_PORT}"
    if len(os.fsencode(longest)) > SUN_PATH_MAX:
        raise ScenarioError(f"{run_dir} is too long for its Unix sockets")


def main(argv: list[str] | None = None) -> int:
    """Run the console demo, keeping its logs and VM configurations.

    Read `--run-dir` from `argv`, or from the command line when it is `None`.
    Check the build-input environment variables, run directory and KVM access
    before starting any processes. Then use `run_cli` to run the exchange
    with signal handling and cleanup. Return 0 on success or 1 on failure;
    argparse handles invalid command-line arguments separately.
    """
    parser = argparse.ArgumentParser(
        description="Run sealed-postcard with Alice and Bob in Firecracker microVMs."
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        required=True,
        help="a fresh, empty, short directory for this run; it is kept afterwards",
    )
    run_dir = parser.parse_args(argv).run_dir.resolve()
    try:
        inputs = Inputs.from_env()
        _check_run_dir(run_dir)
        if (reason := kvm_unavailable()) is not None:
            raise ScenarioError(reason)
    except (ScenarioError, MicroVmError) as exc:
        print(f"sealed-postcard microVMs: {exc}", file=sys.stderr)
        return 1
    status = run_cli(lambda: run_scenario(run_dir, inputs, keep_run_dir=True), _say)
    with contextlib.suppress(OSError):
        outcome = "succeeded" if status == 0 else "failed"
        _say(f"{outcome}; logs and VM specs are kept in {run_dir}")
    return status


if __name__ == "__main__":
    sys.exit(main())
