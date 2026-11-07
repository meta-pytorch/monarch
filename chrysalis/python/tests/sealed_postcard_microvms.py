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
host and forwards their encrypted datagrams:

    VM A                       host                            VM B
    Alice                      Relay                           Bob
    listens on vsock 5000  <-- dials hybrid:5000:<uds_A>
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

This Python program boots VM A with Alice, starts Relay directly on
the host, then boots VM B with Bob. Each VM starts `fc_agent.py` as its
first process.

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

Running the example
-------------------

The pytest suite imports this module. Run directly, it is the standalone demo.
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
import json
import os
import re
import shutil
import signal
import subprocess
import sys
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

# Host vsock port fc_agent.py connects to for commands and exit reports.
CONTROL_PORT = 9000
# Guest port Alice listens on; Relay connects through Firecracker.
ALICE_PORT = 5000
# Host vsock port Bob dials; Firecracker bridges it to Relay's Unix listener.
RELAY_PORT = 5001

# Location of the Rust binary installed in each guest's root filesystem.
GUEST_BINARY = "/usr/local/bin/sealed-postcard"
# Kernel boot options: serial console, read-only root disk, and fc_agent.py
# as the first process.
BOOT_ARGS = (
    "console=ttyS0 reboot=k panic=1 pci=off quiet root=/dev/vda ro init=/fc_agent.py"
)
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
# During cleanup, kill Firecracker and wait for its exit to be collected.
VMM_REAP_TIMEOUT = 10
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
            firecracker=_built("CHR_FC_FIRECRACKER"),
            kernel=_built("CHR_FC_KERNEL"),
            rootfs=_built("CHR_FC_ROOTFS"),
            sealed_postcard=_built("SEALED_POSTCARD"),
        )


def _built(var: str) -> Path:
    """Read a build input's path from the environment variable named by `var`.

    Return its absolute path. Raise `ScenarioError` if the variable is unset
    or the path does not name an existing file.
    """
    value = os.environ.get(var)
    if value is None:
        raise ScenarioError(f"{var} is not set")
    path = Path(value).resolve()
    if not path.is_file():
        raise ScenarioError(f"{var}={path} is not a built file")
    return path


def _kvm_unavailable() -> str | None:
    """Check access to `/dev/kvm`, the device Firecracker uses to run VMs.

    Return an explanation if this user lacks read and write access;
    otherwise return `None`. This does not attempt to start a VM.
    """
    if not os.access("/dev/kvm", os.R_OK | os.W_OK):
        return "/dev/kvm is not usable by this user"
    return None


def _say(message: str) -> None:
    """Print a status message with the demo's prefix and flush stdout."""
    print(f"sealed-postcard microVMs: {message}", flush=True)


class _Owner(Protocol):
    """A common cleanup interface for objects that own this run's processes.

    VMs, Relay and the identity generator need different shutdown steps.
    They are kept in one list so `_cleanup` can release them newest first
    after success, failure or cancellation without knowing those details.
    """

    async def release(self) -> None:
        """Stop the owned process and release its resources, or report failure."""
        ...


class _Vm:
    """Host-side state for one Firecracker VM: its files, control connection
    and process, all owned here for cleanup.
    """

    def __init__(self, workdir: Path) -> None:
        """Prepare this VM's paths and state; `_boot` starts Firecracker later."""

        # This VM's directory for its configuration, sockets and console log.
        self.workdir = workdir
        # Firecracker's host Unix socket, configured as `uds_path`.
        # Guest-to-host connections use neighboring `<uds>_<port>` sockets.
        self.uds: Path = workdir / "v.sock"
        # Firecracker's output, including the guest console and role's output.
        self.console: Path = workdir / "console.log"

        # This VM's control listener on the host, at `<workdir>/v.sock_9000`.
        # Each VM has its own listener accepting its fc_agent.py's connection.
        # `_boot` creates the listener before starting this VM's Firecracker.
        self.control_server: asyncio.Server | None = None
        # Write half of this VM's accepted control connection. This Python
        # program sends startup and signal commands to fc_agent.py through it.
        # When fc_agent.py connects, asyncio starts `_boot`'s `control`
        # coroutine as a task in this Python program's event loop on the host.
        # That task keeps the matching reader and queues reports in `events`.
        self.control: asyncio.StreamWriter | None = None
        # Queue of fc_agent.py's startup and exit reports read from that stream.
        # The reader also queues `closed` when it stops reading.
        # `_exit_event` consumes these events until it finds an exit report.
        self.events: asyncio.Queue[dict[str, object]] = asyncio.Queue()

        # Firecracker process running this VM; absent until `_boot` starts it.
        self.fc: asyncio.subprocess.Process | None = None
        # Records that release has begun, so later calls do not repeat cleanup.
        self._released = False

    @property
    def control_path(self) -> Path:
        """Where sealed_postcard_microvms.py listens for this VM's fc_agent.py.

        fc_agent.py connects to host vsock port `CONTROL_PORT`. Firecracker
        bridges that connection to this host Unix socket, at
        `<uds>_<CONTROL_PORT>`. The two programs then exchange startup and
        signal commands, and startup and exit reports, over that connection.
        """
        return Path(f"{self.uds}_{CONTROL_PORT}")

    async def wait_exit(self, timeout: float) -> int:
        """Wait up to `timeout` seconds for this VM's Firecracker process to exit.

        Return Firecracker's exit code, not the guest role's exit code.
        A timeout raises `TimeoutError` without stopping Firecracker.
        """
        assert self.fc is not None
        return await asyncio.wait_for(self.fc.wait(), timeout)

    async def release(self) -> None:
        """Stop Firecracker and release this VM's host-side control resources.

        Kill Firecracker if it is still running, then wait at most
        `VMM_REAP_TIMEOUT` seconds to collect its exit. Close the control
        connection and listener and remove the listener's socket file,
        even if stopping Firecracker fails.

        Raise `TimeoutError` if its exit could not be collected. Later calls
        do nothing, even if this attempt failed.
        """
        if self._released:
            return
        # Record the cleanup attempt before awaiting, so it cannot start twice.
        self._released = True
        try:
            reaped = await self._reap()
        finally:
            # Release the control resources even if stopping Firecracker failed.
            # `control` is the stored writer for the established connection
            # to this VM's fc_agent.py. Closing it closes that connection.
            if self.control is not None:
                self.control.close()
            # `control_server` is the host listener that accepted the
            # connection, not the connection itself. Stop accepting here.
            if self.control_server is not None:
                self.control_server.close()
            # `control_path` is the listener's Unix-socket pathname,
            # `<uds>_9000`. Remove it; partial boot may never have created it.
            with contextlib.suppress(FileNotFoundError):
                self.control_path.unlink()
        if not reaped:
            raise TimeoutError(f"the VMM for {self.workdir} was not reaped")

    async def _reap(self) -> bool:
        """Kill Firecracker if needed and wait for its exit to be collected.

        Return `True` if Firecracker was never started or its exit was
        collected within `VMM_REAP_TIMEOUT` seconds. Return `False` if that
        wait expires. Other errors propagate to the caller.
        """
        if self.fc is None:
            return True
        if self.fc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                self.fc.kill()
        try:
            await asyncio.wait_for(self.fc.wait(), VMM_REAP_TIMEOUT)
        except TimeoutError:
            return False
        return True


async def _boot(guest: _Vm, inputs: Inputs, command: dict[str, object]) -> None:
    """Open this VM's control listener, then start Firecracker on the host.

    `inputs.firecracker` is the executable. Starting it creates a new host
    OS process that boots and runs this VM using the supplied kernel and disk.

    `guest` holds the host-side state for this VM. `command` tells fc_agent.py
    which binary to start inside it, with which arguments and environment.
    Send that command when fc_agent.py connects to the listener.

    The caller must add `guest` to its cleanup list before calling this, so
    even a partial boot is cleaned up. Return after starting that host process;
    do not wait for fc_agent.py to connect or for Alice or Bob to start.
    """

    async def control(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Exchange commands and reports with this VM's fc_agent.py.

        asyncio starts this as a task on the host when the connection arrives.
        """
        # Keep the writer so `_signal` can send commands on this connection.
        # This task keeps the reader and receives fc_agent.py's reports.
        guest.control = writer
        try:
            # The first JSON line names the role's binary, arguments and
            # environment. fc_agent.py reads it and starts the role in the VM.
            writer.write((json.dumps(command) + "\n").encode())
            # Wait if needed for room in the stream's output buffer.
            await writer.drain()
            # Each following line is one JSON report from fc_agent.py.
            # Queue it for `_exit_event`, which waits for the role's exit.
            while line := await reader.readline():
                guest.events.put_nowait(json.loads(line))
        finally:
            # Tell `_exit_event` the reader has stopped, even on an error,
            # so it does not keep waiting for a report that cannot arrive.
            guest.events.put_nowait({"event": "closed"})

    # First open this VM's host Unix listener at `guest.control_path`.
    # This await waits for the listener to be ready, not for a connection.
    # Firecracker has not started yet, so this VM's fc_agent.py cannot connect.
    # Continue below to write the configuration and start Firecracker.
    # When a connection arrives later, asyncio runs `control(reader, writer)`
    # as a task, supplying that connection's read and write halves.
    guest.control_server = await asyncio.start_unix_server(
        control, str(guest.control_path)
    )

    # Firecracker reads this configuration from `vm.json` below.
    spec = {
        # Boot the supplied Linux kernel. BOOT_ARGS names fc_agent.py as
        # the VM's first process, which will start the requested Rust role.
        "boot-source": {
            "kernel_image_path": str(inputs.kernel),
            "boot_args": BOOT_ARGS,
        },
        # Both VMs use this read-only root filesystem. It contains
        # fc_agent.py and the Rust binary at GUEST_BINARY.
        "drives": [
            {
                "drive_id": "root",
                "path_on_host": str(inputs.rootfs),
                "is_root_device": True,
                "is_read_only": True,
            }
        ],
        "machine-config": {"vcpu_count": 1, "mem_size_mib": 512},
        # CID 3 identifies the guest. `uds_path` is Firecracker's host Unix
        # socket; connections to host port P go to `<uds_path>_P`.
        "vsock": {"guest_cid": 3, "uds_path": str(guest.uds)},
    }
    (guest.workdir / "vm.json").write_text(json.dumps(spec))

    # Now start a host process running Firecracker from the configuration file,
    # without an API socket. It boots the VM, which starts fc_agent.py.
    # fc_agent.py connects to the listener opened above through the vsock bridge.
    # That connection starts the `control` task, which sends the startup command;
    # fc_agent.py receives it and starts Alice or Bob inside the VM.
    # Capture Firecracker's output, including fc_agent.py's and the Rust role's
    # output, in this VM's console log. Commands use the control connection.
    with open(guest.console, "w") as console:
        guest.fc = await asyncio.create_subprocess_exec(
            str(inputs.firecracker),
            "--no-api",
            "--config-file",
            str(guest.workdir / "vm.json"),
            stdin=subprocess.DEVNULL,
            stdout=console,
            stderr=subprocess.STDOUT,
        )


async def _exit_event(guest: _Vm, timeout: float) -> dict[str, object]:
    """Let sealed_postcard_microvms.py wait for the role's exit in this VM.

    Used for Alice and Bob: each VM's fc_agent.py reports its role's exit
    over the control connection. Relay runs on the host, where this Python
    program waits for its process directly instead.

    The control task queues startup reports as well as exit reports. Skip
    startup reports and return the exit report, containing the role's exit
    code or terminating signal. This is not Firecracker's own exit status.

    Raise `ScenarioError` if the control reader stops before an exit report
    arrives, or `TimeoutError` if the whole wait exceeds `timeout` seconds.
    """

    async def first_exit() -> dict[str, object]:
        """Consume queued reports until the role exits or the control reader stops."""
        while True:
            # The control task fills this queue as reports arrive.
            # An empty queue pauses this coroutine, not the control reader.
            event = await guest.events.get()
            if event["event"] == "exit":
                return event
            if event["event"] == "closed":
                # The control task adds `closed` when it stops reading.
                # No exit report arrived before it: fail without waiting longer.
                raise ScenarioError(
                    f"the agent in {guest.workdir} closed without an exit"
                )
            # Other reports, such as `started`, do not tell us the role exited.

    # Bound the entire wait; receiving a startup report does not reset the timer.
    return await asyncio.wait_for(first_exit(), timeout)


def _signal(guest: _Vm, sig: signal.Signals) -> None:
    """Ask this VM's fc_agent.py to signal Alice or Bob.

    Write a JSON signal command over the control connection. fc_agent.py
    receives it and signals the role's process group inside the VM, not
    Firecracker on the host. This call does not wait for delivery or exit.

    Raise `ScenarioError` if fc_agent.py has not connected yet.
    """
    if guest.control is None:
        raise ScenarioError(f"the agent in {guest.workdir} is not connected")
    guest.control.write((json.dumps({"signal": int(sig)}) + "\n").encode())


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
        so `_cleanup` knows Relay's exit could not be collected.
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
        `TimeoutError` if that wait expires so `_cleanup` preserves the run
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


async def _cleanup(
    owners: Sequence[_Owner], run_dir: Path, *, keep_run_dir: bool = False
) -> None:
    """Release every owner, newest first, even when an earlier release fails.

    The run directory is removed only when every release returned and the
    caller doesn't keep it. Otherwise a process in it may still be running, so
    it stays, and the first failure is raised.
    """
    first: Exception | asyncio.CancelledError | None = None
    for owner in reversed(owners):
        try:
            await owner.release()
        except (Exception, asyncio.CancelledError) as exc:  # release the rest
            if first is None:
                first = exc
    if first is None:
        if not keep_run_dir:
            shutil.rmtree(run_dir)
        return
    if isinstance(first, asyncio.CancelledError):
        raise first
    raise RuntimeError(
        f"failed to release every process; preserved run directory {run_dir}"
    ) from first


def _guest_command(role_args: list[str], identity: str) -> dict[str, object]:
    """Build the request telling fc_agent.py to start Alice or Bob inside its VM.

    Name the guest's Rust binary, its arguments, identity bundle environment
    variable and working directory. `identity` contains the bundle's text,
    not a file path. `_boot` sends this dictionary as JSON over the control
    connection; fc_agent.py receives it and starts the process.
    """
    return {
        "executable": GUEST_BINARY,
        "argv": ["sealed-postcard", *role_args],
        "env": {"SEALED_POSTCARD_IDENTITY": identity},
        "cwd": "/",
    }


async def _identities(
    inputs: Inputs, run_dir: Path, owners: list[_Owner]
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
    # or is cancelled, `_cleanup` still has its handle and can kill and reap it.
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
    run_dir: Path, inputs: Inputs, owners: list[_Owner]
) -> tuple[_Vm, _Relay, _Vm]:
    """Boot VM A with Alice, start Relay, then boot VM B with Bob, not waiting
    between them: children retry their parents, and Alice waits for Bob."""
    alice_vm, bob_vm = _Vm(run_dir / "a"), _Vm(run_dir / "b")
    # Relay's Unix listener uses `<run_dir>/b/v.sock_5001` on the host.
    # Firecracker B connects Bob's traffic to that socket. Create B's host
    # directory before starting Relay, rather than waiting until VM B boots.
    alice_vm.workdir.mkdir()
    bob_vm.workdir.mkdir()
    _say("generating one identity bundle per role")
    pids = await _identities(inputs, run_dir, owners)
    ids = run_dir / "ids"

    _say(f"booting VM A with Alice, listening on vsock port {ALICE_PORT}")
    owners.append(alice_vm)
    # Alice listens inside VM A; 4294967295 is CID_ANY, the local wildcard CID.
    alice = ["alice", "--listen", f"native:4294967295:{ALICE_PORT}"]
    # Tell Alice which Chrysalis identity belongs to Bob.
    alice += ["--bob-pid", pids["bob"]]
    await _boot(
        alice_vm, inputs, _guest_command(alice, (ids / "alice.pem").read_text())
    )

    _say("starting Relay on the host")
    # Relay's parent is Alice, reached through Firecracker A's host Unix socket.
    relay_args = ["--parent-pid", pids["alice"]]
    relay_args += ["--parent", f"hybrid:{ALICE_PORT}:{alice_vm.uds}"]
    # Listen where Firecracker B will connect Bob's guest-to-host traffic.
    relay_args += ["--listen", f"unix:{bob_vm.uds}_{RELAY_PORT}"]
    relay = await _Relay.start(
        inputs.sealed_postcard,
        relay_args,
        (ids / "relay.pem").read_text(),
        run_dir / "relay.log",
    )
    owners.append(relay)

    _say(f"booting VM B with Bob, dialing the host on vsock port {RELAY_PORT}")
    owners.append(bob_vm)
    # CID 2 means the host. Firecracker B bridges this connection to Relay's
    # Unix listener above; Bob requires that peer to authenticate as Relay.
    bob = ["bob", "--parent-pid", pids["relay"], "--parent", f"native:2:{RELAY_PORT}"]
    # Alice is the end-to-end peer; Relay forwards their encrypted traffic.
    bob += ["--alice-pid", pids["alice"]]
    await _boot(bob_vm, inputs, _guest_command(bob, (ids / "bob.pem").read_text()))
    return alice_vm, relay, bob_vm


async def _check_outcomes(
    run_dir: Path, inputs: Inputs, alice_vm: _Vm, relay: _Relay, bob_vm: _Vm
) -> None:
    """Confirm the exchange succeeded and stop the remaining roles.

    Alice exits when her exchange completes. Stop Bob and Relay afterwards
    and require successful exits and their expected reports. Wait for each
    VM's Firecracker process to exit, and reject any still running from this run.
    """
    # Alice exits on her own after the postcard and numbered streams complete.
    alice = await _exit_event(alice_vm, ALICE_TIMEOUT)
    _say(f"Alice exited: {alice}")
    if alice.get("exit_code") != 0:
        raise ScenarioError(f"Alice failed: {alice}")
    # fc_agent.py reports Alice's exit, then reboots VM A so Firecracker exits.
    await alice_vm.wait_exit(VMM_EXIT_TIMEOUT)

    # Bob keeps serving until asked to stop; SIGTERM lets him print his report.
    _say("stopping Bob with SIGTERM through his guest agent")
    _signal(bob_vm, signal.SIGTERM)
    bob = await _exit_event(bob_vm, ROLE_EXIT_TIMEOUT)
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

    `run_scenario` calls this before `_cleanup` can remove the run directory.
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
    owners: list[_Owner] = []
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
            await _cleanup(owners, run_dir, keep_run_dir=keep_run_dir)


async def run_with_signals(scenario: Callable[[], Awaitable[None]]) -> None:
    """Let the console demo clean up its run when interrupted.

    `run_cli` calls this inside `asyncio.run`. Normally `scenario` creates
    the `run_scenario` coroutine; cleanup tests can supply a stand-in run.

    The first SIGINT, SIGTERM or SIGHUP cancels the run, letting
    `run_scenario` enter its `finally` block and release its processes.
    Ignore later signals so another cancellation cannot interrupt those
    cleanup waits. Remove the signal handlers when the run ends.
    """
    loop = asyncio.get_running_loop()
    # Run the scenario as a task that the signal handler can cancel.
    task = asyncio.ensure_future(scenario())
    requested = False

    def request_stop(signum: int) -> None:
        """Request cancellation once, leaving subsequent cleanup uninterrupted."""
        nonlocal requested
        # The same handler serves all three signals; only the first cancels.
        if requested:
            return
        requested = True
        task.cancel()
        with contextlib.suppress(OSError):  # SIGHUP may have closed the terminal
            _say(f"received {signal.Signals(signum).name}; releasing every process")

    # The scenario task doesn't run until this coroutine awaits, so the
    # handlers are installed before it acquires anything.
    signals = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    for signum in signals:
        loop.add_signal_handler(signum, request_stop, signum)
    try:
        # Wait for the run, including its cleanup. Errors and cancellation
        # propagate to `run_cli`, which reports a failing process exit status.
        await task
    finally:
        # Remove our handlers whether the run succeeded, failed or was cancelled.
        for signum in signals:
            loop.remove_signal_handler(signum)


def run_cli(scenario: Callable[[], Awaitable[None]]) -> int:
    """Run `scenario` under `run_with_signals` and return a process exit status."""
    try:
        asyncio.run(run_with_signals(scenario))
    except asyncio.CancelledError:
        with contextlib.suppress(OSError):
            _say("interrupted")
        return 1
    except Exception as exc:  # the process boundary: report any failure as status 1
        with contextlib.suppress(OSError):
            _say(f"failed: {exc}")
        return 1
    return 0


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
        if (reason := _kvm_unavailable()) is not None:
            raise ScenarioError(reason)
    except ScenarioError as exc:
        print(f"sealed-postcard microVMs: {exc}", file=sys.stderr)
        return 1
    status = run_cli(lambda: run_scenario(run_dir, inputs, keep_run_dir=True))
    with contextlib.suppress(OSError):
        outcome = "succeeded" if status == 0 else "failed"
        _say(f"{outcome}; logs and VM specs are kept in {run_dir}")
    return status


if __name__ == "__main__":
    sys.exit(main())
