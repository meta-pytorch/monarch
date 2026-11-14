# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Host-side lifecycle for Firecracker guests managed by `fc_agent.py`.

`MicroVm` owns one VM's Firecracker process, control listener and agent
connection. It boots a read-only root image built by `guest_rootfs.bzl`, sends
the agent one program command, receives its reports, forwards signals to the
guest program and stops the VM.

`cleanup` stops the VMs and host processes registered by a run. It removes the
run directory only after all of them have stopped.

`run_cli` is the wrapper for command-line programs that manage VMs. The caller
gives it the operation to run; it returns 0 on success and 1 on failure or
interruption. Ctrl-C, SIGTERM or SIGHUP stops the operation so its cleanup can
finish before `run_cli` returns.

For example, `sealed_postcard_microvms.py` gives `run_cli` the operation that
starts Relay, boots Alice and Bob's VMs, checks their exchange and cleans up.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import signal
import subprocess
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Protocol

# Host vsock port fc_agent.py connects to for commands and exit reports.
CONTROL_PORT = 9000
# Where every guest image built by `guest_rootfs.bzl` holds its program.
GUEST_PROGRAM = "/usr/local/bin/guest-program"
# Kernel boot options: serial console, read-only root disk, and fc_agent.py
# as the first process.
BOOT_ARGS = (
    "console=ttyS0 reboot=k panic=1 pci=off quiet root=/dev/vda ro init=/fc_agent.py"
)
# During cleanup, kill Firecracker and wait at most this many seconds for its
# exit to be collected.
VMM_REAP_TIMEOUT = 10


class MicroVmError(Exception):
    """A VM, its agent or a build input failed in a way the caller reports."""


def built(var: str) -> Path:
    """Read a build input's path from the environment variable named by `var`.

    Return its absolute path. Raise `MicroVmError` if the variable is unset
    or the path does not name an existing file.
    """
    value = os.environ.get(var)
    if value is None:
        raise MicroVmError(f"{var} is not set")
    path = Path(value).resolve()
    if not path.is_file():
        raise MicroVmError(f"{var}={path} is not a built file")
    return path


def kvm_unavailable() -> str | None:
    """Check access to `/dev/kvm`, the device Firecracker uses to run VMs.

    Return an explanation if this user lacks read and write access;
    otherwise return `None`. This does not attempt to start a VM.
    """
    if not os.access("/dev/kvm", os.R_OK | os.W_OK):
        return "/dev/kvm is not usable by this user"
    return None


class Owner(Protocol):
    """A common cleanup interface for objects that own this run's processes.

    VMs and host processes need different shutdown steps. They are kept
    in one list so `cleanup` can release them newest first
    after success, failure or cancellation without knowing those details.
    """

    async def release(self) -> None:
        """Stop the owned process and release its resources, or report failure."""
        ...


class MicroVm:
    """Host-side state for one Firecracker VM: its files, control connection
    and process, all owned here for cleanup.
    """

    def __init__(self, workdir: Path) -> None:
        """Prepare this VM's paths and state; `boot` starts Firecracker later."""

        # This VM's directory for its configuration, sockets and console log.
        self.workdir = workdir
        # Firecracker's host Unix socket, configured as `uds_path`.
        # Guest-to-host connections use neighboring `<uds>_<port>` sockets.
        self.uds: Path = workdir / "v.sock"
        # Firecracker's output, including the guest console and program's output.
        self.console: Path = workdir / "console.log"

        # This VM's control listener on the host, at `<workdir>/v.sock_9000`.
        # Each VM has its own listener accepting its fc_agent.py's connection.
        # `boot` creates the listener before starting this VM's Firecracker.
        self.control_server: asyncio.Server | None = None
        # Write half of this VM's accepted control connection. This Python
        # program sends startup and signal commands to fc_agent.py through it.
        # When fc_agent.py connects, asyncio starts `boot`'s `control`
        # coroutine as a task in this Python program's event loop on the host.
        # That task keeps the matching reader and queues reports in `events`.
        self.control: asyncio.StreamWriter | None = None
        # Queue of fc_agent.py's startup and exit reports read from that stream.
        # The reader also queues `closed` when it stops reading.
        # `exit_event` consumes these events until it finds an exit report.
        self.events: asyncio.Queue[dict[str, object]] = asyncio.Queue()

        # Firecracker process running this VM; absent until `boot` starts it.
        self.fc: asyncio.subprocess.Process | None = None
        # Records that release has begun, so later calls do not repeat cleanup.
        self._released = False

    @property
    def control_path(self) -> Path:
        """Where the host listens for this VM's fc_agent.py.

        fc_agent.py connects to host vsock port `CONTROL_PORT`. Firecracker
        bridges that connection to this host Unix socket, at
        `<uds>_<CONTROL_PORT>`. The two programs then exchange startup and
        signal commands, and startup and exit reports, over that connection.
        """
        return Path(f"{self.uds}_{CONTROL_PORT}")

    async def wait_exit(self, timeout: float) -> int:
        """Wait up to `timeout` seconds for this VM's Firecracker process to exit.

        Return Firecracker's exit code, not the guest program's exit code.
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

    async def boot(
        self, firecracker: Path, kernel: Path, rootfs: Path, command: dict[str, object]
    ) -> None:
        """Open this VM's control listener, then start Firecracker on the host.

        `firecracker` is the executable. Starting it creates a new host OS
        process that boots and runs this VM from `kernel` and the guest image
        `rootfs`.

        `command` tells fc_agent.py which program to start inside it, with which
        arguments and environment. Send that command when fc_agent.py connects
        to the listener.

        The caller must add this VM to its cleanup list before calling this, so
        even a partial boot is cleaned up. Return after starting that host process;
        do not wait for fc_agent.py to connect or for the program to start.
        """

        async def control(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            """Exchange commands and reports with this VM's fc_agent.py.

            asyncio starts this as a task on the host when the connection arrives.
            """
            # Keep the writer so `send_signal` can send commands on this connection.
            # This task keeps the reader and receives fc_agent.py's reports.
            self.control = writer
            try:
                # The first JSON line names the program, its arguments and
                # environment. fc_agent.py reads it and starts it in the VM.
                writer.write((json.dumps(command) + "\n").encode())
                # Wait if needed for room in the stream's output buffer.
                await writer.drain()
                # Each following line is one JSON report from fc_agent.py.
                # Queue it for `exit_event`, which waits for the program's exit.
                while line := await reader.readline():
                    self.events.put_nowait(json.loads(line))
            finally:
                # Tell `exit_event` the reader has stopped, even on an error,
                # so it does not keep waiting for a report that cannot arrive.
                self.events.put_nowait({"event": "closed"})

        # First open this VM's host Unix listener at `self.control_path`.
        # This await waits for the listener to be ready, not for a connection.
        # Firecracker has not started yet, so this VM's fc_agent.py cannot connect.
        # Continue below to write the configuration and start Firecracker.
        # When a connection arrives later, asyncio runs `control(reader, writer)`
        # as a task, supplying that connection's read and write halves.
        self.control_server = await asyncio.start_unix_server(
            control, str(self.control_path)
        )

        # Firecracker reads this configuration from `vm.json` below.
        spec = {
            # Boot the supplied Linux kernel. BOOT_ARGS names fc_agent.py as
            # the VM's first process, which will start the requested program.
            "boot-source": {
                "kernel_image_path": str(kernel),
                "boot_args": BOOT_ARGS,
            },
            # The guest image, read-only. It contains fc_agent.py and the
            # program at GUEST_PROGRAM.
            "drives": [
                {
                    "drive_id": "root",
                    "path_on_host": str(rootfs),
                    "is_root_device": True,
                    "is_read_only": True,
                }
            ],
            "machine-config": {"vcpu_count": 1, "mem_size_mib": 512},
            # CID 3 identifies the guest. `uds_path` is Firecracker's host Unix
            # socket; connections to host port P go to `<uds_path>_P`.
            "vsock": {"guest_cid": 3, "uds_path": str(self.uds)},
        }
        (self.workdir / "vm.json").write_text(json.dumps(spec))

        # Now start a host process running Firecracker from the configuration file,
        # without an API socket. It boots the VM, which starts fc_agent.py.
        # fc_agent.py connects to the listener opened above through the vsock bridge.
        # That connection starts the `control` task, which sends the startup command;
        # fc_agent.py receives it and starts the program inside the VM.
        # Capture Firecracker's output, including fc_agent.py's and the program's
        # output, in this VM's console log. Commands use the control connection.
        with open(self.console, "w") as console:
            self.fc = await asyncio.create_subprocess_exec(
                str(firecracker),
                "--no-api",
                "--config-file",
                str(self.workdir / "vm.json"),
                stdin=subprocess.DEVNULL,
                stdout=console,
                stderr=subprocess.STDOUT,
            )

    async def exit_event(self, timeout: float) -> dict[str, object]:
        """Wait for the exit of the program fc_agent.py started in this VM.

        fc_agent.py reports the program's exit over the control connection.
        The control task queues startup reports as well as exit reports. Skip
        startup reports and return the exit report, containing the program's
        exit code or terminating signal. This is not Firecracker's own exit
        status.

        Raise `MicroVmError` if the control reader stops before an exit report
        arrives, or `TimeoutError` if the whole wait exceeds `timeout` seconds.
        """

        async def first_exit() -> dict[str, object]:
            """Consume queued reports until the program exits or the reader stops."""
            while True:
                # The control task fills this queue as reports arrive.
                # An empty queue pauses this coroutine, not the control reader.
                event = await self.events.get()
                if event["event"] == "exit":
                    return event
                if event["event"] == "closed":
                    # The control task adds `closed` when it stops reading.
                    # No exit report arrived before it: fail without waiting longer.
                    raise MicroVmError(
                        f"the agent in {self.workdir} closed without an exit"
                    )
                # Other reports, such as `started`, do not tell us the program exited.

        # Bound the entire wait; receiving a startup report does not reset the timer.
        return await asyncio.wait_for(first_exit(), timeout)

    def send_signal(self, sig: signal.Signals) -> None:
        """Ask this VM's fc_agent.py to signal the program it started.

        Write a JSON signal command over the control connection. fc_agent.py
        receives it and signals the program's process group inside the VM, not
        Firecracker on the host. This call does not wait for delivery or exit.

        Raise `MicroVmError` if fc_agent.py has not connected yet.
        """
        if self.control is None:
            raise MicroVmError(f"the agent in {self.workdir} is not connected")
        self.control.write((json.dumps({"signal": int(sig)}) + "\n").encode())


async def cleanup(
    owners: Sequence[Owner], run_dir: Path, *, keep_run_dir: bool = False
) -> None:
    """Release every owner, newest first, even when an earlier release fails.

    The run directory is removed only when every release returned and the
    caller doesn't keep it. Otherwise a process in it may still be running, so
    it stays, and the first failure is raised.

    Cancelling the caller doesn't interrupt the releases. They finish first;
    then the cancellation is raised, or the failure if a release failed.
    """
    releases = asyncio.ensure_future(_release_all(owners, run_dir, keep_run_dir))
    try:
        await asyncio.shield(releases)
    except asyncio.CancelledError:
        await releases
        raise


async def _release_all(
    owners: Sequence[Owner], run_dir: Path, keep_run_dir: bool
) -> None:
    """`cleanup`'s releases, in a task that cancelling its caller can't reach."""
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


async def run_with_signals(
    scenario: Callable[[], Awaitable[None]], say: Callable[[str], None]
) -> None:
    """Let a command-line run clean up when interrupted.

    `run_cli` calls this inside `asyncio.run`. `scenario` creates the run's
    coroutine, which releases its owners in a `finally` block; `say` prints
    status messages.

    The first SIGINT, SIGTERM or SIGHUP cancels the run, letting the
    scenario enter its `finally` block and release its processes.
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
            say(f"received {signal.Signals(signum).name}; releasing every process")

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


def run_cli(scenario: Callable[[], Awaitable[None]], say: Callable[[str], None]) -> int:
    """Run `scenario` under `run_with_signals` and return a process exit status."""
    try:
        asyncio.run(run_with_signals(scenario, say))
    except asyncio.CancelledError:
        with contextlib.suppress(OSError):
            say("interrupted")
        return 1
    except Exception as exc:  # the process boundary: report any failure as status 1
        with contextlib.suppress(OSError):
            say(f"failed: {exc}")
        return 1
    return 0
