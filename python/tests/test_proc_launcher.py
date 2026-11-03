# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Procs on a host can be launched and torn down by a Python launcher installed
with ``HostMesh.set_proc_launcher``.

Monarch's contract with a launcher is the ``ProcLauncher`` ABC: Monarch asks it
to ``launch``, ``terminate`` and ``kill`` procs, and the launcher reports each
proc's exit. How a launcher starts and stops what it launches is its own
business; these tests observe only the launcher's own record of what Monarch
asked it to do, and an environment token that only the installed launcher
sets.
"""

import asyncio
import functools
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Callable, Coroutine, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import cast, Literal, TypedDict

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._src.actor.actor_mesh import Actor, Port, shutdown_context
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.host_mesh import HostMesh
from monarch._src.actor.proc_launcher import (
    LaunchOptions,
    ProcExitResult,
    ProcLauncher,
    ProcReachability,
)
from monarch._src.actor.proc_mesh import ProcMesh
from monarch._src.job.process import ProcessJob
from monarch.actor import this_host
from monarch.config import configured
from scoped_state import scoped_state
from typing_extensions import Unpack

TOKEN_VAR = "MONARCH_TEST_LAUNCH_TOKEN"
PROC_ID_VAR = "MONARCH_TEST_LAUNCH_PROC_ID"


class _LauncherParams(TypedDict, total=False):
    """Configuration passed to the test launchers; flags may be omitted."""

    event_log: str
    cleanup_request: str
    token: str
    plain_child: bool
    fail_init: bool
    fail_on_launch: int


_EventName = Literal[
    "launch",
    "launch_failed",
    "terminate",
    "kill",
    "exit",
    "fixture_cleanup",
    "fixture_cleanup_finished",
    "cleanup_started",
    "cleanup_finished",
]


class _EventFields(TypedDict, total=False):
    """Additional log fields supplied by exit reporting and launcher cleanup."""

    exit_code: int | None
    signal: int | None
    proc_ids: list[str]
    exit_codes: dict[str, int | None]
    vsock_uds_path: str | None


class _LauncherEvent(_EventFields):
    """One launcher log record, always identifying the event and Monarch proc."""

    event: _EventName
    proc_id: str


class _LauncherCrash(BaseException):
    """Raised by the test launcher to fail its own actor."""


class _SubprocessLauncher(ProcLauncher):
    """Launch each proc as a child process of the host's process.

    ``params``:
      - ``event_log``: file to append one JSON line per request and exit to.
      - ``token``: value of ``TOKEN_VAR`` in every child's environment.
      - ``plain_child``: run a process that never becomes a proc instead of
        the proc program.
      - ``fail_init``: make the constructor raise.
      - ``fail_on_launch``: on the ``launch`` call with this number, start
        nothing and fail the launcher actor.
      - ``cleanup_request``: file the fixture creates to request child cleanup.
    """

    def __init__(self, params: _LauncherParams | None = None) -> None:
        # The public ABC accepts a plain dict; retain the typed fields in `_config`.
        super().__init__(dict(params) if params is not None else None)
        self._config: _LauncherParams = params if params is not None else {}
        self._launches = 0
        self._children: dict[str, subprocess.Popen[bytes]] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        if self._config.get("fail_init"):
            raise ValueError("launcher refused to start: fail_init is set")

    def _record(
        self, event: _EventName, proc_id: str, **fields: Unpack[_EventFields]
    ) -> None:
        with open(self._config["event_log"], "a") as log:
            log.write(json.dumps({"event": event, "proc_id": proc_id, **fields}) + "\n")

    def _child_command(
        self, opts: LaunchOptions
    ) -> tuple[list[str], str, dict[str, str]]:
        if self._config.get("plain_child"):
            argv = [sys.executable, "-c", "import time; time.sleep(3600)"]
            return argv, sys.executable, dict(os.environ)
        arg0 = opts.arg0 if opts.arg0 is not None else os.path.basename(opts.program)
        env: dict[str, str] = dict(os.environ) if opts.inherit_env else {}
        for name in opts.env_remove:
            env.pop(name, None)
        env.update(opts.env)
        env["HYPERACTOR_MESH_BOOTSTRAP_MODE"] = opts.bootstrap_payload
        env["HYPERACTOR_PROCESS_NAME"] = opts.process_name
        if opts.log_channel is not None:
            env["BOOTSTRAP_LOG_CHANNEL"] = opts.log_channel
        return [arg0, *opts.args], opts.program, env

    def _prepare_launch(
        self, proc_id: str, argv: list[str], executable: str
    ) -> tuple[list[str], str]:
        """Return the command that actually starts the child."""
        return argv, executable

    async def _release(self, proc_id: str) -> None:
        """Release what launching `proc_id` acquired, once its child exits."""

    def _start(self, coro: Coroutine[object, object, None]) -> None:
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    @endpoint(explicit_response_port=True)
    async def launch(
        self,
        exit_port: Port[ProcExitResult],
        proc_id: str,
        opts: LaunchOptions,
    ) -> None:
        self._launches += 1
        if self._launches == self._config.get("fail_on_launch"):
            self._record("launch_failed", proc_id)
            raise _LauncherCrash("injected launcher failure")
        try:
            self._record("launch", proc_id, vsock_uds_path=opts.vsock_uds_path)
            argv, executable, env = self._child_command(opts)
            env[TOKEN_VAR] = self._config["token"]
            env[PROC_ID_VAR] = proc_id
            argv, executable = self._prepare_launch(proc_id, argv, executable)
            child = subprocess.Popen(argv, executable=executable, env=env)
        except Exception as e:
            exit_port.exception(e)
            return
        self._children[proc_id] = child
        self._start(self._report_exit(exit_port, proc_id, child))

    async def _report_exit(
        self,
        exit_port: Port[ProcExitResult],
        proc_id: str,
        child: subprocess.Popen[bytes],
    ) -> None:
        cleanup_request = self._config.get("cleanup_request")
        recovering = False
        while (returncode := child.poll()) is None:
            if (
                cleanup_request is not None
                and not recovering
                and Path(cleanup_request).exists()
            ):
                self._record("fixture_cleanup", proc_id)
                child.kill()
                recovering = True
            await asyncio.sleep(0.05)
        self._children.pop(proc_id, None)
        await self._release(proc_id)
        exit_code, signal = (
            (returncode, None) if returncode >= 0 else (None, -returncode)
        )
        self._record("exit", proc_id, exit_code=exit_code, signal=signal)
        exit_port.send(
            ProcExitResult(
                exit_code=exit_code,
                signal=signal,
                core_dumped=False,
                failed_reason=None,
                stderr_tail=[],
            )
        )
        if recovering:
            self._record("fixture_cleanup_finished", proc_id)

    async def _escalate(
        self, child: subprocess.Popen[bytes], timeout_secs: float
    ) -> None:
        deadline = time.monotonic() + timeout_secs
        while child.poll() is None and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
        if child.poll() is None:
            child.kill()

    @endpoint
    async def terminate(self, proc_id: str, timeout_secs: float) -> None:
        self._record("terminate", proc_id)
        child = self._children.get(proc_id)
        if child is not None and child.poll() is None:
            child.terminate()
            self._start(self._escalate(child, timeout_secs))

    @endpoint
    async def kill(self, proc_id: str) -> None:
        self._record("kill", proc_id)
        child = self._children.get(proc_id)
        if child is not None and child.poll() is None:
            child.kill()

    async def __cleanup__(self, exc: Exception | None) -> None:
        children = list(self._children.items())
        self._record("cleanup_started", "", proc_ids=[p for p, _ in children])
        for _, child in children:
            if child.poll() is None:
                child.kill()
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=2)
            if pending:
                raise TimeoutError("launcher tasks did not stop during cleanup")
        deadline = time.monotonic() + 2
        while any(child.poll() is None for _, child in children):
            if time.monotonic() >= deadline:
                raise TimeoutError("launcher children did not exit during cleanup")
            await asyncio.sleep(0.05)
        self._children.clear()
        self._record(
            "cleanup_finished", "", exit_codes={p: c.returncode for p, c in children}
        )


# Mount these host directories read-write at the same paths in the container.
# They are shared, not copied: writes in the container change the host's files.
_CONTAINER_HOST_DIRS = ("/home", "/data", "/tmp")


def _container_prefix(token: str) -> str:
    """Prefix container names with this test's token so cleanup finds only its containers."""
    return f"monarch-proc-{token[:12]}-"


def _container_root() -> str:
    """Create and return the host directory Podman will use as the container's /.

    Prepare empty directories and links only; do not copy files or mount anything.
    The later `podman run --rootfs` mounts the host directories into this root.
    """
    root = tempfile.mkdtemp(prefix="monarch-container-root-")
    # Prepare locations for system mounts and files Podman creates at startup.
    for name in ("usr", "etc", "run", "proc", "sys", "dev"):
        os.makedirs(os.path.join(root, name))
    # Prepare /home, /data and /tmp for the host-directory mounts.
    for path in _CONTAINER_HOST_DIRS:
        os.makedirs(os.path.join(root, path.lstrip("/")))
    # Make /bin resolve through /usr/bin, and likewise for libraries and /sbin.
    # These links reach the host's files once Podman mounts /usr here.
    for name in ("lib", "lib64", "bin", "sbin"):
        os.symlink(f"usr/{name}", os.path.join(root, name))
    return root


def _remove_container_root(root: str) -> None:
    """Remove the temporary root after the container's mounts are gone.

    Use `podman unshare` so cleanup can delete files owned by container users.
    The previously mounted host directories are not deleted.
    """
    subprocess.run(["podman", "unshare", "rm", "-rf", root], check=False, timeout=60)


def _container_launch_command(name: str, root: str, command: list[str]) -> list[str]:
    """Build, but do not execute, a command using host files in a container."""
    # Mount host /usr and /etc at the same paths, read-only, for Python,
    # system libraries and configuration. Volume syntax is host:container:mode.
    volumes = ["--volume=/usr:/usr:ro", "--volume=/etc:/etc:ro"]
    # Mount existing checkout, data and temporary directories read-write.
    # Omitting the mode selects read-write; files are shared, not copied.
    volumes += [f"--volume={d}:{d}" for d in _CONTAINER_HOST_DIRS if os.path.isdir(d)]
    # The host user's session directory, often /run/user/<UID>, holds temporary
    # files and Unix sockets used to communicate with services such as systemd.
    # The environment variable gives its path; it may be unset.
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR")
    if runtime_dir:
        # Share the host user's runtime directory, including its Unix sockets.
        volumes.append(f"--volume={runtime_dir}:{runtime_dir}")
    cwd = os.getcwd()
    # Keep the caller's working directory when it uses a shared path prefix.
    workdir = [f"--workdir={cwd}"] if cwd.startswith(_CONTAINER_HOST_DIRS) else []
    return [
        "podman",
        "run",
        # Remove the container after exit; its temporary root is cleaned separately.
        "--rm",
        # Give cleanup a name by which it can find this test's container.
        f"--name={name}",
        # Share the host's network: the same addresses, ports and localhost.
        "--network=host",
        # Share the host's process list and PID numbers.
        "--pid=host",
        # Use the mounted /etc/hostname rather than generating a container file.
        "--no-hostname",
        # Likewise, retain the host's /etc/hosts name-to-address mappings.
        "--no-hosts",
        # Keep the caller's user and group IDs inside the container.
        "--userns=keep-id",
        # Do not add account entries to the read-only /etc/passwd and /etc/group.
        "--passwd=false",
        # Pass the host process's environment variables to the command.
        "--env-host",
        # Insert the directory mounts and, when selected, the working directory.
        *volumes,
        *workdir,
        # Use this temporary directory as the container's /, without an image.
        "--rootfs",
        root,
        # Run the original executable and arguments without rewriting their paths.
        *command,
    ]


def _run_podman(*args: str) -> None:
    """Run a Podman management command, such as removing a container, and wait.

    Discard its output and ignore nonzero exit codes. If it takes more than
    120 seconds, kill the command and raise `subprocess.TimeoutExpired`.
    """
    subprocess.run(
        ["podman", *args],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=120,
    )


def _containers(token: str) -> list[str]:
    """List names of this test's containers, including those that have exited.

    Select names with the test token's prefix; exclude other tests' containers.
    A Podman failure or 60-second timeout raises, rather than returning no names.
    """
    result = subprocess.run(
        ["podman", "ps", "--all", "--format={{.Names}}"],
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    prefix = _container_prefix(token)
    return [name for name in result.stdout.split() if name.startswith(prefix)]


def _podman_unavailable() -> str | None:
    """Check whether this machine can run the containers these tests need.

    Return a reason to skip the tests, or None after a successful container probe.
    The probe uses the same mounts and launch options as the proc containers.
    """
    # Reject unsupported machines before creating any temporary files.
    if sys.platform != "linux":
        return "containers need Linux"
    if shutil.which("podman") is None:
        return "podman is not installed"
    # Use a unique name so probe cleanup cannot remove another test's container.
    name = f"monarch-probe-{uuid.uuid4().hex[:12]}"
    root = _container_root()
    try:
        # Actually start a container: finding the Podman executable is not enough.
        # /bin/true exits successfully without doing work inside the container.
        result = subprocess.run(
            _container_launch_command(name, root, ["/bin/true"]),
            # Keep diagnostics for the skip reason if startup fails.
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        # A startup timeout makes these tests unavailable on this machine.
        return "rootless podman timed out starting a container"
    finally:
        # Request removal even after failure or timeout. --force removes a running
        # container, --ignore accepts an already-removed one, and --time=0 allows
        # no grace period before killing it. Then remove the temporary root.
        _run_podman("rm", "--force", "--ignore", "--time=0", name)
        _remove_container_root(root)
    if result.returncode != 0:
        # Limit the skip message to the last 500 characters of Podman's error output.
        return f"rootless podman cannot run a container here: {result.stderr.strip()[-500:]}"
    # The container started and /bin/true completed successfully.
    return None


class _PodmanLauncher(_SubprocessLauncher):
    """Launch each Monarch proc in its own rootless Podman container.

    Inherit `_SubprocessLauncher.launch()` and its exit reporting. The child
    process they start and monitor is `podman run`; the proc runs inside its
    container. Override `_prepare_launch()` to supply that command and `_release()`
    to remove its temporary root after exit. Override `terminate()` and `kill()`
    to stop the container rather than just the host-side Podman process.

    Call `super().__init__()` for configuration and child/task bookkeeping.
    During cleanup, remove containers first, then call `super().__cleanup__()`
    to stop and reap remaining Podman processes and cancel tracked tasks.
    Finally, remove any remaining temporary roots.

    Each container mounts the host's system and data directories at their usual
    paths and shares the host's network and process list. The proc command and
    its connection to the Monarch host stay unchanged. No container image is used.
    """

    def __init__(self, params: _LauncherParams | None = None) -> None:
        """Initialize the launcher and bookkeeping for the containers it starts."""
        super().__init__(params)
        # Monarch proc ID -> Podman container name, used to stop, kill or remove
        # the container belonging to that proc.
        self._containers: dict[str, str] = {}
        # Monarch proc ID -> temporary host directory used as the container's /.
        # Retain its path so cleanup can delete it after the container is removed.
        self._roots: dict[str, str] = {}

    def _prepare_launch(
        self, proc_id: str, argv: list[str], executable: str
    ) -> tuple[list[str], str]:
        """Prepare a container launch for this proc; do not start it yet.

        Record its container name and temporary root under the Monarch proc ID.
        Return `(argv, executable)` for `_SubprocessLauncher.launch()` to pass
        to `subprocess.Popen` when it starts the process.
        Podman runs on the host, and the original proc command runs inside it.
        """
        # `_config` holds the parameters passed to `hosts.set_proc_launcher()`.
        # Its `token` is the test's random identifier, used to find its containers.
        # Use the current number of tracked roots as the container name's suffix.
        name = f"{_container_prefix(self._config['token'])}{len(self._roots)}"
        root = _container_root()
        self._containers[proc_id] = name
        self._roots[proc_id] = root
        return _container_launch_command(name, root, [executable, *argv[1:]]), "podman"

    async def _release(self, proc_id: str) -> None:
        """Forget this proc's container name and remove its temporary root.

        `_SubprocessLauncher._report_exit()` calls this after `podman run` exits.
        `--rm` handles container removal; this deletes the leftover root directory.
        Run that blocking cleanup in asyncio's default thread pool.
        """
        # Forget the container name now that the Podman launch process has exited.
        self._containers.pop(proc_id, None)
        # Take its root path out of bookkeeping; no entry means no root to remove.
        root = self._roots.pop(proc_id, None)
        if root is not None:
            # Removing the root waits for a Podman command. Run it on a worker
            # thread so the actor's asyncio loop stays responsive. None selects
            # the loop's default thread pool; root is the cleanup function's argument.
            await asyncio.get_running_loop().run_in_executor(
                None, _remove_container_root, root
            )

    async def _run_podman(self, *args: str) -> None:
        """Run a Podman management command on a worker thread and await its exit.

        The module-level `_run_podman()` blocks; using asyncio's default thread
        pool keeps the actor's loop responsive while the command runs.
        """
        await asyncio.get_running_loop().run_in_executor(
            None, functools.partial(_run_podman, *args)
        )

    def _podman_in_background(self, *args: str) -> None:
        """Schedule a Podman command without waiting for it to finish.

        Inherited `_start()` tracks the asyncio task so launcher cleanup can
        cancel it. Cancelling that task does not stop a command already running
        in the thread pool.
        """
        self._start(self._run_podman(*args))

    @endpoint
    async def terminate(self, proc_id: str, timeout_secs: float) -> None:
        """Request a graceful stop of this proc's container, then return.

        Schedule `podman stop`: send the container's stop signal, then force a
        kill if it has not exited within the grace period. Round the requested
        period up to whole seconds, with a minimum of one second. An unknown
        proc ID causes no Podman command; exit reporting runs separately.
        """
        self._record("terminate", proc_id)
        name = self._containers.get(proc_id)
        if name is not None:
            grace = str(max(1, math.ceil(timeout_secs)))
            self._podman_in_background("stop", f"--time={grace}", name)

    @endpoint
    async def kill(self, proc_id: str) -> None:
        """Request an immediate kill of this proc's container, then return.

        Schedule `podman kill`, which sends SIGKILL by default. An unknown proc
        ID causes no Podman command; inherited exit reporting confirms the exit.
        """
        self._record("kill", proc_id)
        name = self._containers.get(proc_id)
        if name is not None:
            self._podman_in_background("kill", name)

    async def __cleanup__(self, exc: Exception | None) -> None:
        """Remove remaining containers, clean up their launch processes and roots.

        First await forced container removal. Then `_SubprocessLauncher` cleanup
        stops and reaps remaining `podman run` processes and cancels tracked
        asyncio tasks. Finally, delete leftover root directories. Run blocking
        Podman commands and root deletion in the thread pool.
        """
        loop = asyncio.get_running_loop()
        for name in list(self._containers.values()):
            await loop.run_in_executor(
                None, functools.partial(_run_podman, "rm", "--force", name)
            )
        await super().__cleanup__(exc)
        for root in self._roots.values():
            await loop.run_in_executor(None, _remove_container_root, root)
        self._roots.clear()


class _TokenReporter(Actor):
    """Report the environment and process hosting this test actor.

    Tests spawn these actors in procs created by the launcher. In the Podman
    tests they run inside the containers; other tests use ordinary subprocesses.
    """

    @endpoint
    async def token(self) -> str | None:
        """Read the test token the launcher put in this proc's environment."""
        return os.environ.get(TOKEN_VAR)

    @endpoint
    async def proc_id(self) -> str | None:
        """Read the Monarch proc ID supplied by the launcher, not the OS PID."""
        return os.environ.get(PROC_ID_VAR)

    @endpoint
    async def in_container(self) -> bool:
        """Check for Podman's container marker in this proc's filesystem."""
        # Check the actor's filesystem, not the launcher's.
        return Path("/run/.containerenv").exists()

    @endpoint
    async def stop_own_process_soon(self) -> int:
        """Return this proc's OS PID, then suspend the process shortly afterward.

        Delay SIGSTOP by 0.2 seconds to allow the reply to be sent first. SIGSTOP
        freezes the process rather than terminating it; tests use this to make
        a proc unresponsive and use the returned PID to check its process state.
        """
        asyncio.get_running_loop().call_later(0.2, os.kill, os.getpid(), signal.SIGSTOP)
        return os.getpid()


def _events(event_log: str) -> list[_LauncherEvent]:
    """Read the launcher log so tests can check requests and process exits.

    Read in the test process. `_SubprocessLauncher._record()` writes each event
    from the launcher actor, outside the containers it starts. `event_log` is
    the path chosen by the test for that log. Return its JSON records in file order.
    Return [] if the launcher has not created the log yet.
    """
    path = Path(event_log)
    if not path.exists():
        return []
    # `_record()` is the sole writer of these test logs; its fields define the schema.
    return [
        cast(_LauncherEvent, json.loads(line)) for line in path.read_text().splitlines()
    ]


def _launcher_params(tmp: str, **extra: Unpack[_LauncherParams]) -> _LauncherParams:
    """Prepare launcher configuration for observing and cleaning up test procs.

    Run in the test process, outside the launched containers. `tmp` is the
    temporary directory created by the test, typically under /tmp. Generate
    paths in that directory without creating the files. The test passes this
    dictionary to `hosts.set_proc_launcher()`; the launcher stores it in `_config`.
    `extra` adds or overrides parameters, such as `plain_child`.
    """
    return {
        # The launcher writes requests and exit reports here; the test reads them
        # with `_events()` to check what actually happened.
        "event_log": os.path.join(tmp, f"events-{uuid.uuid4().hex}.jsonl"),
        # The fixture creates this marker during teardown. The launcher's exit
        # monitors see it and kill remaining children so cleanup can finish.
        "cleanup_request": os.path.join(tmp, f"cleanup-{uuid.uuid4().hex}"),
        # A random test identifier, passed to child environments and used as the
        # Podman container-name prefix so cleanup selects only this test's containers.
        "token": uuid.uuid4().hex,
        **extra,
    }


@contextmanager
def _client_launcher(params: _LauncherParams) -> Iterator[HostMesh]:
    """Install the subprocess launcher on the test's own client host for this scope.

    Yield that host so the test can spawn procs through the installed launcher.
    On exit, create the cleanup marker so the launcher's exit monitors kill
    remaining children, then shut down the test's Monarch context. Request a
    two-second host shutdown budget and wait up to ten seconds for completion.
    """
    try:
        host = this_host()
        host.set_proc_launcher(_SubprocessLauncher, dict(params)).get(timeout=60)
        yield host
    finally:
        Path(params["cleanup_request"]).touch()
        shutdown_context(host_timeout=2).get(timeout=10)


def _tokens(procs: ProcMesh) -> list[str | None]:
    """Ask an actor in each proc for the test token in that proc's environment.

    Run from the test process. Spawn one `_TokenReporter` per proc, call their
    `token()` endpoints and wait up to 60 seconds for the replies. A reporter
    returns None if its proc did not receive the token.
    """
    reporters = procs.spawn("reporters", _TokenReporter)
    return list(reporters.token.call().get(timeout=60).values())


def _exited(params: _LauncherParams, proc_ids: set[str]) -> list[_LauncherEvent]:
    """Wait for the launcher to log an exit for every requested Monarch proc ID.

    Read the log from the test process, with `_wait_for_events()`'s 30-second
    deadline. Return the whole observed log, including non-exit events. This
    checks exit reporting; it does not stop the procs.
    """
    return _wait_for_events(
        params["event_log"],
        lambda ev: proc_ids <= {e["proc_id"] for e in ev if e["event"] == "exit"},
        f"an exit report for each of {sorted(proc_ids)}",
    )


def _wait_until_stopped(pid: int, timeout: float = 10.0) -> None:
    """Wait until Linux reports that this OS process is suspended, not exited.

    Read /proc/<pid>/stat from the test process every 0.05 seconds. Its state
    `T` confirms that SIGSTOP took effect before the test requests teardown.
    `pid` is an OS PID, not a Monarch proc ID. Fail the test if the deadline
    expires without seeing the suspended state.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
        if state == "T":
            return
        time.sleep(0.05)
    pytest.fail(f"process {pid} did not stop within {timeout}s")


def _wait_for_events(
    event_log: str,
    done: Callable[[list[_LauncherEvent]], bool],
    what: str,
    timeout: float = 30.0,
) -> list[_LauncherEvent]:
    """Wait for recorded launcher events to satisfy the test's condition.

    Run in the test process. Read the whole log every 0.1 seconds and pass its
    records to `done`; return that snapshot when `done(events)` is True.
    At the deadline, fail the test with `what` (the condition's description)
    and the last observed records so the failure shows what the launcher did.
    """
    deadline = time.monotonic() + timeout
    while True:
        events = _events(event_log)
        if done(events):
            return events
        if time.monotonic() >= deadline:
            pytest.fail(f"timed out waiting for {what}; launcher events: {events}")
        time.sleep(0.1)


# Python subprocess launcher tests (no Podman).
# These check ordinary child-process launching, shutdown and installation rules.
# The installation-order tests first spawn through Monarch's native launcher,
# then check that installing a Python launcher on that host is rejected.


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_launch_and_exit_are_delegated() -> None:
    token = uuid.uuid4().hex
    with tempfile.TemporaryDirectory() as tmp:
        event_log = os.path.join(tmp, "events.jsonl")
        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            hosts = state.hosts
            hosts.set_proc_launcher(
                _SubprocessLauncher, {"event_log": event_log, "token": token}
            ).get(timeout=60)

            procs = hosts.spawn_procs(per_host={"procs": 2})
            reporters = procs.spawn("reporters", _TokenReporter)
            tokens = list(reporters.token.call().get(timeout=60).values())
            assert tokens == [token, token], (
                f"every proc should carry the launcher's token, got {tokens}"
            )
            launched = {
                e["proc_id"] for e in _events(event_log) if e["event"] == "launch"
            }
            assert len(launched) == 2, f"expected two launch requests, got {launched}"

            procs.stop().get(timeout=60)
            events = _wait_for_events(
                event_log,
                lambda ev: launched
                <= {e["proc_id"] for e in ev if e["event"] == "exit"},
                "an exit report for every launched proc",
            )
            exit_codes = {
                e["proc_id"]: e["exit_code"] for e in events if e["event"] == "exit"
            }
            assert all(exit_codes[p] == 0 for p in launched), (
                f"every proc should exit cleanly after stop, got {exit_codes}"
            )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_teardown_is_delegated() -> None:
    token = uuid.uuid4().hex
    with (
        configured(host_spawn_ready_timeout="3s"),
        tempfile.TemporaryDirectory() as tmp,
    ):
        event_log = os.path.join(tmp, "events.jsonl")
        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            hosts = state.hosts
            hosts.set_proc_launcher(
                _SubprocessLauncher,
                {"event_log": event_log, "token": token, "plain_child": True},
            ).get(timeout=60)

            procs = hosts.spawn_procs(per_host={"procs": 1})
            with pytest.raises(Exception) as spawn_failure:
                procs.initialized.get(timeout=60)
            assert not isinstance(spawn_failure.value, TimeoutError), (
                "the spawn should fail on its readiness timeout, not outlast the wait"
            )

            events = _wait_for_events(
                event_log,
                lambda ev: [e["event"] for e in ev] == ["launch", "kill", "exit"],
                "launch, kill and exit for the child that never became a proc",
            )
            assert len({e["proc_id"] for e in events}) == 1, (
                f"all three events should be for the same proc, got {events}"
            )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_install_after_first_spawn_fails() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            hosts = state.hosts
            hosts.spawn_procs(per_host={"procs": 1}).initialized.get(timeout=60)
            with pytest.raises(
                Exception, match="launcher already initialized"
            ) as failure:
                hosts.set_proc_launcher(
                    _SubprocessLauncher, dict(_launcher_params(tmp))
                ).get(timeout=60)
            assert "host rank 0" in str(failure.value), str(failure.value)


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_second_install_fails_and_first_stays() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        first, second = _launcher_params(tmp), _launcher_params(tmp)
        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            hosts = state.hosts
            hosts.set_proc_launcher(_SubprocessLauncher, dict(first)).get(timeout=60)
            with pytest.raises(Exception, match="launcher already initialized"):
                hosts.set_proc_launcher(_SubprocessLauncher, dict(second)).get(
                    timeout=60
                )

            procs = hosts.spawn_procs(per_host={"procs": 1})
            assert _tokens(procs) == [first["token"]], (
                "procs should still be launched by the first launcher"
            )


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_failed_constructor_leaves_host_installable() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            hosts = state.hosts
            with pytest.raises(Exception, match="launcher refused to start") as failure:
                hosts.set_proc_launcher(
                    _SubprocessLauncher, dict(_launcher_params(tmp, fail_init=True))
                ).get(timeout=60)
            assert "host rank 0" in str(failure.value), str(failure.value)

            good = _launcher_params(tmp)
            hosts.set_proc_launcher(_SubprocessLauncher, dict(good)).get(timeout=60)
            procs = hosts.spawn_procs(per_host={"procs": 1})
            assert _tokens(procs) == [good["token"]], (
                "the failed install should leave the host installable"
            )


@pytest.mark.timeout(180)
@isolate_in_subprocess
def test_failed_launcher_does_not_hang_host() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        params = _launcher_params(tmp, fail_on_launch=2)
        job = ProcessJob({"hosts": 1})
        try:
            hosts = job.state(cached_path=None).hosts
            hosts.set_proc_launcher(_SubprocessLauncher, dict(params)).get(timeout=60)
            hosts.spawn_procs(per_host={"procs": 1}, name="first").initialized.get(
                timeout=60
            )

            started = time.monotonic()
            with pytest.raises(Exception, match="launcher actor stopped"):
                hosts.spawn_procs(per_host={"procs": 1}, name="second").initialized.get(
                    timeout=60
                )
            assert time.monotonic() - started < 20, (
                "the spawn should fail on the stopped launcher, not on the "
                "default 30s readiness timeout"
            )
            with pytest.raises(Exception, match="launcher actor stopped"):
                hosts.spawn_procs(per_host={"procs": 1}, name="third").initialized.get(
                    timeout=20
                )

            events = [e["event"] for e in _events(params["event_log"])]
            assert events.count("launch") == 1, events
            assert events.count("launch_failed") == 1, events
            hosts.shutdown().get(timeout=30)
        finally:
            # Nothing supervises the first proc once its launcher has failed;
            # it is still in the host's session, which this reaps.
            job._kill()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_terminate_routes_to_launcher() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        params = _launcher_params(tmp)
        with (
            configured(process_exit_timeout="2s"),
            scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state,
        ):
            hosts = state.hosts
            hosts.set_proc_launcher(_SubprocessLauncher, dict(params)).get(timeout=60)
            procs = hosts.spawn_procs(per_host={"procs": 2})
            reporters = procs.spawn("reporters", _TokenReporter)
            proc_ids = [
                proc_id
                for proc_id in reporters.proc_id.call().get(timeout=60).values()
                if proc_id is not None
            ]
            assert len(proc_ids) == 2, f"both procs should report their id: {proc_ids}"
            stopped = proc_ids[0]
            stopped_pid = (
                reporters.slice(procs=0)
                .stop_own_process_soon.call_one()
                .get(timeout=30)
            )
            _wait_until_stopped(stopped_pid)

            procs.stop().get(timeout=60)
            events = _exited(params, set(proc_ids))
            terminated = [e["proc_id"] for e in events if e["event"] == "terminate"]
            assert terminated == [stopped], (
                f"only the stopped proc should be terminated, got {terminated}"
            )
            order = [e["event"] for e in events if e["proc_id"] == stopped]
            assert order.index("terminate") < order.index("exit"), order


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_launches_on_client_host() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        params = _launcher_params(tmp)
        with _client_launcher(params) as host:
            procs = host.spawn_procs(per_host={"procs": 2})
            assert _tokens(procs) == [params["token"]] * 2
            launched = {
                e["proc_id"]
                for e in _events(params["event_log"])
                if e["event"] == "launch"
            }
            assert len(launched) == 2, launched

            procs.stop().get(timeout=60)
            _exited(params, launched)


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_install_error_names_only_the_failed_host() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        with scoped_state(ProcessJob({"hosts": 2}), cached_path=None) as state:
            hosts = state.hosts
            hosts.slice(hosts=0).spawn_procs(per_host={"procs": 1}).initialized.get(
                timeout=60
            )
            with pytest.raises(Exception, match="1 of 2 hosts") as failure:
                hosts.set_proc_launcher(
                    _SubprocessLauncher, dict(_launcher_params(tmp))
                ).get(timeout=60)
            message = str(failure.value)
            assert "host rank 0" in message and "host rank 1" not in message, message


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_recovery_after_bounded_wait_expires() -> None:
    with (
        configured(host_spawn_ready_timeout="60s"),
        tempfile.TemporaryDirectory() as tmp,
    ):
        params = _launcher_params(tmp, plain_child=True)
        with pytest.raises(TimeoutError) as recovery:
            with _client_launcher(params) as host:
                procs = host.spawn_procs(per_host={"procs": 1})
                _wait_for_events(
                    params["event_log"],
                    lambda ev: any(e["event"] == "launch" for e in ev),
                    "a launch before forcing the observer timeout",
                )
                started = time.monotonic()
                with pytest.raises(TimeoutError) as observer:
                    procs.initialized.get(timeout=1)
                raise observer.value
        assert recovery.value is observer.value, (
            "fixture cleanup should preserve the observer's timeout"
        )
        assert time.monotonic() - started < 15, (
            "fixture recovery should finish before the 60s startup timeout"
        )
        events = _events(params["event_log"])
        launched = {e["proc_id"] for e in events if e["event"] == "launch"}
        recovered = {e["proc_id"] for e in events if e["event"] == "fixture_cleanup"}
        finished = {
            e["proc_id"] for e in events if e["event"] == "fixture_cleanup_finished"
        }
        assert len(launched) == 1, events
        assert recovered == finished == launched, events
        exited = {e["proc_id"] for e in events if e["event"] == "exit"}
        assert launched <= exited, events


# Podman container launcher tests.
# Only the two tests below install `_PodmanLauncher`. They check successful
# launch/exit and cleanup after a startup timeout. Skip if the container probe fails.


def _wait_for_no_containers(token: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while (remaining := _containers(token)) and time.monotonic() < deadline:
        time.sleep(0.2)
    assert not remaining, f"containers outlived their procs: {remaining}"


def test_proc_launcher_reachability_defaults_to_unix() -> None:
    assert ProcLauncher.reachability is ProcReachability.UNIX
    assert _SubprocessLauncher.reachability is ProcReachability.UNIX


@pytest.mark.timeout(240)
def test_launch_and_exit_in_containers() -> None:
    if (reason := _podman_unavailable()) is not None:
        pytest.skip(reason)
    _launch_and_exit_in_containers()


@isolate_in_subprocess
def _launch_and_exit_in_containers() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        params = _launcher_params(tmp)
        try:
            with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
                hosts = state.hosts
                hosts.set_proc_launcher(_PodmanLauncher, dict(params)).get(timeout=60)

                procs = hosts.spawn_procs(per_host={"procs": 2})
                reporters = procs.spawn("reporters", _TokenReporter)
                tokens = list(reporters.token.call().get(timeout=120).values())
                assert tokens == [params["token"]] * 2, tokens
                inside = list(reporters.in_container.call().get(timeout=60).values())
                assert inside == [True, True], "every proc should run in a container"
                launched = {
                    e["proc_id"]
                    for e in _events(params["event_log"])
                    if e["event"] == "launch"
                }
                assert len(launched) == 2, launched

                procs.stop().get(timeout=60)
                events = _exited(params, launched)
                exit_codes = {
                    e["proc_id"]: e["exit_code"] for e in events if e["event"] == "exit"
                }
                assert all(exit_codes[p] == 0 for p in launched), exit_codes
            _wait_for_no_containers(params["token"])
        finally:
            for name in _containers(params["token"]):
                _run_podman("rm", "--force", name)


@pytest.mark.timeout(240)
def test_teardown_in_containers() -> None:
    if (reason := _podman_unavailable()) is not None:
        pytest.skip(reason)
    _teardown_in_containers()


@isolate_in_subprocess
def _teardown_in_containers() -> None:
    with (
        configured(host_spawn_ready_timeout="10s"),
        tempfile.TemporaryDirectory() as tmp,
    ):
        params = _launcher_params(tmp, plain_child=True)
        try:
            with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
                hosts = state.hosts
                hosts.set_proc_launcher(_PodmanLauncher, dict(params)).get(timeout=60)

                procs = hosts.spawn_procs(per_host={"procs": 1})
                with pytest.raises(Exception) as spawn_failure:
                    procs.initialized.get(timeout=60)
                assert not isinstance(spawn_failure.value, TimeoutError), (
                    "the spawn should fail on its readiness timeout, not outlast the wait"
                )
                events = _wait_for_events(
                    params["event_log"],
                    lambda ev: [e["event"] for e in ev] == ["launch", "kill", "exit"],
                    "launch, kill and exit for the container that never became a proc",
                )
                assert len({e["proc_id"] for e in events}) == 1, events
            _wait_for_no_containers(params["token"])
        finally:
            for name in _containers(params["token"]):
                _run_podman("rm", "--force", name)


class _HybridVsockLauncher(_SubprocessLauncher):
    """A subprocess launcher that declares its procs run behind a hybrid-vsock VMM."""

    reachability = ProcReachability.HYBRID_VSOCK


def test_hybrid_vsock_launch_options() -> None:
    if sys.platform != "linux":
        pytest.skip("hybrid vsock reachability is Linux only")
    _hybrid_vsock_launch_options()


@isolate_in_subprocess
def _hybrid_vsock_launch_options() -> None:
    with (
        configured(host_spawn_ready_timeout="10s"),
        tempfile.TemporaryDirectory() as tmp,
    ):
        params = _launcher_params(tmp, plain_child=True)
        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            hosts = state.hosts
            hosts.set_proc_launcher(_HybridVsockLauncher, dict(params)).get(timeout=60)

            procs = hosts.spawn_procs(per_host={"procs": 1})
            with pytest.raises(Exception) as spawn_failure:
                procs.initialized.get(timeout=60)
            assert not isinstance(spawn_failure.value, TimeoutError), (
                "the spawn should fail on its readiness timeout, not outlast the wait"
            )
            events = _wait_for_events(
                params["event_log"],
                lambda ev: [e["event"] for e in ev] == ["launch", "kill", "exit"],
                "launch, kill and exit for the program that never became a proc",
            )
            uds = events[0].get("vsock_uds_path")
            assert uds is not None and uds.endswith("/v.sock"), events[0]
            vm_dir = Path(uds).parent
            assert vm_dir.name.startswith("vm-"), uds

            deadline = time.monotonic() + 30
            while vm_dir.exists() and time.monotonic() < deadline:
                time.sleep(0.1)
            assert not vm_dir.exists(), f"{vm_dir} outlived its proc"
