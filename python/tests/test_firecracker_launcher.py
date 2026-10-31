# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Python proc launchers that run each proc in a Firecracker microVM.

The launcher declares ``ProcReachability.HYBRID_VSOCK``: Monarch gives it the
VMM socket path and puts vsock addresses in the bootstrap payload, which the
launcher passes to the guest unchanged.
"""

import asyncio
import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import signal
import subprocess
import tempfile
import time
import uuid
from collections.abc import Callable, Coroutine, Iterable, Iterator
from pathlib import Path
from typing import Any

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._src.actor.actor_mesh import Actor, Port
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.proc_launcher import (
    LaunchOptions,
    ProcExitResult,
    ProcLauncher,
    ProcReachability,
)
from monarch._src.job.process import ProcessJob
from monarch.config import configured
from scoped_state import scoped_state

TOKEN_VAR = "MONARCH_TEST_LAUNCH_TOKEN"
# The guest agent's control port; Monarch reserves 10000-10002 on the same uds.
CONTROL_PORT = 9000
AGENT = Path(__file__).with_name("fc_agent.py")


def _cache() -> Path:
    return Path(os.environ.get("MONARCH_FC_CACHE", "/tmp/monarch-fc-cache"))


# Bound on one preparation command. Installation waits less than this, so an
# expired preparation surfaces as an installation timeout, and the test's
# ProcessJob recovery reclaims the command along with the host's worker.
_PREPARE_TIMEOUT_SECS = 900


def _run(*args: str) -> None:
    subprocess.run(args, check=True, capture_output=True, timeout=_PREPARE_TIMEOUT_SECS)


@contextlib.contextmanager
def _cache_lock() -> Iterator[None]:
    """Serialize cache builds across concurrent tests."""
    _cache().mkdir(parents=True, exist_ok=True)
    with open(_cache() / ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def _content_key(*parts: str, files: Iterable[Path]) -> str:
    """A cache key over `parts` and the paths and contents of `files`."""
    key = hashlib.sha256("\0".join(parts).encode())
    for path in files:
        with open(path, "rb") as f:
            key.update(f"\0{path}\0".encode())
            key.update(hashlib.file_digest(f, "sha256").digest())
    return key.hexdigest()[:12]


def _tree_files(root: Path) -> list[Path]:
    """Every file under `root`, following symlinks as `cp -rL` does."""
    return sorted(
        Path(dirpath, name)
        for dirpath, _, names in os.walk(root, followlinks=True)
        for name in names
    )


# Each fbpkg and the file this test needs from it.
_FBPKGS = {
    "vm.firecracker": "firecracker/firecracker",
    "vm.firecracker.kernel": "vmlinux",
    "vm.firecracker.rootfs": "rootfs.ext3",
}


def _artifacts() -> Path:
    """Firecracker, the guest kernel and the base rootfs, from fbpkgs."""
    root = _cache() / "fbpkg"
    with _cache_lock():
        for pkg, needed in _FBPKGS.items():
            if not (root / pkg / needed).exists():
                shutil.rmtree(root / pkg, ignore_errors=True)
                (root / pkg).mkdir(parents=True)
                _run("fbpkg", "fetch", f"{pkg}:latest", "-d", str(root / pkg))
    return root


def _rootfs(artifacts: Path) -> Path:
    """The base rootfs with the agent as init and the disks' mount points.
    Shared read-only by every VM. Cached by the agent and the base package's
    checksums file, which names its version and its files' digests."""
    pkg = artifacts / "vm.firecracker.rootfs"
    base = pkg / "rootfs.ext3"
    (checksums,) = pkg.glob("*.CHECKSUMS")
    out = _cache() / f"rootfs-{_content_key(files=[AGENT, checksums])}.ext3"
    with _cache_lock():
        if out.exists():
            return out
        tmp = out.with_suffix(".tmp")
        tmp.unlink(missing_ok=True)
        _run("cp", "--reflink=auto", str(base), str(tmp))
        for cmd in (
            "mkdir /platform",
            "mkdir /program",
            "mkdir /data",
            "mkdir /usr/local/fbcode",
            f"write {AGENT} /fc_agent.py",
            "set_inode_field /fc_agent.py mode 0100755",
        ):
            _run("debugfs", "-w", "-R", cmd, str(tmp))
        tmp.rename(out)
    return out


def _ext4_from(stage: Path, out: Path) -> None:
    size = int(
        subprocess.check_output(
            ["du", "-sb", str(stage)], timeout=_PREPARE_TIMEOUT_SECS
        ).split()[0]
    )
    tmp = out.with_suffix(".tmp")
    tmp.unlink(missing_ok=True)
    _run("truncate", "-s", str(int(size * 1.3) + (256 << 20)), str(tmp))
    _run("mkfs.ext4", "-q", "-d", str(stage), str(tmp))
    tmp.rename(out)


_PLATFORM = Path("/usr/local/fbcode/platform010")
# Loaded with dlopen, so absent from any ldd output.
_DLOPENED = ("libnss_files.so.2", "libnss_dns.so.2", "libresolv.so.2", "libgcc_s.so.1")


def _is_elf(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"\x7fELF"
    except OSError:
        return False


def _par_library_path(link_tree: Path) -> str:
    """The ``LD_LIBRARY_PATH`` the PAR's bootstrap script sets, without the
    parts it inherits from the caller's environment."""
    bootstrap = (link_tree / "_bootstrap.sh").read_text()
    line = next(
        line
        for line in bootstrap.splitlines()
        if line.startswith("export LD_LIBRARY_PATH=")
    )
    value = line.removeprefix("export LD_LIBRARY_PATH=").replace(
        "$BASE_DIR", str(link_tree)
    )
    return ":".join(
        part
        for part in value.split(":")
        if part and "$" not in part and "{" not in part
    )


def _platform_closure(program: str) -> list[Path]:
    """The platform files the proc program loads: the Python standard
    library, the dlopen'd libraries glibc needs, and every shared library the
    program's ELF files depend on, resolved by the platform loader with the
    PAR's library path."""
    par = Path(program)
    link_tree = par.with_name(par.stem + "#link-tree")
    python_lib = _PLATFORM / "lib/python3.12"
    elves = [
        path
        for root in (link_tree, python_lib / "lib-dynload")
        for path in root.rglob("*")
        if path.is_file() and _is_elf(path)
    ]
    env = {**os.environ, "LD_LIBRARY_PATH": _par_library_path(link_tree)}
    needed: set[Path] = set()
    for elf in elves:
        # The platform loader resolves dependencies as the guest will; the
        # host's ldd would resolve them against the host's own libraries.
        out = subprocess.run(
            [str(_PLATFORM / "lib/ld.so"), "--list", str(elf)],
            capture_output=True,
            text=True,
            env=env,
            timeout=_PREPARE_TIMEOUT_SECS,
        ).stdout
        for line in out.splitlines():
            for token in line.split():
                if token.startswith(str(_PLATFORM) + "/"):
                    needed.add(Path(token))
    needed.update(_PLATFORM / "lib" / name for name in _DLOPENED)
    files = {path for path in needed if path.exists()}
    files.update(path for path in python_lib.rglob("*") if path.is_file())
    return sorted(files)


def _platform_image(program: str) -> Path:
    """An ext4 image holding the platform files the proc program loads, at
    their host paths. Cached by the closure's contents."""
    closure = _platform_closure(program)
    out = _cache() / f"platform-{_content_key(files=closure)}.ext4"
    with _cache_lock():
        if out.exists():
            return out
        stage = Path(tempfile.mkdtemp(prefix="platform-", dir=_cache()))
        try:
            for path in closure:
                dest = stage / path.relative_to("/")
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, dest)
            _ext4_from(stage, out)
        finally:
            shutil.rmtree(stage, ignore_errors=True)
    return out


def _program_image(program: str, cwd: str) -> Path:
    """An ext4 image of the proc program at its host path: the PAR script, its
    link tree with symlinks resolved, and an empty directory at `cwd`, the
    working directory procs are started in. The guest mounts the image's
    /data and /home over its own, so `cwd` must be under one of them. Cached
    by the contents it copies."""
    par = Path(program)
    link_tree = par.with_name(par.stem + "#link-tree")
    key = _content_key(cwd, files=[par, *_tree_files(link_tree)])
    out = _cache() / f"program-{key}.ext4"
    with _cache_lock():
        if out.exists():
            return out
        stage = Path(tempfile.mkdtemp(prefix="program-", dir=_cache()))
        try:
            dest = stage / par.parent.relative_to("/")
            dest.mkdir(parents=True)
            _run("cp", str(par), str(dest / par.name))
            _run("cp", "-rL", str(link_tree), str(dest / link_tree.name))
            (stage / Path(cwd).relative_to("/")).mkdir(parents=True, exist_ok=True)
            _ext4_from(stage, out)
        finally:
            shutil.rmtree(stage, ignore_errors=True)
    return out


class _Vm:
    """One microVM running one proc."""

    def __init__(self, workdir: Path, uds: str) -> None:
        self.workdir = workdir
        self.uds = uds
        self.fc: asyncio.subprocess.Process | None = None
        self.control_server: asyncio.base_events.Server | None = None
        self.control: asyncio.StreamWriter | None = None
        # Set once the agent's control connection reaches EOF.
        self.control_closed = asyncio.Event()
        self.guest_exit: dict[str, Any] | None = None
        self.kernel: str | None = None

    def close(self) -> None:
        if self.control_server is not None:
            self.control_server.close()
        if self.control is not None:
            self.control.close()
        with contextlib.suppress(FileNotFoundError):
            os.unlink(f"{self.uds}_{CONTROL_PORT}")
        shutil.rmtree(self.workdir, ignore_errors=True)

    async def abort(self) -> None:
        """Release everything a boot that never completed acquired."""
        if self.fc is not None and self.fc.returncode is None:
            self.fc.kill()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self.fc.wait(), 10)
        self.close()


class _FirecrackerLauncher(ProcLauncher):
    """Launch each proc in its own Firecracker microVM.

    ``params``:
      - ``event_log``: file to append one JSON line per request and exit to.
      - ``token``: value of ``TOKEN_VAR`` in every proc's environment.
      - ``program``: the proc program, whose disk image is built up front.
      - ``plain_child``: run a process that never becomes a proc instead.
    """

    reachability = ProcReachability.HYBRID_VSOCK

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        super().__init__(params)
        self._config: dict[str, Any] = params if params is not None else {}
        self._vms: dict[str, _Vm] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._artifacts = _artifacts()
        self._rootfs = _rootfs(self._artifacts)
        # Built here, not in `launch`, so image building never counts against
        # the readiness timeout.
        self._platform = _platform_image(self._config["program"])
        self._cwd = os.getcwd()
        self._program = _program_image(self._config["program"], self._cwd)

    def _record(self, event: str, proc_id: str, **fields: object) -> None:
        with open(self._config["event_log"], "a") as log:
            log.write(json.dumps({"event": event, "proc_id": proc_id, **fields}) + "\n")

    def _start(self, coro: Coroutine[object, object, None]) -> None:
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _command(self, opts: LaunchOptions) -> dict[str, object]:
        env: dict[str, str] = dict(os.environ) if opts.inherit_env else {}
        for name in opts.env_remove:
            env.pop(name, None)
        env.update(opts.env)
        env["HYPERACTOR_MESH_BOOTSTRAP_MODE"] = opts.bootstrap_payload
        env["HYPERACTOR_PROCESS_NAME"] = opts.process_name
        if opts.log_channel is not None:
            env["BOOTSTRAP_LOG_CHANNEL"] = opts.log_channel
        env[TOKEN_VAR] = self._config["token"]
        if self._config.get("plain_child"):
            executable, argv = "/usr/bin/sleep", ["sleep", "3600"]
        else:
            arg0 = (
                opts.arg0 if opts.arg0 is not None else os.path.basename(opts.program)
            )
            executable, argv = opts.program, [arg0, *opts.args]
        return {"executable": executable, "argv": argv, "env": env, "cwd": self._cwd}

    @endpoint(explicit_response_port=True)
    async def launch(
        self, exit_port: Port[ProcExitResult], proc_id: str, opts: LaunchOptions
    ) -> None:
        self._record("launch", proc_id, vsock_uds_path=opts.vsock_uds_path)
        if opts.vsock_uds_path is None:
            error = ValueError(
                "a hybrid-vsock launcher needs LaunchOptions.vsock_uds_path"
            )
            self._record("launch_failed", proc_id, error=repr(error))
            exit_port.exception(error)
            return
        vm = _Vm(
            Path(tempfile.mkdtemp(prefix=f"fc-{self._config['token'][:12]}-")),
            opts.vsock_uds_path,
        )
        try:
            await self._boot(vm, opts)
        except BaseException as e:
            # The VM never reached `_vms`, so nothing else will release it.
            await vm.abort()
            if not isinstance(e, Exception):
                raise
            self._record("launch_failed", proc_id, error=repr(e))
            exit_port.exception(e)
            return
        self._vms[proc_id] = vm
        self._start(self._report_exit(exit_port, proc_id, vm))

    async def _boot(self, vm: _Vm, opts: LaunchOptions) -> None:
        command = self._command(opts)

        async def control(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
            vm.control = w
            try:
                w.write((json.dumps(command) + "\n").encode())
                await w.drain()
                while line := await r.readline():
                    msg = json.loads(line)
                    if msg["event"] == "started":
                        vm.kernel = msg["kernel"]
                    elif msg["event"] == "exit":
                        vm.guest_exit = msg
            finally:
                vm.control_closed.set()

        # A guest connection to (CID 2, CONTROL_PORT) arrives here.
        vm.control_server = await asyncio.start_unix_server(
            control, f"{vm.uds}_{CONTROL_PORT}"
        )
        spec = {
            "boot-source": {
                "kernel_image_path": str(
                    self._artifacts / "vm.firecracker.kernel/vmlinux"
                ),
                "boot_args": "console=ttyS0 reboot=k panic=1 pci=off quiet "
                "root=/dev/vda ro init=/fc_agent.py",
            },
            "drives": [
                {
                    "drive_id": "root",
                    "path_on_host": str(self._rootfs),
                    "is_root_device": True,
                    "is_read_only": True,
                },
                {
                    "drive_id": "platform",
                    "path_on_host": str(self._platform),
                    "is_root_device": False,
                    "is_read_only": True,
                },
                {
                    "drive_id": "program",
                    "path_on_host": str(self._program),
                    "is_root_device": False,
                    "is_read_only": True,
                },
            ],
            "machine-config": {"vcpu_count": 2, "mem_size_mib": 2048},
            "vsock": {"guest_cid": 3, "uds_path": vm.uds},
        }
        (vm.workdir / "vm.json").write_text(json.dumps(spec))
        with open(vm.workdir / "console.log", "w") as console:
            vm.fc = await asyncio.create_subprocess_exec(
                str(self._artifacts / "vm.firecracker/firecracker/firecracker"),
                "--no-api",
                "--config-file",
                str(vm.workdir / "vm.json"),
                stdin=subprocess.DEVNULL,
                stdout=console,
                stderr=subprocess.STDOUT,
            )

    async def _report_exit(
        self, exit_port: Port[ProcExitResult], proc_id: str, vm: _Vm
    ) -> None:
        assert vm.fc is not None
        vmm_rc = await vm.fc.wait()
        # The VMM can exit before its last control data is read: drain the
        # agent's connection, within a bound, before classifying the exit.
        # The agent sends nothing until it has read its command, which the
        # handler writes only after setting `vm.control`.
        if vm.control is not None:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(vm.control_closed.wait(), 10)
        self._vms.pop(proc_id, None)
        vm.close()
        guest = vm.guest_exit
        if guest is not None:
            exit_code, sig, failed = (
                guest["exit_code"],
                guest["signal"],
                guest.get("failed_reason"),
            )
        elif vmm_rc < 0:
            exit_code, sig, failed = None, -vmm_rc, None
        else:
            exit_code, sig, failed = (
                None,
                None,
                f"vm exited ({vmm_rc}) without a proc exit",
            )
        self._record(
            "exit",
            proc_id,
            exit_code=exit_code,
            signal=sig,
            failed_reason=failed,
            kernel=vm.kernel,
        )
        exit_port.send(
            ProcExitResult(
                exit_code=exit_code,
                signal=sig,
                core_dumped=False,
                failed_reason=failed,
                stderr_tail=[],
            )
        )

    async def _escalate(self, vm: _Vm, timeout_secs: float) -> None:
        assert vm.fc is not None
        try:
            await asyncio.wait_for(asyncio.shield(vm.fc.wait()), timeout_secs)
        except asyncio.TimeoutError:
            vm.fc.kill()

    @endpoint
    async def terminate(self, proc_id: str, timeout_secs: float) -> None:
        self._record("terminate", proc_id)
        vm = self._vms.get(proc_id)
        if vm is None:
            return
        if vm.control is not None:
            vm.control.write((json.dumps({"signal": signal.SIGTERM}) + "\n").encode())
        self._start(self._escalate(vm, timeout_secs))

    @endpoint
    async def kill(self, proc_id: str) -> None:
        self._record("kill", proc_id)
        vm = self._vms.get(proc_id)
        if vm is not None and vm.fc is not None and vm.fc.returncode is None:
            vm.fc.kill()

    async def __cleanup__(self, exc: Exception | None) -> None:
        for vm in list(self._vms.values()):
            if vm.fc is not None and vm.fc.returncode is None:
                vm.fc.kill()
        if self._tasks:
            await asyncio.wait(list(self._tasks), timeout=10)


class _Reporter(Actor):
    @endpoint
    async def token(self) -> str | None:
        return os.environ.get(TOKEN_VAR)

    @endpoint
    async def boot_id(self) -> str:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()


def _events(path: str) -> list[dict[str, Any]]:
    p = Path(path)
    return (
        [json.loads(line) for line in p.read_text().splitlines()] if p.exists() else []
    )


def _wait_for(
    path: str,
    done: Callable[[list[dict[str, Any]]], bool],
    what: str,
    timeout: float = 60,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout
    while not done(events := _events(path)):
        if time.monotonic() > deadline:
            pytest.fail(f"timed out waiting for {what}: {events}")
        time.sleep(0.1)
    return events


def _firecrackers(token: str) -> list[str]:
    out = subprocess.run(
        ["pgrep", "-u", str(os.getuid()), "-af", "firecracker"],
        capture_output=True,
        text=True,
    ).stdout
    marker = f"fc-{token[:12]}-"
    return [line for line in out.splitlines() if marker in line]


def _wait_for_no_firecrackers(token: str) -> None:
    deadline = time.monotonic() + 30
    while (left := _firecrackers(token)) and time.monotonic() < deadline:
        time.sleep(0.2)
    assert not left, f"microVMs outlived their procs: {left}"


def _kvm_unavailable() -> str | None:
    if not os.access("/dev/kvm", os.R_OK | os.W_OK):
        return "/dev/kvm is not usable by this user"
    return None


def _params(tmp: str, **extra: object) -> dict[str, Any]:
    return {
        "event_log": os.path.join(tmp, "events.jsonl"),
        "token": uuid.uuid4().hex,
        "program": os.environ["FB_LPAR_INVOKED_NAME"],
        **extra,
    }


def test_vm_abort_releases_partial_boot() -> None:
    """A boot that fails after acquiring its listener, work directory and VMM
    process releases all three."""

    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vm = _Vm(Path(tempfile.mkdtemp(dir=tmp)), os.path.join(tmp, "v.sock"))
            vm.control_server = await asyncio.start_unix_server(
                lambda r, w: None, f"{vm.uds}_{CONTROL_PORT}"
            )
            vm.fc = await asyncio.create_subprocess_exec("sleep", "60")
            await asyncio.wait_for(vm.abort(), 30)
            assert vm.fc.returncode is not None, "the VMM process should be reaped"
            assert not vm.workdir.exists()
            assert not os.path.exists(f"{vm.uds}_{CONTROL_PORT}")
            with pytest.raises(OSError):
                await asyncio.open_unix_connection(f"{vm.uds}_{CONTROL_PORT}")

    asyncio.run(scenario())


def _session_members(sid: int) -> list[int]:
    """The live processes in session `sid`."""
    members = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            stat = Path("/proc", entry, "stat").read_text()
        except OSError:
            continue
        # The fields after the parenthesized command name begin with the
        # state, parent, process group and session.
        state, _, _, session = stat.rpartition(")")[2].split()[:4]
        if int(session) == sid and state != "Z":
            members.append(int(entry))
    return members


@pytest.mark.timeout(300)
# oss_skip: exercises the preparation path of the VM tests, which run on
# devservers and Sandcastle only.
@pytest.mark.oss_skip
def test_expired_preparation_is_reclaimed() -> None:
    """An installation that expires while the launcher is preparing its
    artifacts leaves no process behind once the job is recovered."""
    _expired_preparation_is_reclaimed()


@isolate_in_subprocess
def _expired_preparation_is_reclaimed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # An fbpkg whose fetch never finishes.
        bin_dir = Path(tmp, "bin")
        bin_dir.mkdir()
        pidfile = Path(tmp, "fbpkg.pid")
        fbpkg = bin_dir / "fbpkg"
        fbpkg.write_text(f"#!/bin/sh\necho $$ > {pidfile}\nexec sleep 3600\n")
        fbpkg.chmod(0o755)
        os.environ["PATH"] = f"{bin_dir}:{os.environ['PATH']}"
        os.environ["MONARCH_FC_CACHE"] = os.path.join(tmp, "cache")
        session: int | None = None
        with pytest.raises(TimeoutError):
            with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
                hosts = state.hosts
                try:
                    hosts.set_proc_launcher(_FirecrackerLauncher, _params(tmp)).get(
                        timeout=60
                    )
                except TimeoutError:
                    pid = int(pidfile.read_text())
                    session = os.getsid(pid)
                    assert session != os.getsid(0), "fbpkg should run in the worker"
                    assert pid in _session_members(session), "fbpkg should be running"
                    raise
        assert session is not None
        deadline = time.monotonic() + 30
        while (left := _session_members(session)) and time.monotonic() < deadline:
            time.sleep(0.2)
        assert not left, f"processes outlived the job's recovery: {left}"


@pytest.mark.timeout(1500)
# oss_skip: needs fbpkg and KVM, available on devservers and Sandcastle only.
@pytest.mark.oss_skip
def test_launch_and_exit_in_microvms() -> None:
    if (reason := _kvm_unavailable()) is not None:
        pytest.skip(reason)
    _launch_and_exit_in_microvms()


@isolate_in_subprocess
def _launch_and_exit_in_microvms() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        params = _params(tmp)
        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            hosts = state.hosts
            hosts.set_proc_launcher(_FirecrackerLauncher, params).get(timeout=600)

            procs = hosts.spawn_procs(per_host={"procs": 2})
            reporters = procs.spawn("reporters", _Reporter)
            tokens = list(reporters.token.call().get(timeout=300).values())
            assert tokens == [params["token"]] * 2, tokens
            host_boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
            boot_ids = set(reporters.boot_id.call().get(timeout=60).values())
            assert len(boot_ids) == 2 and host_boot_id not in boot_ids, (
                f"each proc should run under its own kernel: {boot_ids}"
            )
            launched = {
                e["proc_id"]
                for e in _events(params["event_log"])
                if e["event"] == "launch"
            }
            assert len(launched) == 2, launched

            procs.stop().get(timeout=60)
            events = _wait_for(
                params["event_log"],
                lambda ev: launched
                <= {e["proc_id"] for e in ev if e["event"] == "exit"},
                "an exit report for every launched proc",
            )
            exits = {e["proc_id"]: e for e in events if e["event"] == "exit"}
            assert all(exits[p]["exit_code"] == 0 for p in launched), exits
        _wait_for_no_firecrackers(params["token"])


@pytest.mark.timeout(1200)
# oss_skip: needs fbpkg and KVM, available on devservers and Sandcastle only.
@pytest.mark.oss_skip
def test_teardown_in_microvms() -> None:
    if (reason := _kvm_unavailable()) is not None:
        pytest.skip(reason)
    _teardown_in_microvms()


@isolate_in_subprocess
def _teardown_in_microvms() -> None:
    with (
        configured(host_spawn_ready_timeout="10s"),
        tempfile.TemporaryDirectory() as tmp,
    ):
        params = _params(tmp, plain_child=True)
        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            hosts = state.hosts
            hosts.set_proc_launcher(_FirecrackerLauncher, params).get(timeout=600)
            procs = hosts.spawn_procs(per_host={"procs": 1})
            with pytest.raises(Exception) as spawn_failure:
                procs.initialized.get(timeout=120)
            assert not isinstance(spawn_failure.value, TimeoutError), (
                "the spawn should fail on its readiness timeout, not outlast the wait"
            )
            # The host reports `ReadyProcError::Timeout` for a proc that never
            # calls back (hyperactor_mesh/src/host.rs).
            assert "Timeout" in str(spawn_failure.value), spawn_failure.value
            events = _wait_for(
                params["event_log"],
                lambda ev: [e["event"] for e in ev] == ["launch", "kill", "exit"],
                "launch, kill and exit for the VM that never became a proc",
            )
            assert len({e["proc_id"] for e in events}) == 1, events
            assert events[-1]["signal"] == signal.SIGKILL, events
        _wait_for_no_firecrackers(params["token"])
