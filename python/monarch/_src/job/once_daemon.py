# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import fcntl
import os
import pickle
import signal
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass


class _Shutdown:
    """Sentinel pickled over the socket to request process shutdown."""


@dataclass
class _LockRecord:
    config_bytes: bytes
    pid: int
    socket_path: str


class OnceDaemon:
    """Handle to a background daemon that runs at most once per lock path.

    Obtain via :func:`OnceDaemon.create` or :func:`find_daemon`.

    The process protocol: each connection receives a single pickled message.
    Any object triggers a refresh and gets a pickled response back.
    A :class:`_Shutdown` sentinel asks the process to exit.

    The subprocess is launched as ``subprocess_args + [socket_path, str(lock_fd)]``.
    It must bind the socket immediately to signal readiness.
    ``lock_fd`` is an inherited fd the process must keep open (holds the flock).
    The lock file is written entirely by :meth:`create`. The process's stdout and
    stderr are appended to :func:`daemon_log_path` of the lock path.
    """

    def __init__(self, socket_path: str, pid: int) -> None:
        self._socket_path = socket_path
        self._pid = pid
        self._conn: "socket.socket | None" = None
        self._file: "object | None" = None

    @classmethod
    def create(
        cls,
        lock_path: str,
        config_key: object,
        subprocess_args: "list[str]",
        env: "dict[str, str] | None" = None,
    ) -> "OnceDaemon":
        """Ensure a background process is running with the given config.

        - If no process is running, launch one.
        - If a process is running with the same config_key, reuse it.
        - If a process is running with a different config_key, shut it down
          and relaunch.
        """
        # @lint-ignore PYTHONPICKLEISBAD
        new_key_bytes = pickle.dumps(config_key)

        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            acquired = _try_acquire_lock(fd)

            if acquired:
                socket_path = tempfile.mktemp(suffix=".sock", prefix="monarch_")
                _write_record(fd, _LockRecord(new_key_bytes, 0, socket_path))
                proc_env = None if env is None else {**os.environ, **env}
                log_path = daemon_log_path(lock_path)

                # The process outlives the caller, so an inherited stdout or
                # stderr would keep the caller's pipe (e.g. `monarch apply | cat`)
                # open.
                with open(log_path, "ab") as log:
                    proc = subprocess.Popen(
                        [*subprocess_args, socket_path, str(fd)],
                        env=proc_env,
                        pass_fds=(fd,),
                        stdin=subprocess.DEVNULL,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                    )
                _write_record(fd, _LockRecord(new_key_bytes, proc.pid, socket_path))

                os.close(fd)
                fd = -1

                _wait_for_socket(socket_path, pid=proc.pid, log_path=log_path)
                return cls(socket_path, proc.pid)

            else:
                os.close(fd)
                fd = -1

                rec = _read_record(lock_path)
                if rec is not None and rec.config_bytes == new_key_bytes:
                    return cls(rec.socket_path, rec.pid)

                # Config differs — shut down old process and retry.
                if rec is not None:
                    cls(rec.socket_path, rec.pid).shutdown()
                return cls.create(lock_path, config_key, subprocess_args, env=env)

        finally:
            if fd != -1:
                os.close(fd)

    def _connect(self) -> None:
        if self._conn is None:
            conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            conn.connect(self._socket_path)
            self._conn = conn
            self._file = conn.makefile("rb")

    def send(self, obj: object) -> "OnceDaemon":
        """Pickle *obj* and send it to the process."""
        self._connect()
        # @lint-ignore PYTHONPICKLEISBAD
        self._conn.sendall(pickle.dumps(obj))  # pyre-ignore[16]
        return self

    def get(self) -> object:
        """Receive and unpickle the next response from the process."""
        self._connect()
        # @lint-ignore PYTHONPICKLEISBAD
        return pickle.load(self._file)  # pyre-ignore[6]

    def shutdown(self) -> None:
        """Ask the process to shut down and wait for it to exit."""
        self.send(_Shutdown())
        _wait_for_pid_exit(self._pid)


def daemon_log_path(lock_path: str) -> str:
    """Return the file that the daemon locked by *lock_path* logs to."""
    return os.path.splitext(lock_path)[0] + ".log"


def find_daemon(lock_path: str) -> "OnceDaemon | None":
    """Return a handle to the daemon locked by *lock_path*, or None.

    Returns None if no lock record exists or if the lock is no longer held
    (i.e. the daemon has died).  Does not launch a new daemon.
    """
    rec = _read_record(lock_path)
    if rec is None:
        return None
    # The daemon holds the lock for its lifetime.  If we can
    # acquire it the process is dead → treat as not found.
    try:
        fd = os.open(lock_path, os.O_RDWR, 0o600)
    except FileNotFoundError:
        return None
    try:
        if _try_acquire_lock(fd):
            return None
        return OnceDaemon(rec.socket_path, rec.pid)
    finally:
        os.close(fd)


def _pid_alive(pid: int) -> bool:
    # A child that exited stays a zombie, which still answers kill(pid, 0),
    # until its parent reaps it; reap our own children here.
    try:
        if os.waitpid(pid, os.WNOHANG)[0] == pid:
            return False
    except ChildProcessError:
        pass  # Not our child; its own parent reaps it.
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_for_socket(
    socket_path: str,
    pid: int = 0,
    timeout: float = 60.0,
    log_path: "str | None" = None,
) -> None:
    deadline = time.monotonic() + timeout
    see_log = "" if log_path is None else f" See {log_path}."
    while True:
        if pid and not _pid_alive(pid):
            raise RuntimeError(
                f"Daemon (pid {pid}) exited before the socket "
                f"{socket_path!r} became ready.{see_log}"
            )
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.connect(socket_path)
                return
            finally:
                sock.close()
        except OSError:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Process did not become ready on {socket_path!r} "
                    f"within {timeout}s.{see_log}"
                )
            time.sleep(0.05)


def _try_acquire_lock(fd: int) -> bool:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False


def _wait_for_pid_exit(pid: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        if not _pid_alive(pid):
            return
        if time.monotonic() >= deadline:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                return
            time.sleep(0.1)
            return
        time.sleep(0.05)


def _read_record(lock_path: str) -> "_LockRecord | None":
    try:
        with open(lock_path, "rb") as f:
            data = f.read()
        if not data:
            return None
        # @lint-ignore PYTHONPICKLEISBAD
        return pickle.loads(data)
    except Exception:
        return None


def _write_record(fd: int, record: _LockRecord) -> None:
    os.lseek(fd, 0, os.SEEK_SET)
    os.ftruncate(fd, 0)
    # @lint-ignore PYTHONPICKLEISBAD
    os.write(fd, pickle.dumps(record))
