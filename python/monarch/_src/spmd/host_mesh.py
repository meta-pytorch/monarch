# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Stand up a Monarch host mesh from a torchrun/torchx-style SPMD entry point,
without going through the Monarch job API.

:func:`host_mesh_from_store` is the public entry point: called collectively
on every rank, it spawns a worker subprocess on each local rank 0, publishes
the worker addresses through the caller-supplied store, and on global rank 0
calls :func:`monarch.actor.attach_to_workers` to build the host mesh. Non-
primary ranks return ``None``.

Worker subprocesses are fire-and-forget. Each child inherits the read end
of an ``os.pipe()`` whose write end stays open in the parent for the
lifetime of the process; the parent never writes. The kernel closes the
write end when the parent dies by *any* means — clean exit, segfault,
SIGKILL — which delivers EOF to the child's blocking read and lets it
``os._exit`` on its own.

IPC transport places the socket inside a per-worker tmpdir under
``tempfile.gettempdir()``. The tmpdir is intentionally left behind — the
OS's temp reaper cleans it up, matching hyperactor's own behavior
(hyperactor doesn't ``unlink`` path-based sockets either; see the
``// TODO: we just leak these`` comment in ``hyperactor/src/channel/net.rs``).
Linux abstract sockets (``ipc://@...``) would avoid the leak entirely,
but hyperactor's ``from_zmq_url`` parser currently interprets ``@`` as
the TCP alias separator and rejects abstract-socket URLs.

The child-side entry point lives in :mod:`monarch._src.spmd._worker_entry`.
"""

import logging
import os
import socket
import subprocess
import sys
import tempfile
import threading
from collections.abc import Mapping
from typing import Final, Protocol

from monarch._src.actor.host_mesh import HostMesh
from monarch.actor import attach_to_workers, enable_transport

logger: logging.Logger = logging.getLogger(__name__)

try:
    from __manifest__ import fbmake  # noqa

    _IN_PAR: bool = bool(fbmake.get("par_style"))
except ImportError:
    _IN_PAR = False

_WORKER_ENTRY_MODULE: Final[str] = "monarch._src.spmd._worker_entry"
_ADDR_ENV: Final[str] = "_MONARCH_WORKER_ADDR"
_PARENT_WATCH_FD_ENV: Final[str] = "_MONARCH_WORKER_PARENT_WATCH_FD"


class _StoreLike(Protocol):
    """Subset of ``torch.distributed.Store`` used by :func:`host_mesh_from_store`."""

    def set(self, key: str, value: str | bytes) -> None: ...
    def get(self, key: str) -> bytes: ...


def _worker_address(
    transport: str,
    *,
    monarch_port: int,
    name: str,
    ipc_dir: str | None,
) -> str:
    """Build a ZMQ-style listen address for ``run_worker_loop_forever``.

    ``monarch_port == 0`` is rejected for TCP and metatls transports: the
    kernel would assign a free port at bind time, but the caller can't
    learn that port without a round-trip from the worker, so reserving
    the port pre-spawn via a short-lived socket races against other
    listeners on the host. Forcing an explicit port keeps the parent's
    pre-computed address honest.
    """
    if transport == "ipc":
        assert ipc_dir is not None
        return f"ipc://{ipc_dir}/{name}"
    if transport in ("tcp", "metatls", "metatls-hostname"):
        if monarch_port == 0:
            raise ValueError(
                f"monarch_port must be an explicit non-zero port for "
                f"transport {transport!r}"
            )
        if transport == "tcp":
            return f"tcp://{socket.getfqdn()}:{monarch_port}"
        return f"metatls://{socket.getfqdn()}:{monarch_port}"
    raise ValueError(
        f"unsupported transport {transport!r}; expected one of "
        "'tcp', 'metatls', 'metatls-hostname', 'ipc'"
    )


def _watch_worker(
    proc: subprocess.Popen[bytes],
    name: str,
    addr: str,
) -> None:
    """Daemon thread that waits on the worker and logs its exit status.

    ``proc.wait`` inside the thread also reaps the zombie so dead workers
    don't pile up during the parent's lifetime. The thread holds the only
    reference to ``proc``, which keeps the ``Popen`` alive (avoiding the
    ``ResourceWarning`` that ``__del__`` would otherwise emit on an
    unwaited live child).
    """

    def _waiter() -> None:
        pid = proc.pid
        try:
            proc.wait()
            code = proc.returncode
        except Exception as e:
            logger.exception(
                "monarch worker %s (pid=%d addr=%s) errored while waiting: %s",
                name,
                pid,
                addr,
                e,
            )
            return
        if code == 0:
            logger.info(
                "monarch worker %s exited pid=%d exit_code=%d addr=%s",
                name,
                pid,
                code,
                addr,
            )
        else:
            logger.warning(
                "monarch worker %s died unexpectedly pid=%d exit_code=%d addr=%s",
                name,
                pid,
                code,
                addr,
            )

    threading.Thread(
        target=_waiter, daemon=True, name=f"monarch-worker-watch-{name}"
    ).start()


def _worker_addr_key(group_rank: int) -> str:
    return f"monarch/spmd/workers/{group_rank}"


def _resolve_topology_field(override: int | None, env_name: str) -> int:
    """Pick a torchelastic topology value from an override or from the env."""
    if override is not None:
        return override
    value = os.environ.get(env_name)
    if value is None:
        raise RuntimeError(
            f"host_mesh_from_store requires the {env_name} env var "
            "(torchelastic: RANK, LOCAL_RANK, WORLD_SIZE, LOCAL_WORLD_SIZE) "
            "or the matching keyword argument"
        )
    return int(value)


def host_mesh_from_store(
    store: _StoreLike,
    *,
    monarch_port: int = 0,
    name: str = "monarch_worker",
    transport: str = "ipc",
    rank: int | None = None,
    local_rank: int | None = None,
    world_size: int | None = None,
    local_world_size: int | None = None,
) -> HostMesh | None:
    """Stand up a Monarch host mesh from a torchrun/torchx-style SPMD context.

    Must be called collectively on every rank of an SPMD job. Each local
    rank 0 spawns a ``run_worker_loop_forever`` subprocess on its host via
    :func:`_spawn_worker_process` and publishes the worker's listen address
    into ``store``. Global rank 0 then reads every address out of the store
    and calls :func:`monarch.actor.attach_to_workers` to build the host
    mesh, which it returns. Non-primary ranks return ``None``.

    Rank topology defaults to the standard torchelastic env vars
    (``RANK``, ``LOCAL_RANK``, ``WORLD_SIZE``, ``LOCAL_WORLD_SIZE``). Any
    of the four can be overridden via the matching keyword argument.

    Worker subprocesses are fire-and-forget: they exit on their own when
    this process exits via the parent-watch pipe (see module docstring).
    Callers who need to tear a worker down early can ``os.kill`` it by PID
    — the watcher thread logs the exit.

    Args:
        store: A ``torch.distributed.Store`` (or any object with matching
            ``set(key, value)`` / ``get(key) -> bytes``) shared across
            ranks. Used to broadcast worker addresses from each local
            rank 0 to global rank 0.
        monarch_port: Port the worker binds on for TCP/metatls transports.
            Must be an explicit non-zero port; ignored for IPC. ``0`` is
            rejected because the kernel would assign a port at bind time
            that the parent can't learn pre-spawn without racing other
            listeners.
        name: Prefix for the host mesh and per-host worker names; the
            worker on group rank ``g`` is named ``f"{name}_{g}"``.
        transport: Worker listen scheme — one of ``"ipc"`` (default),
            ``"tcp"``, ``"metatls"``, or ``"metatls-hostname"``. ``"ipc"``
            is single-host only but the default so we don't silently open
            arbitrary TCP ports on user machines; multi-host callers must
            explicitly opt into a network transport and supply a
            ``monarch_port``.
        rank: Overrides ``RANK`` when set.
        local_rank: Overrides ``LOCAL_RANK`` when set.
        world_size: Overrides ``WORLD_SIZE`` when set.
        local_world_size: Overrides ``LOCAL_WORLD_SIZE`` when set.
    """
    env_rank = _resolve_topology_field(rank, "RANK")
    env_local_rank = _resolve_topology_field(local_rank, "LOCAL_RANK")
    env_world_size = _resolve_topology_field(world_size, "WORLD_SIZE")
    env_local_world_size = _resolve_topology_field(local_world_size, "LOCAL_WORLD_SIZE")
    if env_world_size % env_local_world_size != 0:
        raise ValueError(
            f"WORLD_SIZE ({env_world_size}) must be divisible by "
            f"LOCAL_WORLD_SIZE ({env_local_world_size})"
        )
    num_hosts = env_world_size // env_local_world_size
    group_rank = env_rank // env_local_world_size

    enable_transport(transport)

    if env_local_rank == 0:
        addr, _pid = _spawn_worker_process(
            transport=transport,
            monarch_port=monarch_port,
            name=f"{name}_{group_rank}",
        )
        store.set(_worker_addr_key(group_rank), addr.encode())

    if env_rank != 0:
        return None

    worker_addrs = [store.get(_worker_addr_key(i)).decode() for i in range(num_hosts)]
    return attach_to_workers(
        name=name,
        # Currently "trust_all_connections" is the only supported option.
        ca="trust_all_connections",
        workers=list(worker_addrs),
    )


def _spawn_worker_process(
    *,
    transport: str = "ipc",
    monarch_port: int = 0,
    name: str = "monarch_worker",
    env: Mapping[str, str] | None = None,
) -> tuple[str, int]:
    """Spawn a ``run_worker_loop_forever`` subprocess.

    Returns ``(addr, pid)``. The subprocess is fire-and-forget: a daemon
    watcher thread holds the ``Popen`` and ``wait()``s on it (reaping the
    zombie), and the parent-watch pipe ensures the child exits when the
    parent does. Callers who want early teardown can ``os.kill(pid, …)``.

    ``monarch_port`` must be an explicit non-zero port for TCP/metatls
    transports (see :func:`_worker_address`).
    """
    ipc_dir: str | None = None
    if transport == "ipc":
        # Per-worker tmpdir under the system temp dir. Left behind on exit
        # — we can't rely on ``atexit`` because abnormal exits (segfault,
        # SIGKILL) wouldn't trigger it, and hyperactor itself doesn't
        # ``unlink`` path-based sockets.
        ipc_dir = tempfile.mkdtemp(prefix="monarch_worker_")

    addr = _worker_address(
        transport, monarch_port=monarch_port, name=name, ipc_dir=ipc_dir
    )

    # The parent never writes to parent_watch_write_fd; it holds the fd
    # open so the child's read blocks indefinitely. Kernel closes the fd
    # when this process exits — cleanly or otherwise — delivering EOF to
    # the child's read. The fd is intentionally leaked: we want it to
    # survive for the full lifetime of the process and only close when
    # the kernel reaps us.
    parent_watch_read_fd, parent_watch_write_fd = os.pipe()

    child_env: dict[str, str] = {
        **os.environ,
        _ADDR_ENV: addr,
        _PARENT_WATCH_FD_ENV: str(parent_watch_read_fd),
        "HYPERACTOR_PROCESS_NAME": name,
    }
    if env is not None:
        child_env.update(env)

    if _IN_PAR:
        # sys.executable in PAR/XAR is the bare interpreter and cannot import
        # archive-bundled modules. Re-invoke the PAR with PAR_MAIN_OVERRIDE to
        # dispatch to the worker entry module.
        child_env["PAR_MAIN_OVERRIDE"] = _WORKER_ENTRY_MODULE
        cmd = [sys.argv[0]]
    else:
        cmd = [sys.executable, "-m", _WORKER_ENTRY_MODULE]

    try:
        proc = subprocess.Popen(
            cmd,
            env=child_env,
            start_new_session=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            pass_fds=(parent_watch_read_fd,),
        )
    except Exception:
        os.close(parent_watch_read_fd)
        os.close(parent_watch_write_fd)
        raise

    # The parent only holds the write end; the child owns the read end.
    os.close(parent_watch_read_fd)

    logger.info("monarch worker %s spawned pid=%d addr=%s", name, proc.pid, addr)
    _watch_worker(proc, name, addr)

    return addr, proc.pid
