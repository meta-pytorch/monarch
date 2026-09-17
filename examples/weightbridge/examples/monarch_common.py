# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Monarch actor plumbing shared by the toy example (:mod:`workers_monarch`) and the 30B replay
(:mod:`bench_monarch`).

Everything here is a workaround for something Monarch does differently from Ray, and each one silently
produces a wrong or wedged run rather than an error if it is missing. They are collected in one module so
the two front-ends cannot drift apart on any of them.
"""

from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import configure
from monarch._src.actor.actor_mesh import _set_context
from monarch.actor import attach_to_workers, context, current_rank

logger = logging.getLogger("wbridge.example.monarch_common")

# LOAD-BEARING: without it this driver's agent serves on an abstract unix socket (ipc://@...), whose
# namespace is local to this node, so remote workers cannot dial back and attach dies with
# MESH_ATTACH_CONFIG_TIMEOUT. SlurmJob sets exactly this (monarch/_src/job/slurm.py:87).
configure(default_transport=ChannelTransport.TcpWithHostname)


def my_rank() -> int:
    """This actor's index within its proc mesh — the worker's TP rank."""
    return int(current_rank()["gpus"])


def bind_gpu(dev: int) -> int:
    """Pin this proc (really: this *thread*) to GPU *dev*.

    Monarch's ``spawn_procs({"gpus": N})`` does NOT set ``CUDA_VISIBLE_DEVICES`` per proc (verified: every
    actor otherwise reports the same PCI bus id), unlike Ray's ``num_gpus=1``. Without this every rank on
    a node shares device 0 — and since Monarch picks a NIC by the owning GPU's ordinal, they would all
    share one NIC too, silently collapsing the per-rank bandwidth the multi-NIC measurements depend on.
    Must run before the adapter is built: wbridge captures ``cuda:{current_device()}`` at construction.
    Must also run on the same thread as everything that follows — ``set_device`` is thread-local, which is
    half the reason :class:`ActorThread` owns exactly one thread.
    """
    import torch

    if torch.cuda.is_available():
        dev = dev % torch.cuda.device_count()
        torch.cuda.set_device(dev)
        torch.zeros(1, device="cuda")  # materialize the primary context on THIS device
        return dev
    return -1


class ActorThread:
    """One dedicated thread per actor; every WeightBridge call runs on it, never on the actor's loop.

    Three separate reasons it has to be exactly this — one thread, owned, and off the event loop:

    * **Off the loop.** A Monarch RDMA completion arrives as a message to the submitting actor, and only
      the actor's event loop processes messages. An endpoint that blocks its own loop on ``Future.get()``
      can therefore never observe its own transfer: it hangs forever with nothing in the log. That is what
      wedged the first end-to-end run.
    * **One thread.** ``torch``'s current CUDA device is thread-local, so a pool that hands successive
      calls to different threads would run ``send_weights`` on device 0 no matter what :func:`bind_gpu`
      did. WeightBridge also builds adapter state on the thread that later uses it.
    * **Owned.** The Monarch context is a contextvar that ``run_in_executor`` does not propagate, so the
      thread is primed once with the actor's context before any work reaches it.
    """

    def __init__(self, name: str) -> None:
        self._ex = ThreadPoolExecutor(max_workers=1, thread_name_prefix=name)
        self._primed = False

    async def run(self, fn, *a):
        loop = asyncio.get_running_loop()
        if not self._primed:
            # context() is only meaningful on the actor's own thread, so read it here and replay it there.
            ctx = context()
            await loop.run_in_executor(self._ex, _set_context, ctx)
            self._primed = True
        return await loop.run_in_executor(self._ex, fn, *a)


def attach_hosts(worker_addrs: list[str], attempts: int = 5, name: str = "wbexample"):
    """Attach to already-serving worker loops, retrying the flaky handshake.

    The attach intermittently dies with MESH_ATTACH_CONFIG_TIMEOUT when a worker loop is still settling
    (most often on a node reused straight after a previous run). Retrying is far cheaper than losing the
    whole job to it.
    """
    last = None
    for attempt in range(1, attempts + 1):
        try:
            hosts = attach_to_workers(
                name=f"{name}{attempt}",
                ca="trust_all_connections",
                workers=worker_addrs,
            )
            hosts.initialized.get()
            return hosts
        except Exception as e:  # noqa: BLE001
            last = e
            logger.warning(
                "monarch attach attempt %d/%d failed: %s",
                attempt,
                attempts,
                str(e).strip().splitlines()[-1][:100],
            )
            time.sleep(10)
    raise RuntimeError(f"could not attach to Monarch workers {worker_addrs}: {last}")


def host_index(worker_addrs: list[str], ip: str) -> int:
    """Index of the worker address serving *ip*; the host slice to place a role on."""
    for i, addr in enumerate(worker_addrs):
        if (
            addr.rsplit(":", 1)[0].removeprefix("tcp://") == ip
        ):  # addr is "tcp://<ip>:<port>"
            return i
    raise RuntimeError(f"no Monarch worker for host {ip} in {worker_addrs}")
