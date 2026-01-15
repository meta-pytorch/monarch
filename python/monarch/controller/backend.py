# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import logging
import os
import socket
from abc import ABC, abstractmethod
from typing import List, NamedTuple, Optional, Sequence, Tuple

from monarch._src.actor.shape import iter_ranks, Slices as Ranks
from monarch.common import messages
from monarch_supervisor import (
    Context,
    FunctionCall,
    Host,
    Process,
    ProcessExited as ProcessExitedMsg,
)
from torch.distributed import TCPStore


logger = logging.getLogger(__name__)


class Backend(ABC):
    @abstractmethod
    def send(self, ranks: Ranks, msg) -> None:
        raise NotImplementedError()

    @abstractmethod
    def recvready(self, timeout: Optional[float]) -> Sequence[Tuple[int, NamedTuple]]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def world_size(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def gpu_per_host(self):
        raise NotImplementedError()


class ProcessBackend(Backend):
    def __init__(
        self,
        ctx: Context,
        hosts: List[Host],
        gpu_per_host: int,
        _processes=None,
        _store=None,
    ):
        self.ctx = ctx
        self.hosts = hosts
        self.store = self._create_store() if _store is None else _store
        self._gpu_per_host = gpu_per_host
        self.worker_processes = (
            self._create_pg(ctx, hosts, gpu_per_host, self.store)
            if _processes is None
            else _processes
        )
        self.exiting = False
        self.process_to_rank = {p: p.rank for p in self.worker_processes}
        self.live_processes_per_rank: List[List[Process]] = [
            [p] for p in self.worker_processes
        ]

    @property
    def world_size(self):
        return len(self.worker_processes)

    @property
    def gpu_per_host(self) -> int:
        return self._gpu_per_host

    def send(self, ranks: Ranks, msg) -> None:
        handler = getattr(self, msg.__class__.__name__, None)
        if handler is not None:
            handler(ranks, msg)
        self._send(ranks, msg)

    def _send(self, ranks: Ranks, msg):
        # the intent is for this to be optimized as tree broadcast
        # base on if members of tree nodes overlap with a slice.
        for rank in iter_ranks(ranks):
            self.worker_processes[rank].send(msg)

    def CommandGroup(self, ranks: Ranks, msg: messages.CommandGroup):
        for command in msg.commands:
            handler = getattr(self, command.__class__.__name__, None)
            if handler is not None:
                handler(ranks, command)

    def CreatePipe(self, ranks: Ranks, msg: messages.CreatePipe):
        for rank in iter_ranks(ranks):
            # In general, pipes on different workers may need to have different behavior.
            # For example, two data loader pipes operating on the same dataset should
            # load different shards of the dataset. In order to do this, each pipe process
            # on the worker needs to know the number of instances of the pipe (e.g. len(pipe_ranks))
            # and its unique rank among all instances of the pipe (e.g., i).
            proc = self.worker_processes[rank].host.create_process(
                FunctionCall(
                    "monarch.worker.worker.pipe_main",
                    f"{msg.key}-{rank}",
                    msg.max_messages,
                ),
                env={"CUDA_VISIBLE_DEVICES": ""},
                name=f"pipe-{rank}",
            )
            self.live_processes_per_rank[rank].append(proc)
            self.process_to_rank[proc] = rank

    def ProcessExited(
        self, sender: Process, msg: ProcessExitedMsg
    ) -> List[Tuple[int, NamedTuple]]:
        return self._process_exited(sender, msg.result)

    def Restarted(
        self, sender: Process, restarted: messages.Restarted
    ) -> List[Tuple[int, NamedTuple]]:
        return self._process_exited(sender, restarted.result)

    def _process_exited(
        self, sender: Process, result: int | Exception
    ) -> List[Tuple[int, NamedTuple]]:
        rank = self.process_to_rank[sender]
        if result != 0:
            if not self.exiting or self.worker_processes[rank] is sender:
                kind = (
                    "worker"
                    if self.worker_processes[rank] is sender
                    else "pipe_process"
                )
                raise RuntimeError(f"Unexpected {kind} exit on rank {rank}")

        live_procs = self.live_processes_per_rank[rank]
        live_procs.remove(sender)
        if len(live_procs) == 0:
            return [(rank, ProcessExitedMsg(0))]
        return []

    def Exit(self, ranks: Ranks, msg: messages.Exit):
        self.exiting = True
        for rank in iter_ranks(ranks):
            # ideally we are more kind to these processes.
            # but first we need to develop the API for asking them
            # to suspend, restore, fast forward, rewind, etc.
            worker = self.worker_processes[rank]
            for proc in self.live_processes_per_rank[rank]:
                if worker is not proc:
                    proc.signal()
            self.worker_processes[rank].send(msg)

    def recvready(self, timeout: Optional[float]) -> Sequence[Tuple[int, NamedTuple]]:
        result = []
        for sender, msg in self.ctx.recvready(timeout):
            handler = getattr(self, msg.__class__.__name__, None)
            if handler is not None:
                result.extend(handler(sender, msg))
                continue
            elif isinstance(sender, Process):
                result.append((sender.rank, msg))
            else:
                logger.warning("TODO: ignoring non-worker message: %s %s", sender, msg)
        return result

    @staticmethod
    def _create_store():
        if os.environ.get("INSIDE_RE_WORKER"):
            hostname = "localhost"
        else:
            hostname = socket.gethostname()

        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            sock.bind(("::", 0))
            port = sock.getsockname()[1]
            store = TCPStore(
                hostname,
                port,
                is_master=True,
                use_libuv=False,
                master_listen_fd=sock.detach(),
            )
        return store

    @staticmethod
    def _create_pg(
        ctx: Context, hosts: List[Host], gpu_per_host: int, store, _restartable=False
    ):
        env = {
            # cuda event cache disabled pending fix for:
            # https://github.com/pytorch/pytorch/issues/143470
            "TORCH_NCCL_CUDA_EVENT_CACHE": "0",
            # disable nonblocking comm until D68727854 lands.
            "TORCH_NCCL_USE_COMM_NONBLOCKING": "0",
            # supervisor_pipe is a unique ID per Host object,
            # so it lets us put multiple processes on the same GPU.
            "NCCL_HOSTID": "$SUPERVISOR_PIPE",
            "STORE_HOSTNAME": store.host,
            "STORE_PORT": str(store.port),
        }
        for name, value in os.environ.items():
            if name.startswith("NCCL_") and name not in env:
                env[name] = value
        return ctx.create_process_group(
            hosts,
            FunctionCall(
                "monarch.worker.worker.worker_main", _restartable=_restartable
            ),
            processes_per_host=gpu_per_host,
            env=env,
            name="worker",
        )
