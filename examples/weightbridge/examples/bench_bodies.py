# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Orchestrator-agnostic bodies for the frameworkless 30B replay.

Same split as :mod:`worker_bodies` does for the toy example: the *work* — rebuild the captured specs,
construct a real ``WeightSender``/``WeightReceiver``, run K transfers, digest the result — lives here, and
:mod:`bench_transfer` (Ray) and :mod:`bench_monarch` (Monarch) each wrap it in their own actor type. Monarch
RDMA is only reachable from inside a Monarch actor, so the second wrapper is not optional if we want to
measure that transport on the real 30B layout.

Correctness is the reason this module exists rather than the bodies staying inline in ``bench_transfer``:
the replay was perf-only (``torch.empty`` worker values, no check), and comparing two transports needs the
values to be reproducible. With ``--seed`` the sender's shards are fixed by ``(name, rank)`` and each
receiver digests its post-consume wksd; the digests must be identical under Mooncake and under Monarch.
That is a full-path equivalence check — real 30B sharding, dedup exchange and consume — with no
Megatron/SGLang in the loop.
"""

from __future__ import annotations

import os
import pickle
import socket
import time

import torch
from loadspec_replay import rebuild, wksd_digest
from utils import apply_network_env, network_env_vars
from wbridge.backend.receiver import WeightReceiver
from wbridge.backend.sender import SenderArgs, WeightSender

# Distinct salts per role. The receiver's wksd starts as *its own* deterministic noise, not the sender's,
# so a region the transport silently never wrote does not accidentally already hold the right bytes.
SENDER_SALT = "wbridge-replay-sender"
RECEIVER_SALT = "wbridge-replay-receiver"


def _wait_for_replay_drain(barrier, participant: str, epoch: int) -> None:
    """Join the replay-only full-drain barrier for one completed epoch.

    The barrier is deliberately outside WeightBridge.  A receiver arrives only after
    ``poll_requests()`` has joined its E+C worker and synchronized CUDA; a sender arrives only after
    ``wait_send_complete()``.  Consequently the barrier's release proves that no participant can start
    epoch N+1 while any participant is still draining epoch N.
    """
    if barrier is None:
        return
    if not participant:
        raise ValueError("replay drain barrier requires a participant id")
    import ray

    ray.get(barrier.arrive.remote(participant, epoch))


def _load(rec_path: str) -> dict:
    with open(rec_path, "rb") as f:
        return pickle.load(f)


class ReplayReceiverBody:
    """One captured rollout rank: a real :class:`WeightReceiver` over the recorded LoadSpec."""

    def init(
        self,
        rec_path: str,
        ipc: str,
        num_workers: int,
        provider: str,
        iface: str,
        phys_dev: int,
        seed: bool = False,
    ) -> int:
        apply_network_env(provider, iface)
        # Distinct physical GPU per rank, so receiver<->receiver dedup exchange is genuinely cross-device
        # (as under SGLang) rather than a same-device memcpy that would flatter the numbers.
        torch.cuda.set_device(phys_dev)
        if os.environ.get("WBRIDGE_HBM_DEBUG"):
            free_bytes, total_bytes = torch.cuda.mem_get_info()
            print(
                "WBHBM_ACTOR "
                f"stage=receiver_device_selected host={socket.gethostname()} pid={os.getpid()} "
                f"rank=unknown requested_device={phys_dev} current_device={torch.cuda.current_device()} "
                f"visible={os.environ.get('CUDA_VISIBLE_DEVICES', '')} "
                f"free_bytes={free_bytes} total_bytes={total_bytes}",
                flush=True,
            )
        rec = _load(rec_path)
        self.rank = rec["rank"]
        src, dtype_spec, load_spec, wksd = rebuild(
            rec, seed_salt=RECEIVER_SALT if seed else ""
        )
        self.wksd = wksd
        if os.environ.get("WBRIDGE_HBM_DEBUG"):
            free_bytes, total_bytes = torch.cuda.mem_get_info()
            print(
                "WBHBM_ACTOR "
                f"stage=receiver_model_allocated host={socket.gethostname()} pid={os.getpid()} "
                f"rank={self.rank} requested_device={phys_dev} current_device={torch.cuda.current_device()} "
                f"visible={os.environ.get('CUDA_VISIBLE_DEVICES', '')} "
                f"free_bytes={free_bytes} total_bytes={total_bytes} "
                f"allocated_bytes={torch.cuda.memory_allocated()} reserved_bytes={torch.cuda.memory_reserved()}",
                flush=True,
            )
        self.receiver = WeightReceiver(
            ipc,
            rec["rank"],
            src,
            dtype_spec,
            load_spec,
            wksd,
            num_workers=num_workers,
            receiver_staging=bool(rec.get("receiver_staging")),
        )
        self._recv_timings: list[dict[str, int]] = []
        return self.rank

    def run_recv(
        self,
        k_updates: int,
        timeout_s: float = 1800.0,
        drain_barrier=None,
        participant: str = "",
    ) -> int:
        self._recv_timings = []
        got, t_end = 0, time.time() + timeout_s
        while got < k_updates:
            if time.time() > t_end:
                raise TimeoutError(
                    f"receiver rank {self.rank}: {got}/{k_updates} updates"
                )

            start_ns = None
            mono_start_ns = None

            def before_receive(_epoch: int) -> None:
                nonlocal start_ns, mono_start_ns
                # These are the replay equivalent of the rollout block_start/consume_end markers:
                # the receiver has observed the update and is about to enter the blocking receive,
                # exchange, and in-place consume path. Wall time is intentional because E2E spans nodes;
                # the per-rank monotonic duration is retained as a clock-skew-independent cross-check.
                start_ns = time.time_ns()
                mono_start_ns = time.perf_counter_ns()

            updated = self.receiver.poll_requests(before_receive=before_receive)
            if updated:
                mono_end_ns = time.perf_counter_ns()
                end_ns = time.time_ns()
                assert start_ns is not None and mono_start_ns is not None
                self._recv_timings.append(
                    {
                        "epoch": got,
                        "block_start_ns": start_ns,
                        "consume_end_ns": end_ns,
                        "block_end_ns": end_ns,
                        "block_duration_ns": mono_end_ns - mono_start_ns,
                    }
                )
                # All replay timing boundaries above are fixed before trace/control-profile output.
                self.receiver.flush_profile_outputs()
                _wait_for_replay_drain(drain_barrier, participant, got)
                got += 1
            else:
                time.sleep(0.005)
        return got

    def timings(self) -> dict:
        """Return lightweight replay timestamps collected outside the transfer's CUDA work."""
        return {"rank": self.rank, "epochs": list(self._recv_timings)}

    def digest(self) -> dict:
        """Post-consume content digest. Compared across transports, never inside the timed loop."""
        return {"rank": self.rank, "digest": wksd_digest(self.wksd)}


class ReplaySenderBody:
    """One captured trainer rank: a real :class:`WeightSender` over the recorded LoadSpec."""

    def init(
        self,
        rec_path: str,
        sender_args: SenderArgs,
        provider: str,
        iface: str,
        phys_dev: int,
        seed: bool = False,
    ) -> int:
        apply_network_env(provider, iface)
        torch.cuda.set_device(phys_dev)
        rec = _load(rec_path)
        self.rank = rec["rank"]
        src, dtype_spec, load_spec, wksd = rebuild(
            rec, seed_salt=SENDER_SALT if seed else ""
        )
        self.wksd = wksd
        self.sender = WeightSender(sender_args, rec["rank"], src, load_spec, wksd)
        self._send_timings: list[dict[str, int]] = []
        return self.rank

    def run_send(self, k_updates: int, drain_barrier=None, participant: str = ""):
        t0 = time.time()
        self.sender.connect()
        connect_s = time.time() - t0
        wtts = []
        self._send_timings = []
        for epoch in range(k_updates):
            t = time.time()
            send_start_ns = time.time_ns()
            mono_start_ns = time.perf_counter_ns()
            ev = self.sender.send()
            if ev is not None:
                ev.synchronize()
            trainer_end_ns = time.time_ns()
            trainer_block_ns = time.perf_counter_ns() - mono_start_ns
            self.sender.wait_send_complete()  # block until delivered + consumed by all receivers
            local_complete_ns = time.time_ns()
            local_complete_duration_ns = time.perf_counter_ns() - mono_start_ns
            wtts.append(time.time() - t)
            self._send_timings.append(
                {
                    "epoch": epoch,
                    "send_start_ns": send_start_ns,
                    "trainer_end_ns": trainer_end_ns,
                    "trainer_block_ns": trainer_block_ns,
                    "local_complete_ns": local_complete_ns,
                    "local_complete_duration_ns": local_complete_duration_ns,
                }
            )
            # Preserve both the report block and the optional full-delivery timing before emitting files/logs.
            self.sender.flush_profile_outputs()
            _wait_for_replay_drain(drain_barrier, participant, epoch)
        nr = getattr(self.sender, "num_rounds", -1)
        return self.rank, connect_s, wtts, nr

    def timings(self) -> dict:
        """Return trainer enqueue/block timestamps without changing the legacy replay result tuple."""
        return {"rank": self.rank, "epochs": list(self._send_timings)}

    def plan_stats(self) -> dict:
        router = getattr(self.sender, "router", None)
        return {
            "rank": self.rank,
            "planner_mode": getattr(router, "planner_mode", "unknown"),
            "rounds": len(getattr(router, "global_rounds", ())),
            "rollout_rdma_peak_bytes": getattr(
                router, "planned_rollout_rdma_peak_bytes", None
            ),
        }

    def digest(self) -> dict:
        return {"rank": self.rank, "digest": wksd_digest(self.wksd)}
