# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Ladder benchmark: walk from the Monarch RDMA microbenchmark up to WeightBridge's real write pattern,
one axis of complexity at a time, to identify the source of the throughput gap.

Context. On the captured replay at equal NIC count, Monarch's bulk wire time was materially worse than
Mooncake's. This benchmark isolates four independent differences from the real sender, walking them one
at a time and reporting GB/s per rank at each step:

* **ranks** — 1 writer per node vs 8 (the real topology).
* **dst memory** — direct GPU destinations vs a staged receiver's CPU-pinned arena; Monarch picks
  a NIC *per region*: device memory takes the NIC co-located with its GPU, host memory is hashed over all
  tied-best NICs. Hash collisions across 8 concurrent ranks would show up here and nowhere else.
* **fan-out** — one-to-one rank pairing vs writing to 4 receiver ranks per round,
  which is 4 destination sessions and 4 separate ``RDMAAction`` submits in flight.
* **issuing thread** — actor-thread submission vs the sender's Stage-2 daemon thread with the actor
  context replayed onto it.

Total bytes per writer per iteration is held constant across levels, so GB/s is directly comparable: with
``--fanout 4`` each of the 4 targets receives a quarter of the same total.

Host 0 writes and host 1 is the target, matching the real trainer-to-rollout direction.
"""

from __future__ import annotations

import argparse
import statistics
import time

import torch
from monarch.actor import Actor, endpoint
from monarch_common import ActorThread, attach_hosts, bind_gpu, my_rank

# Per-op tile. 64 MiB is MonarchEngine's default; prior sweeps showed little steady-state sensitivity
# across the tested range, so this is held fixed rather than swept again.
CHUNK = 64 << 20


class Target(Actor):
    """Host 1: owns the destination buffer and publishes it as per-chunk RDMABuffers."""

    def __init__(self) -> None:
        self.rank = my_rank()
        self.gpu = bind_gpu(self.rank)
        self._t: torch.Tensor | None = None
        self._bufs: list = []

    @endpoint
    async def publish(self, nbytes: int, dst_mem: str, force_nic: bool = False) -> dict:
        import os
        import socket

        from monarch.rdma import get_rdma_backend, RDMABuffer

        if force_nic:
            # Mechanism probe. Monarch pins DEVICE memory to the NIC co-located with its GPU, but hashes
            # HOST memory over all tied-best NICs — with 8 concurrent ranks that can collide, leaving some
            # NICs doubled and others idle. gpu:<ordinal> forces the same deterministic 1-NIC-per-rank
            # choice the device path gets. If this restores the cpu-destination bandwidth, hashing is the
            # mechanism. Set before the first RDMA call in this proc so it is in place at registration.
            os.environ["MONARCH_RDMA_IBVERBS_TARGET"] = f"gpu:{self.gpu}"
        if dst_mem == "cuda":
            self._t = torch.zeros(nbytes, dtype=torch.uint8, device="cuda")
            torch.cuda.synchronize()
        else:
            # Pinned host memory: what a staged receiver's CPU arena looks like to the NIC.
            self._t = torch.zeros(nbytes, dtype=torch.uint8).pin_memory()
        self._bufs = [
            RDMABuffer(self._t[off : off + min(CHUNK, nbytes - off)])
            for off in range(0, nbytes, CHUNK)
        ]
        return {
            "host": socket.gethostname(),
            "rank": self.rank,
            "gpu": self.gpu,
            "n": len(self._bufs),
            "backend": get_rdma_backend(),
        }

    @endpoint
    async def handles(self):
        return self._bufs


class Writer(Actor):
    """Host 0: writes into `fanout` targets' buffers, from the actor thread or a dedicated thread."""

    def __init__(self, targets) -> None:
        self.rank = my_rank()
        self.gpu = bind_gpu(self.rank)
        self._targets = (
            targets  # peer mesh as a CONSTRUCTOR arg; as an endpoint arg it hangs
        )
        self._src: torch.Tensor | None = None
        self._thread = ActorThread(f"ladder-w{self.rank}")

    @endpoint
    async def run(
        self,
        nbytes: int,
        fanout: int,
        nranks: int,
        reps: int,
        on_thread: bool,
        gil_load: int = 0,
        switch_s: float = 0.0,
        coalesce: bool = False,
    ) -> dict:
        import socket
        import sys
        import threading

        # A GIL-holding Python thread only yields every sys.getswitchinterval() (default 5 ms), so each
        # completion the actor loop needs to observe can wait a full handoff. Monarch's own test_gil_stall
        # calls this out. Shrinking the interval should buy most of the throughput back if that latency
        # -- rather than raw CPU starvation -- is what costs the bandwidth.
        if switch_s:
            sys.setswitchinterval(switch_s)

        from monarch.rdma import RDMAAction

        # Handles must be fetched from INSIDE the consuming actor; relaying them via the driver hangs.
        per = nbytes // fanout
        peers = [(self.rank + k) % nranks for k in range(fanout)]
        handles = [await self._targets.slice(gpus=p).handles.call_one() for p in peers]

        if self._src is None or self._src.numel() != nbytes:
            self._src = torch.full((nbytes,), 0x5A, dtype=torch.uint8, device="cuda")
        torch.cuda.synchronize()

        def _build():
            """One RDMAAction per destination session — the same shape sender.py issues per round.

            With `coalesce`, every destination's ops go into a SINGLE action instead. An RDMAAction's ops
            may target different peers, so this is legal, and it collapses `fanout` submits/Futures/
            completion messages into one. That is the candidate fix if per-action cost is what scales.
            """
            acts = []
            one = RDMAAction() if coalesce else None
            for i in range(fanout):
                act = one if coalesce else RDMAAction()
                base = i * per
                for j in range(0, per, CHUNK):
                    n = min(CHUNK, per - j)
                    # handles[i] tiles the WHOLE target buffer; take the tiles covering our slice.
                    h = handles[i][(base + j) // CHUNK]
                    act.write_remote(h, self._src[base + j : base + j + n])
                if not coalesce:
                    acts.append(act)
            return [one] if coalesce else acts

        # Monarch delivers an RDMA completion as a message to the actor, so observing it needs the GIL
        # and the actor's event loop. The real sender runs Python-heavy WeightBridge work (packing, spec
        # walking) on the very same proc, which this reproduces; Mooncake polls a CQ in C++ with the GIL
        # released and is structurally immune.
        stop = threading.Event()
        burners = []
        for _ in range(gil_load):

            def _burn():
                x = 0
                while not stop.is_set():
                    for k in range(5000):
                        x += k * k

            t = threading.Thread(target=_burn, daemon=True)
            t.start()
            burners.append(t)

        times = []
        for i in range(reps + 1):
            torch.cuda.synchronize()
            t0 = time.perf_counter()
            if on_thread:
                # Submit AND complete off the actor's loop, exactly as the Stage-2 daemon thread does.
                def _go():
                    futs = [a.submit(timeout=300) for a in _build()]
                    for f in futs:
                        f.get()

                await self._thread.run(_go)
            else:
                acts = _build()
                futs = [a.submit(timeout=300) for a in acts]
                for f in futs:
                    await f
            torch.cuda.synchronize()
            dt = time.perf_counter() - t0
            if i:  # drop the first: registration + QP warmup
                times.append(dt)
        stop.set()
        for t in burners:
            t.join(timeout=5)
        return {
            "host": socket.gethostname(),
            "rank": self.rank,
            "gpu": self.gpu,
            "median_s": statistics.median(times),
            "best_s": min(times),
        }


# (label, ranks, dst_mem, fanout, on_thread, force_nic) — each row changes ONE axis from its baseline.
# (label, ranks, dst_mem, fanout, on_thread, force_nic, gil, switch_s, coalesce)
LEVELS = [
    ("f1  1 peer   separate actions ", 8, "cuda", 1, True, False, 0, 0.0, False),
    ("f2  2 peers  separate actions ", 8, "cuda", 2, True, False, 0, 0.0, False),
    ("f4  4 peers  separate actions ", 8, "cuda", 4, True, False, 0, 0.0, False),
    ("f8  8 peers  separate actions ", 8, "cuda", 8, True, False, 0, 0.0, False),
    ("f8  8 peers  ONE coalesced act", 8, "cuda", 8, True, False, 0, 0.0, True),
    ("f4  4 peers  ONE coalesced act", 8, "cuda", 4, True, False, 0, 0.0, True),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--workers", required=True, help="comma-separated tcp://ip:port worker loops"
    )
    ap.add_argument(
        "--mib",
        type=int,
        default=512,
        help="bytes written per writer per iteration, MiB",
    )
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument(
        "--only", default="", help="comma-separated level indices to run (default all)"
    )
    cli = ap.parse_args()

    workers = [w.strip() for w in cli.workers.split(",") if w.strip()]
    hosts = attach_hosts(workers, name="wbladder")
    nbytes = cli.mib << 20
    keep = {int(x) for x in cli.only.split(",") if x.strip()} if cli.only else None
    print(
        f"[ladder] hosts={hosts.extent} bytes/writer/iter={cli.mib} MiB chunk={CHUNK >> 20} MiB "
        f"reps={cli.reps}",
        flush=True,
    )
    print(f"\n  {'level':46} {'GB/s/rank':>10} {'aggregate GB/s':>15}", flush=True)

    rows = []
    for idx, (
        label,
        ranks,
        dst_mem,
        fanout,
        on_thread,
        force_nic,
        gil,
        sw,
        co,
    ) in enumerate(LEVELS):
        if keep is not None and idx not in keep:
            continue
        try:
            tp = hosts.slice(hosts=1).spawn_procs({"gpus": ranks})
            wp = hosts.slice(hosts=0).spawn_procs({"gpus": ranks})
            tp.initialized.get()
            wp.initialized.get()
            targets = tp.spawn(f"tgt{idx}", Target)
            targets.initialized.get()
            writers = wp.spawn(f"wrt{idx}", Writer, targets)
            writers.initialized.get()

            # Every target publishes the FULL nbytes so any writer's slice of it is addressable.
            pub = [
                v
                for _, v in targets.publish.call(
                    nbytes=nbytes, dst_mem=dst_mem, force_nic=force_nic
                ).get()
            ]
            if any(p["backend"] != "ibverbs" for p in pub):
                print(f"  {label:46} FAIL: backend not ibverbs", flush=True)
                continue

            res = [
                v
                for _, v in writers.run.call(
                    nbytes=nbytes,
                    fanout=fanout,
                    nranks=ranks,
                    reps=cli.reps,
                    on_thread=on_thread,
                    gil_load=gil,
                    switch_s=sw,
                    coalesce=co,
                ).get()
            ]
            # Per-rank GB/s from each rank's own median; aggregate from the SLOWEST rank, since the
            # round does not finish until the last writer does.
            per_rank = statistics.median([nbytes / r["median_s"] / 1e9 for r in res])
            agg = len(res) * nbytes / max(r["median_s"] for r in res) / 1e9
            print(f"  {label:46} {per_rank:10.2f} {agg:15.2f}", flush=True)
            rows.append((label, per_rank, agg))
        except Exception as e:  # noqa: BLE001
            print(
                f"  {label:46} ERROR: {str(e).strip().splitlines()[-1][:90]}",
                flush=True,
            )

    if len(rows) > 1:
        print(
            "\n  step-to-step change in per-rank GB/s (where the bandwidth actually goes):",
            flush=True,
        )
        for i in range(1, len(rows)):
            prev, cur = rows[i - 1], rows[i]
            ratio = cur[1] / prev[1] if prev[1] else float("nan")
            print(f"    {prev[0][:20]:22} -> {cur[0][:20]:22} x{ratio:.2f}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
