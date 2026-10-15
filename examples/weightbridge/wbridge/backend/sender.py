# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Trainer Worker side of WeightBridge: :class:`WeightSender` drives the WeightBridge data plane.

A Trainer Worker rank joins a merged metadata group with Rollout Workers, using the per-engine ZMQ
coordinators for discovery and ``connect``.  Per-update readiness is data-driven: the first completed bulk
round's CPU-pinned sequence flag wakes receiver rank 0.  Weight chunks move by one-sided RDMA writes in P2P
rounds defined by a shared :class:`~wbridge.backend.router.WeightRouter`.  A legacy ``receive`` doorbell is
retained only for the unusual layout in which an engine's rank 0 has no trainer input and therefore cannot
observe a data flag.  See :class:`SenderArgs` for transport options.
"""

import json
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass

import torch
import torch.distributed as dist
import zmq
from wbridge.backend.control_channel import CONNECT, RECEIVE, WORLD_QUERY
from wbridge.backend.router import WBEndpoint
from wbridge.utils.data import LoadSpec, ShardSpec

logger = logging.getLogger(__name__)
from wbridge.backend import gantt


def _recv_lane_predecessors(
    round_peers: list[list[int] | tuple[int, ...]],
    depth: int,
) -> tuple[list[dict[int, int | None]], dict[tuple[int, int], int]]:
    """Return the previous and final writer generations for isolated remote RECV lanes.

    A physical ingress lane is identified by ``(destination receiver, round % depth)`` from one trainer's
    perspective (the trainer rank itself selects the receiver-side sender lane). Peer sets may change every
    round, so predecessors must be tracked per destination rather than by a single round per parity.
    """
    assert depth >= 1
    last: dict[tuple[int, int], int] = {}
    pred: list[dict[int, int | None]] = []
    for ri, peers in enumerate(round_peers):
        par = ri % depth
        current: dict[int, int | None] = {}
        for peer in sorted(peers):
            key = (peer, par)
            current[peer] = last.get(key)
            last[key] = ri
        pred.append(current)
    return pred, last


def _receiver_rank_has_input(router, receiver_local_rank: int) -> bool:
    """Whether a deduplicated receiver shard overlaps at least one trainer shard.

    Engine rank 0 normally observes an input-ready flag and needs no external update doorbell.  A receiver
    with no overlap has no peer flag slot at all, so sender rank 0 keeps the old coordinator notification as
    a narrow fallback for just that engine.
    """
    dst = router.recv_specs[receiver_local_rank]
    return any(ShardSpec.compute_overlap(src, dst).entries for src in router.send_specs)


@dataclass
class SenderArgs:
    """Transport args forwarded to :class:`WeightSender`.

    Attributes:
        world_size: Number of Trainer Worker ranks participating in the connect group.
        protocol: Data-plane backend. ``"efa"`` selects Mooncake on AWS EFA, ``"monarch"`` selects
            Monarch's libibverbs backend, and ``"tcp"`` selects Mooncake for local/toy runs.
        receiver_urls: ZMQ ``tcp://host:port`` endpoints of the per-engine coordinators, one per rollout engine.
        master_addr: Host/IP of the Trainer Worker rank-0 process used for the Gloo metadata rendezvous.
        master_port: TCP port for the Gloo metadata group.
    """

    world_size: int
    receiver_urls: list[str]
    master_addr: str
    master_port: int
    protocol: str = "efa"
    # Sender-staging (SS): pack -> offload to a CPU grid -> RDMA from CPU (instead of RDMA straight from the
    # GPU wire buffer). Lets send() return a CUDA event after pack+offload while the CPU->remote RDMA
    # overlaps the trainer's next step. Off by default (the validated straight-from-GPU path).
    sender_staging: bool = False


class WeightSender(WBEndpoint):
    """Packs and P2P-sends sharded weights from Trainer Workers to :class:`WeightReceiver` peers.

    Rank 0 must call :meth:`connect` before :meth:`send`; that establishes the process group
    (and rank-0 only drives the Rollout Engine HTTP endpoints). Each round, :attr:`save_weights` fills
    the logical chunk buffers that :attr:`load_weights` on the receiver will consume (same
    :class:`~wbridge.utils.data.ShardSpec` layout).
    """

    def __init__(
        self,
        args: SenderArgs,
        rank: int,
        shard_spec: ShardSpec,
        load_spec: LoadSpec,
        wksd: dict[str, torch.Tensor],
    ) -> None:
        self.cuda_device = f"cuda:{torch.cuda.current_device()}"

        self.protocol = args.protocol
        self.receiver_urls = args.receiver_urls
        self.world_size = args.world_size
        self.init_method = f"tcp://{args.master_addr}:{args.master_port}"
        self.sender_staging = (
            args.sender_staging
        )  # WBEndpoint switch (read by router._init_engine/_setup)

        self.rank = rank
        self.shard_spec = shard_spec
        self.dtype_spec = {}
        # HF<->worker mapping + live params; the fused model->wire CopyPlan is built once at connect.
        self.load_spec = load_spec
        self.wksd = wksd

        self.connected = False
        # Persistent Stage-2 (RDMA) thread + its coordination, created at connect().
        self._send_thread = None
        self._sq = None
        self._cv = None
        self._completed: set = set()
        self._werr: list = []
        self._drained_count = 0
        self._offload_ev: dict[
            int, object
        ] = {}  # SS: per-round D2H completion events (wire-reuse gate)
        self._fallback_receive_engines: list[int] = []
        # Bulk completion is independent per receiver. RdmaEngine exposes blocking wait(handles), but no
        # wait-any primitive, so one endpoint-lifetime waiter per destination turns each completion directly
        # into that receiver's DATA-ready flag without blocking the Stage-2 thread's ACK scan for others.
        self._wire_wait_queues: dict[int, queue.Queue] = {}
        self._wire_wait_threads: dict[int, threading.Thread] = {}
        self._wire_wait_lock = threading.Lock()

    def connect(self) -> None:
        """Query each engine coordinator's receiver count over ZMQ, then form the merged connection.

        Every Trainer rank opens a ZMQ ``DEALER`` to each per-engine coordinator (``receiver_urls``,
        ``tcp://``) and queries the receiver world size — the coordinator's ``ROUTER`` multiplexes all
        trainer clients. rank 0 then fires the ``connect`` message to each coordinator and joins the Gloo
        rendezvous BEFORE gathering the acks: the receiver joins Gloo while handling ``connect``, which
        needs rank 0 present, so awaiting the ack first would deadlock. :meth:`set_up_connection` runs on
        every Trainer rank with the same ``init_method`` + total ``world_size`` (trainers + all Rollout
        Workers).
        """
        import time as _time

        _t_start = _time.time()
        print(f"[wbridge-sender] rank {self.rank} entering connect()", flush=True)

        if self.group is not None:
            dist.destroy_process_group(self.group)

        # One DEALER per engine coordinator; the coordinator ROUTER serves every trainer rank.
        self._ctx = zmq.Context.instance()
        self._coord = [self._ctx.socket(zmq.DEALER) for _ in self.receiver_urls]
        for d, url in zip(self._coord, self.receiver_urls):
            d.connect(url)

        # Query receiver world sizes (retry the whole round-trip — the coordinator may not be bound yet).
        rollout_num_workers = [
            self._coord_request(i, {"type": WORLD_QUERY}, timeout_s=5.0, retries=60)[
                "world_size"
            ]
            for i in range(len(self._coord))
        ]
        total_world_size = self.world_size + sum(rollout_num_workers)

        pg_init_args = {
            "protocol": self.protocol,
            "init_method": self.init_method,
            "world_size": total_world_size,
            "rank": self.rank,
            "group_name": "wbridge",
            "sender_world_size": self.world_size,
        }

        if self.rank == 0:
            # Fire connect (non-blocking send) to each coordinator, contiguous rank base per engine — do
            # NOT await the ack before joining Gloo below (see the deadlock note in the docstring).
            base_rank = self.world_size
            for d, num_workers in zip(self._coord, rollout_num_workers):
                d.send(
                    json.dumps(
                        {**pg_init_args, "type": CONNECT, "rank": base_rank}
                    ).encode("utf-8")
                )
                base_rank += num_workers

        print(
            f"[wbridge-sender] rank {self.rank} total_ws={total_world_size}, entering set_up_connection "
            f"(elapsed={_time.time() - _t_start:.1f}s)",
            flush=True,
        )
        self.set_up_connection(**pg_init_args)

        if self.rank == 0:
            # Receiver ranks are contiguous by engine in the merged metadata group.  Most engine rank-0s
            # have trainer input and use its first data flag as the update doorbell.  Preserve a coordinator
            # notification only where no such flag can ever exist.
            receiver_local_base = 0
            self._fallback_receive_engines = []
            for engine_idx, num_workers in enumerate(rollout_num_workers):
                if not _receiver_rank_has_input(self.router, receiver_local_base):
                    self._fallback_receive_engines.append(engine_idx)
                receiver_local_base += num_workers
            if self._fallback_receive_engines:
                logger.warning(
                    "wbridge sender rank 0: engine(s) %s have a zero-input rank 0; retaining the legacy "
                    "per-update coordinator doorbell for those engine(s)",
                    self._fallback_receive_engines,
                )

        # Now gather the connect acks (they complete during set_up_connection's Gloo rendezvous).
        if self.rank == 0:
            for i, d in enumerate(self._coord):
                reply = self._coord_recv(d, timeout_s=600.0)
                if reply.get("status") != "success":
                    raise RuntimeError(
                        f"wbridge connect: coordinator {self.receiver_urls[i]} -> {reply}"
                    )

        # Only rank 0 drives connect and the zero-input fallback; other ranks needed world-query only.
        if self.rank != 0:
            for d in self._coord:
                d.close(linger=0)
            self._coord = []

        # Persistent Stage-2 (RDMA) thread: lets send() return a CUDA event after pack+offload while the
        # transfer overlaps the trainer's next step. Started once; daemon (dies with the process).
        self._sq = queue.Queue()
        self._cv = threading.Condition()
        self._completed = set()
        self._werr = []
        self._drained_count = 0
        # Flag writes publish directly from Stage-2 into exclusive per-round words. The reaper only retires
        # async handles; it does not serialize publication or participate in protocol progress.
        self._flag_reaper_ensure()
        self._send_thread = threading.Thread(
            target=self._send_worker, name="wbridge-send-adapter", daemon=True
        )
        self._send_thread.start()

        self.connected = True

    # ---- ZMQ coordinator helpers (rank 0 keeps DEALERs only for zero-input-rank fallback) ----
    def _coord_recv(self, dealer, *, timeout_s: float) -> dict:
        """Block up to *timeout_s* for a coordinator reply on *dealer*; raise on timeout."""
        if not dealer.poll(int(timeout_s * 1000)):
            raise TimeoutError(
                f"wbridge sender rank {self.rank}: no coordinator reply within {timeout_s:.0f}s"
            )
        return json.loads(dealer.recv().decode("utf-8"))

    def _coord_request(
        self, idx: int, msg: dict, *, timeout_s: float, retries: int
    ) -> dict:
        """Send *msg* to coordinator *idx* and return its reply, retrying the whole round-trip.

        On timeout the DEALER is discarded and recreated (dropping the queued send) so a late-starting
        coordinator doesn't accumulate duplicate requests. The surviving socket stays in ``self._coord``.
        """
        for _ in range(retries):
            d = self._coord[idx]
            d.send(json.dumps(msg).encode("utf-8"))
            if d.poll(int(timeout_s * 1000)):
                return json.loads(d.recv().decode("utf-8"))
            d.close(linger=0)
            nd = self._ctx.socket(zmq.DEALER)
            nd.connect(self.receiver_urls[idx])
            self._coord[idx] = nd
        raise RuntimeError(
            f"wbridge sender rank {self.rank}: coordinator {self.receiver_urls[idx]} request "
            f"{msg.get('type')!r} failed after {retries} attempts"
        )

    def _await_peer_consumed(self, peer: int, prev_ri: int, wt: int) -> None:
        """Wait until *peer* drained our previous use of its isolated trainer-ingress lane."""
        seq = wt * self.num_rounds + prev_ri + 1
        self._poll_flag(peer, seq)

    def _ensure_wire_waiters(self, peers: set[int]) -> None:
        """Create one persistent bulk-completion/DATA-ready waiter per destination."""
        # Direct integration tests construct WeightSender with __new__(), so keep this helper self-initializing.
        if not hasattr(self, "_wire_wait_lock"):
            self._wire_wait_queues = {}
            self._wire_wait_threads = {}
            self._wire_wait_lock = threading.Lock()
        with self._wire_wait_lock:
            for peer in sorted(peers):
                if peer in self._wire_wait_threads:
                    continue
                work_q: queue.Queue = queue.Queue()
                thread = threading.Thread(
                    target=self._wire_waiter,
                    args=(peer, work_q),
                    name=f"wbridge-wire-ready-{peer}",
                    daemon=True,
                )
                self._wire_wait_queues[peer] = work_q
                self._wire_wait_threads[peer] = thread
                thread.start()

    def _stop_wire_waiters(self) -> None:
        """Stop persistent destination waiters after Stage-2 has drained all submitted work."""
        if not hasattr(self, "_wire_wait_lock"):
            return
        with self._wire_wait_lock:
            queues = list(self._wire_wait_queues.values())
            threads = list(self._wire_wait_threads.values())
            self._wire_wait_queues = {}
            self._wire_wait_threads = {}
        for work_q in queues:
            work_q.put(None)
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=5.0)

    def _wire_waiter(self, peer: int, work_q: queue.Queue) -> None:
        """Turn one peer's ordered bulk completions directly into DATA-ready publications.

        A destination queue preserves that peer's generation order. Different queues wait concurrently, so
        a slow peer cannot delay completion detection or DATA-ready publication for any other receiver.
        """
        while True:
            task = work_q.get()
            if task is None:
                return
            wt, ri, seq, bid, submitted_at, submit_done_at, is_ipc, result_q = task
            error: BaseException | None = None
            wait_s = 0.0
            landed_at = submit_done_at
            wait_started_at = submitted_at
            try:
                if not is_ipc:
                    if bid is not None:
                        wait_t0 = time.perf_counter()
                        wait_started_at = time.time()
                        self.engine.wait([bid])
                        wait_s = time.perf_counter() - wait_t0
                        landed_at = time.time()
                # Nothing—not even Gantt bookkeeping—belongs between observed bulk completion and this
                # data-before-flag submission. The spans are recorded retrospectively below.
                flag_started_at = time.time()
                self._flag_emit(0, peer, seq)
                flag_submitted_at = time.time()
                if not is_ipc:
                    gantt.rec(
                        "send-adapter",
                        self._rank,
                        wt,
                        f"rdma_peer_wait_{peer}",
                        ri,
                        wait_started_at,
                        landed_at,
                    )
                    # This is the authoritative trainer->receiver wire interval used by receiver Gantts.
                    # Its t0 is sampled immediately before this peer's write_async submission, rather than
                    # inferred from when the receiver happened to enter its polling loop.
                    gantt.rec(
                        "send-wire",
                        self._rank,
                        wt,
                        f"rdma_peer_{peer}",
                        ri,
                        submitted_at,
                        landed_at,
                    )
                gantt.rec(
                    "send-adapter",
                    self._rank,
                    wt,
                    f"data_ready_peer_{peer}",
                    ri,
                    flag_started_at,
                    flag_submitted_at,
                )
                self._trace_state(
                    "sender_peer_data_ready",
                    epoch=wt,
                    round=ri,
                    peer=peer,
                    seq=seq,
                )
                if is_ipc:
                    # The receiver pulls from our pack buffer only after DATA-ready. Its ACK is therefore the
                    # source-lifetime fence; keeping this wait on the peer worker avoids blocking other peers.
                    with gantt.span(
                        "send-adapter",
                        self._rank,
                        wt,
                        f"ipc_pull_wait_peer_{peer}",
                        ri,
                    ):
                        self._poll_flag(peer, seq)
            except BaseException as exc:  # noqa: BLE001 - surfaced by the owning Stage-2 round
                error = exc
            result_q.put((peer, error, wait_s, landed_at))

    def _send_worker(self) -> None:
        """Persistent Stage-2: pull per-round items, RDMA-write, wait, write done-flags; drain per epoch.

        Uses the epoch *wt* carried in each item (NOT :attr:`_epoch`, which advances the instant send() has
        enqueued the epoch) for flag sequence numbers. Runs the same write+flag+ping-pong body as the old
        per-send completion thread; the source pointer per round is the GPU wire buffer (SS-off) or the CPU
        grid slot (SS-on), decided by send().
        """
        if getattr(self, "_relay_enabled", False):
            self._relay_send_worker()
            return
        # Remote RECV ownership is (receiver, parity, trainer-sender). Peer sets vary by round, so one scalar
        # predecessor per parity can wait for an unrelated receiver and miss the receiver actually being reused.
        recv_pred, last_recv_use = _recv_lane_predecessors(
            [list(overlaps) for _full, overlaps in self.router.local_rounds],
            self._recv_depth,
        )
        # A merged receiver overlays every trainer lane with one shared PREP parity. Its FREE/ACK therefore
        # describes the receiver's whole slot, not this sender's last contribution. Override both in-epoch
        # predecessors and epoch-drain generations with the receiver-published global slot schedule.
        for peer in getattr(self, "_merged_recv_peers", set()):
            for parity, ri in self._merged_recv_last[peer].items():
                last_recv_use[(peer, parity)] = ri
        while True:
            self._trace_state("sender_worker_idle")
            item = self._sq.get()
            if item is None:
                self._stop_wire_waiters()
                return
            if item[0] == "round":
                _, wt, ri, ready_ev, peers, srcs = item
                try:
                    self._trace_state(
                        "sender_round_enter", epoch=wt, round=ri, peers=peers
                    )
                    topo_debug = os.environ.get("WBRIDGE_TOPO_DEBUG") == "1"
                    if topo_debug:
                        print(
                            f"TDBG-SEND r{self._rank} wt{wt} ri{ri} ENTER peers={peers}",
                            flush=True,
                        )
                    # Stage-2 waits only for this round's source pack/offload. Destination ACK gates are
                    # scanned non-blockingly below; bulk completions block only their persistent peer waiter.
                    if ready_ev is not None:
                        with gantt.span(
                            "send-adapter", self._rank, wt, "offload_wait", ri
                        ):
                            self._trace_state(
                                "sender_pack_event_wait", epoch=wt, round=ri
                            )
                            ready_ev.synchronize()  # pack (SS-off) / D2H offload (SS) of ri complete
                            self._trace_state(
                                "sender_pack_event_done", epoch=wt, round=ri
                            )
                    dst = self._arena_send_dst
                    sz = self._fuse_sizes[ri]
                    # Co-located receivers PULL round ri out of our pack buffer over NVLink (CUDA-IPC) once
                    # they see the done-flag, so there is nothing for us to push: they are excluded from the
                    # RDMA batch entirely. Everyone else takes the unchanged RDMA path.
                    sn = [p for p in peers if p in self._sn_peers]
                    xn = [p for p in peers if p not in self._sn_peers]
                    seq = wt * self.num_rounds + ri + 1
                    gate_prev = {
                        p: (
                            self._merged_recv_pred[p].get(ri)
                            if p in self._merged_recv_peers
                            else recv_pred[ri].get(p)
                        )
                        for p in peers
                        # Non-merged same-node destinations are receiver-pulled and retain their existing
                        # inline ACK. Merged same-node destinations still need the whole-slot FREE gate.
                        if p in xn or p in self._merged_recv_peers
                    }
                    gate_seq = {
                        p: wt * self.num_rounds + prev_ri + 1
                        for p, prev_ri in gate_prev.items()
                        if prev_ri is not None
                    }
                    gate_started = time.time()
                    last_gate_at = gate_started
                    pending_submit = set(peers)
                    pending_complete = set(peers)
                    result_q: queue.Queue = queue.Queue()
                    self._ensure_wire_waiters(set(peers))
                    submit_first: float | None = None
                    submit_last: float | None = None
                    bulk_last: float | None = None
                    self._trace_state(
                        "sender_peer_progress_enter",
                        epoch=wt,
                        round=ri,
                        peers=peers,
                        gates=sorted((p, gate_prev[p]) for p in gate_seq),
                    )
                    if topo_debug and gate_seq:
                        print(
                            f"TDBG-SEND r{self._rank} wt{wt} ri{ri} AWAIT_ACK "
                            f"gates={sorted((p, gate_prev[p]) for p in gate_seq)}",
                            flush=True,
                        )
                    deadline = time.time() + 600.0
                    while pending_complete:
                        progress = False
                        # A peer becomes independently submit-eligible as soon as its own destination slot is
                        # free. Never block here: the scanner continues checking every other destination.
                        ready_peers = [
                            p
                            for p in sorted(pending_submit)
                            if p not in gate_seq or self._flag_reached(p, gate_seq[p])
                        ]
                        for p in ready_peers:
                            now = time.time()
                            if p in gate_seq:
                                gantt.rec(
                                    "send-adapter",
                                    self._rank,
                                    wt,
                                    f"await_peer_{p}",
                                    ri,
                                    gate_started,
                                    now,
                                )
                                last_gate_at = max(last_gate_at, now)
                            is_ipc = p in sn
                            submitted_at = now
                            submit_done_at = now
                            bid = None
                            if not is_ipc and dst[p][ri] is not None:
                                self._trace_state(
                                    "sender_peer_bulk_submit",
                                    epoch=wt,
                                    round=ri,
                                    peer=p,
                                )
                                submit_perf = time.perf_counter()
                                submitted_at = time.time()
                                with gantt.span(
                                    "send-adapter",
                                    self._rank,
                                    wt,
                                    f"rdma_peer_submit_{p}",
                                    ri,
                                ):
                                    bid = self.engine.write_async(
                                        self.peer_session[p],
                                        [srcs[p]],
                                        [dst[p][ri]],
                                        [sz[p]],
                                    )
                                submit_done_at = time.time()
                                if self._ctlp:
                                    self._ctl_acc(
                                        "b_submit", time.perf_counter() - submit_perf, 0
                                    )
                                    self._ctl_acc("b_peers", 1.0, 0)
                                    self._ctl_acc(
                                        "b_handles", float(bid is not None), 0
                                    )
                                self._tstats["wire_rdma_bytes"] += sz[p]
                                if self._ctlp:
                                    self._ctl_bytes = (
                                        getattr(self, "_ctl_bytes", 0) + sz[p]
                                    )
                                submit_first = (
                                    submitted_at
                                    if submit_first is None
                                    else min(
                                        submit_first,
                                        submitted_at,
                                    )
                                )
                                submit_last = (
                                    submit_done_at
                                    if submit_last is None
                                    else max(
                                        submit_last,
                                        submit_done_at,
                                    )
                                )
                            elif is_ipc:
                                self._tstats["wire_ipc_bytes"] += sz[p]
                            self._wire_wait_queues[p].put(
                                (
                                    wt,
                                    ri,
                                    seq,
                                    bid,
                                    submitted_at,
                                    submit_done_at,
                                    is_ipc,
                                    result_q,
                                )
                            )
                            pending_submit.remove(p)
                            progress = True

                        # Peer waiters publish DATA-ready before reporting completion. Drain every result that
                        # is already available, then return to ACK scanning without waiting on a slow handle.
                        while True:
                            try:
                                peer, error, wait_s, landed_at = result_q.get_nowait()
                            except queue.Empty:
                                break
                            if error is not None:
                                raise RuntimeError(
                                    f"wbridge sender rank {self._rank}: peer {peer} completion/DATA-ready "
                                    f"failed at epoch={wt} round={ri}"
                                ) from error
                            if peer not in pending_complete:
                                raise RuntimeError(
                                    f"wbridge sender rank {self._rank}: duplicate completion for peer "
                                    f"{peer} epoch={wt} round={ri}"
                                )
                            pending_complete.remove(peer)
                            if peer in xn and dst[peer][ri] is not None:
                                bulk_last = (
                                    landed_at
                                    if bulk_last is None
                                    else max(bulk_last, landed_at)
                                )
                                if self._ctlp:
                                    self._ctl_acc("b_wait", wait_s, 0)
                            progress = True

                        if pending_complete and not progress:
                            now = time.time()
                            if now >= deadline:
                                raise TimeoutError(
                                    f"wbridge sender rank {self._rank}: waited 600s for peer progress "
                                    f"epoch={wt} round={ri} submit={sorted(pending_submit)} "
                                    f"complete={sorted(pending_complete)}"
                                )
                            time.sleep(1e-4)

                    if gate_seq:
                        gantt.rec(
                            "send-adapter",
                            self._rank,
                            wt,
                            "await",
                            ri,
                            gate_started,
                            last_gate_at,
                        )
                    if submit_first is not None and submit_last is not None:
                        gantt.rec(
                            "send-adapter",
                            self._rank,
                            wt,
                            "rdma_submit",
                            ri,
                            submit_first,
                            submit_last,
                        )
                    if submit_first is not None and bulk_last is not None:
                        # Backward-compatible aggregate envelope. Exact destination intervals are the
                        # rdma_peer_<receiver-rank> records emitted by the persistent waiters.
                        gantt.rec(
                            "send-adapter",
                            self._rank,
                            wt,
                            "rdma_wait",
                            ri,
                            submit_first,
                            bulk_last,
                        )
                        gantt.rec(
                            "send-adapter",
                            self._rank,
                            wt,
                            "rdma_write",
                            ri,
                            submit_first,
                            bulk_last,
                        )
                    self._trace_state(
                        "sender_input_flags_done",
                        epoch=wt,
                        round=ri,
                        seq=seq,
                        peers=peers,
                    )
                    if topo_debug:
                        print(
                            f"TDBG-SEND r{self._rank} wt{wt} ri{ri} RECV_FLAGS_DONE seq={seq}",
                            flush=True,
                        )
                    with self._cv:
                        self._completed.add(ri)
                        self._cv.notify_all()
                    self._trace_state("sender_round_done", epoch=wt, round=ri, seq=seq)
                except BaseException as e:  # noqa: BLE001
                    with self._cv:
                        self._werr.append(e)
                        self._cv.notify_all()
                    self._stop_wire_waiters()
                    return
            else:  # ("epoch_end", wt, last_users)
                _, wt, last_users = item
                try:
                    self._trace_state(
                        "sender_epoch_drain",
                        epoch=wt,
                        rounds=last_users,
                        recv_lanes=[
                            (p, par, ri)
                            for (p, par), ri in sorted(last_recv_use.items())
                        ],
                    )
                    with gantt.span("send-adapter", self._rank, wt, "drain", -1):
                        # Every remote ingress lane must be drained before epoch N+1 can reuse it. Same-node
                        # pack readers were already waited inline above, immediately after their round.
                        for (p, _par), prev_ri in sorted(last_recv_use.items()):
                            self._await_peer_consumed(p, prev_ri, wt)
                    # Every last-round ACK above causally requires its sender completion flag to have landed.
                    # Surface a failure already observed by the off-path handle reaper, but do not wait for
                    # outstanding flag writes: exclusive slots and causal reuse make that unnecessary.
                    self._flag_reaper_check()
                except BaseException as e:  # noqa: BLE001
                    with self._cv:
                        self._werr.append(e)
                        self._cv.notify_all()
                    self._stop_wire_waiters()
                    return
                # Detach every event/control sample while the epoch is quiescent, but do not write anything
                # yet. Publish transfer completion first so post-block profiling I/O can never become the
                # next send()'s previous-epoch wait.
                profile_output = self._take_profile_output(wt, self.num_rounds)
                with self._cv:
                    self._drained_count += 1
                    self._cv.notify_all()
                self._trace_state("sender_epoch_drained", epoch=wt)
                # The trainer may already have recorded block_end, or may still be inside an equality/debug
                # completion wait. The per-epoch gate emits in the former case and parks the snapshot in the
                # latter; both orderings keep file/logger output outside the reported interval.
                self._defer_profile_output(wt, profile_output)

    def _relay_send_worker_legacy(self) -> None:
        """Stage-2 progress for ordered, independent trainer→group-head edges.

        A round-wide completion barrier turns one slow group into a burst/gap pattern on every other edge.
        Instead, each ``(head, group)`` edge advances independently.  Write ``r`` is submitted only after
        that edge's write ``r-1`` completed and the head ACKed the previous use of its destination parity
        (normally ``r-2``).  Different edges remain concurrent.
        """
        while True:
            item = self._sq.get()
            if item is None:
                self._stop_relay_bulk_waiters()
                return
            try:
                if item[0] != "relay_round":
                    raise RuntimeError(
                        f"relay epoch must start with a round, got {item[0]!r}"
                    )
                wt = item[1]
                result_q: queue.Queue = queue.Queue()
                states: dict[tuple[int, int], dict] = {}
                previous_operation: dict[tuple[int, int], tuple[int, int]] = {}
                previous_parity: dict[tuple[int, int, int], tuple[int, int]] = {}
                epoch_active: list[tuple[int, int]] | None = None
                deadline = time.time() + 600.0

                def ingest(current: tuple) -> None:
                    nonlocal epoch_active
                    kind = current[0]
                    if kind == "relay_round":
                        _, current_wt, ri, ready_event, gids = current
                        if current_wt != wt:
                            raise RuntimeError(
                                f"interleaved relay epochs {wt} and {current_wt}"
                            )
                        edges = set()
                        for gid in gids:
                            key = (ri, gid)
                            if key in states:
                                raise RuntimeError(f"duplicate relay send state {key}")
                            head = self.router.relay_group(gid)["head"]
                            edge = (head, gid)
                            parity_edge = (head, gid, ri % 2)
                            states[key] = {
                                "ri": ri,
                                "gid": gid,
                                "seq": wt * self.num_rounds + ri + 1,
                                "head": head,
                                "ready_event": ready_event,
                                "pack_ready": ready_event is None,
                                "pack_ready_at": time.time()
                                if ready_event is None
                                else None,
                                "write_pred": previous_operation.get(edge),
                                "parity_pred": previous_parity.get(parity_edge),
                                "submitted": False,
                                "write_done": False,
                            }
                            previous_operation[edge] = key
                            previous_parity[parity_edge] = key
                            edges.add(edge)
                        self._ensure_relay_bulk_waiters(edges)
                    elif kind == "relay_epoch_end":
                        _, current_wt, active = current
                        if current_wt != wt:
                            raise RuntimeError(
                                f"interleaved relay epoch end {current_wt}, expected {wt}"
                            )
                        if epoch_active is not None:
                            raise RuntimeError(f"duplicate relay epoch end for {wt}")
                        epoch_active = list(active)
                    else:
                        raise RuntimeError(f"unexpected relay sender item {kind!r}")

                ingest(item)
                while True:
                    progress = False

                    # Keep accepting newly packed rounds while older edges make progress.  In particular,
                    # never wait for every group in round r before admitting round r+1.
                    while True:
                        try:
                            queued = self._sq.get_nowait()
                        except queue.Empty:
                            break
                        if queued is None:
                            raise RuntimeError(
                                f"relay sender stopped during epoch {wt}"
                            )
                        ingest(queued)
                        progress = True

                    while True:
                        try:
                            peer, gid, ri, seq, error, _landed_at = (
                                result_q.get_nowait()
                            )
                        except queue.Empty:
                            break
                        key = (ri, gid)
                        state = states.get(key)
                        if (
                            state is None
                            or state["head"] != peer
                            or state["seq"] != seq
                        ):
                            raise RuntimeError(
                                f"unexpected trainer relay completion peer={peer} group={gid} "
                                f"round={ri} seq={seq}"
                            )
                        if error is not None:
                            raise RuntimeError(
                                f"trainer relay edge failed peer={peer} group={gid} "
                                f"epoch={wt} round={ri}"
                            ) from error
                        if state["write_done"]:
                            raise RuntimeError(
                                f"duplicate trainer relay completion {key}"
                            )
                        state["write_done"] = True
                        with self._cv:
                            self._relay_completed.add(key)
                            self._cv.notify_all()
                        progress = True

                    for key in sorted(states):
                        state = states[key]
                        ready_event = state["ready_event"]
                        if (
                            not state["pack_ready"]
                            and ready_event is not None
                            and ready_event.query()
                        ):
                            state["pack_ready"] = True
                            state["pack_ready_at"] = time.time()
                            progress = True
                        if not state["pack_ready"] or state["submitted"]:
                            continue

                        write_pred = state["write_pred"]
                        if (
                            write_pred is not None
                            and not states[write_pred]["write_done"]
                        ):
                            continue
                        parity_pred = state["parity_pred"]
                        if parity_pred is not None:
                            prior = states[parity_pred]
                            if not self._relay_flag_reached(
                                self._RELAY_ACK_KIND,
                                state["head"],
                                state["gid"],
                                prior["seq"],
                            ):
                                continue

                        ri, gid, head = state["ri"], state["gid"], state["head"]
                        size = self._relay_sizes[ri][gid]
                        src = self._relay_send_buf[gid][ri % 2].data_ptr()
                        dst = self._relay_send_dst[gid][ri]
                        if dst is None:
                            raise RuntimeError(
                                f"relay head {head} has no destination group={gid} round={ri}"
                            )
                        submitted_at = time.time()
                        handle = self.engine.write_async(
                            self._relay_peer_session[head],
                            [src],
                            [dst],
                            [size],
                        )
                        self._tstats["wire_rdma_bytes"] += size
                        self._relay_bulk_wait_queues[(head, gid)].put(
                            (
                                wt,
                                ri,
                                state["seq"],
                                handle,
                                submitted_at,
                                "trainer_head",
                                result_q,
                            )
                        )
                        state["submitted"] = True
                        gantt.rec(
                            "send-adapter",
                            self._rank,
                            wt,
                            "relay_dispatch_gate",
                            ri,
                            state["pack_ready_at"],
                            submitted_at,
                        )
                        progress = True

                    if epoch_active is not None:
                        if set(epoch_active) != set(states):
                            raise RuntimeError(
                                f"relay epoch {wt} states disagree: queued={sorted(states)} "
                                f"declared={sorted(epoch_active)}"
                            )
                        if all(state["write_done"] for state in states.values()):
                            # Heads return a DATA sequence only after local readers and the full downstream
                            # chain consumed this group/round. This preserves wait_send_complete().
                            with gantt.span(
                                "send-adapter", self._rank, wt, "relay_drain", -1
                            ):
                                for ri, gid in epoch_active:
                                    head = self.router.relay_group(gid)["head"]
                                    seq = wt * self.num_rounds + ri + 1
                                    self._poll_relay_flag(
                                        self._RELAY_DATA_KIND,
                                        head,
                                        gid,
                                        seq,
                                    )
                            self._flag_reaper_check()
                            profile_output = self._take_profile_output(
                                wt, self.num_rounds
                            )
                            with self._cv:
                                self._drained_count += 1
                                self._cv.notify_all()
                            self._defer_profile_output(wt, profile_output)
                            break

                    if not progress:
                        if time.time() >= deadline:
                            pending = {
                                key: {
                                    field: state[field]
                                    for field in (
                                        "pack_ready",
                                        "submitted",
                                        "write_done",
                                    )
                                }
                                for key, state in states.items()
                                if not state["write_done"]
                            }
                            raise TimeoutError(
                                f"trainer relay progress timeout epoch={wt} pending={pending} "
                                f"epoch_end={epoch_active is not None}"
                            )
                        try:
                            queued = self._sq.get(timeout=1e-4)
                        except queue.Empty:
                            continue
                        if queued is None:
                            raise RuntimeError(
                                f"relay sender stopped during epoch {wt}"
                            )
                        ingest(queued)
            except BaseException as exc:  # noqa: BLE001
                with self._cv:
                    self._werr.append(exc)
                    self._cv.notify_all()
                self._stop_relay_bulk_waiters()
                return

    def _relay_group_send_lane(
        self,
        gid: int,
        work_q: queue.Queue,
        done_q: queue.Queue,
    ) -> None:
        """Persistent trainer→head progress lane for one replica group.

        Blocking pack-event, RDMA-completion, ACK, and full-delivery waits in one group never hold progress
        for another group. Processing round tasks serially deliberately preserves the requested write r-1
        completion dependency; the remembered parity sequence is the independent r-2 destination fence.
        """
        group = self.router.relay_group(gid)
        head = group["head"]
        parity_last: dict[int, int] = {}
        while True:
            task = work_q.get()
            if task is None:
                return
            kind = task[0]
            try:
                if kind == "round":
                    _, wt, ri, ready_event = task
                    seq = wt * self.num_rounds + ri + 1
                    if ready_event is not None:
                        ready_event.synchronize()
                    pack_ready_at = time.time()
                    prior_seq = parity_last.get(ri % 2)
                    if prior_seq is not None:
                        self._poll_relay_flag(
                            self._RELAY_ACK_KIND,
                            head,
                            gid,
                            prior_seq,
                        )
                    size = self._relay_sizes[ri][gid]
                    src = self._relay_send_buf[gid][ri % 2].data_ptr()
                    dst = self._relay_send_dst[gid][ri]
                    if dst is None:
                        raise RuntimeError(
                            f"relay head {head} has no destination group={gid} round={ri}"
                        )
                    submitted_at = time.time()
                    handle = self.engine.write_async(
                        self._relay_peer_session[head],
                        [src],
                        [dst],
                        [size],
                    )
                    self._tstats["wire_rdma_bytes"] += size
                    if handle is not None:
                        self.engine.wait([handle])
                    landed_at = time.time()
                    # Preserve the data-before-DATA publication fence on this exact group lane.
                    self._relay_emit(self._RELAY_DATA_KIND, head, gid, seq)
                    gantt.rec(
                        "relay-wire",
                        self._rank,
                        wt,
                        f"trainer_head_peer_{head}_group_{gid}",
                        ri,
                        submitted_at,
                        landed_at,
                    )
                    gantt.rec(
                        "send-adapter",
                        self._rank,
                        wt,
                        "relay_dispatch_gate",
                        ri,
                        pack_ready_at,
                        submitted_at,
                    )
                    parity_last[ri % 2] = seq
                    with self._cv:
                        self._relay_completed.add((ri, gid))
                        self._cv.notify_all()
                elif kind == "epoch_end":
                    _, wt, rounds = task
                    with gantt.span("send-adapter", self._rank, wt, "relay_drain", -1):
                        for ri in rounds:
                            self._poll_relay_flag(
                                self._RELAY_DATA_KIND,
                                head,
                                gid,
                                wt * self.num_rounds + ri + 1,
                            )
                    done_q.put((gid, wt, None))
                else:
                    raise RuntimeError(f"unexpected relay group task {kind!r}")
            except BaseException as exc:  # noqa: BLE001 - surfaced by the adapter coordinator
                done_q.put((gid, task[1] if len(task) > 1 else -1, exc))
                return

    def _relay_send_worker(self) -> None:
        """Fan packed rounds into one persistent progress thread per replica group."""
        group_ids = sorted(self._relay_send_buf)
        done_q: queue.Queue = queue.Queue()
        group_queues = {gid: queue.Queue() for gid in group_ids}
        threads = {
            gid: threading.Thread(
                target=self._relay_group_send_lane,
                args=(gid, group_queues[gid], done_q),
                name=f"wbridge-relay-send-g{gid}",
                daemon=True,
            )
            for gid in group_ids
        }
        for thread in threads.values():
            thread.start()
        try:
            while True:
                item = self._sq.get()
                if item is None:
                    return
                kind = item[0]
                if kind == "relay_round":
                    _, wt, ri, ready_event, gids = item
                    for gid in gids:
                        group_queues[gid].put(("round", wt, ri, ready_event))
                    continue
                if kind != "relay_epoch_end":
                    raise RuntimeError(f"unexpected relay adapter item {kind!r}")
                _, wt, active = item
                active_by_gid: dict[int, list[int]] = {}
                for ri, gid in active:
                    active_by_gid.setdefault(gid, []).append(ri)
                for gid, rounds in active_by_gid.items():
                    group_queues[gid].put(("epoch_end", wt, rounds))
                errors = []
                for _ in active_by_gid:
                    result_gid, result_wt, error = done_q.get()
                    if result_wt != wt or result_gid not in active_by_gid:
                        errors.append(
                            RuntimeError(
                                f"unexpected relay group completion gid={result_gid} wt={result_wt}; "
                                f"expected wt={wt} groups={sorted(active_by_gid)}"
                            )
                        )
                    if error is not None:
                        errors.append(error)
                if errors:
                    raise errors[0]
                self._flag_reaper_check()
                profile_output = self._take_profile_output(wt, self.num_rounds)
                with self._cv:
                    self._drained_count += 1
                    self._cv.notify_all()
                self._defer_profile_output(wt, profile_output)
        except BaseException as exc:  # noqa: BLE001
            with self._cv:
                self._werr.append(exc)
                self._cv.notify_all()
        finally:
            for work_q in group_queues.values():
                work_q.put(None)
            for thread in threads.values():
                thread.join(timeout=5.0)

    def _relay_send(self) -> "torch.cuda.Event | None":
        wt = self._epoch
        with self._cv:
            while self._drained_count < wt and not self._werr:
                self._cv.wait()
            if self._werr:
                raise self._werr[0]
            self._relay_completed = set()

        if self.rank == 0 and self._fallback_receive_engines:
            for index in self._fallback_receive_engines:
                self._coord[index].send(json.dumps({"type": RECEIVE}).encode("utf-8"))
            for index in self._fallback_receive_engines:
                reply = self._coord_recv(self._coord[index], timeout_s=600.0)
                if reply.get("status") != "success":
                    raise RuntimeError(f"relay receive fallback failed: {reply}")

        active = [
            (ri, sorted(specs))
            for ri, specs in enumerate(self._relay_send_specs)
            if specs
        ]
        last_user: dict[tuple[int, int], tuple[int, int]] = {}
        returned_event = None
        previous_pack_event = None
        active_edges: list[tuple[int, int]] = []
        for ri, gids in active:
            # Keep copy-engine/kernel dispatch ordered across adjacent rounds even though they use opposite
            # parity buffers.  This is an explicit completion dependency (rather than merely relying on
            # same-stream launch order), matching the per-edge write/assemble/consume pacing below.
            if previous_pack_event is not None:
                previous_pack_event.synchronize()
            waits = {
                last_user[(gid, ri % 2)] for gid in gids if (gid, ri % 2) in last_user
            }
            if waits:
                with self._cv:
                    while not waits <= self._relay_completed and not self._werr:
                        self._cv.wait()
                    if self._werr:
                        raise self._werr[0]
            with gantt.span("sender", self._rank, wt, "relay_pack", ri):
                plan = self._relay_pack_plans[ri]
                if plan is not None:
                    plan.run()
            ready_event = torch.cuda.Event() if torch.cuda.is_available() else None
            if ready_event is not None:
                ready_event.record()
            self._sq.put(("relay_round", wt, ri, ready_event, gids))
            for gid in gids:
                last_user[(gid, ri % 2)] = (ri, gid)
                active_edges.append((ri, gid))
            returned_event = ready_event
            previous_pack_event = ready_event
        self._sq.put(("relay_epoch_end", wt, active_edges))
        self._epoch += 1
        return returned_event

    def send(self) -> "torch.cuda.Event | None":
        """Pack (+ CPU offload for sender-staging) on this thread; RDMA on the persistent Stage-2 thread.

        Returns a :class:`torch.cuda.Event` that fires when packing (+ the CPU offload, under SS) is
        complete — i.e. the model weights are safe to overwrite. The GPU->remote (SS-off) or CPU->remote
        (SS) RDMA then finishes asynchronously on :meth:`_send_worker`; the receiver's own
        ``poll_requests`` call gates its generation on arrival, and the NEXT send() blocks at entry until this
        epoch has fully drained (so the wire buffers / CPU grid are free to reuse). Returns ``None`` off-GPU.
        """
        if (
            not self.connected
            or self.shard_spec is None
            or self.router is None
            or self.engine is None
            or self.device is None
        ):
            raise RuntimeError("WeightSender.send requires connect() first")
        if getattr(self, "_relay_enabled", False):
            return self._relay_send()

        wt = self._epoch
        ss = self.sender_staging
        self._trace_state("send_enter", epoch=wt)

        # Gate on the previous epoch's full drain (all RDMA landed + all RECV consumed => buffers free), then
        # reset the per-epoch round-completion set the Stage-2 thread populates (and the SS-off pack gate reads).
        with self._cv:
            while self._drained_count < wt and not self._werr:
                self._trace_state(
                    "send_previous_epoch_wait", epoch=wt, drained=self._drained_count
                )
                self._cv.wait()
            if self._werr:
                raise self._werr[0]
            self._completed = set()
        self._offload_ev = {}

        if self.rank == 0 and self._fallback_receive_engines:
            with gantt.span("sender", self._rank, wt, "receive_post", 0):
                self._trace_state(
                    "send_receive_fallback_post",
                    epoch=wt,
                    engines=self._fallback_receive_engines,
                )
                for i in self._fallback_receive_engines:
                    self._coord[i].send(json.dumps({"type": RECEIVE}).encode("utf-8"))
                for i in self._fallback_receive_engines:
                    d = self._coord[i]
                    reply = self._coord_recv(d, timeout_s=600.0)
                    if reply.get("status") != "success":
                        raise RuntimeError(
                            f"wbridge receive fallback: coordinator {self.receiver_urls[i]} -> {reply}"
                        )

        router = self.router
        rounds = [(ri, fs, ov) for ri, (fs, ov) in enumerate(router.local_rounds) if ov]
        if self._rank == 0 and wt == 0:
            _tot = sum(
                self._fuse_sizes[ri].get(p, 0) for ri, _, ov in rounds for p in ov
            )
            logger.info(
                "wbridge rank 0: sender RDMA = %.3f GiB/epoch over %d rounds (for BW: bytes / rdma_write span)",
                _tot / 1024**3,
                len(rounds),
            )
        last_user: dict[int, int] = {}
        returned_ev = None
        for ri, full_spec, overlap_specs in rounds:
            b = ri % self._NUM_BUF
            peers = list(overlap_specs.keys())
            if b in last_user:  # gate reuse of GPU wire buffer b
                if ss:
                    # Freed by its D2H offload; make this pack's stream wait that event (GPU dep, no host block).
                    if self._offload_ev.get(last_user[b]) is not None:
                        torch.cuda.current_stream().wait_event(
                            self._offload_ev[last_user[b]]
                        )
                else:
                    with self._cv:  # freed when the Stage-2 RDMA read completed
                        while last_user[b] not in self._completed and not self._werr:
                            self._trace_state(
                                "send_pack_buffer_wait",
                                epoch=wt,
                                round=ri,
                                prior_round=last_user[b],
                            )
                            self._cv.wait()
                    if self._werr:
                        raise self._werr[0]
            with gantt.span("sender", self._rank, wt, "pack", ri):
                self._trace_state("send_pack", epoch=wt, round=ri, peers=peers)
                if self._fuse_fallback[ri]:
                    self._two_stage_save(
                        full_spec,
                        overlap_specs,
                        {p: self._data_buf[p][b] for p in peers},
                    )
                else:
                    self._fuse_plans[ri].run()
            if ss:
                # Offload each peer's packed prefix GPU wire -> CPU grid slot (one batched Local D2H hop).
                srcs_off = [self._data_buf[p][b].data_ptr() for p in peers]
                dsts_off = [self._cpu_grid[p][ri].data_ptr() for p in peers]
                szs_off = [self._fuse_sizes[ri][p] for p in peers]
                ready_ev = self.local_engine.write_async(
                    self.local_engine.session_id(), srcs_off, dsts_off, szs_off
                )
                self._offload_ev[ri] = ready_ev
                rdma_srcs = {
                    p: self._cpu_grid[p][ri].data_ptr() for p in peers
                }  # RDMA reads the CPU grid
            else:
                # Same-node peers read this buffer from another process, so give them a GPU-visible fence
                # for "pack(ri) landed" on the stream pack ran on. Recorded before the done-flag (which
                # Stage-2 writes only after ready_ev.synchronize()), so a puller that observes the flag can
                # only ever wait on this record or a later one — never an earlier one.
                for p in peers:
                    if p in self._sn_peers:
                        self._pack_ready_event[p].record()
                ready_ev = torch.cuda.Event() if torch.cuda.is_available() else None
                if ready_ev is not None:
                    ready_ev.record()  # pack complete (default stream)
                rdma_srcs = {
                    p: self._data_buf[p][b].data_ptr() for p in peers
                }  # RDMA reads the GPU wire
            self._sq.put(("round", wt, ri, ready_ev, peers, rdma_srcs))
            self._trace_state("send_round_queued", epoch=wt, round=ri, peers=peers)
            returned_ev = ready_ev
            last_user[b] = ri
        self._sq.put(("epoch_end", wt, sorted(set(last_user.values()))))
        self._epoch += 1
        self._trace_state("send_enqueued", epoch=wt)
        return returned_ev

    def wait_send_complete(self) -> None:
        """Block until the last :meth:`send`'s epoch has fully drained on the Stage-2 thread — i.e. every
        receiver has consumed (loaded) the update, so 'weights delivered on return' holds. Used in debugging
        mode (``--check-weight-update-equal``), where the trainer reads the rollout's weights immediately
        after; production skips it (the rollout's own poll_requests call gates generation) so the CPU->remote
        RDMA overlaps the trainer's next step."""
        with self._cv:
            while self._drained_count < self._epoch and not self._werr:
                self._trace_state(
                    "send_complete_wait",
                    epoch=self._epoch - 1,
                    drained=self._drained_count,
                )
                self._cv.wait()
            if self._werr:
                raise self._werr[0]
        self._trace_state("send_complete", epoch=self._epoch - 1)
