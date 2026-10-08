# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Rollout Worker side of WeightBridge: the per-worker :class:`WeightReceiver`.

Control-plane model (per rollout engine):

* A standalone **coordinator** process (:mod:`wbridge.backend.coordinator`) handles discovery and relays the
  one-time ``connect`` request over ZMQ to **only receiver rank 0** (identity ``worker-0``). Per-update
  readiness normally comes from the data plane itself: a sender completion sequence in pinned CPU memory.
* **Receiver rank 0** is the sole decision maker. Each :meth:`WeightReceiver.poll_requests` round it peeks
  its first active input round and forwards one ``empty|kick|receive`` decision to the other TP ranks over a
  ZMQ **star** (IPC for single-node engines, TCP for multi-node engines; see
  :class:`~wbridge.backend.control_channel.ControlChannel`).
* **Every other rank joins** the same :meth:`WeightReceiver.poll_requests` round and waits for rank 0's
  decision, so all TP ranks act in lockstep. Connect and the weight transfer both run on the **main
  thread, blocking**, inside that call.

The data plane is one-sided RDMA writes (see :class:`~wbridge.backend.router.WBEndpoint`), not a process
group. :meth:`WeightReceiver.poll_requests` drives the transfer from the main thread.  The default
receiver schedule is serial; the opt-in depth-2 three-stage schedule moves external exchange + internal
consume onto a cross-round progress worker so sender landing, fused prepare, and two independently-gated E+C
rounds can occupy the parity slots concurrently. External completion waiters persist across updates; the
per-update progress worker is joined before ``poll_requests`` returns. There is no ``cpu_direct`` mode.
Replica-group relay is the exception: the main thread returns after its own model consumes and successor
writes finish. A persistent retirement worker keeps PREP lifetime accounting and recursive full-delivery
notification off that blocking path.
"""

import logging
import os
import queue
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

import torch
from wbridge.backend import gantt
from wbridge.backend.control_channel import (
    CONNECT_REQUEST,
    ControlChannel,
    CoordinatorClient,
    EMPTY,
    RECEIVE_KICK,
    RECEIVE_REQUEST,
)
from wbridge.backend.router import (
    _arena_peer_predecessors,
    _arena_slot_offset,
    _arena_slot_predecessors,
    _relay_round_predecessors,
    WBEndpoint,
)
from wbridge.utils.data import LoadSpec, ShardSpec

logger = logging.getLogger(__name__)


def _doff_source_predecessors(
    active_rounds: list[int],
    ext_recv_by_round: list[tuple[int, ...]],
    depth: int,
    rank: int,
) -> list[dict[int, tuple[int, int]]]:
    """Return cyclic reuse predecessors for each fixed ``(source, DOFF slot)``.

    This is topology-pipeline metadata.  Callers on the generic serial path must
    not build it because that path intentionally has no topology round tables.
    """
    if depth <= 0:
        raise ValueError(f"DOFF depth must be positive, got {depth}")
    predecessors: list[dict[int, tuple[int, int]]] = [{} for _ in ext_recv_by_round]
    final: dict[tuple[int, int], int] = {}
    for ri in active_rounds:
        for source in {rank, *ext_recv_by_round[ri]}:
            final[(source, ri % depth)] = ri
    previous: dict[tuple[int, int], int] = {}
    for ri in active_rounds:
        for source in {rank, *ext_recv_by_round[ri]}:
            key = (source, ri % depth)
            predecessors[ri][source] = (
                (0, previous[key]) if key in previous else (-1, final[key])
            )
            previous[key] = ri
    return predecessors


class WeightReceiver(WBEndpoint):
    """One receiver per rollout TP rank.

    Rank 0 mediates the control plane via :class:`~wbridge.backend.control_channel.ControlChannel`; all
    ranks perform ``connect`` / ``_receive_weights`` on the main thread inside :meth:`poll_requests`.
    """

    def __init__(
        self,
        controller_ipc_name: str,
        rank: int,
        shard_spec: ShardSpec,
        dtype_spec: dict[str, torch.dtype],
        load_spec: LoadSpec,
        wksd: dict[str, torch.Tensor],
        *,
        num_workers: int,
        control_hub_name: str | None = None,
        receiver_staging: bool = False,
    ) -> None:
        self.cuda_device = f"cuda:{torch.cuda.current_device()}"

        self.rank = rank
        self.num_workers = num_workers
        self.shard_spec = shard_spec
        self.dtype_spec = dtype_spec
        # HF<->worker mapping + live params; the fused wire->model CopyPlan is built once at connect.
        self.load_spec = load_spec
        self.wksd = wksd
        self.receiver_staging = (
            receiver_staging  # WBEndpoint switch (read by router._init_engine/_setup)
        )

        self._connected = False
        self._recv_count = (
            0  # RECEIVE_REQUESTs handled (for WBRIDGE_RECV_PROFILE_WT targeting)
        )
        # Internal two-phase state used by poll_requests: readiness selects a decision, then the same
        # scheduler-thread call executes it.
        self._pending: Optional[dict[str, Any]] = None

        # Hub star (MAIN thread, all ranks): rank0 broadcasts one decision per tick, peers block for it. This
        # per-tick rendezvous keeps the engine's TP ranks in lockstep, so a blocking collective (the connect
        # rendezvous, the receiver<->receiver all-gather) is entered by all ranks together — never with one
        # rank stuck in an unrelated collective (which is what deadlocked when this was decoupled). Created +
        # used only on the main thread.
        self._chan = ControlChannel(
            controller_ipc_name,
            rank,
            num_workers,
            hub_endpoint=control_hub_name,
        )

        # rank0 ONLY: a receiving thread owns the trainer-facing coordinator intake. In normal operation it
        # carries discovery/connect only; RECEIVE is retained as a zero-input-rank fallback. It sets _ext_req;
        # the main thread reads it during poll_requests and broadcasts decisions via the hub (notification stays
        # on the main thread — that is what preserves TP lockstep).
        self._controller_ipc_name = controller_ipc_name
        self._ext_req: Optional[dict[str, Any]] = None
        self._ext_lock = threading.Lock()
        self._ext_cv = threading.Condition(self._ext_lock)
        self._stop = False
        self._ctrl_thread: Optional[threading.Thread] = None
        # External bulk completions use one persistent waiter per destination.  The transport exposes a
        # blocking wait rather than wait-any, so a single waiter would reintroduce head-of-line blocking when
        # overlapping groups send to several destinations.  These daemon workers span every transfer epoch;
        # individual E+C rounds only enqueue completed submissions and never call Thread.start().
        self._topo_out_wait_queues: dict[int, queue.Queue] = {}
        self._topo_out_wait_threads: dict[int, threading.Thread] = {}
        self._topo_out_wait_lock = threading.Lock()
        # Exact protocol generations observed/completed by the cross-round internal-consume dispatcher.
        self._topo_lane_seen: dict[tuple[int, int | None], int] = {}
        self._topo_lane_consumed: dict[tuple[int, int | None], int] = {}
        # Relay completion has two lifetimes. The scheduler-visible receive ends once this worker has
        # consumed its model data and every successor write has completed. PREP may still be read by a
        # same-node peer, and full-delivery DATA still has to propagate back through the chain. One persistent
        # retirement worker handles both without keeping the rollout engine blocked. PREP is released on the
        # foreground path after forwarding + offload; `_relay_doff_released` is the cross-epoch fence that
        # prevents an epoch-retired DOFF slot from being overwritten while a local reader still consumes it.
        self._relay_retire_lock = threading.Lock()
        self._relay_retire_q: queue.Queue | None = None
        self._relay_retire_thread: threading.Thread | None = None
        self._relay_retire_errors: list[BaseException] = []
        self._relay_prep_released: dict[tuple[int, int], int] = {}
        self._relay_doff_released: dict[tuple[int, int], int] = {}
        self._relay_group_executor = None
        if rank == 0:
            self._ctrl_thread = threading.Thread(
                target=self._coordinator_worker,
                name="wbridge-recv-adapter",
                daemon=True,
            )
            self._ctrl_thread.start()

        # RS-on: a per-rank receive-worker lands ALL sender writes into the full-depth CPU arena while the
        # scheduler keeps generating. Only after the complete local epoch is in CPU does it H2D the first
        # active round into the isolated GPU ingress arena and publish _rs_gpu_ready. Rank 0's local GPU-ready
        # flag causes GO; peers quiesce on that GO and wait for their own flag before assemble/repack.
        self._rs_cv = threading.Condition()
        self._rs_go: Optional[int] = (
            None  # epoch the main thread asked the worker to receive
        )
        self._rs_receiving = (
            -1
        )  # epoch currently in flight (avoids re-signaling every tick)
        self._rs_staged = -1  # epoch the worker has fully landed in CPU
        self._rs_gpu_ready = (
            -1
        )  # epoch whose first active round is fully landed in GPU RECV
        self._rs_kicked = -1  # epoch rank0 has broadcast a KICK for (phase-1 latch)
        self._rs_err: Optional[BaseException] = None
        self._recv_thread: Optional[threading.Thread] = None
        if receiver_staging:
            self._recv_thread = threading.Thread(
                target=self._recv_worker, name="wbridge-recv-cpu", daemon=True
            )
            self._recv_thread.start()

    def stop(self) -> None:
        """Stop rank0's coordinator thread + the RS receive-worker, and close the hub star."""
        self._stop = True
        self._stop_relay_retire_worker()
        self._stop_relay_bulk_waiters()
        executor = getattr(self, "_relay_group_executor", None)
        self._relay_group_executor = None
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
        with self._ext_cv:
            self._ext_cv.notify_all()
        with self._rs_cv:
            self._rs_cv.notify_all()
        with self._topo_out_wait_lock:
            topo_queues = list(self._topo_out_wait_queues.values())
            topo_threads = list(self._topo_out_wait_threads.values())
        for work_q in topo_queues:
            work_q.put(None)
        for th in (self._ctrl_thread, self._recv_thread, *topo_threads):
            if th is not None and th.is_alive():
                th.join(timeout=5.0)
        self._flag_reaper_stop()
        if self._chan is not None:
            self._chan.close()

    def _teardown(self) -> None:
        """Retire the relay control worker before the base tears down its transports and buffers."""
        self._stop_relay_retire_worker()
        executor = getattr(self, "_relay_group_executor", None)
        self._relay_group_executor = None
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
        super()._teardown()

    def _init_relay_retire_state(self) -> None:
        """Lazily initialize retirement state for low-level endpoints constructed with ``__new__``."""
        if hasattr(self, "_relay_retire_lock"):
            if not hasattr(self, "_relay_doff_released"):
                self._relay_doff_released = {}
            return
        self._relay_retire_lock = threading.Lock()
        self._relay_retire_q = None
        self._relay_retire_thread = None
        self._relay_retire_errors = []
        self._relay_prep_released = {}
        self._relay_doff_released = {}

    def _relay_retire_check(self) -> None:
        self._init_relay_retire_state()
        with self._relay_retire_lock:
            error = self._relay_retire_errors[0] if self._relay_retire_errors else None
        if error is not None:
            raise RuntimeError(
                f"wbridge rank {self._rank}: asynchronous relay retirement failed"
            ) from error

    def _ensure_relay_retire_worker(self) -> None:
        self._init_relay_retire_state()
        self._relay_retire_check()
        with self._relay_retire_lock:
            thread = self._relay_retire_thread
            if thread is not None:
                if not thread.is_alive():
                    raise RuntimeError(
                        f"wbridge rank {self._rank}: relay retirement worker stopped unexpectedly"
                    )
                return
            work_q: queue.Queue = queue.Queue()
            thread = threading.Thread(
                target=self._relay_retire_worker,
                args=(work_q,),
                name="wbridge-relay-retire",
                daemon=True,
            )
            self._relay_retire_q = work_q
            self._relay_retire_thread = thread
            thread.start()

    def _stop_relay_retire_worker(self) -> None:
        self._init_relay_retire_state()
        with self._relay_retire_lock:
            work_q = self._relay_retire_q
            thread = self._relay_retire_thread
        if work_q is not None and thread is not None:
            work_q.put(None)
            thread.join(timeout=5.0)
            if thread.is_alive():
                raise RuntimeError(
                    f"wbridge rank {getattr(self, '_rank', getattr(self, 'rank', -1))}: "
                    "relay retirement worker did not stop"
                )
        with self._relay_retire_lock:
            self._relay_retire_q = None
            self._relay_retire_thread = None
            self._relay_retire_errors = []
            self._relay_prep_released = {}
            self._relay_doff_released = {}

    def _relay_local_readers_done(self, gid: int, ri: int, seq: int) -> bool:
        """Return whether every other same-node member has finished reading this DOFF slot."""
        readers = self._relay_local_readers.get(gid, ())
        if not readers:
            return True
        depth = getattr(self, "_relay_doff_depth", 1)
        channel = self._relay_local_channel[(gid, ri % depth)]
        return all(
            self._relay_local_flags.consumed(
                self._relay_local_slot_of[reader],
                channel,
            )
            >= seq
            for reader in readers
        )

    def _relay_mark_prep_released(self, gid: int, ri: int, seq: int) -> None:
        self._init_relay_retire_state()
        key = (gid, ri % 2)
        with self._relay_retire_lock:
            self._relay_prep_released[key] = max(
                seq,
                self._relay_prep_released.get(key, 0),
            )

    def _relay_prep_was_released(self, gid: int, ri: int, seq: int) -> bool:
        self._init_relay_retire_state()
        with self._relay_retire_lock:
            return self._relay_prep_released.get((gid, ri % 2), 0) >= seq

    def _relay_mark_doff_released(self, gid: int, ri: int, seq: int) -> None:
        self._init_relay_retire_state()
        key = (gid, ri % getattr(self, "_relay_doff_depth", 1))
        with self._relay_retire_lock:
            self._relay_doff_released[key] = max(
                seq,
                self._relay_doff_released.get(key, 0),
            )

    def _relay_doff_was_released(self, gid: int, ri: int, seq: int) -> bool:
        self._init_relay_retire_state()
        with self._relay_retire_lock:
            return (
                self._relay_doff_released.get(
                    (gid, ri % getattr(self, "_relay_doff_depth", 1)),
                    0,
                )
                >= seq
            )

    @staticmethod
    def _relay_local_exit_ready(owner_states: dict, consume_states: dict) -> bool:
        """The worker may resume compute after its consumes and successor writes complete.

        Same-node readers of an owned DOFF slot and recursive downstream delivery are deliberately absent:
        the retirement worker owns those lifetimes after this predicate becomes true. PREP was already
        released after its successor write and offload copy completed.
        """
        return all(state["done"] for state in consume_states.values()) and all(
            state["relay_done"] for state in owner_states.values()
        )

    def _defer_relay_retirement(self, tasks: list[dict]) -> None:
        if not tasks:
            return
        self._ensure_relay_retire_worker()
        with self._relay_retire_lock:
            work_q = self._relay_retire_q
        assert work_q is not None
        work_q.put(tasks)

    def _relay_retire_worker(self, work_q: queue.Queue) -> None:
        """Retire independent DOFF slots and recursively publish full delivery off-path."""
        pending: list[dict] = []
        try:
            while True:
                if not pending:
                    batch = work_q.get()
                    if batch is None:
                        return
                    pending.extend(batch)
                while True:
                    try:
                        batch = work_q.get_nowait()
                    except queue.Empty:
                        break
                    if batch is None:
                        return
                    pending.extend(batch)

                progress = False
                remaining = []
                for task in pending:
                    if not task["doff_released"] and self._relay_local_readers_done(
                        task["gid"],
                        task["ri"],
                        task["seq"],
                    ):
                        task["doff_released"] = True
                        self._relay_mark_doff_released(
                            task["gid"],
                            task["ri"],
                            task["seq"],
                        )
                        progress = True

                    successor = task["succ"]
                    downstream_done = successor is None or self._relay_flag_reached(
                        self._RELAY_DATA_KIND,
                        successor,
                        task["gid"],
                        task["seq"],
                    )
                    if task["doff_released"] and downstream_done:
                        for peer in task["upstream_peers"]:
                            self._relay_emit(
                                self._RELAY_DATA_KIND,
                                peer,
                                task["gid"],
                                task["seq"],
                            )
                        self._trace_state(
                            "relay_retired",
                            epoch=task["wt"],
                            group=task["gid"],
                            round=task["ri"],
                        )
                        progress = True
                    else:
                        remaining.append(task)
                pending = remaining
                if pending and not progress:
                    time.sleep(1e-4)
        except BaseException as exc:  # noqa: BLE001 - surfaced on the next receiver interaction
            with self._relay_retire_lock:
                self._relay_retire_errors.append(exc)
            logger.exception(
                "wbridge rank %d: relay retirement worker failed", self._rank
            )

    def _ensure_topo_out_waiters(self, peers: set[int]) -> None:
        """Create the endpoint-lifetime external-completion waiter for each destination in ``peers``."""
        # A few low-level integration drivers construct endpoints with ``__new__`` to bypass the application
        # control adapter. Keep this data-plane helper self-initializing for those legitimate direct users.
        if not hasattr(self, "_topo_out_wait_lock"):
            self._topo_out_wait_queues = {}
            self._topo_out_wait_threads = {}
            self._topo_out_wait_lock = threading.Lock()
        with self._topo_out_wait_lock:
            for peer in sorted(peers):
                if peer in self._topo_out_wait_threads:
                    continue
                work_q: queue.Queue = queue.Queue()
                thread = threading.Thread(
                    target=self._topo_out_waiter,
                    args=(peer, work_q),
                    name=f"wbridge-ext-ready-{peer}",
                    daemon=True,
                )
                self._topo_out_wait_queues[peer] = work_q
                self._topo_out_wait_threads[peer] = thread
                thread.start()

    def _topo_out_waiter(self, peer: int, work_q: queue.Queue) -> None:
        """Wait queued writes to one peer in generation order and publish their READY sequences."""
        while True:
            task = work_q.get()
            if task is None:
                return
            wt, ri, seq, bid, submitted_at, result_q = task
            error: BaseException | None = None
            try:
                with gantt.span("receiver", self._rank, wt, "ext_peer_wait", ri):
                    self.engine.wait([bid])
                gantt.rec(
                    "receiver",
                    self._rank,
                    wt,
                    "ext_peer_xfer",
                    ri,
                    submitted_at,
                    time.time(),
                )
                self._trace_state(
                    "topo_external_peer_done",
                    epoch=wt,
                    round=ri,
                    peer=peer,
                    seq=seq,
                )
                with gantt.span("receiver", self._rank, wt, "ext_flag", ri):
                    self._flag_emit(1, peer, seq)
            except BaseException as exc:  # noqa: BLE001 - delivered to the owning E+C dispatcher
                error = exc
            result_q.put((ri, peer, error))

    # ----- rank0 coordinator intake (own thread; decoupled from the scheduler tick) -----
    def _coordinator_worker(self) -> None:
        """rank0: own the coordinator DEALER and post connect/fallback requests for the main thread.

        Normal updates no longer traverse this socket: input sequence flags are their doorbell. One external
        request remains in flight until the main thread consumes it.
        """
        cc = None
        try:
            cc = CoordinatorClient(self._controller_ipc_name, self.num_workers)
            while not self._stop:
                req = cc.poll(1000)
                if req is None:
                    continue
                cc.ack()
                with self._ext_cv:
                    self._ext_req = req
                    self._ext_cv.notify_all()
                    while self._ext_req is not None and not self._stop:
                        self._ext_cv.wait()
        except Exception:  # noqa: BLE001
            logger.exception("wbridge rank 0: coordinator thread failed")
        finally:
            if cc is not None:
                cc.close()

    # ----- RS receive-worker (per rank; lands sender writes in CPU while the scheduler keeps running) -----
    def _recv_worker(self) -> None:
        """On KICK, stage a complete epoch in CPU, then preload its first active GPU receive round.

        Network receive and its ACK pipeline perform no GPU work, so SGLang continues throughout that long
        leg. After *all* local rounds have landed, one isolated CPU->GPU ingress copy runs on the local staging
        stream. Its completed CUDA event is converted to the host-side ``_rs_gpu_ready`` epoch flag that rank
        0 uses for GO (and peers wait on after GO).
        """
        while not self._stop:
            with self._rs_cv:
                while self._rs_go is None and not self._stop:
                    self._rs_cv.wait()
                if self._stop:
                    return
                epoch = self._rs_go
                self._rs_go = None
            try:
                # Background span: the sender->receiver RDMA landing into CPU, off the scheduler thread. Its
                # duration vs the main-thread GPU consume is the rollout-side overlap (blocked << total).
                with gantt.span("recv-cpu", self._rank, epoch, "recv_to_cpu", -1):
                    self._receive_to_cpu(epoch)
                with self._rs_cv:
                    self._rs_staged = epoch
                    self._rs_cv.notify_all()
                first_ri = self._first_active_round()
                with gantt.span(
                    "recv-cpu",
                    self._rank,
                    epoch,
                    "first_h2d",
                    -1 if first_ri is None else first_ri,
                ):
                    self._rs_stage_first_to_gpu(epoch)
            except BaseException as e:  # noqa: BLE001 — surfaced to the scheduler thread via poll_requests
                with self._rs_cv:
                    self._rs_err = e
                    self._rs_cv.notify_all()
                return
            with self._rs_cv:
                self._rs_gpu_ready = epoch
                self._rs_cv.notify_all()
            self._trace_state(
                "rs_gpu_recv_ready", epoch=epoch, round=self._first_active_round()
            )

    def _first_active_round(self) -> int | None:
        """First global round in which this receiver has at least one trainer contributor."""
        if self.router is None:
            return None
        return next(
            (ri for ri, (_fs, ov) in enumerate(self.router.local_rounds) if ov), None
        )

    def _sender_input_ready(self, epoch: int) -> bool:
        """Non-blockingly test all trainer flags for this rank's first active input round.

        Under RS this means the first round has landed in CPU and is the KICK doorbell. With RS off it means
        the first round has landed directly in GPU (except the established same-node pull path, where it means
        the sender's GPU pack source is ready). A monotonic sequence makes stale prior-epoch flags harmless.
        """
        if getattr(self, "_relay_enabled", False):
            self._relay_retire_check()
            # Preserve the original overlap contract: rank 0 stops generation as soon as its first complete
            # group chunk has landed at this node's representative, not after every group/round is present.
            # The first node observes trainer DATA at a head; later nodes observe predecessor DATA. This is
            # essential for independent rollout engines: a non-head engine has no direct trainer flag with
            # which to bootstrap its GO decision.
            candidates = []
            for gid in getattr(self, "_relay_owned_gids", ()):
                group = self.router.relay_group(gid)
                position = group["chain"].index(self._rank)
                for ri, spec in enumerate(group["round_specs"]):
                    if spec.entries:
                        input_peers = (
                            tuple(sorted(group["trainer_specs"][ri]))
                            if position == 0
                            else (group["chain"][position - 1],)
                        )
                        candidates.append((ri, gid, input_peers))
                        break
            if not candidates:
                return False
            ri, gid, input_peers = min(candidates)
            seq = epoch * self.num_rounds + ri + 1
            return all(
                self._relay_flag_reached(self._RELAY_DATA_KIND, peer, gid, seq)
                for peer in input_peers
            )
        ri = self._first_active_round()
        if ri is None:
            return False
        seq = epoch * self.num_rounds + ri + 1
        overlap_specs = self.router.local_rounds[ri][1]
        return all(self._flag_reached(peer, seq) for peer in overlap_specs)

    def _rs_stage_first_to_gpu(self, epoch: int) -> None:
        """H2D the first active round after the complete RS CPU epoch has landed.

        The local staging engine returns a CUDA event and ``wait`` synchronizes it before the worker publishes
        ``_rs_gpu_ready``. The destination is the isolated trainer-ingress arena, never live model storage.
        """
        ri = self._first_active_round()
        if ri is None:
            return
        assert self.local_engine is not None
        src, dst, sz = self._rs_h2d[ri]
        if not src:
            return
        with torch.cuda.device(self.cuda_device):
            handle = self.local_engine.write_async(
                self.local_engine.session_id(), src, dst, sz
            )
            self.local_engine.wait([handle])

    def _receive_to_cpu(self, epoch: int) -> None:
        """Poll every round's senders (their RDMA into our full-depth CPU arena) and landing-ack all rounds
        EXCEPT the last, using *epoch*'s flag sequence. The last round's ack is deferred to the main thread,
        right after its H2D (:meth:`_receive_weights`): the consume H2Ds rounds in order, so the last round's
        consume-ack implies every CPU slot has been read -> it doubles as a whole-epoch 'consumed' barrier the
        sender's epoch-end drain waits on, so the next epoch (which reuses these depth-1-across-epochs CPU
        slots, and whose RECEIVE must not race our epoch advance) cannot start until we've fully consumed this
        one. Landing-acking the earlier rounds keeps the sender's per-round pipeline flowing. No assemble here.

        This is the fix for the production overlap deadlock: acking EVERY round at landing let the sender (and
        a faster rollout engine) sprint a whole epoch ahead of a slower engine, and the depth-1 per-round flag
        state can't represent that skew -> wedge; the last-round consume-ack paces the sender to the consume."""
        rounds = [ri for ri, (_fs, ov) in enumerate(self.router.local_rounds) if ov]
        last = rounds[-1] if rounds else -1
        for ri in rounds:
            ov = self.router.local_rounds[ri][1]
            seq = epoch * self.num_rounds + ri + 1
            for peer in ov:
                self._poll_flag(
                    peer, seq
                )  # sender wrote round ri into our CPU RECV slot
            if ri != last:
                for peer in ov:
                    self._write_flag(
                        peer, seq
                    )  # landing-ack: frees the sender's per-round pipeline

    # ----- per-round rendezvous (MAIN thread, every scheduler iteration) -----
    def is_update_ready(self) -> bool:
        """Internal readiness phase used by :meth:`poll_requests`.

        Runs one scheduler control round, with receiver rank 0 as the sole decision maker.

        RS-off: rank 0 broadcasts GO when all senders contributing to its first active round have published
        their data-before-flag sequences. RS-on: that same observation broadcasts KICK; every CPU worker then
        stages the complete local epoch and preloads its first active round while SGLang continues. Rank 0
        broadcasts GO on its host-side GPU-receive flag. Peers quiesce with rank 0 and wait for their own
        flag before the receive phase runs, preserving TP lockstep.
        """
        rs = self.receiver_staging and self._connected
        if self.rank == 0:
            with self._ext_cv:
                req = self._ext_req
                self._ext_req = None
                self._ext_cv.notify_all()  # release the coordinator thread for the next request
            decision = self._rank0_decision(req, staged=rs)
            if decision.get("type") == RECEIVE_REQUEST and gantt.ON:
                # Peers consume the hub's FIFO decision stream one item per scheduler tick. Timestamp GO at
                # its source so a queued-EMPTY backlog appears explicitly as decision_lag in the Gantt.
                decision["_gantt_broadcast_t0"] = time.time()
            self._chan.broadcast(decision)
        else:
            if rs:
                with self._rs_cv:
                    if self._rs_err is not None:
                        raise self._rs_err
            decision = self._chan.poll_decision(500 if rs else 5000)
            if decision is None:
                self._pending = None
                return False  # slow rank0; retry next tick (not a hang)
        kind = decision.get("type")
        if kind == CONNECT_REQUEST:
            self._pending = decision
            return True
        if kind == RECEIVE_KICK:
            # RS phase 1: kick this rank's receive-worker (once per epoch); keep generating (the overlap).
            with self._rs_cv:
                if (
                    self._rs_receiving != self._epoch
                    and self._rs_gpu_ready != self._epoch
                ):
                    self._rs_go = self._epoch
                    self._rs_receiving = self._epoch
                    self._rs_cv.notify_all()
            self._trace_state("receive_kick", epoch=self._epoch)
            self._pending = None
            return False
        if kind == RECEIVE_REQUEST:
            broadcast_t0 = decision.get("_gantt_broadcast_t0")
            if broadcast_t0 is not None:
                gantt.rec(
                    "receiver",
                    self._rank,
                    self._epoch,
                    "decision_lag",
                    -1,
                    float(broadcast_t0),
                    time.time(),
                )
            # RS-off: main-thread receive. RS-on: GO quiesces every TP rank; a lagging peer waits for its own
            # host-side GPU-receive flag before touching the fused prepare/exchange path.
            self._pending = decision
            return True
        self._pending = None
        return False

    def _rank0_decision(
        self, req: Optional[dict[str, Any]], *, staged: bool
    ) -> dict[str, Any]:
        """Build rank 0's data-driven KICK/GO decision for one scheduler tick."""
        if req is not None and req.get("type") == CONNECT_REQUEST:
            return req
        if not self._connected:
            return {"type": EMPTY}

        # Only a rank with no input flag can use the legacy coordinator doorbell. If a mixed-version sender
        # posts RECEIVE for a normal rank, ignore its timing and still wait for the causally stronger data flag.
        fallback = (
            req is not None
            and req.get("type") == RECEIVE_REQUEST
            and self._first_active_round() is None
        )
        if not staged:
            ready = fallback or self._sender_input_ready(self._epoch)
            return (
                {"type": RECEIVE_REQUEST, "epoch": self._epoch}
                if ready
                else {"type": EMPTY}
            )

        with self._rs_cv:
            if self._rs_err is not None:
                raise self._rs_err
            if self._rs_kicked != self._epoch and (
                fallback or self._sender_input_ready(self._epoch)
            ):
                self._rs_kicked = self._epoch
                return {"type": RECEIVE_KICK, "epoch": self._epoch}
            kicked = self._rs_kicked == self._epoch
            gpu_ready = self._rs_gpu_ready == self._epoch
        return (
            {"type": RECEIVE_REQUEST, "epoch": self._epoch}
            if (kicked and gpu_ready)
            else {"type": EMPTY}
        )

    def poll_requests(
        self, before_receive: Callable[[int], None] | None = None
    ) -> bool:
        """Poll one control round and execute its pending action on the main thread.

        ``before_receive`` runs on every receiver rank after a real weight-update request has been
        selected, but before any model weight is mutated.  Its argument is the update epoch.  It is not
        called for an empty poll, a receiver-staging KICK, or the one-time connection request.

        Returns ``True`` only after a weight update has been received and loaded.  Connection setup and
        polls without a completed update return ``False``.  Like the former two-call adapter sequence,
        this method must be driven on every model-parallel rank on every scheduler tick.
        """
        if not self.is_update_ready():
            return False

        decision = self._pending
        if (
            before_receive is not None
            and isinstance(decision, dict)
            and decision.get("type") == RECEIVE_REQUEST
        ):
            before_receive(int(decision["epoch"]))
        return self.request_update()

    def request_update(self) -> bool:
        """Internal execution phase used by :meth:`poll_requests`; blocks on the main thread."""
        decision = self._pending
        self._pending = None
        if decision is None:
            return False
        kind = decision.get("type")
        if kind == CONNECT_REQUEST:
            self._do_connect(decision)
            return False
        if kind == RECEIVE_REQUEST:
            assert self._connected and self.engine is not None, "receive before connect"
            self._trace_state("receive_request_enter", epoch=self._epoch)
            self._run_receive(staged=self.receiver_staging)
            self._trace_state("receive_request_done", epoch=self._epoch - 1)
            return True
        return False

    # ----- persistent receiving thread (owns the control channel; RS-on also lands data in CPU) ----------
    def _run_receive(self, staged: bool = False) -> None:
        """Run one weight receive, optionally captured by a torch profiler.

        Set ``WBRIDGE_RECV_PROFILE_WT=<n>`` to profile the n-th RECEIVE (0-based; n>=1 is a warm/
        steady-state transfer) and dump a chrome trace to ``WBRIDGE_RECV_PROFILE_DIR``. Trace serialization,
        Gantt JSONL writes, and control-profile log output are deferred until the integration records the
        matching rollout ``block_end``. This is the only
        way to see the rollout-side ``recv_poll/consume/ack`` breakdown: :meth:`_receive_weights` runs on
        the SGLang scheduler thread, so the trainer-side profiler can't reach it. The capture wraps the
        transfer in :func:`gantt.capture` so the ``wbridge::`` span labels from :func:`gantt.span`
        light up for just this one WT even when ``WBRIDGE_PROFILE`` is unset. Negligible overhead otherwise.
        """
        import os as _os
        import tempfile as _tempfile

        wt = self._epoch
        nrounds = sum(bool(overlap) for _full, overlap in self.router.local_rounds)
        profile_idx = self._recv_count
        self._recv_count += 1
        target = _os.environ.get("WBRIDGE_RECV_PROFILE_WT", "").strip()
        if not (target.isdigit() and int(target) == profile_idx):
            self._receive_weights(staged)
            self._capture_profile_outputs(wt, nrounds)
            return
        from torch.profiler import profile as _profile, ProfilerActivity

        d = _os.path.abspath(
            _os.path.expanduser(
                _os.environ.get("WBRIDGE_RECV_PROFILE_DIR", _tempfile.gettempdir())
            )
        )
        with (
            _profile(activities=[ProfilerActivity.CPU, ProfilerActivity.CUDA]) as _p,
            gantt.capture(),
        ):
            self._receive_weights(staged)
        path = _os.path.join(
            d, f"wbridge_recv_rank{self.rank}_pid{_os.getpid()}_wt{profile_idx}.json.gz"
        )

        def export_trace() -> None:
            _os.makedirs(d, exist_ok=True)
            _p.export_chrome_trace(path)
            logger.info(
                "wbridge rank %d receiver profile WT %d -> %s",
                self.rank,
                profile_idx,
                path,
            )

        self._capture_profile_outputs(wt, nrounds, (export_trace,))

    # ----- actions (main thread, blocking) ----------------------------------
    def _do_connect(self, decision: dict[str, Any]) -> None:
        """Set up the merged process group (blocking collective with the senders)."""
        data = {k: v for k, v in decision.items() if k != "type"}
        data["rank"] = data["rank"] + self.rank  # base rank -> this rank's global rank

        self.set_up_connection(**data)
        self._connected = True
        with self._rs_cv:
            self._rs_go = None
            self._rs_receiving = -1
            self._rs_staged = -1
            self._rs_gpu_ready = -1
            self._rs_kicked = -1
            self._rs_err = None

    def _receive_weights(self, staged: bool = False) -> None:
        """Receive one update through isolated trainer-ingress and rollout-exchange buffers.

        When ``staged`` (RS-on), the sender->receiver P2P landing already happened on the receiving thread
        into the full-depth CPU arena; this method prepends a per-round CPU->GPU staging hop (into the same
        parity-selected GPU arena RECV zone prepare reads) and skips the sender poll/ack. Everything else — the
        fused prepare, external exchange, and internal consume over the GPU arena — is identical to RS-off.

        When the topology-aware path is available, the three-stage schedule runs external exchange, DOFF
        offload, and internal consume on a progress worker. With two registered slots the calling thread may
        prepare the opposite parity concurrently. With one registered slot, ACK after A+R gates the trainer's
        next RECV write, while the next A+R waits only for the previous SEND's external reads and SEND→DOFF
        copy. GRECV reuse remains independently gated by OFFLOAD and DOFF reuse by local DONE.

        Data lands by one-sided RDMA writes into this rank's packed arena. RECV and fused own/send prepare use
        slot ``r % depth``; compact GRECV contains one slot per ``(direct external source, r % depth)``.
        Same-node source columns are read directly into model tensors and allocate no local staging slot. Per round: poll
        senders → fused prepare → external exchange → readiness-driven internal consume → aggregate CONS.
        RS-on retains its serial GPU schedule.
        """
        if getattr(self, "_relay_enabled", False):
            self._receive_weights_relay(staged)
            return
        if getattr(self, "_direct_same_node", False):
            if staged:
                raise RuntimeError(
                    "direct same-node consume does not support receiver staging"
                )
            self._receive_weights_direct_same_node()
            return
        router = self.router
        rounds = [ri for ri, (_fs, ov) in enumerate(router.local_rounds) if ov]
        wt = self._epoch
        sw = router.sender_ws
        self._trace_state(
            "receive_enter",
            epoch=wt,
            rounds=rounds,
            topo=self._topo_ok,
            three_stage=os.environ.get("WBRIDGE_RECV_3STAGE", "1") == "1",
        )
        if staged:
            # GO is based on rank 0's GPU receive flag. A lagging peer has quiesced its scheduler and waits
            # here for its own completed first-round H2D before entering fused prepare/exchange.
            with self._rs_cv:
                while self._rs_gpu_ready != wt and self._rs_err is None:
                    self._rs_cv.wait()
                if self._rs_err is not None:
                    raise self._rs_err
        if getattr(self, "_con_stream", None) is None:
            self._con_stream = torch.cuda.Stream()
        self._flag_reaper_ensure()  # cleanup only; producers publish flags directly with write_async()
        three_stage_requested = os.environ.get("WBRIDGE_RECV_3STAGE", "1") == "1"
        # The topology path records its inter-node-column visibility event from the E+C worker, in round
        # order. The single-phase path records one reusable CUDA-IPC event during prepare; allowing prepare(r+1) to
        # re-record it before a peer has waited for round r would reintroduce the historical visibility race.
        # Keep the first implementation deliberately narrow until that path has per-parity IPC events.
        three_stage = bool(three_stage_requested and not staged and self._topo_ok)
        if self._topo_ok and not three_stage:
            raise RuntimeError(
                "fixed DOFF internal offload requires WBRIDGE_RECV_3STAGE=1 and receiver staging off"
            )
        merged_recv_prep = bool(getattr(self, "_merged_recv_prep", False))
        if merged_recv_prep and not three_stage:
            raise RuntimeError(
                "WBRIDGE_MERGED_RECV_PREP=1 requires the depth-2 topology-aware 3-stage receiver"
            )
        if three_stage_requested and not three_stage and self._epoch == 0:
            logger.warning(
                "wbridge rank %d: 3-stage receiver requested but unavailable "
                "(staged=%s topo_ok=%s); using serial receiver schedule",
                self._rank,
                staged,
                self._topo_ok,
            )
        recv_scratch: torch.Tensor | None = None
        scratch_lifetime_t0 = 0.0
        if merged_recv_prep:
            scratch_lifetime_t0 = time.time()
            with gantt.span("receiver", self._rank, wt, "recv_scratch_alloc", -1):
                recv_scratch = torch.empty(
                    max(self._recv_payload_S, 1),
                    dtype=torch.uint8,
                    device=self._arena.device,
                )
                scratch_base = recv_scratch.data_ptr()
                # The tiny descriptor updates run on the default stream, ahead of every snapshot/A+R launch.
                for plan in self._arena_prepare:
                    plan.rebase_sources(scratch_base)
        # DEBUG: per-slice fingerprint cross-check. Logs a position-weighted checksum of each peer sub-slice
        # on the SEND side (my send-staging, just after the RDMA submit) and the RECV side (peer's grecv,
        # after the ready-flag rendezvous, before consume). Correlating SEND(i->peer=j) with RECV(j,peer=i)
        # per (wt,ri) localizes the dedup corruption: all-match => fault in prepare/consume; any
        # mismatch => the transfer/flag handshake delivered wrong bytes for that (round, i->j) slice.
        xchk = os.environ.get("WBRIDGE_XCHECK") == "1"
        if xchk and self._epoch == 0:
            logger.warning(
                "[wbridge] rank %d WBRIDGE_XCHECK=1 — per-slice fingerprint logging ON",
                self._rank,
            )

        def _fp(off: int, nb: int) -> int:
            # Allocation-light checksum (the arena is HBM-tight): fused int64 reductions over uint8 VIEWS,
            # no widened copy and no per-slice arange (those OOM'd the transfer). A full byte-sum plus a
            # strided byte-sum gives enough position sensitivity to catch partial/reordered corruption.
            if nb <= 0:
                return 0
            s = self._arena[off : off + nb]  # uint8 view, no copy
            total = int(s.sum(dtype=torch.int64).item())  # fused widen+reduce -> scalar
            strided = int(
                s[::7].sum(dtype=torch.int64).item()
            )  # strided view sum -> scalar
            return total * 1000003 + strided

        def _consume(ri: int) -> None:
            """Fuse own + all filled grecv slots into the model on ``_con_stream``.

            This is the generic staged fallback. Topology uses per-source internal-consume kernels and never
            calls this local GRECV fan-in plan.
            """
            plan = self._arena_consume[ri]
            if plan is not None:
                # DIAG: split the wait for peers' grecv writes (ready-events land) from the copy kernel, so
                # the gantt separates cross-process WAIT (cons_wait) from actual WORK (consume).
                with gantt.span("receiver", self._rank, wt, "cons_wait", ri):
                    self._trace_state(
                        "consume_stream_wait",
                        epoch=wt,
                        round=ri,
                        internal=(
                            self._topo_int_peers_by_round[ri] if self._topo_ok else []
                        ),
                    )
                    self._con_stream.synchronize()
                    self._trace_state("consume_stream_ready", epoch=wt, round=ri)
                with gantt.span("receiver", self._rank, wt, "consume", ri):
                    self._trace_state("consume_kernel", epoch=wt, round=ri)
                    with torch.cuda.stream(self._con_stream):
                        plan.run()  # own + peers' slices -> model
                    # LOAD-BEARING: the consumed-flag below is written only after this host sync, so a peer's
                    # write-after-read reuse of our grecv[peer] is safe with NO event on the reuse path. Do NOT
                    # weaken to enqueue-only-then-flag, or a peer could clobber grecv mid-consume.
                    self._con_stream.synchronize()  # consume done before AGH is freed
                    self._trace_state("consume_done", epoch=wt, round=ri)

        def _finish(ri: int, bids: dict) -> None:
            seq = self._seq(ri)
            # Cross-node peers only: they PUSH their grecv[me] slice via the selected RDMA backend, so we
            # rendezvous on the ready/consumed flags. Same-node peers are handled by the PULL in the exchange
            # above (we read their send[me] into our own grecv on _con_stream), and their ready-flag was set
            # post-prepare.
            xnode = [p for p in self._repl_peers if p not in self._repl_same_node]
            if xnode:
                with gantt.span("receiver", self._rank, wt, "agh_xfer", ri):
                    with gantt.span("receiver", self._rank, wt, "agh_wait", ri):
                        for peer in xnode:
                            if bids.get(peer) is not None:
                                self.engine.wait(
                                    [bids[peer]]
                                )  # my push landed (RDMA completion)
                    with gantt.span("receiver", self._rank, wt, "agh_flag", ri):
                        for peer in xnode:
                            self._write_repl_flag(
                                peer, seq
                            )  # signal peers their grecv[me] is ready
                    with gantt.span("receiver", self._rank, wt, "agh_poll", ri):
                        for peer in xnode:
                            self._poll_repl_flag(
                                peer, seq
                            )  # wait peers' push into my grecv[peer]
            # Generic fallback only: same-node grecv[peer] was filled by our own pull-read on _con_stream, so consume
            # (also on _con_stream) is naturally ordered after it — no cross-process visibility fence needed.
            if xchk and self._repl_peers:
                rd_x = self._arena_layout[ri]
                with torch.cuda.stream(
                    self._con_stream
                ):  # fp AFTER the fence -> reflects consume's view
                    for peer in self._repl_peers:
                        go = rd_x["grecv"].get(peer - self.router.sender_ws)
                        if go and go[1]:
                            g_off = go[0]
                            print(
                                f"WXCHK RECV grank={self._rank} peer={peer} wt={wt} ri={ri} "
                                f"off={g_off} nb={go[1]} fp={_fp(g_off, go[1])}",
                                flush=True,
                            )
            _consume(ri)
            # Round ri done. cons-flag(seq) tells each peer: same-node -> we PULLED your send[me] (you may
            # reuse its PREP parity); cross-node -> we consumed our grecv[peer] parity (you may overwrite it
            # two rounds later at depth 2).
            with gantt.span(
                "receiver", self._rank, wt, "cons_flag", ri
            ):  # DIAG: measure the flag handshake
                for peer in self._repl_peers:
                    self._flag_emit(2, peer, seq)

        def _sn_send_peers(rd_layout: dict) -> list[int]:
            """Same-node peers that pull OUR send[peer] in the round with layout ``rd_layout`` (send>0)."""
            return [
                p
                for p in self._repl_peers
                if p in self._repl_same_node
                and (p - sw) in rd_layout["send"]
                and rd_layout["send"][p - sw][1] > 0
            ]

        def _reuse_seq(pred: tuple[int, int] | None) -> int | None:
            """Resolve a cyclic ``(epoch_delta, round)`` predecessor for this transfer epoch."""
            if pred is None:
                return None
            epoch_delta, pred_ri = pred
            pred_epoch = wt + epoch_delta
            return None if pred_epoch < 0 else self._seq_at(pred_epoch, pred_ri)

        def _topo_round(
            ri: int, seq: int, peer_gates: dict[int, tuple[int, int]]
        ) -> None:
            """Run external exchange and readiness-driven fused internal consume for one round.

            Every exact external ingress slot becomes READY independently. This worker launches its matching
            direct peer-to-model descriptor kernel as soon as that slot arrives, queries completions while
            slower slots are still pending, and commits the slot to its same-node owner. The owner releases
            the external writer once self and every local downstream reader have committed that exact slot.
            """
            ext_send_peers = self._topo_ext_send_peers_by_round[ri]
            ext_recv_peers = self._topo_ext_recv_peers_by_round[ri]
            int_peers = self._topo_int_peers_by_round[ri]
            int_readers = self._topo_int_readers_by_round[ri]
            grecv_slot = ri % self._recv_depth
            consume_plans = self._topo_internal_consume_plan[ri]
            lane_sort = lambda lane: (lane[0], -1 if lane[1] is None else lane[1])
            bids: dict = {}
            submitted_at: dict[int, float] = {}
            _td = os.environ.get("WBRIDGE_TOPO_DEBUG") == "1"
            self._trace_state(
                "topo_enter",
                epoch=wt,
                round=ri,
                seq=seq,
                external_send=ext_send_peers,
                external_recv=ext_recv_peers,
                internal_consume=sorted(consume_plans, key=lane_sort),
                readers=int_readers,
            )
            if _td:
                print(
                    f"TDBG r{self._rank} wt{wt} ri{ri} ENTER "
                    f"ext_send={ext_send_peers} ext_recv={ext_recv_peers} "
                    f"consume={sorted(consume_plans, key=lane_sort)} readers={int_readers}",
                    flush=True,
                )

            external_started = time.time()
            with gantt.span("receiver", self._rank, wt, "ext_submit", ri):
                self._trace_state("topo_external_submit", epoch=wt, round=ri, seq=seq)
                for peer in ext_send_peers:
                    spans = self._topo_ext_xfer[peer][ri]
                    if not spans:
                        continue
                    gate = peer_gates.get(peer)
                    gate_seq = _reuse_seq(gate)
                    if gate_seq is not None:
                        # Target emits this generation only after every local internal-consume kernel that
                        # read the prior generation of this GRECV parity has completed.
                        with gantt.span(
                            "receiver", self._rank, wt, "ext_reuse_gate", ri
                        ):
                            self._poll_repl_cons_flag(peer, gate_seq)
                    src, dst, size = zip(*spans)
                    with gantt.span("receiver", self._rank, wt, "ext_write_submit", ri):
                        submitted_at[peer] = time.time()
                        bids[peer] = self.engine.write_async(
                            self._repl_peer_session[peer],
                            list(src),
                            list(dst),
                            list(size),
                        )
                    self._tstats["agh_rdma_bytes"] += sum(size)

            # Outgoing destinations become READY independently through endpoint-lifetime waiters. The
            # synchronous debug schedule still drains them before returning, but never creates a round-local
            # thread (and therefore has no Thread.start/GIL handoff in its READY critical path).
            ready_result_q: queue.Queue = queue.Queue()
            pending_ready = set(bids)
            ready_errors: list[tuple[int, BaseException]] = []
            self._ensure_topo_out_waiters(pending_ready)
            for peer, bid in bids.items():
                self._topo_out_wait_queues[peer].put(
                    (wt, ri, seq, bid, submitted_at[peer], ready_result_q)
                )

            # ---- Per-slot internal consume. ----
            # Channel zero advertises parity-slotted PREP/own bytes. Exact external grecv slots are published
            # independently below as each writer's completion flag arrives.
            with gantt.span("receiver", self._rank, wt, "internal_own_ready_flag", ri):
                for reader in int_readers:
                    self._repl_ready_event[reader].record()
                if int_readers:
                    self._repl_local_flags.publish_ready(seq)

            own_lane = self._topo_internal_consume_own_lane[ri]
            lane_bytes = self._topo_internal_consume_bytes_by_lane[ri]
            release_deps = self._topo_ext_release_readers_by_round[ri]
            pending_ext = set(ext_recv_peers)
            pending_launch = set(consume_plans)
            pending_complete: set[tuple[int, int | None]] = set()
            completed: set[tuple[int, int | None]] = set()
            published_slots: set[int] = set()
            pending_releases = set(ext_recv_peers)
            assert pending_releases == set(release_deps), (
                f"missing external release dependencies: "
                f"releases={sorted(pending_releases)} deps={sorted(release_deps)}"
            )
            observed_commits: set[tuple[int, int]] = set()
            launch_wall: dict[tuple[int, int | None], float] = {}
            ready_wall = {lane: time.time() for lane in pending_launch}
            ext_wall = {peer: time.time() for peer in pending_ext}
            slot_ready_wall: dict[int, float] = {}
            external_recorded = False
            deadline = time.time() + 600.0
            warned = 0.0

            def _lane_ready(lane: tuple[int, int | None]) -> bool:
                owner, source = lane
                if owner == self._rank:
                    return source is None or source in published_slots
                if source is None:
                    return self._repl_flag_reached(owner, seq)
                return self._topo_slot_ready_reached(owner, source, grecv_slot, seq)

            with gantt.span(
                "receiver", self._rank, wt, "internal_consume_dispatch", ri
            ):
                while (
                    pending_ext
                    or pending_launch
                    or pending_complete
                    or pending_releases
                ):
                    progress = False
                    while True:
                        try:
                            _done_ri, peer, error = ready_result_q.get_nowait()
                        except queue.Empty:
                            break
                        pending_ready.remove(peer)
                        if error is not None:
                            ready_errors.append((peer, error))
                        progress = True
                    error = ready_errors[0] if ready_errors else None
                    if error is not None:
                        peer, exc = error
                        raise RuntimeError(
                            f"wbridge rank {self._rank}: external completion/READY failed for peer "
                            f"{peer} epoch {wt} round {ri}"
                        ) from exc

                    # Publish each exact ingress slot immediately, rather than waiting for the whole column.
                    ext_done = [
                        peer
                        for peer in sorted(pending_ext)
                        if self._repl_flag_reached(peer, seq)
                    ]
                    for peer in ext_done:
                        now = time.time()
                        gantt.rec(
                            "receiver",
                            self._rank,
                            wt,
                            "ext_recv_wait",
                            ri,
                            ext_wall[peer],
                            now,
                        )
                        with gantt.span(
                            "receiver", self._rank, wt, "internal_slot_ready_flag", ri
                        ):
                            self._publish_topo_slot_ready(
                                peer,
                                grecv_slot,
                                release_deps.get(peer, ()),
                                seq,
                            )
                        slot_ready_wall[peer] = now
                        published_slots.add(peer)
                        pending_ext.remove(peer)
                        self._trace_state(
                            "internal_slot_ready",
                            epoch=wt,
                            round=ri,
                            source=peer,
                            seq=seq,
                        )
                        progress = True

                    if not pending_ext and not external_recorded:
                        now = time.time()
                        gantt.rec(
                            "receiver",
                            self._rank,
                            wt,
                            "external_exchange",
                            ri,
                            external_started,
                            now,
                        )
                        self._trace_state(
                            "topo_external_done", epoch=wt, round=ri, seq=seq
                        )
                        external_recorded = True
                        progress = True

                    # Launch every independently-ready (source-column owner, external slot) lane.
                    ready_lanes = [
                        lane
                        for lane in sorted(pending_launch, key=lane_sort)
                        if _lane_ready(lane)
                    ]
                    for lane in ready_lanes:
                        owner, source = lane
                        now = time.time()
                        gantt.rec(
                            "receiver",
                            self._rank,
                            wt,
                            "internal_consume_ready_wait",
                            ri,
                            ready_wall[lane],
                            now,
                        )
                        stream = self._topo_internal_consume_stream[lane]
                        if owner != self._rank:
                            ready_event = (
                                self._repl_peer_ready_event[owner]
                                if source is None
                                else self._repl_peer_topo_slot_ready_event[owner][
                                    (source, grecv_slot)
                                ]
                            )
                            stream.wait_event(ready_event)
                        with gantt.span(
                            "receiver", self._rank, wt, "internal_consume_submit", ri
                        ):
                            launch_wall[lane] = time.time()
                            self._trace_state(
                                "internal_consume_launch",
                                epoch=wt,
                                round=ri,
                                peer=owner,
                                source=source,
                                seq=seq,
                            )
                            with torch.cuda.stream(stream):
                                consume_plans[lane].run()
                                self._topo_internal_consume_event[lane].record(stream)
                        if owner != self._rank:
                            self._tstats["agh_ipc_bytes"] += lane_bytes[lane]
                        pending_launch.remove(lane)
                        pending_complete.add(lane)
                        progress = True

                    # Query completion while other slots are still waiting for READY. This is what lets a
                    # fast slot commit instead of inheriting the slowest source column's delay.
                    done_lanes = [
                        lane
                        for lane in sorted(pending_complete, key=lane_sort)
                        if self._topo_internal_consume_event[lane].query()
                    ]
                    for lane in done_lanes:
                        owner, source = lane
                        now = time.time()
                        gantt.rec(
                            "receiver",
                            self._rank,
                            wt,
                            "internal_consume",
                            ri,
                            launch_wall[lane],
                            now,
                        )
                        self._trace_state(
                            "internal_consume_done",
                            epoch=wt,
                            round=ri,
                            peer=owner,
                            source=source,
                            seq=seq,
                        )
                        if owner != self._rank:
                            # Channel zero protects the owner's PREP parity; the source/parity channel
                            # protects only grecv[source, parity]. A lane may commit both when own is attached.
                            if own_lane.get(owner) == lane:
                                self._flag_emit(2, owner, seq)
                            if source is not None:
                                self._write_topo_slot_cons_flag(
                                    owner, source, grecv_slot, seq
                                )
                        pending_complete.remove(lane)
                        completed.add(lane)
                        progress = True

                    # Owner-side fan-in: self consume plus each exact reader commit releases one parity slot.
                    releasable: list[int] = []
                    for source in sorted(pending_releases):
                        if (self._rank, source) not in completed:
                            continue
                        readers_done = True
                        for reader in release_deps.get(source, ()):
                            key = (source, reader)
                            if key in observed_commits:
                                continue
                            if self._topo_slot_cons_flag_reached(
                                reader,
                                source,
                                grecv_slot,
                                seq,
                            ):
                                observed_commits.add(key)
                                now = time.time()
                                gantt.rec(
                                    "receiver",
                                    self._rank,
                                    wt,
                                    "slot_cons_peer_wait",
                                    ri,
                                    slot_ready_wall.get(source, external_started),
                                    now,
                                )
                            else:
                                readers_done = False
                        if readers_done:
                            releasable.append(source)
                    for source in releasable:
                        now = time.time()
                        gantt.rec(
                            "receiver",
                            self._rank,
                            wt,
                            "external_slot_hold",
                            ri,
                            slot_ready_wall[source],
                            now,
                        )
                        with gantt.span("receiver", self._rank, wt, "cons_flag", ri):
                            self._flag_emit(2, source, seq)
                        pending_releases.remove(source)
                        self._trace_state(
                            "external_slot_released",
                            epoch=wt,
                            round=ri,
                            source=source,
                            seq=seq,
                        )
                        progress = True

                    if progress:
                        continue
                    now = time.time()
                    if now >= deadline:
                        raise TimeoutError(
                            f"wbridge rank {self._rank}: waited 600s for per-slot internal consume "
                            f"epoch={wt} round={ri} ext={sorted(pending_ext)} "
                            f"launch={sorted(pending_launch, key=lane_sort)} "
                            f"complete={sorted(pending_complete, key=lane_sort)} "
                            f"release={sorted(pending_releases)} seq={seq}"
                        )
                    elapsed = now - (deadline - 600.0)
                    if elapsed - warned >= 30.0:
                        warned = elapsed
                        logger.warning(
                            "wbridge rank %d: waiting %.0fs for per-slot internal consume "
                            "epoch=%d round=%d ext=%s launch=%s complete=%s release=%s seq=%d",
                            self._rank,
                            elapsed,
                            wt,
                            ri,
                            sorted(pending_ext),
                            sorted(pending_launch, key=lane_sort),
                            sorted(pending_complete, key=lane_sort),
                            sorted(pending_releases),
                            seq,
                        )
                    time.sleep(1e-4)

            if _td:
                print(
                    f"TDBG r{self._rank} wt{wt} ri{ri} SLOT_CONSUME_DONE "
                    f"lanes={sorted(completed, key=lane_sort)}",
                    flush=True,
                )
            # Outbound PREP sources remain read-only until their independent writes complete. Drain before
            # this E+C round releases its parity slot to a later prepare.
            with gantt.span("receiver", self._rank, wt, "ext_send_drain", ri):
                while pending_ready:
                    _done_ri, peer, error = ready_result_q.get()
                    pending_ready.remove(peer)
                    if error is not None:
                        ready_errors.append((peer, error))
            if ready_errors:
                peer, exc = ready_errors[0]
                raise RuntimeError(
                    f"wbridge rank {self._rank}: external completion/READY failed for peer {peer} "
                    f"epoch {wt} round {ri}"
                ) from exc
            if _td:
                print(
                    f"TDBG r{self._rank} wt{wt} ri{ri} EXT_FLAGS_DONE peers={ext_send_peers}",
                    flush=True,
                )
            if _td:
                print(f"TDBG r{self._rank} wt{wt} ri{ri} ROUND_DONE", flush=True)
            self._trace_state("topo_round_done", epoch=wt, round=ri, seq=seq)

        # Stage 3 owns SEND/external exchange plus the fixed DOFF shadow. Registered SEND/GRECV bytes are never
        # read by internal peers: own SEND is copied to DOFF after A+R, and each incoming GRECV is copied to its
        # source-exclusive DOFF slot as soon as DATA arrives. The GRECV writer receives OFFLOAD when that D2D
        # copy completes; local readers receive READY for DOFF and return DONE only to release DOFF reuse.
        ec_q: "queue.Queue[tuple[int, int, dict[int, tuple[int, int]]] | None] | None" = (
            queue.Queue() if three_stage else None
        )
        ec_slot_free: set[int] = set()
        ec_completed: set[int] = set()
        ec_cv = threading.Condition()
        ec_err: list[BaseException] = []

        # Cyclic predecessor for each fixed (source, DOFF slot). With DOFF=1 every source has one reusable
        # internal buffer across rounds; larger values select round%DOFF independently per source. A source's
        # next copy waits for all readers of only its own previous generation.  The serial fallback (including
        # receiver staging) deliberately has no topology tables and never executes the E+C worker, so do not
        # inspect topology-only metadata on that path.
        doff_pred: list[dict[int, tuple[int, int]]] = (
            _doff_source_predecessors(
                rounds,
                self._topo_ext_recv_peers_by_round,
                self._doff_depth,
                self._rank,
            )
            if three_stage
            else [{} for _ in router.local_rounds]
        )

        if not hasattr(self, "_doff_copy_stream"):
            self._doff_copy_stream = {}
            self._doff_copy_event = {}
            self._doff_slot_released = {}

        def _ec_worker() -> None:
            assert ec_q is not None
            lane_sort = lambda lane: (lane[0], -1 if lane[1] is None else lane[1])
            out_result_q: queue.Queue = queue.Queue()
            out_peers = {
                peer for ri in rounds for peer in self._topo_ext_send_peers_by_round[ri]
            }
            self._ensure_topo_out_waiters(out_peers)
            if not hasattr(self, "_topo_lane_seen"):
                self._topo_lane_seen = {}
                self._topo_lane_consumed = {}
            active: dict[int, dict] = {}
            lane_inflight: set[tuple[int, int | None]] = set()
            stopping = False

            def _register(
                item: tuple[int, int, dict[int, tuple[int, int]], tuple[int, ...]],
            ) -> None:
                ri, seq, peer_gates, input_release_peers = item
                if ri in active:
                    raise RuntimeError(f"duplicate active topology round {ri}")
                now = time.time()
                ext_send_peers = self._topo_ext_send_peers_by_round[ri]
                ext_recv_peers = self._topo_ext_recv_peers_by_round[ri]
                consume_plans = self._topo_internal_consume_plan[ri]
                release_deps = self._topo_ext_release_readers_by_round[ri]
                source_readers = {
                    self._rank: tuple(self._topo_int_readers_by_round[ri])
                }
                source_readers.update(
                    {source: tuple(release_deps[source]) for source in ext_recv_peers}
                )
                copy_sources = set(source_readers)
                source_lane = self._topo_internal_consume_source_lane[ri]
                missing_lanes = [
                    source
                    for source in copy_sources
                    if (self._rank, source) not in source_lane
                ]
                if missing_lanes:
                    raise RuntimeError(
                        f"missing local DOFF consume lanes round={ri} sources={sorted(missing_lanes)}"
                    )
                doff_gate = {
                    source: gate_seq
                    for source, pred in doff_pred[ri].items()
                    if (gate_seq := _reuse_seq(pred)) is not None
                }
                ctx = {
                    "ri": ri,
                    "seq": seq,
                    "doff_slot": ri % self._doff_depth,
                    "peer_gates": peer_gates,
                    "input_release_peers": input_release_peers,
                    "ext_send_peers": ext_send_peers,
                    "ext_recv_peers": ext_recv_peers,
                    "int_readers": self._topo_int_readers_by_round[ri],
                    "consume_plans": consume_plans,
                    "lane_sources": self._topo_internal_consume_lane_sources[ri],
                    "source_lane": source_lane,
                    "lane_bytes": self._topo_internal_consume_bytes_by_lane[ri],
                    "source_readers": source_readers,
                    "doff_gate": doff_gate,
                    "pending_out_submit": set(ext_send_peers),
                    "pending_out_wait": set(),
                    "out_gate_wall": {peer: now for peer in ext_send_peers},
                    "out_submit_recorded": False,
                    "pending_ext": set(ext_recv_peers),
                    "data_arrived": set(),
                    "data_arrival_wall": {},
                    "pending_copy_submit": copy_sources,
                    "pending_copy_complete": set(),
                    "copy_launch_wall": {},
                    "copy_ready_wall": {},
                    "copy_done": set(),
                    "published_sources": set(),
                    "pending_launch": set(consume_plans),
                    "pending_complete": set(),
                    "completed": set(),
                    "pending_doff_release": set(copy_sources),
                    "observed_commits": set(),
                    "launch_wall": {},
                    "ready_wall": {lane: now for lane in consume_plans},
                    "ext_wall": {peer: now for peer in ext_recv_peers},
                    "external_recorded": not ext_recv_peers,
                    "prep_free": False,
                    "registered_wall": now,
                    "deadline": now + 600.0,
                    "warned": 0.0,
                }
                active[ri] = ctx
                self._trace_state(
                    "topo_enter",
                    epoch=wt,
                    round=ri,
                    seq=seq,
                    external_send=ext_send_peers,
                    external_recv=ext_recv_peers,
                    internal_consume=sorted(consume_plans, key=lane_sort),
                    readers=ctx["int_readers"],
                )

            def _lane_ready(ctx: dict, lane: tuple[int, int | None]) -> bool:
                owner, source = lane
                sources = ctx["lane_sources"][lane]
                if owner == self._rank:
                    return all(item in ctx["published_sources"] for item in sources)
                return all(
                    self._topo_slot_ready_reached(
                        owner,
                        item,
                        ctx["doff_slot"],
                        ctx["seq"],
                    )
                    for item in sources
                )

            try:
                with torch.cuda.device(self._arena.device):
                    while True:
                        progress = False

                        # When idle, sleep on the round queue. With active rounds, drain newly prepared work
                        # without blocking and then return to the 100-us slot/event scan.
                        if not active and not stopping:
                            self._trace_state("ec_worker_idle", epoch=wt)
                            item = ec_q.get()
                            if item is None:
                                stopping = True
                            else:
                                self._trace_state(
                                    "ec_round_dequeue",
                                    epoch=wt,
                                    round=item[0],
                                    seq=item[1],
                                )
                                _register(item)
                                progress = True
                        while not stopping:
                            try:
                                item = ec_q.get_nowait()
                            except queue.Empty:
                                break
                            if item is None:
                                stopping = True
                                break
                            self._trace_state(
                                "ec_round_dequeue",
                                epoch=wt,
                                round=item[0],
                                seq=item[1],
                            )
                            _register(item)
                            progress = True

                        # Persistent per-destination waiters report bulk completion + READY publication here.
                        while True:
                            try:
                                done_ri, peer, error = out_result_q.get_nowait()
                            except queue.Empty:
                                break
                            if error is not None:
                                raise RuntimeError(
                                    f"wbridge rank {self._rank}: external completion/READY failed for "
                                    f"peer {peer} epoch {wt} round {done_ri}"
                                ) from error
                            ctx = active.get(done_ri)
                            if ctx is None or peer not in ctx["pending_out_wait"]:
                                raise RuntimeError(
                                    f"unexpected external completion round={done_ri} peer={peer}"
                                )
                            ctx["pending_out_wait"].remove(peer)
                            progress = True

                        finished: list[int] = []
                        for ri in sorted(active):
                            ctx = active[ri]
                            seq = ctx["seq"]

                            # Submit every destination whose exact prior parity slot has been released. The
                            # scanner never blocks on one peer, so a ready r+1 transfer can pass an unrelated
                            # r straggler. Completion is handled by the endpoint-lifetime per-peer waiter.
                            ready_out = []
                            for peer in sorted(ctx["pending_out_submit"]):
                                # READY/CONS use one monotonic cross-node flag per peer. Preserve that peer's
                                # round order even though its two GRECV parities are independent: once the
                                # older write is submitted, this write may follow immediately (before either
                                # completes), but a higher READY sequence must never overtake a lower one.
                                prior_pending = any(
                                    prior_ri < ri
                                    and peer in prior_ctx["pending_out_submit"]
                                    for prior_ri, prior_ctx in active.items()
                                )
                                if prior_pending:
                                    continue
                                gate_seq = _reuse_seq(ctx["peer_gates"].get(peer))
                                if gate_seq is None or self._repl_cons_flag_reached(
                                    peer, gate_seq
                                ):
                                    ready_out.append((peer, gate_seq))
                            for peer, gate_seq in ready_out:
                                now = time.time()
                                if gate_seq is not None:
                                    gantt.rec(
                                        "receiver",
                                        self._rank,
                                        wt,
                                        "ext_reuse_gate",
                                        ri,
                                        ctx["out_gate_wall"][peer],
                                        now,
                                    )
                                spans = self._topo_ext_xfer[peer][ri]
                                if not spans:
                                    ctx["pending_out_submit"].remove(peer)
                                    progress = True
                                    continue
                                src, dst, size = zip(*spans)
                                with gantt.span(
                                    "receiver", self._rank, wt, "ext_write_submit", ri
                                ):
                                    submitted_at = time.time()
                                    bid = self.engine.write_async(
                                        self._repl_peer_session[peer],
                                        list(src),
                                        list(dst),
                                        list(size),
                                    )
                                self._tstats["agh_rdma_bytes"] += sum(size)
                                ctx["pending_out_submit"].remove(peer)
                                ctx["pending_out_wait"].add(peer)
                                self._topo_out_wait_queues[peer].put(
                                    (wt, ri, seq, bid, submitted_at, out_result_q)
                                )
                                self._trace_state(
                                    "topo_external_submit",
                                    epoch=wt,
                                    round=ri,
                                    peer=peer,
                                    seq=seq,
                                )
                                progress = True
                            if (
                                not ctx["pending_out_submit"]
                                and not ctx["out_submit_recorded"]
                            ):
                                gantt.rec(
                                    "receiver",
                                    self._rank,
                                    wt,
                                    "ext_submit",
                                    ri,
                                    ctx["registered_wall"],
                                    time.time(),
                                )
                                ctx["out_submit_recorded"] = True

                            # Observe every external DATA flag independently. Landing and DOFF availability are
                            # separate: DATA may wait safely in its GRECV parity while a DOFF=1 predecessor is
                            # still being read.
                            ext_done = [
                                peer
                                for peer in sorted(ctx["pending_ext"])
                                if self._repl_flag_reached(peer, seq)
                            ]
                            for peer in ext_done:
                                now = time.time()
                                gantt.rec(
                                    "receiver",
                                    self._rank,
                                    wt,
                                    "ext_recv_wait",
                                    ri,
                                    ctx["ext_wall"][peer],
                                    now,
                                )
                                ctx["data_arrival_wall"][peer] = now
                                ctx["data_arrived"].add(peer)
                                ctx["pending_ext"].remove(peer)
                                self._trace_state(
                                    "external_data_arrived",
                                    epoch=wt,
                                    round=ri,
                                    source=peer,
                                    seq=seq,
                                )
                                progress = True
                            if not ctx["pending_ext"] and not ctx["external_recorded"]:
                                gantt.rec(
                                    "receiver",
                                    self._rank,
                                    wt,
                                    "external_exchange",
                                    ri,
                                    ctx["registered_wall"],
                                    time.time(),
                                )
                                ctx["external_recorded"] = True
                                progress = True

                            # Copy each available source into its exclusive DOFF slot as soon as only that
                            # source's prior DOFF generation is free. READY is published after the copy/event
                            # records are enqueued; GPU readers wait those exact IPC events. OFFLOAD is delayed
                            # until the local copy event actually completes.
                            copy_ready = []
                            for source in sorted(ctx["pending_copy_submit"]):
                                if (
                                    source != self._rank
                                    and source not in ctx["data_arrived"]
                                ):
                                    continue
                                gate_seq = ctx["doff_gate"].get(source)
                                key = (source, ctx["doff_slot"])
                                if (
                                    gate_seq is None
                                    or self._doff_slot_released.get(key, 0) >= gate_seq
                                ):
                                    copy_ready.append((source, gate_seq))
                            for source, gate_seq in copy_ready:
                                now = time.time()
                                if gate_seq is not None:
                                    gantt.rec(
                                        "receiver",
                                        self._rank,
                                        wt,
                                        "doff_reuse_wait",
                                        ri,
                                        ctx["registered_wall"],
                                        now,
                                    )
                                key = (source, ctx["doff_slot"])
                                stream = self._doff_copy_stream.setdefault(
                                    key,
                                    torch.cuda.Stream(device=self._arena.device),
                                )
                                event = self._doff_copy_event.setdefault(
                                    key, torch.cuda.Event()
                                )
                                doff_rd = self._doff_layout[ri]
                                if source == self._rank:
                                    src_off = _arena_slot_offset(
                                        0,
                                        ri,
                                        self._recv_depth,
                                        self._arena_S,
                                    )
                                    dst_off, nb = doff_rd["own"]
                                else:
                                    source_rl = source - sw
                                    src_off, nb = self._arena_layout[ri]["grecv"][
                                        source_rl
                                    ]
                                    dst_off, dst_nb = doff_rd["grecv"][source_rl]
                                    assert dst_nb == nb
                                with gantt.span(
                                    "receiver", self._rank, wt, "doff_copy_submit", ri
                                ):
                                    ctx["copy_launch_wall"][source] = time.time()
                                    with torch.cuda.stream(stream):
                                        self._doff_arena[dst_off : dst_off + nb].copy_(
                                            self._arena[src_off : src_off + nb],
                                            non_blocking=True,
                                        )
                                        event.record(stream)
                                    self._publish_topo_slot_ready(
                                        source,
                                        ctx["doff_slot"],
                                        ctx["source_readers"][source],
                                        seq,
                                        stream=stream,
                                    )
                                ctx["copy_ready_wall"][source] = now
                                ctx["published_sources"].add(source)
                                ctx["pending_copy_submit"].remove(source)
                                ctx["pending_copy_complete"].add(source)
                                self._trace_state(
                                    "doff_copy_launch",
                                    epoch=wt,
                                    round=ri,
                                    source=source,
                                    slot=ctx["doff_slot"],
                                    seq=seq,
                                    bytes=nb,
                                )
                                progress = True

                            done_copies = [
                                source
                                for source in sorted(ctx["pending_copy_complete"])
                                if self._doff_copy_event[
                                    (source, ctx["doff_slot"])
                                ].query()
                            ]
                            for source in done_copies:
                                now = time.time()
                                gantt.rec(
                                    "receiver",
                                    self._rank,
                                    wt,
                                    "doff_copy",
                                    ri,
                                    ctx["copy_launch_wall"][source],
                                    now,
                                )
                                ctx["pending_copy_complete"].remove(source)
                                ctx["copy_done"].add(source)
                                if source != self._rank:
                                    gantt.rec(
                                        "receiver",
                                        self._rank,
                                        wt,
                                        "external_slot_hold",
                                        ri,
                                        ctx["data_arrival_wall"][source],
                                        now,
                                    )
                                    with gantt.span(
                                        "receiver",
                                        self._rank,
                                        wt,
                                        "offload_flag",
                                        ri,
                                    ):
                                        self._flag_emit(2, source, seq)
                                    self._trace_state(
                                        "external_slot_offloaded",
                                        epoch=wt,
                                        round=ri,
                                        source=source,
                                        seq=seq,
                                    )
                                progress = True

                            ready_lanes = [
                                lane
                                for lane in sorted(ctx["pending_launch"], key=lane_sort)
                                if lane not in lane_inflight
                                and _lane_ready(ctx, lane)
                                # READY may arrive out of round order across the two active parity slots.
                                # Different lanes are independent, but one lane's local sequence/IPC event
                                # is monotonic: never let a newer generation overtake an older live one.
                                and not any(
                                    prior_ctx["seq"] < seq
                                    and (
                                        lane in prior_ctx["pending_launch"]
                                        or lane in prior_ctx["pending_complete"]
                                    )
                                    for prior_ctx in active.values()
                                )
                            ]
                            for lane in ready_lanes:
                                owner, source = lane
                                now = time.time()
                                gantt.rec(
                                    "receiver",
                                    self._rank,
                                    wt,
                                    "internal_consume_ready_wait",
                                    ri,
                                    ctx["ready_wall"][lane],
                                    now,
                                )
                                prior = self._topo_lane_seen.get(lane, -1)
                                if seq <= prior:
                                    raise RuntimeError(
                                        f"non-monotonic internal lane {lane}: seq={seq} prior={prior}"
                                    )
                                stream = self._topo_internal_consume_stream[lane]
                                for source_key in ctx["lane_sources"][lane]:
                                    ready_event = (
                                        self._doff_copy_event[
                                            (source_key, ctx["doff_slot"])
                                        ]
                                        if owner == self._rank
                                        else self._repl_peer_topo_slot_ready_event[
                                            owner
                                        ][(source_key, ctx["doff_slot"])]
                                    )
                                    stream.wait_event(ready_event)
                                with gantt.span(
                                    "receiver",
                                    self._rank,
                                    wt,
                                    "internal_consume_submit",
                                    ri,
                                ):
                                    ctx["launch_wall"][lane] = time.time()
                                    with torch.cuda.stream(stream):
                                        ctx["consume_plans"][lane].run()
                                        self._topo_internal_consume_event[lane].record(
                                            stream
                                        )
                                if owner != self._rank:
                                    self._tstats["agh_ipc_bytes"] += ctx["lane_bytes"][
                                        lane
                                    ]
                                self._topo_lane_seen[lane] = seq
                                lane_inflight.add(lane)
                                ctx["pending_launch"].remove(lane)
                                ctx["pending_complete"].add(lane)
                                self._trace_state(
                                    "internal_consume_launch",
                                    epoch=wt,
                                    round=ri,
                                    peer=owner,
                                    source=source,
                                    seq=seq,
                                )
                                progress = True

                            done_lanes = [
                                lane
                                for lane in sorted(
                                    ctx["pending_complete"], key=lane_sort
                                )
                                if self._topo_internal_consume_event[lane].query()
                            ]
                            for lane in done_lanes:
                                owner, source = lane
                                now = time.time()
                                gantt.rec(
                                    "receiver",
                                    self._rank,
                                    wt,
                                    "internal_consume",
                                    ri,
                                    ctx["launch_wall"][lane],
                                    now,
                                )
                                if owner != self._rank:
                                    for source_key in ctx["lane_sources"][lane]:
                                        self._write_topo_slot_cons_flag(
                                            owner,
                                            source_key,
                                            ctx["doff_slot"],
                                            seq,
                                        )
                                self._topo_lane_consumed[lane] = seq
                                lane_inflight.remove(lane)
                                ctx["pending_complete"].remove(lane)
                                ctx["completed"].add(lane)
                                self._trace_state(
                                    "internal_consume_done",
                                    epoch=wt,
                                    round=ri,
                                    peer=owner,
                                    source=source,
                                    seq=seq,
                                )
                                progress = True

                            # DONE is local-only and frees one source's DOFF generation. It never gates GRECV:
                            # that registered slot was already released by OFFLOAD at copy completion.
                            releasable: list[int] = []
                            for source in sorted(ctx["pending_doff_release"]):
                                local_lane = ctx["source_lane"][(self._rank, source)]
                                if local_lane not in ctx["completed"]:
                                    continue
                                readers_done = True
                                for reader in ctx["source_readers"][source]:
                                    key = (source, reader)
                                    if key in ctx["observed_commits"]:
                                        continue
                                    if self._topo_slot_cons_flag_reached(
                                        reader,
                                        source,
                                        ctx["doff_slot"],
                                        seq,
                                    ):
                                        ctx["observed_commits"].add(key)
                                        gantt.rec(
                                            "receiver",
                                            self._rank,
                                            wt,
                                            "doff_reader_wait",
                                            ri,
                                            ctx["copy_ready_wall"].get(
                                                source, ctx["registered_wall"]
                                            ),
                                            time.time(),
                                        )
                                    else:
                                        readers_done = False
                                if readers_done:
                                    releasable.append(source)
                            for source in releasable:
                                now = time.time()
                                gantt.rec(
                                    "receiver",
                                    self._rank,
                                    wt,
                                    "doff_slot_hold",
                                    ri,
                                    ctx["copy_ready_wall"][source],
                                    now,
                                )
                                self._doff_slot_released[(source, ctx["doff_slot"])] = (
                                    seq
                                )
                                ctx["pending_doff_release"].remove(source)
                                self._trace_state(
                                    "doff_slot_released",
                                    epoch=wt,
                                    round=ri,
                                    source=source,
                                    slot=ctx["doff_slot"],
                                    seq=seq,
                                )
                                progress = True

                            # SEND/PREP parity is free once its local DOFF copy and every outbound RDMA read
                            # finish. Incoming GRECV processing and downstream DOFF readers are unrelated.
                            if (
                                not ctx["prep_free"]
                                and self._rank in ctx["copy_done"]
                                and not ctx["pending_out_submit"]
                                and not ctx["pending_out_wait"]
                            ):
                                now = time.time()
                                gantt.rec(
                                    "receiver",
                                    self._rank,
                                    wt,
                                    "send_slot_hold",
                                    ri,
                                    ctx["registered_wall"],
                                    now,
                                )
                                ctx["prep_free"] = True
                                if merged_recv_prep:
                                    with gantt.span(
                                        "receiver",
                                        self._rank,
                                        wt,
                                        "ack_send",
                                        ri,
                                    ):
                                        for peer in ctx["input_release_peers"]:
                                            self._flag_emit(0, peer, ctx["seq"])
                                with ec_cv:
                                    ec_slot_free.add(ri)
                                    ec_cv.notify_all()
                                progress = True

                            if (
                                ctx["prep_free"]
                                and not ctx["pending_ext"]
                                and not ctx["pending_copy_submit"]
                                and not ctx["pending_copy_complete"]
                                and not ctx["pending_launch"]
                                and not ctx["pending_complete"]
                                and not ctx["pending_doff_release"]
                            ):
                                now = time.time()
                                gantt.rec(
                                    "receiver",
                                    self._rank,
                                    wt,
                                    "internal_consume_dispatch",
                                    ri,
                                    ctx["registered_wall"],
                                    now,
                                )
                                gantt.rec(
                                    "recv-ec",
                                    self._rank,
                                    wt,
                                    "exchange_consume",
                                    ri,
                                    ctx["registered_wall"],
                                    now,
                                )
                                finished.append(ri)
                                progress = True
                                continue

                            now = time.time()
                            if now >= ctx["deadline"]:
                                raise TimeoutError(
                                    f"wbridge rank {self._rank}: waited 600s for cross-round slot "
                                    f"progress epoch={wt} round={ri} "
                                    f"out_submit={sorted(ctx['pending_out_submit'])} "
                                    f"out_wait={sorted(ctx['pending_out_wait'])} "
                                    f"ext={sorted(ctx['pending_ext'])} "
                                    f"copy_submit={sorted(ctx['pending_copy_submit'])} "
                                    f"copy_complete={sorted(ctx['pending_copy_complete'])} "
                                    f"launch={sorted(ctx['pending_launch'], key=lane_sort)} "
                                    f"complete={sorted(ctx['pending_complete'], key=lane_sort)} "
                                    f"doff_release={sorted(ctx['pending_doff_release'])} seq={seq}"
                                )
                            elapsed = now - ctx["registered_wall"]
                            if elapsed - ctx["warned"] >= 30.0:
                                ctx["warned"] = elapsed
                                logger.warning(
                                    "wbridge rank %d: waiting %.0fs for cross-round slot progress "
                                    "epoch=%d round=%d out_submit=%s out_wait=%s ext=%s "
                                    "copy_submit=%s copy_complete=%s launch=%s complete=%s "
                                    "doff_release=%s seq=%d",
                                    self._rank,
                                    elapsed,
                                    wt,
                                    ri,
                                    sorted(ctx["pending_out_submit"]),
                                    sorted(ctx["pending_out_wait"]),
                                    sorted(ctx["pending_ext"]),
                                    sorted(ctx["pending_copy_submit"]),
                                    sorted(ctx["pending_copy_complete"]),
                                    sorted(ctx["pending_launch"], key=lane_sort),
                                    sorted(ctx["pending_complete"], key=lane_sort),
                                    sorted(ctx["pending_doff_release"]),
                                    seq,
                                )

                        for ri in finished:
                            ctx = active.pop(ri)
                            with ec_cv:
                                ec_completed.add(ri)
                                ec_cv.notify_all()
                            self._trace_state(
                                "ec_round_done",
                                epoch=wt,
                                round=ri,
                                seq=ctx["seq"],
                            )

                        if stopping and not active:
                            self._trace_state("ec_worker_stop", epoch=wt)
                            return
                        if not progress:
                            time.sleep(1e-4)
            except BaseException as e:  # noqa: BLE001 — re-raised on the poll_requests thread
                with ec_cv:
                    ec_err.append(e)
                    ec_cv.notify_all()

        def _wait_ec(ri: int) -> None:
            self._trace_state("ec_slot_wait", epoch=wt, round=ri)
            with ec_cv:
                while ri not in ec_slot_free and not ec_err:
                    ec_cv.wait()
                if ec_err:
                    raise ec_err[0]
            self._trace_state("ec_slot_done", epoch=wt, round=ri)

        ec_thread: threading.Thread | None = None
        if three_stage:
            ec_thread = threading.Thread(
                target=_ec_worker, name="wbridge-recv-ec", daemon=True
            )
            ec_thread.start()

        slot_pred = _arena_slot_predecessors(rounds, self._recv_depth)
        peer_pred = (
            self._topo_peer_predecessors
            if self._topo_ok
            else _arena_peer_predecessors(
                self._arena_layout,
                depth=self._recv_depth,
            )
        )
        for ri in rounds:
            seq = self._seq(ri)
            overlap_specs = router.local_rounds[ri][1]
            self._trace_state(
                "round_enter",
                epoch=wt,
                round=ri,
                seq=seq,
                senders=sorted(overlap_specs),
            )
            # The previous active round in this *global-round parity slot* owns the PREP bytes that assemble(ri)
            # will reuse.  Active-list position is not parity when this receiver skips a global round.
            slot_gate = slot_pred[ri]
            peer_gates = peer_pred[ri]
            if not staged:
                # RS-off: poll senders' done-flags for round ri. A cross-node sender RDMA-filled RECV(ri)
                # during external+internal-consume(prev) (RECV∩PREP(prev)=∅), so its flag means "bytes landed"; a
                # SAME-NODE sender wrote nothing — its flag means "my pack buffer holds round ri" and we pull.
                with gantt.span("receiver", self._rank, wt, "poll", ri):
                    for peer in overlap_specs:
                        self._poll_flag(peer, seq)
                self._trace_state("round_input_ready", epoch=wt, round=ri, seq=seq)
                if os.environ.get("WBRIDGE_TOPO_DEBUG") == "1":
                    print(
                        f"TDBG r{self._rank} wt{wt} ri{ri} INPUT_DONE senders={list(overlap_specs)}",
                        flush=True,
                    )
                # Same-node senders bypass RDMA: read round ri straight out of the sender's pack buffer
                # over NVLink (CUDA-IPC) into the RECV slot assemble is about to read. Issued on the DEFAULT
                # stream, so the existing post-assemble event sync below covers it and the ack we send after
                # it truthfully means "your pack buffer is free". Same direction as the dedup exchange's
                # pull: reading peer IPC memory is substantially faster than writing it.
                recv = self._arena_layout[ri]["recv"]
                sn = [p for p in overlap_specs if p in self._sn_senders]
                self._tstats["wire_rdma_bytes"] += sum(
                    recv[p][1] for p in overlap_specs if p not in self._sn_senders
                )
                if sn:
                    with gantt.span("receiver", self._rank, wt, "ipc_pull", ri):
                        for peer in sn:
                            off, nb = recv[peer]
                            if nb == 0:
                                continue
                            # Our RECV slot lives in this round's arena parity (what the assemble plan reads);
                            # the sender packed round ri at offset 0 of its parity-(ri % its _NUM_BUF) buffer.
                            a_off = _arena_slot_offset(
                                off, ri, self._recv_depth, self._recv_S
                            )
                            src = self._peer_pack_buf[peer][
                                ri % self._peer_pack_num_buf[peer]
                            ]
                            torch.cuda.current_stream().wait_event(
                                self._peer_pack_event[peer]
                            )
                            self._recv_arena[a_off : a_off + nb].copy_(src[:nb])
                            self._tstats["wire_ipc_bytes"] += nb
            else:
                # RS-on: the receive-worker POLLED this round into the full-depth CPU arena and landing-acked
                # every round but the LAST. Stage this round's per-sender slices CPU->GPU RECV zone (Triton
                # needs GPU); for the last round, ack AFTER the H2D. That consume-ack (H2Ds run in round order,
                # so it means every CPU slot has been read) is the whole-epoch 'consumed' barrier the sender's
                # epoch-end drain waits on: it paces the sender to our consume, keeps the engines in lockstep,
                # and stops epoch N+1's RECEIVE racing our epoch advance (the production overlap deadlock).
                # RS keeps the sender on RDMA (host DRAM can't be CUDA-IPC mapped), so every byte of
                # this round arrived over the fabric.
                self._tstats["wire_rdma_bytes"] += sum(
                    self._arena_layout[ri]["recv"][p][1] for p in overlap_specs
                )
                src, dst, sz = self._rs_h2d[ri]
                preloaded = ri == rounds[0] and self._rs_gpu_ready == wt
                if src and not preloaded:
                    with gantt.span("receiver", self._rank, wt, "h2d", ri):
                        self.local_engine.wait(
                            [
                                self.local_engine.write_async(
                                    self.local_engine.session_id(), src, dst, sz
                                )
                            ]
                        )
                if ri == rounds[-1]:
                    for peer in overlap_specs:
                        self._write_flag(
                            peer, seq
                        )  # last round H2D'd -> whole epoch consumed
            # Before prepare overwrites this SEND parity, wait only for its prior outbound RDMA reads and local
            # SEND->DOFF copy. Topology readers consume DOFF and therefore do not participate in this gate.
            if slot_gate is not None:
                # RECV(ri) may land beside E+C(slot_gate); prepare writes SEND/PREP and waits for that physical
                # parity's narrow send-slot-free condition.
                # Put this AFTER the sender poll: waiting for RECV(ri) is useful work overlapped with E+C.
                if three_stage:
                    with gantt.span(
                        "receiver", self._rank, wt, "ec_slot_gate", slot_gate
                    ):
                        _wait_ec(slot_gate)
                if not self._topo_ok:
                    sn_gate = _sn_send_peers(self._arena_layout[slot_gate])
                    if sn_gate:
                        with gantt.span("receiver", self._rank, wt, "reuse_gate", ri):
                            for peer in sn_gate:
                                self._poll_repl_cons_flag(peer, self._seq(slot_gate))
            # Prepare remains ordered on the calling thread. E+C may overlap adjacent prepared rounds;
            # slotted source readers are covered by slot_gate and remote GRECV parity destinations by their
            # exact per-peer/parity generation gates.
            with gantt.span("receiver", self._rank, wt, "assemble", ri):
                if merged_recv_prep:
                    assert recv_scratch is not None
                    # Snapshot the complete stable trainer-lane prefix before PREP overwrites this merged
                    # parity slot. The following A+R plan reads the same offsets from scratch on this stream.
                    slot_base = _arena_slot_offset(
                        0,
                        ri,
                        self._recv_depth,
                        self._recv_S,
                    )
                    with gantt.span(
                        "receiver",
                        self._rank,
                        wt,
                        "recv_snapshot_submit",
                        ri,
                    ):
                        recv_scratch[: self._recv_payload_S].copy_(
                            self._arena[slot_base : slot_base + self._recv_payload_S],
                            non_blocking=True,
                        )
                # One kernel: RECV -> own plus any unique partial send payload. A full send aliases own, so
                # the common one-group case writes only the canonical bytes and has no repack destination.
                self._trace_state("assemble_kernel", epoch=wt, round=ri, seq=seq)
                self._arena_prepare[ri].run()
                # PULL: capture prepare completion so same-node peers get a GPU-visible fence before they read
                # our send[peer]. Recorded on the default stream BEFORE the ready-flag below.
                # (Topo 2-phase records its own-visibility event later, in _topo_round's phase 1.5.)
                if not self._topo_ok:
                    for peer in _sn_send_peers(self._arena_layout[ri]):
                        self._repl_ready_event[peer].record()
                self._trace_state("assemble_stream_wait", epoch=wt, round=ri, seq=seq)
                ev = torch.cuda.Event()
                ev.record()
                ev.synchronize()  # fused prepare landed in HBM
                self._trace_state("assemble_done", epoch=wt, round=ri, seq=seq)
            if merged_recv_prep and ri == rounds[-1]:
                # The final A+R synchronization proves no prepare descriptor still reads scratch. Dropping
                # the tensor returns its block to PyTorch's allocator; it is not retained by WeightBridge.
                recv_scratch = None
                gantt.rec(
                    "receiver",
                    self._rank,
                    wt,
                    "recv_scratch_lifetime",
                    -1,
                    scratch_lifetime_t0,
                    time.time(),
                )
            # PULL: my send-staging is now in HBM — flag each same-node peer that it may pull it (event recorded
            # above precedes this flag, so the reader's wait_event observes THIS round's record once it sees it).
            # (Topo 2-phase emits its column-ready flag later, in _topo_round's phase 1.5.)
            if not self._topo_ok:
                with gantt.span(
                    "receiver", self._rank, wt, "ready_flag", ri
                ):  # DIAG: measure the flag handshake
                    for peer in _sn_send_peers(self._arena_layout[ri]):
                        self._flag_emit(1, peer, seq)
                if xchk:
                    for peer in _sn_send_peers(self._arena_layout[ri]):
                        s_off, s_nb = self._arena_layout[ri]["send"][peer - sw]
                        s_off = _arena_slot_offset(
                            s_off, ri, self._recv_depth, self._arena_S
                        )
                        print(
                            f"WXCHK SEND grank={self._rank} peer={peer} wt={wt} ri={ri} "
                            f"off={s_off} nb={s_nb} fp={_fp(s_off, s_nb)}",
                            flush=True,
                        )
            # A+R completion is the only local prerequisite for topology E+C. Publish the prepared round to
            # its progress worker before trainer ACKs: ACK submission is independent RECV-lane bookkeeping,
            # and host scheduling jitter there must not delay rollout external exchange.
            if self._topo_ok and three_stage:
                assert ec_q is not None
                self._trace_state("ec_round_enqueue", epoch=wt, round=ri, seq=seq)
                ec_q.put((ri, seq, peer_gates, tuple(self.peers)))
            # Ack actual source senders: their RECV(ri) lane is drained, so each may reuse this receiver/parity
            # on its next contributing round. (RS-on already acked right after the H2D above — earliest point
            # the CPU slot is free — so skip here.)
            if not staged and not merged_recv_prep:
                with gantt.span(
                    "receiver", self._rank, wt, "ack_send", ri
                ):  # DIAG: serial synchronous acks
                    for peer in overlap_specs:
                        self._flag_emit(0, peer, seq)
                self._trace_state("sender_acks_done", epoch=wt, round=ri, seq=seq)
                if os.environ.get("WBRIDGE_TOPO_DEBUG") == "1":
                    print(
                        f"TDBG r{self._rank} wt{wt} ri{ri} ACK_DONE senders={list(overlap_specs)}",
                        flush=True,
                    )
            if self._topo_ok:
                # Topology-aware multi-group all-gather: packed cross-node columns then possibly multi-span
                # same-node pulls, consume + cons handshake — all inside _topo_round. Nothing else in the
                # round body applies, so skip the single-phase exchange below.
                if not three_stage:
                    _topo_round(ri, seq, peer_gates)
                continue
            # Exchange: fill my grecv[peer] for each shared peer. Same-node -> PULL peer's send[me] over NVLink
            # (a fast IPC READ; the reverse-direction WRITE is substantially slower). Cross-node -> keep
            # the RDMA push of my send[peer] into the peer's grecv[me] (RDMA is direction-agnostic).
            bids: dict = {}
            if self._repl_peers:
                with gantt.span("receiver", self._rank, wt, "agh_submit", ri):
                    rd = self._arena_layout[ri]
                    for peer in self._repl_peers:
                        p_rl = peer - sw
                        if peer in self._repl_same_node:
                            # PULL: read the peer's send[me] slice from its arena straight into our grecv[peer].
                            # Gated on the peer's ready-flag (it produced send[me]) + its production event (GPU
                            # visibility). We fill our OWN grecv on _con_stream, so consume needs no extra fence.
                            g = rd["grecv"].get(p_rl)
                            ps_off = self._repl_peer_send_off[peer][ri]
                            if not g or g[1] == 0 or ps_off is None:
                                continue
                            # Our GRECV offset already selects this round's shared-bank parity; ps_off is
                            # parity-baked into the peer's arena because its send source is part of PREP.
                            g_off = g[0]
                            g_nb = g[1]
                            self._poll_repl_flag(
                                peer, seq
                            )  # peer produced its send[me]
                            self._con_stream.wait_event(
                                self._repl_peer_ready_event[peer]
                            )  # GPU-visible prepare
                            with torch.cuda.stream(self._con_stream):
                                self._arena[g_off : g_off + g_nb].copy_(
                                    self._repl_peer_arena[peer][ps_off : ps_off + g_nb]
                                )
                            self._tstats["agh_ipc_bytes"] += g_nb
                            if xchk:
                                print(
                                    f"WXCHK PULL grank={self._rank} peer={peer} wt={wt} ri={ri} "
                                    f"g_off={g_off} nb={g_nb} ps_off={ps_off}",
                                    flush=True,
                                )
                        else:
                            # Cross-node PUSH: gate on the peer freeing its shared grecv[me] slot, then
                            # Cross-node RDMA write (completion implies remote landing/visibility).
                            dst = self._arena_peer_dst[peer][ri]
                            if (
                                dst is None
                                or p_rl not in rd["send"]
                                or rd["send"][p_rl][1] == 0
                            ):
                                continue
                            gate = peer_gates.get(p_rl)
                            gate_seq = _reuse_seq(gate)
                            if gate_seq is not None:
                                with gantt.span(
                                    "receiver", self._rank, wt, "reuse_gate", ri
                                ):
                                    self._poll_repl_cons_flag(peer, gate_seq)
                            s_off, s_nb = rd["send"][p_rl]
                            s_off = _arena_slot_offset(
                                s_off, ri, self._recv_depth, self._arena_S
                            )
                            bids[peer] = self.engine.write_async(
                                self._repl_peer_session[peer],
                                [self._arena.data_ptr() + s_off],
                                [dst],
                                [s_nb],
                            )
                            self._tstats["agh_rdma_bytes"] += s_nb
                            if xchk:
                                print(
                                    f"WXCHK SEND grank={self._rank} peer={peer} wt={wt} ri={ri} "
                                    f"off={s_off} nb={s_nb} fp={_fp(s_off, s_nb)}",
                                    flush=True,
                                )
            # Consume + rendezvous + consumed-flag for this round, inline (serial). The flag releases each
            # peer's next write to the same parity destination in our shared GRECV bank.
            _finish(ri, bids)
        if three_stage:
            assert ec_q is not None and ec_thread is not None
            self._trace_state("ec_drain_wait", epoch=wt)
            ec_q.put(None)
            ec_thread.join()
            self._trace_state("ec_drain_done", epoch=wt)
            if ec_err:
                raise ec_err[0]
        # Do not wait for flag completions at epoch close. Exclusive message slots remain immutable until
        # causal protocol progress makes their next-epoch reuse safe; the reaper retires handles off-path.
        self._flag_reaper_check()
        self._trace_state("receive_final_cuda_wait", epoch=wt)
        torch.cuda.synchronize()
        self._epoch += 1
        self._trace_state("receive_done", epoch=wt)

    def _receive_weights_direct_same_node(self) -> None:
        """Consume each round directly from co-located trainer pack buffers.

        The sender's DATA-ready sequence is published only after its pack event has been recorded.  Once all
        contributing senders are ready, one fused plan waits those imported events and reads their CUDA-IPC
        mappings directly into the live rollout parameters.  ACK is emitted only after that model-copy kernel
        finishes, making it the pack-buffer lifetime fence.  No receiver exchange or staging allocation
        participates in the update.
        """
        router = self.router
        assert router is not None
        wt = self._epoch
        rounds = [
            ri for ri, (_full, overlaps) in enumerate(router.local_rounds) if overlaps
        ]
        self._trace_state("direct_receive_enter", epoch=wt, rounds=rounds)
        self._flag_reaper_ensure()
        for ri in rounds:
            seq = self._seq(ri)
            overlap_specs = router.local_rounds[ri][1]
            with gantt.span("receiver", self._rank, wt, "direct_ready_wait", ri):
                for peer in overlap_specs:
                    self._poll_flag(peer, seq)
            plan = self._direct_consume_plan[ri]
            if plan is None:
                raise RuntimeError(
                    f"missing direct same-node consume plan for active round {ri}"
                )
            with gantt.span("receiver", self._rank, wt, "direct_consume", ri):
                stream = torch.cuda.current_stream()
                for peer in overlap_specs:
                    stream.wait_event(self._peer_pack_event[peer])
                plan.run()
                completed = torch.cuda.Event()
                completed.record(stream)
                completed.synchronize()
            self._tstats["wire_ipc_bytes"] += sum(
                spec.nbytes(self.dtype_spec) for spec in overlap_specs.values()
            )
            with gantt.span("receiver", self._rank, wt, "direct_done", ri):
                for peer in overlap_specs:
                    self._flag_emit(0, peer, seq)
            self._trace_state(
                "direct_round_done",
                epoch=wt,
                round=ri,
                seq=seq,
                senders=sorted(overlap_specs),
                launches=plan.launch_count,
            )
        self._flag_reaper_check()
        torch.cuda.synchronize()
        self._epoch += 1
        self._trace_state("direct_receive_done", epoch=wt)

    def _receive_weights_relay_legacy(self, staged: bool = False) -> None:
        """Drive one depth-2 replica-group head/relay/consume epoch.

        Every ``(group, round)`` is an independent state machine. Heads wait for their trainer lanes and
        assemble directly into canonical PREP; later node representatives copy their predecessor's canonical
        RECV into PREP. PREP simultaneously feeds the next RDMA hop and same-node CUDA-IPC consumers. Its
        parity is released only after both those readers finish. Within one group, assemble, relay-write, and
        internal-consume each advance in round order; different groups remain independent. The blocking call
        ends after this worker's consumes and successor writes. A persistent retirement worker subsequently
        releases any PREP slots still held by same-node readers and recursively reports full downstream
        consumption, preserving the trainer's ``wait_send_complete`` contract without blocking rollout.
        """
        if staged:
            raise RuntimeError("replica-group relay does not support receiver staging")
        router = self.router
        assert router is not None
        wt = self._epoch
        sw = router.sender_ws
        seq_of = lambda ri: wt * self.num_rounds + ri + 1
        self._flag_reaper_ensure()
        self._relay_retire_check()

        owner_states: dict[tuple[int, int], dict] = {}
        consume_states: dict[tuple[int, int], dict] = {}
        for group in router._relay_groups:
            gid = group["id"]
            active_rounds = [
                ri for ri, spec in enumerate(group["round_specs"]) if spec.entries
            ]
            round_predecessors = _relay_round_predecessors(active_rounds, depth=2)
            last_round_by_parity = {
                parity: max(ri for ri in active_rounds if ri % 2 == parity)
                for parity in {ri % 2 for ri in active_rounds}
            }
            if gid in self._relay_owned_gids:
                chain = group["chain"]
                position = chain.index(self._rank)
                pred = chain[position - 1] if position else None
                succ = chain[position + 1] if position + 1 < len(chain) else None
                for ri, spec in enumerate(group["round_specs"]):
                    if not spec.entries:
                        continue
                    key = (gid, ri)
                    operation_pred, parity_pred = round_predecessors[ri]
                    state = {
                        "gid": gid,
                        "ri": ri,
                        "seq": seq_of(ri),
                        "pred": pred,
                        "succ": succ,
                        "input_peers": (
                            tuple(sorted(group["trainer_specs"][ri]))
                            if pred is None
                            else (pred,)
                        ),
                        # Adjacent operations are deliberately ordered even though they use opposite
                        # parity buffers.  The parity predecessor remains the stronger storage-lifetime
                        # fence: it cannot be overwritten until its relay and every local read finish.
                        "operation_pred": (
                            (gid, operation_pred)
                            if operation_pred is not None
                            else None
                        ),
                        "prep_pred": (gid, parity_pred)
                        if parity_pred is not None
                        else None,
                        "prior_epoch_prep_seq": (
                            None
                            if wt == 0
                            else (wt - 1) * self.num_rounds
                            + last_round_by_parity[ri % 2]
                            + 1
                        ),
                        "input_ready": False,
                        "prepare_launched": False,
                        "prep_ready": False,
                        "acks_sent": False,
                        "relay_submitted": False,
                        "relay_done": succ is None,
                        "local_consumed": False,
                        "prep_free": False,
                        "upstream_done": False,
                        "started": time.time(),
                    }
                    owner_states[key] = state
            if (self._rank - sw) in group["members"]:
                owner = sw + group["owner_of"][self._rank - sw]
                for ri, spec in enumerate(group["round_specs"]):
                    if spec.entries:
                        operation_pred, _parity_pred = round_predecessors[ri]
                        consume_states[(gid, ri)] = {
                            "gid": gid,
                            "ri": ri,
                            "seq": seq_of(ri),
                            "owner": owner,
                            "operation_pred": (
                                (gid, operation_pred)
                                if operation_pred is not None
                                else None
                            ),
                            "launched": False,
                            "done": False,
                            "started": time.time(),
                        }

        result_q: queue.Queue = queue.Queue()
        forward_edges = {
            (state["succ"], state["gid"])
            for state in owner_states.values()
            if state["succ"] is not None
        }
        self._ensure_relay_bulk_waiters(forward_edges)
        if not hasattr(self, "_relay_forward_last"):
            self._relay_forward_last = {}
        deadline = time.time() + 600.0

        def _prep_predecessor_free(state: dict) -> bool:
            predecessor = state["prep_pred"]
            if predecessor is not None:
                return owner_states[predecessor]["prep_free"]
            prior_seq = state["prior_epoch_prep_seq"]
            return prior_seq is None or self._relay_prep_was_released(
                state["gid"],
                state["ri"],
                prior_seq,
            )

        def _operation_finished(state: dict, states: dict, field: str) -> bool:
            predecessor = state["operation_pred"]
            return predecessor is None or states[predecessor][field]

        def _incoming_ready(state: dict) -> bool:
            return all(
                self._relay_flag_reached(
                    self._RELAY_DATA_KIND,
                    peer,
                    state["gid"],
                    state["seq"],
                )
                for peer in state["input_peers"]
            )

        def _local_ready(state: dict) -> bool:
            owner = state["owner"]
            gid, ri, seq = state["gid"], state["ri"], state["seq"]
            if owner == self._rank:
                return owner_states[(gid, ri)]["prep_ready"]
            bank = self._relay_peer_local_flags[owner]
            channel = self._relay_peer_channel[owner][(gid, ri % 2)]
            return bank.ready(channel) >= seq

        while True:
            progress = False

            while True:
                try:
                    peer, gid, ri, _seq, error, _landed = result_q.get_nowait()
                except queue.Empty:
                    break
                if error is not None:
                    raise RuntimeError(
                        f"relay forward failed peer={peer} group={gid} round={ri} epoch={wt}"
                    ) from error
                state = owner_states[(gid, ri)]
                if state["succ"] != peer or state["relay_done"]:
                    raise RuntimeError(
                        f"unexpected relay completion peer={peer} group={gid} round={ri}"
                    )
                state["relay_done"] = True
                progress = True

            # Receive and prepare every independently-ready group slot.
            for key in sorted(owner_states, key=lambda item: (item[1], item[0])):
                state = owner_states[key]
                gid, ri, seq = state["gid"], state["ri"], state["seq"]
                group = router.relay_group(gid)
                if not state["input_ready"] and _incoming_ready(state):
                    state["input_ready"] = True
                    if state["pred"] is None:
                        self._tstats["wire_rdma_bytes"] += sum(
                            spec.nbytes(self.dtype_spec)
                            for spec in group["trainer_specs"][ri].values()
                        )
                    else:
                        self._tstats["agh_rdma_bytes"] += group["round_specs"][
                            ri
                        ].nbytes(
                            self.dtype_spec,
                        )
                    gantt.rec(
                        "receiver",
                        self._rank,
                        wt,
                        "relay_recv_wait",
                        ri,
                        state["started"],
                        time.time(),
                    )
                    progress = True
                if (
                    state["input_ready"]
                    and not state["prepare_launched"]
                    and _prep_predecessor_free(state)
                    and _operation_finished(state, owner_states, "prep_ready")
                ):
                    stream = self._relay_prepare_stream[(gid, ri % 2)]
                    with torch.cuda.stream(stream):
                        self._relay_prepare_plan[(gid, ri)].run()
                        for reader in self._relay_local_readers.get(gid, ()):
                            self._relay_ready_event[(gid, ri % 2, reader)].record(
                                stream
                            )
                        self._relay_prepare_event[(gid, ri % 2)].record(stream)
                    state["prepare_launched"] = True
                    state["prepare_t0"] = time.time()
                    progress = True
                if (
                    state["prepare_launched"]
                    and not state["prep_ready"]
                    and self._relay_prepare_event[(gid, ri % 2)].query()
                ):
                    now = time.time()
                    gantt.rec(
                        "receiver",
                        self._rank,
                        wt,
                        "relay_assemble",
                        ri,
                        state["prepare_t0"],
                        now,
                    )
                    state["prep_ready"] = True
                    for peer in state["input_peers"]:
                        self._relay_emit(self._RELAY_ACK_KIND, peer, gid, seq)
                    state["acks_sent"] = True
                    readers = self._relay_local_readers.get(gid, ())
                    if readers:
                        channel = self._relay_local_channel[(gid, ri % 2)]
                        self._relay_local_flags.publish_ready(seq, channel)
                    progress = True

                # Forward after this group's prior write completed, once PREP is ready and this successor's
                # exact prior use of the destination parity has been ACKed.
                succ = state["succ"]
                if (
                    state["prep_ready"]
                    and succ is not None
                    and not state["relay_submitted"]
                    and _operation_finished(state, owner_states, "relay_done")
                ):
                    edge = (succ, gid, ri % 2)
                    prior_seq = self._relay_forward_last.get(edge)
                    if prior_seq is None or self._relay_flag_reached(
                        self._RELAY_ACK_KIND,
                        succ,
                        gid,
                        prior_seq,
                    ):
                        size = group["round_specs"][ri].nbytes(self.dtype_spec)
                        src = self._relay_prep_buf[gid][ri % 2].data_ptr()
                        dst = self._relay_forward_dst[gid][ri]
                        if dst is None:
                            raise RuntimeError(
                                f"relay successor {succ} omitted group={gid} round={ri} destination"
                            )
                        submitted_at = time.time()
                        handle = self.engine.write_async(
                            self._relay_peer_session[succ],
                            [src],
                            [dst],
                            [size],
                        )
                        self._tstats["agh_rdma_bytes"] += size
                        self._relay_bulk_wait_queues[(succ, gid)].put(
                            (
                                wt,
                                ri,
                                seq,
                                handle,
                                submitted_at,
                                "relay_forward",
                                result_q,
                            )
                        )
                        self._relay_forward_last[edge] = seq
                        state["relay_submitted"] = True
                        state["relay_t0"] = submitted_at
                        progress = True

            # Each model worker consumes one canonical PREP per replication group. Different groups remain
            # independent; opposite parities within a group are explicitly ordered by operation completion.
            for key in sorted(consume_states, key=lambda item: (item[1], item[0])):
                state = consume_states[key]
                gid, ri, seq, owner = (
                    state["gid"],
                    state["ri"],
                    state["seq"],
                    state["owner"],
                )
                if (
                    not state["launched"]
                    and _local_ready(state)
                    and _operation_finished(state, consume_states, "done")
                ):
                    stream = self._relay_consume_stream[(gid, ri % 2)]
                    ready_event = (
                        self._relay_prepare_event[(gid, ri % 2)]
                        if owner == self._rank
                        else self._relay_peer_ready_event[(gid, ri % 2)]
                    )
                    stream.wait_event(ready_event)
                    with torch.cuda.stream(stream):
                        self._relay_consume_plan[(gid, ri)].run()
                        self._relay_consume_event[(gid, ri % 2)].record(stream)
                    state["launched"] = True
                    state["launch_t0"] = time.time()
                    progress = True
                if (
                    state["launched"]
                    and not state["done"]
                    and self._relay_consume_event[(gid, ri % 2)].query()
                ):
                    now = time.time()
                    gantt.rec(
                        "receiver",
                        self._rank,
                        wt,
                        "relay_internal_consume",
                        ri,
                        state["launch_t0"],
                        now,
                    )
                    state["done"] = True
                    if owner != self._rank:
                        bank = self._relay_peer_local_flags[owner]
                        channel = self._relay_peer_channel[owner][(gid, ri % 2)]
                        bank.publish_consumed(
                            self._relay_peer_slot_of_me[owner],
                            seq,
                            channel,
                        )
                        self._tstats["agh_ipc_bytes"] += router.relay_group(gid)[
                            "round_specs"
                        ][ri].nbytes(self.dtype_spec)
                    progress = True

            # PREP release is local: outbound RDMA finished and every same-node member finished reading.
            # Full-delivery propagation is recursive and additionally waits for the successor's subtree.
            for key in sorted(owner_states, key=lambda item: (item[1], item[0])):
                state = owner_states[key]
                gid, ri, seq = state["gid"], state["ri"], state["seq"]
                if not state["local_consumed"] and consume_states[(gid, ri)]["done"]:
                    if self._relay_local_readers_done(gid, ri, seq):
                        state["local_consumed"] = True
                        progress = True
                if (
                    not state["prep_free"]
                    and state["local_consumed"]
                    and state["relay_done"]
                ):
                    state["prep_free"] = True
                    self._relay_mark_prep_released(gid, ri, seq)
                    gantt.rec(
                        "receiver",
                        self._rank,
                        wt,
                        "relay_prep_hold",
                        ri,
                        state.get("prepare_t0", state["started"]),
                        time.time(),
                    )
                    progress = True
                downstream_done = state["succ"] is None or self._relay_flag_reached(
                    self._RELAY_DATA_KIND,
                    state["succ"],
                    gid,
                    seq,
                )
                if (
                    state["local_consumed"]
                    and downstream_done
                    and not state["upstream_done"]
                ):
                    if state["pred"] is not None:
                        self._relay_emit(
                            self._RELAY_DATA_KIND,
                            state["pred"],
                            gid,
                            seq,
                        )
                    else:
                        # The head reports full chain consumption to exactly the trainers that contributed
                        # bytes to this canonical group chunk.
                        for trainer in router.relay_group(gid)["trainer_specs"][ri]:
                            self._relay_emit(
                                self._RELAY_DATA_KIND,
                                trainer,
                                gid,
                                seq,
                            )
                    state["upstream_done"] = True
                    progress = True

            if self._relay_local_exit_ready(owner_states, consume_states):
                retire_tasks = []
                for state in owner_states.values():
                    if state["upstream_done"]:
                        continue
                    gid, ri = state["gid"], state["ri"]
                    upstream_peers = (
                        (state["pred"],)
                        if state["pred"] is not None
                        else tuple(sorted(router.relay_group(gid)["trainer_specs"][ri]))
                    )
                    retire_tasks.append(
                        {
                            "wt": wt,
                            "gid": gid,
                            "ri": ri,
                            "seq": state["seq"],
                            "succ": state["succ"],
                            "upstream_peers": upstream_peers,
                            "prep_released": state["prep_free"],
                        }
                    )
                self._defer_relay_retirement(retire_tasks)
                break
            if not progress:
                if time.time() >= deadline:
                    owner_pending = {
                        key: {
                            field: state[field]
                            for field in (
                                "input_ready",
                                "prepare_launched",
                                "prep_ready",
                                "relay_submitted",
                                "relay_done",
                                "local_consumed",
                                "prep_free",
                                "upstream_done",
                            )
                        }
                        for key, state in owner_states.items()
                        if not state["relay_done"]
                    }
                    consume_pending = [
                        key
                        for key, state in consume_states.items()
                        if not state["done"]
                    ]
                    raise TimeoutError(
                        f"replica relay timeout epoch={wt} owner={owner_pending} "
                        f"consume={consume_pending}"
                    )
                time.sleep(1e-4)

        self._flag_reaper_check()
        self._epoch += 1
        self._trace_state("relay_receive_local_done", epoch=wt)

    def _receive_relay_group(self, wt: int, gid: int) -> list[dict]:
        """Progress one replica group independently on a persistent executor thread.

        Registered PREP is both the ingress destination and relay source. A head snapshots its trainer lanes
        into epoch scratch before assembling back into PREP; downstream representatives already receive the
        canonical layout. Every representative then starts its successor write and PREP->DOFF copy
        independently. ACK is delayed until both readers of PREP (relay and offload) finish, whereas model
        consumption and recursive full delivery continue from DOFF.
        """
        router = self.router
        assert router is not None
        group = router.relay_group(gid)
        sw = router.sender_ws
        seq_of = lambda ri: wt * self.num_rounds + ri + 1
        active_rounds = [
            ri for ri, spec in enumerate(group["round_specs"]) if spec.entries
        ]
        if not active_rounds:
            return []
        dependencies = _relay_round_predecessors(active_rounds, depth=2)
        doff_previous: dict[int, int] = {}
        doff_predecessor: dict[int, int | None] = {}
        last_doff_by_slot: dict[int, int] = {}
        for ri in active_rounds:
            slot = ri % self._relay_doff_depth
            doff_predecessor[ri] = doff_previous.get(slot)
            doff_previous[slot] = ri
            last_doff_by_slot[slot] = ri
        last_prep_by_parity = {
            parity: max(ri for ri in active_rounds if ri % 2 == parity)
            for parity in {ri % 2 for ri in active_rounds}
        }

        owns = gid in self._relay_owned_gids
        consumes = (self._rank - sw) in group["members"]
        owner_states: dict[int, dict] = {}
        consume_states: dict[int, dict] = {}
        scratch = None
        if owns:
            chain = group["chain"]
            position = chain.index(self._rank)
            pred = chain[position - 1] if position else None
            succ = chain[position + 1] if position + 1 < len(chain) else None
            if pred is None:
                scratch = torch.empty(
                    self._relay_head_scratch_size[gid],
                    dtype=torch.uint8,
                    device=self.device,
                )
            for ri in active_rounds:
                operation_pred, prep_pred = dependencies[ri]
                doff_slot = ri % self._relay_doff_depth
                owner_states[ri] = {
                    "gid": gid,
                    "ri": ri,
                    "seq": seq_of(ri),
                    "pred": pred,
                    "succ": succ,
                    "input_peers": (
                        tuple(sorted(group["trainer_specs"][ri]))
                        if pred is None
                        else (pred,)
                    ),
                    "operation_pred": operation_pred,
                    "prep_pred": prep_pred,
                    "doff_pred": doff_predecessor[ri],
                    "prior_epoch_prep_seq": (
                        None
                        if wt == 0
                        else (wt - 1) * self.num_rounds
                        + last_prep_by_parity[ri % 2]
                        + 1
                    ),
                    "prior_epoch_doff_seq": (
                        None
                        if wt == 0
                        else (wt - 1) * self.num_rounds
                        + last_doff_by_slot[doff_slot]
                        + 1
                    ),
                    "input_ready": False,
                    "prepare_launched": False,
                    "prep_ready": False,
                    "offload_launched": False,
                    "doff_ready": False,
                    "relay_submitted": False,
                    "relay_done": succ is None,
                    "acks_sent": False,
                    "local_consumed": False,
                    "prep_free": False,
                    "upstream_done": False,
                    "started": time.time(),
                }
        if consumes:
            owner = sw + group["owner_of"][self._rank - sw]
            for ri in active_rounds:
                operation_pred, _prep_pred = dependencies[ri]
                consume_states[ri] = {
                    "gid": gid,
                    "ri": ri,
                    "seq": seq_of(ri),
                    "owner": owner,
                    "operation_pred": operation_pred,
                    "launched": False,
                    "done": False,
                    "started": time.time(),
                }

        result_q: queue.Queue = queue.Queue()
        if owns and owner_states[active_rounds[0]]["succ"] is not None:
            self._ensure_relay_bulk_waiters(
                {(owner_states[active_rounds[0]]["succ"], gid)}
            )
        deadline = time.time() + 600.0

        def operation_done(state: dict, states: dict, field: str) -> bool:
            predecessor = state["operation_pred"]
            return predecessor is None or states[predecessor][field]

        def prep_slot_free(state: dict) -> bool:
            predecessor = state["prep_pred"]
            if predecessor is not None:
                return owner_states[predecessor]["prep_free"]
            prior_seq = state["prior_epoch_prep_seq"]
            return prior_seq is None or self._relay_prep_was_released(
                gid, state["ri"], prior_seq
            )

        def doff_slot_free(state: dict) -> bool:
            predecessor = state["doff_pred"]
            if predecessor is not None:
                return owner_states[predecessor]["local_consumed"]
            prior_seq = state["prior_epoch_doff_seq"]
            return prior_seq is None or self._relay_doff_was_released(
                gid, state["ri"], prior_seq
            )

        def incoming_ready(state: dict) -> bool:
            return all(
                self._relay_flag_reached(
                    self._RELAY_DATA_KIND,
                    peer,
                    gid,
                    state["seq"],
                )
                for peer in state["input_peers"]
            )

        def local_doff_ready(state: dict) -> bool:
            owner = state["owner"]
            ri, seq = state["ri"], state["seq"]
            if owner == self._rank:
                return owner_states[ri]["doff_ready"]
            bank = self._relay_peer_local_flags[owner]
            channel = self._relay_peer_channel[owner][
                (gid, ri % self._relay_doff_depth)
            ]
            return bank.ready(channel) >= seq

        with torch.cuda.device(self.device):
            while True:
                progress = False

                while True:
                    try:
                        peer, result_gid, ri, _seq, error, _landed = (
                            result_q.get_nowait()
                        )
                    except queue.Empty:
                        break
                    if error is not None:
                        raise RuntimeError(
                            f"relay forward failed peer={peer} group={gid} round={ri} epoch={wt}"
                        ) from error
                    if result_gid != gid or owner_states[ri]["succ"] != peer:
                        raise RuntimeError(
                            f"unexpected relay completion peer={peer} group={result_gid} round={ri}"
                        )
                    owner_states[ri]["relay_done"] = True
                    progress = True

                for ri in active_rounds:
                    if not owns:
                        break
                    state = owner_states[ri]
                    seq = state["seq"]
                    parity = ri % 2
                    doff_slot = ri % self._relay_doff_depth
                    size = group["round_specs"][ri].nbytes(self.dtype_spec)

                    if not state["input_ready"] and incoming_ready(state):
                        state["input_ready"] = True
                        if state["pred"] is None:
                            self._tstats["wire_rdma_bytes"] += sum(
                                spec.nbytes(self.dtype_spec)
                                for spec in group["trainer_specs"][ri].values()
                            )
                        else:
                            self._tstats["agh_rdma_bytes"] += size
                        gantt.rec(
                            "receiver",
                            self._rank,
                            wt,
                            "relay_recv_wait",
                            ri,
                            state["started"],
                            time.time(),
                        )
                        progress = True

                    # Heads snapshot trainer lanes and assemble back into PREP. Downstream input already has
                    # canonical packed layout, so DATA completion itself makes PREP ready.
                    if state["pred"] is None:
                        if (
                            state["input_ready"]
                            and not state["prepare_launched"]
                            and prep_slot_free(state)
                            and operation_done(state, owner_states, "prep_ready")
                        ):
                            assert scratch is not None
                            stream = self._relay_prepare_stream[(gid, parity)]
                            with torch.cuda.stream(stream):
                                snapshot_bytes = self._relay_snapshot_bytes[(gid, ri)]
                                scratch[:snapshot_bytes].copy_(
                                    self._relay_prep_buf[gid][parity][:snapshot_bytes]
                                )
                                plan = self._relay_prepare_plan[(gid, ri)]
                                plan.rebase_sources(scratch.data_ptr())
                                plan.run()
                                self._relay_prepare_event[(gid, parity)].record(stream)
                            state["prepare_launched"] = True
                            state["prepare_t0"] = time.time()
                            progress = True
                        if (
                            state["prepare_launched"]
                            and not state["prep_ready"]
                            and self._relay_prepare_event[(gid, parity)].query()
                        ):
                            state["prep_ready"] = True
                            state["prep_ready_at"] = time.time()
                            gantt.rec(
                                "receiver",
                                self._rank,
                                wt,
                                "relay_assemble",
                                ri,
                                state["prepare_t0"],
                                state["prep_ready_at"],
                            )
                            progress = True
                    elif (
                        state["input_ready"]
                        and not state["prep_ready"]
                        and prep_slot_free(state)
                        and operation_done(state, owner_states, "prep_ready")
                    ):
                        state["prep_ready"] = True
                        state["prep_ready_at"] = time.time()
                        progress = True

                    # PREP->DOFF is ordered per group, and a DOFF generation cannot be overwritten until all
                    # model readers of its previous generation finish.
                    if (
                        state["prep_ready"]
                        and not state["offload_launched"]
                        and doff_slot_free(state)
                        and operation_done(state, owner_states, "doff_ready")
                    ):
                        stream = self._relay_offload_stream[(gid, doff_slot)]
                        with torch.cuda.stream(stream):
                            self._relay_doff_buf[gid][doff_slot][:size].copy_(
                                self._relay_prep_buf[gid][parity][:size]
                            )
                            for reader in self._relay_local_readers.get(gid, ()):
                                self._relay_ready_event[
                                    (gid, doff_slot, reader)
                                ].record(stream)
                            self._relay_offload_event[(gid, doff_slot)].record(stream)
                        state["offload_launched"] = True
                        state["offload_t0"] = time.time()
                        progress = True
                    if (
                        state["offload_launched"]
                        and not state["doff_ready"]
                        and self._relay_offload_event[(gid, doff_slot)].query()
                    ):
                        state["doff_ready"] = True
                        now = time.time()
                        gantt.rec(
                            "receiver",
                            self._rank,
                            wt,
                            "relay_doff_copy",
                            ri,
                            state["offload_t0"],
                            now,
                        )
                        readers = self._relay_local_readers.get(gid, ())
                        if readers:
                            channel = self._relay_local_channel[(gid, doff_slot)]
                            self._relay_local_flags.publish_ready(seq, channel)
                        progress = True

                    # Preserve the requested adjacent-round completion fence. Destination parity reuse is the
                    # independent ACK for this successor's previous use of the same PREP slot.
                    succ = state["succ"]
                    if (
                        state["prep_ready"]
                        and succ is not None
                        and not state["relay_submitted"]
                        and operation_done(state, owner_states, "relay_done")
                    ):
                        edge = (succ, gid, parity)
                        prior_seq = self._relay_forward_last.get(edge)
                        if prior_seq is None or self._relay_flag_reached(
                            self._RELAY_ACK_KIND,
                            succ,
                            gid,
                            prior_seq,
                        ):
                            submitted_at = time.time()
                            handle = self.engine.write_async(
                                self._relay_peer_session[succ],
                                [self._relay_prep_buf[gid][parity].data_ptr()],
                                [self._relay_forward_dst[gid][ri]],
                                [size],
                            )
                            self._tstats["agh_rdma_bytes"] += size
                            self._relay_bulk_wait_queues[(succ, gid)].put(
                                (
                                    wt,
                                    ri,
                                    seq,
                                    handle,
                                    submitted_at,
                                    "relay_forward",
                                    result_q,
                                )
                            )
                            self._relay_forward_last[edge] = seq
                            state["relay_submitted"] = True
                            progress = True

                    # ACK now describes the merged PREP destination lifetime, not merely assembly. No input
                    # writer may reuse this parity until both forwarding and offload have stopped reading it.
                    if (
                        not state["prep_free"]
                        and state["doff_ready"]
                        and state["relay_done"]
                    ):
                        state["prep_free"] = True
                        self._relay_mark_prep_released(gid, ri, seq)
                        for peer in state["input_peers"]:
                            self._relay_emit(self._RELAY_ACK_KIND, peer, gid, seq)
                        state["acks_sent"] = True
                        gantt.rec(
                            "receiver",
                            self._rank,
                            wt,
                            "relay_prep_hold",
                            ri,
                            state["prep_ready_at"],
                            time.time(),
                        )
                        progress = True

                for ri in active_rounds:
                    if not consumes:
                        break
                    state = consume_states[ri]
                    owner = state["owner"]
                    doff_slot = ri % self._relay_doff_depth
                    if (
                        not state["launched"]
                        and local_doff_ready(state)
                        and operation_done(state, consume_states, "done")
                    ):
                        stream = self._relay_consume_stream[(gid, doff_slot)]
                        ready_event = (
                            self._relay_offload_event[(gid, doff_slot)]
                            if owner == self._rank
                            else self._relay_peer_ready_event[(gid, doff_slot)]
                        )
                        stream.wait_event(ready_event)
                        with torch.cuda.stream(stream):
                            self._relay_consume_plan[(gid, ri)].run()
                            self._relay_consume_event[(gid, doff_slot)].record(stream)
                        state["launched"] = True
                        state["launch_t0"] = time.time()
                        progress = True
                    if (
                        state["launched"]
                        and not state["done"]
                        and self._relay_consume_event[(gid, doff_slot)].query()
                    ):
                        state["done"] = True
                        gantt.rec(
                            "receiver",
                            self._rank,
                            wt,
                            "relay_internal_consume",
                            ri,
                            state["launch_t0"],
                            time.time(),
                        )
                        if owner != self._rank:
                            bank = self._relay_peer_local_flags[owner]
                            channel = self._relay_peer_channel[owner][(gid, doff_slot)]
                            bank.publish_consumed(
                                self._relay_peer_slot_of_me[owner],
                                state["seq"],
                                channel,
                            )
                            self._tstats["agh_ipc_bytes"] += group["round_specs"][
                                ri
                            ].nbytes(
                                self.dtype_spec,
                            )
                        progress = True

                if owns:
                    for ri in active_rounds:
                        state = owner_states[ri]
                        if (
                            not state["local_consumed"]
                            and consume_states[ri]["done"]
                            and self._relay_local_readers_done(gid, ri, state["seq"])
                        ):
                            state["local_consumed"] = True
                            self._relay_mark_doff_released(gid, ri, state["seq"])
                            progress = True
                        downstream_done = state[
                            "succ"
                        ] is None or self._relay_flag_reached(
                            self._RELAY_DATA_KIND,
                            state["succ"],
                            gid,
                            state["seq"],
                        )
                        if (
                            state["local_consumed"]
                            and downstream_done
                            and not state["upstream_done"]
                        ):
                            for peer in state["input_peers"]:
                                self._relay_emit(
                                    self._RELAY_DATA_KIND,
                                    peer,
                                    gid,
                                    state["seq"],
                                )
                            state["upstream_done"] = True
                            progress = True

                if self._relay_local_exit_ready(owner_states, consume_states):
                    tasks = []
                    for ri, state in owner_states.items():
                        if state["upstream_done"]:
                            continue
                        tasks.append(
                            {
                                "wt": wt,
                                "gid": gid,
                                "ri": ri,
                                "seq": state["seq"],
                                "succ": state["succ"],
                                "upstream_peers": state["input_peers"],
                                "doff_released": state["local_consumed"],
                            }
                        )
                    return tasks

                if not progress:
                    if time.time() >= deadline:
                        raise TimeoutError(
                            f"replica relay timeout epoch={wt} group={gid} "
                            f"owner={owner_states} consume={consume_states}"
                        )
                    time.sleep(1e-4)

    def _receive_weights_relay(self, staged: bool = False) -> None:
        """Run one independent persistent CPU progress lane per replica group."""
        if staged:
            raise RuntimeError("replica-group relay does not support receiver staging")
        wt = self._epoch
        self._flag_reaper_ensure()
        self._relay_retire_check()
        if not hasattr(self, "_relay_forward_last"):
            self._relay_forward_last = {}

        gids = sorted(set(self._relay_owned_gids) | set(self._relay_consume_owner))
        if not gids:
            raise RuntimeError(
                f"relay receiver rank {self._rank} has no replica groups"
            )
        executor = getattr(self, "_relay_group_executor", None)
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=len(gids),
                thread_name_prefix=f"wbridge-relay-r{self._rank}",
            )
            self._relay_group_executor = executor
        futures = [executor.submit(self._receive_relay_group, wt, gid) for gid in gids]
        retire_tasks = []
        for future in futures:
            retire_tasks.extend(future.result())
        self._defer_relay_retirement(retire_tasks)
        self._flag_reaper_check()
        self._epoch += 1
        self._trace_state("relay_receive_local_done", epoch=wt)
