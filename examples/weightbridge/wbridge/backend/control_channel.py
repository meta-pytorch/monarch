# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Torch-free per-engine control star for WeightBridge receivers.

Two pieces, split by which thread owns their ZMQ sockets (sockets are NOT thread-safe):

* :class:`CoordinatorClient` — rank0's trainer-facing intake (the ``worker-0`` DEALER to the coordinator).
  Owned by rank0's **receiving thread**; normally it carries connect only, plus the zero-input update
  fallback, independently of the scheduler tick.
* :class:`ControlChannel` — the rank0<->peers **hub star**, used on the **main (scheduler) thread**. Every
  scheduler tick uses a prepare/ack/commit barrier, so all of an engine's TP ranks have reached the hook
  before any rank proceeds. This includes ``empty`` ticks: the barrier bounds the queue to one decision and
  avoids making peer ranks sleep until a long polling timeout.

rank0's main thread reads the state the receiving thread posted and feeds it into
:meth:`ControlChannel.broadcast`. This module imports only ``zmq`` (no torch/fastapi) so it is unit-testable
without a GPU.
"""

import json
import logging
import os
import tempfile
import time
from typing import Any, Optional

import zmq

logger = logging.getLogger(__name__)

# Message types for coordinator <-> rank0 and rank0 <-> peers.
CONNECT_REQUEST = "connect_request"
RECEIVE_REQUEST = "receive_request"
RECEIVE_KICK = (
    "receive_kick"  # rank0 -> peers (RS-on phase 1): start the receive-to-CPU worker
)
EMPTY = "empty"
REGISTER = "register"  # peer -> rank0 hub startup registration
WORKER_REGISTER = (
    "worker_register"  # rank0 -> coordinator: reports this engine's receiver count
)

# Internal rank0 <-> peer synchronization fields.  They stay private to this module and are stripped before
# a decision is returned to WeightReceiver.
_SYNC_SEQ = "_wbridge_control_seq"
_SYNC_PHASE = "_wbridge_control_phase"
_SYNC_PREPARE = "prepare"
_SYNC_COMMIT = "commit"
_SYNC_ACK = "ack"

# Message types for trainer <-> coordinator (external, over the coordinator's TCP ROUTER).
WORLD_QUERY = "world_query"  # -> {status, world_size}
CONNECT = "connect"  # + process-group args -> {status}
RECEIVE = "receive"  # -> {status}

# The coordinator's trainer-facing TCP port is derived deterministically from SGLang's HTTP server port,
# so every process (all TP ranks + the trainer's discovery) computes the same endpoint with no handoff.
WBRIDGE_PORT_OFFSET = 1717
WBRIDGE_HUB_PORT_OFFSET = WBRIDGE_PORT_OFFSET + 1

# This startup guard covers both registration hops: receiver rank 0 joining
# the standalone coordinator and the remaining model-parallel ranks joining
# rank 0's hub.  Large-model LoadSpec construction can stagger those ranks by
# many minutes, so launchers need one consistent knob for both waits.
STARTUP_REGISTER_TIMEOUT_ENV = "WBRIDGE_COORDINATOR_REGISTER_TIMEOUT_S"


def startup_register_timeout_s() -> float:
    """Return the startup-only receiver registration timeout."""
    raw = os.environ.get(STARTUP_REGISTER_TIMEOUT_ENV, "600").strip()
    try:
        timeout_s = float(raw)
    except ValueError as exc:
        raise ValueError(
            f"{STARTUP_REGISTER_TIMEOUT_ENV} must be a positive number, got {raw!r}"
        ) from exc
    if timeout_s <= 0:
        raise ValueError(
            f"{STARTUP_REGISTER_TIMEOUT_ENV} must be positive, got {raw!r}"
        )
    return timeout_s


def coordinator_ipc(server_port: int) -> str:
    """Deterministic rank0-facing IPC path for the coordinator, derived from the engine's server port."""
    ipc_dir = os.path.abspath(
        os.path.expanduser(
            os.environ.get("WBRIDGE_COORDINATOR_IPC_DIR", tempfile.gettempdir())
        )
    )
    return f"ipc://{os.path.join(ipc_dir, f'wbridge_coord_{server_port}.sock')}"


def coordinator_tcp_port(server_port: int) -> int:
    """Deterministic trainer-facing TCP port for the coordinator, derived from the engine's server port."""
    return server_port + WBRIDGE_PORT_OFFSET


def multi_node_hub_addr(
    server_port: int,
    dist_init_addr: str | None,
    nnodes: int,
) -> str | None:
    """Return a rank-0-hosted TCP hub for a multi-node model-parallel engine.

    A filesystem IPC endpoint is visible only to processes on one host.  For a
    multi-node SGLang engine, all ranks already agree on ``dist_init_addr``;
    reuse its rank-0 host with a deterministic, coordinator-adjacent port.
    Single-node engines return ``None`` and retain the lower-overhead IPC hub.
    """
    if nnodes <= 1:
        return None
    if not dist_init_addr:
        raise ValueError("multi-node receiver hub requires dist_init_addr")
    address = dist_init_addr.strip()
    if address.startswith("["):
        close = address.find("]")
        if close <= 1:
            raise ValueError(f"invalid bracketed dist_init_addr: {dist_init_addr!r}")
        host = address[1:close]
        host_for_zmq = f"[{host}]"
    else:
        if ":" not in address:
            raise ValueError(f"dist_init_addr has no port: {dist_init_addr!r}")
        host_for_zmq = address.rsplit(":", 1)[0]
    return f"tcp://{host_for_zmq}:{server_port + WBRIDGE_HUB_PORT_OFFSET}"


def hub_addr(controller_ipc_name: str) -> str:
    """The rank0 hub reuses the coordinator IPC path with a distinct suffix, so every worker
    (which all know the coordinator IPC) can derive the same address without extra discovery."""
    return f"{controller_ipc_name}.hub"


class CoordinatorClient:
    """rank0's trainer-facing coordinator intake (the ``worker-0`` DEALER). Owned by the receiving thread.

    Sends :data:`WORKER_REGISTER` on creation (so the coordinator can answer the trainer's world query
    without a separate config path), then blocks on coordinator requests, acking each so the trainer can
    proceed. Normal updates are flag-driven; ``receive`` is only a zero-input fallback. All socket ops happen
    on the one thread that creates it.
    """

    def __init__(
        self,
        controller_ipc_name: str,
        num_workers: int,
        *,
        ctx: Optional[zmq.Context] = None,
    ) -> None:
        self._ctx = ctx if ctx is not None else zmq.Context()
        self._owns_ctx = ctx is None
        self._sock = self._ctx.socket(zmq.DEALER)
        self._sock.setsockopt_string(zmq.IDENTITY, "worker-0")
        self._sock.connect(controller_ipc_name)
        # ZMQ queues this if the coordinator has not bound yet (lazy connect).
        self._sock.send_string(
            json.dumps({"type": WORKER_REGISTER, "num_workers": num_workers})
        )
        self._poller = zmq.Poller()
        self._poller.register(self._sock, zmq.POLLIN)

    def poll(self, timeout_ms: int = 1000) -> Optional[dict[str, Any]]:
        """Block up to *timeout_ms* for the next trainer request; return it or ``None`` on timeout."""
        if not dict(self._poller.poll(timeout_ms)):
            return None
        return json.loads(self._sock.recv_string())

    def ack(self) -> None:
        """Ack the current request (lets the coordinator reply to the trainer)."""
        self._sock.send_string(json.dumps({"status": "ack"}))

    def close(self) -> None:
        try:
            self._sock.close(linger=0)
            if self._owns_ctx:
                self._ctx.term()
        except Exception:
            pass


class ControlChannel:
    """rank0<->peers hub star (ZMQ only), used on the MAIN thread to keep TP ranks in lockstep.

    Topology per engine::

        rank0 --ROUTER hub / DEALER--> peer-1 .. peer-{N-1}

    Every decision is sent in two phases. Every peer acknowledges ``prepare`` from its scheduler thread and
    waits for ``commit``; rank 0 sends ``commit`` only after every peer has acknowledged. Thus no receiver can
    leave an in-flight SGLang collective for a blocking WeightBridge collective while another receiver is
    still participating in it. Applying the same barrier to ``empty`` decisions prevents both an idle FIFO
    backlog and the multi-second peer polling stalls that would otherwise desynchronize TP ranks. The
    trainer-facing intake is a separate :class:`CoordinatorClient` (rank0's receiving thread).
    """

    def __init__(
        self,
        controller_ipc_name: str,
        rank: int,
        num_workers: int,
        *,
        hub_endpoint: str | None = None,
        ctx: Optional[zmq.Context] = None,
        reg_timeout_s: float | None = None,
    ) -> None:
        self.rank = rank
        self.num_workers = num_workers
        self._ctx = ctx if ctx is not None else zmq.Context()
        self._owns_ctx = ctx is None
        hub = (
            hub_endpoint if hub_endpoint is not None else hub_addr(controller_ipc_name)
        )

        if rank == 0:
            self._hub = self._ctx.socket(zmq.ROUTER)
            self._hub.bind(hub)
            self._peer_ids: list[bytes] = []
            self._decision_seq = 0
            self._await_registration(
                startup_register_timeout_s() if reg_timeout_s is None else reg_timeout_s
            )
        else:
            self._peer = self._ctx.socket(zmq.DEALER)
            self._peer.setsockopt_string(zmq.IDENTITY, f"peer-{rank}")
            self._peer.connect(hub)
            self._poller = zmq.Poller()
            self._poller.register(self._peer, zmq.POLLIN)
            self._peer.send_string(json.dumps({"type": REGISTER, "rank": rank}))

    def _await_registration(self, timeout_s: float) -> None:
        """rank0: block until all ``num_workers-1`` peers have registered on the hub."""
        expected = self.num_workers - 1
        if expected <= 0:
            return
        poller = zmq.Poller()
        poller.register(self._hub, zmq.POLLIN)
        deadline = time.time() + timeout_s
        while len(self._peer_ids) < expected:
            if dict(poller.poll(1000)):
                ident, _payload = self._hub.recv_multipart()
                if ident not in self._peer_ids:
                    self._peer_ids.append(ident)
            elif time.time() > deadline:
                raise TimeoutError(
                    f"receiver rank0 hub: only {len(self._peer_ids)}/{expected} peers registered"
                )
        logger.info(
            "wbridge receiver rank0: %d peers registered on hub", len(self._peer_ids)
        )

    @staticmethod
    def _public_decision(decision: dict[str, Any]) -> dict[str, Any]:
        """Remove synchronization metadata before exposing a decision to WeightReceiver."""
        return {k: v for k, v in decision.items() if k not in (_SYNC_SEQ, _SYNC_PHASE)}

    def broadcast(self, decision: dict[str, Any]) -> None:
        """Rank 0: barrier-broadcast one scheduler-tick decision.

        Peers acknowledge the prepare from the scheduler thread and wait for a commit. Rank 0 returns only
        after all peers are quiescent and the commit has been published. ``EMPTY`` follows the same bounded
        handshake, so it cannot accumulate in front of an actionable decision or leave peers waiting for the
        polling timeout.
        """

        self._decision_seq += 1
        seq = self._decision_seq
        prepare = {**decision, _SYNC_SEQ: seq, _SYNC_PHASE: _SYNC_PREPARE}
        payload = json.dumps(prepare).encode("utf-8")
        for ident in self._peer_ids:
            self._hub.send_multipart([ident, payload])

        pending = set(self._peer_ids)
        if pending:
            poller = zmq.Poller()
            poller.register(self._hub, zmq.POLLIN)
            timeout_s = startup_register_timeout_s()
            deadline = time.time() + timeout_s
            while pending:
                remaining_ms = max(1, min(1000, int((deadline - time.time()) * 1000)))
                if not dict(poller.poll(remaining_ms)):
                    if time.time() >= deadline:
                        missing = sorted(
                            ident.decode("utf-8", "replace") for ident in pending
                        )
                        raise TimeoutError(
                            f"receiver rank0 control barrier seq={seq}: "
                            f"only {len(self._peer_ids) - len(pending)}/{len(self._peer_ids)} peers acked; "
                            f"missing={missing}"
                        )
                    continue
                frames = self._hub.recv_multipart()
                if len(frames) != 2:
                    logger.warning(
                        "receiver rank0 control barrier: ignoring malformed frames=%d",
                        len(frames),
                    )
                    continue
                ident, raw = frames
                try:
                    message = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    logger.warning(
                        "receiver rank0 control barrier: ignoring malformed peer payload"
                    )
                    continue
                if (
                    message.get(_SYNC_PHASE) == _SYNC_ACK
                    and message.get(_SYNC_SEQ) == seq
                ):
                    pending.discard(ident)
                else:
                    logger.warning(
                        "receiver rank0 control barrier seq=%d: ignoring unexpected message from %r: %r",
                        seq,
                        ident,
                        message,
                    )

        commit = {**decision, _SYNC_SEQ: seq, _SYNC_PHASE: _SYNC_COMMIT}
        payload = json.dumps(commit).encode("utf-8")
        for ident in self._peer_ids:
            self._hub.send_multipart([ident, payload])

    def poll_decision(self, timeout_ms: int = 5000) -> Optional[dict[str, Any]]:
        """Peer scheduler thread: wait for rank 0's next scheduler-tick decision.

        The initial prepare wait is bounded by *timeout_ms* so idle schedulers continue normally.  Once a
        prepare arrives, this rank acknowledges that it is quiescent and waits for the matching commit; no
        WeightBridge action is visible to the caller before every peer has acknowledged.
        """
        if not dict(self._poller.poll(timeout_ms)):
            return None
        decision = json.loads(self._peer.recv_string())
        if decision.get(_SYNC_PHASE) != _SYNC_PREPARE:
            # Compatibility with a pre-barrier rank 0, useful during rolling tests.  Production snapshots
            # keep sender and receiver sources identical, so this path is not used in a campaign.
            return self._public_decision(decision)

        seq = decision.get(_SYNC_SEQ)
        self._peer.send_string(json.dumps({_SYNC_SEQ: seq, _SYNC_PHASE: _SYNC_ACK}))
        deadline = time.time() + startup_register_timeout_s()
        while True:
            remaining_ms = max(1, min(1000, int((deadline - time.time()) * 1000)))
            if not dict(self._poller.poll(remaining_ms)):
                if time.time() >= deadline:
                    raise TimeoutError(
                        f"receiver peer {self.rank} timed out waiting for control commit seq={seq}"
                    )
                continue
            commit = json.loads(self._peer.recv_string())
            if commit.get(_SYNC_PHASE) == _SYNC_COMMIT and commit.get(_SYNC_SEQ) == seq:
                return self._public_decision(commit)
            raise RuntimeError(
                f"receiver peer {self.rank} expected control commit seq={seq}, got {commit!r}"
            )

    def close(self) -> None:
        try:
            if self.rank == 0:
                self._hub.close(linger=0)
            else:
                self._peer.close(linger=0)
            if self._owns_ctx:
                self._ctx.term()
        except Exception:
            pass
