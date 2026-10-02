# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Standalone per-engine ZMQ control-plane coordinator for WeightBridge.

Replaces the former FastAPI ``WeightReceiverController``. Runs as its OWN process (spawned once per
rollout engine) so external control requests from the Trainer are serviced continuously, independent of
any rollout worker's scheduler tick. Pure ``zmq`` + JSON — no torch, no FastAPI.

**Thin relay.** It forwards the Trainer's one-time ``connect`` (and rare zero-input ``receive`` fallback)
to receiver rank 0 over the node-local IPC ROUTER and routes rank 0's ack back over the Trainer-facing TCP
ROUTER; it answers the Trainer's
``world_query`` locally from the count rank 0 reports at startup (:data:`WORKER_REGISTER`). rank 0 keeps
its own peer fan-out hub (see :mod:`wbridge.backend.control_channel`) — the coordinator never talks to the
peers, so the lockstep rendezvous is unchanged.

Run as ``python -m wbridge.backend.coordinator --ipc <addr> --tcp-port <n>`` (usually via :func:`spawn`).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import threading
import time
from typing import Any, Optional

import zmq
from wbridge.backend.control_channel import (
    CONNECT,
    CONNECT_REQUEST,
    RECEIVE,
    RECEIVE_REQUEST,
    STARTUP_REGISTER_TIMEOUT_ENV,
    startup_register_timeout_s,
    WORKER_REGISTER,
    WORLD_QUERY,
)

logger = logging.getLogger(__name__)

COORDINATOR_REGISTER_TIMEOUT_ENV = STARTUP_REGISTER_TIMEOUT_ENV


def _coordinator_register_timeout_s() -> float:
    """Backward-compatible wrapper for the shared startup registration guard."""
    return startup_register_timeout_s()


class WeightReceiverController:
    """ZMQ control-plane relay between Trainer rank 0 and one rollout engine's receiver rank 0.

    Binds two ROUTERs: ``tcp_endpoint`` (Trainer-facing) and ``ipc_name`` (rank0-facing; the same address
    receiver rank 0 connects its ``worker-0`` DEALER to). Call :meth:`serve` to run the relay loop.
    """

    def __init__(
        self, ipc_name: str, tcp_endpoint: str, *, ctx: Optional[zmq.Context] = None
    ) -> None:
        self._ipc_name = ipc_name
        self._tcp_endpoint = tcp_endpoint
        self._ctx = ctx if ctx is not None else zmq.Context.instance()
        self._rank0 = self._ctx.socket(
            zmq.ROUTER
        )  # receiver rank 0 connects here (identity worker-0)
        self._rank0.bind(ipc_name)
        self._trainer = self._ctx.socket(
            zmq.ROUTER
        )  # Trainer rank 0 connects here (over TCP)
        self._trainer.bind(tcp_endpoint)
        self._worker_num: Optional[int] = None

    @property
    def ipc_name(self) -> str:
        return self._ipc_name

    # ---- rank0-facing IPC (same wire protocol as the former controller) ----
    def _send_to_rank0(self, payload: dict[str, Any]) -> None:
        self._rank0.send_multipart([b"worker-0", json.dumps(payload).encode("utf-8")])

    def _gather_ack(self, *, timeout_s: float = 600.0) -> bool:
        """Block until receiver rank 0 replies on the IPC ROUTER; return whether it acked."""
        if not self._rank0.poll(int(timeout_s * 1000)):
            raise TimeoutError(
                f"wbridge coordinator: no ack from receiver rank 0 within {timeout_s:.0f}s"
            )
        msg = self._rank0.recv_multipart()[1]
        return json.loads(msg.decode("utf-8")).get("status") == "ack"

    def _await_register(self, *, timeout_s: float = 600.0) -> None:
        """Block until receiver rank 0 reports its worker count (:data:`WORKER_REGISTER`)."""
        if not self._rank0.poll(int(timeout_s * 1000)):
            raise TimeoutError(
                f"wbridge coordinator: receiver rank 0 did not register within {timeout_s:.0f}s"
            )
        msg = json.loads(self._rank0.recv_multipart()[1].decode("utf-8"))
        assert msg.get("type") == WORKER_REGISTER, (
            f"expected WORKER_REGISTER, got {msg.get('type')!r}"
        )
        self._worker_num = int(msg["num_workers"])
        logger.info(
            "wbridge coordinator: receiver rank 0 registered (world_size=%d)",
            self._worker_num,
        )

    # ---- relay loop ----
    def serve(self) -> None:
        """Block: wait for rank 0's registration, then relay Trainer requests until killed."""
        self._await_register(timeout_s=_coordinator_register_timeout_s())
        logger.info("wbridge coordinator: serving Trainer on %s", self._tcp_endpoint)
        while True:
            ident, payload = self._trainer.recv_multipart()
            try:
                reply = self._handle(json.loads(payload.decode("utf-8")))
            except Exception as e:  # noqa: BLE001 — one bad request must not kill the relay
                logger.warning("wbridge coordinator: request failed: %s", e)
                reply = {"status": "error", "error": str(e)}
            self._trainer.send_multipart([ident, json.dumps(reply).encode("utf-8")])

    def _handle(self, req: dict[str, Any]) -> dict[str, Any]:
        t = req.get("type")
        if t == WORLD_QUERY:
            return {"status": "success", "world_size": self._worker_num}
        if t == CONNECT:
            self._send_to_rank0(
                {
                    "type": CONNECT_REQUEST,
                    **{k: v for k, v in req.items() if k != "type"},
                }
            )
            return {"status": "success" if self._gather_ack() else "error"}
        if t == RECEIVE:
            self._send_to_rank0({"type": RECEIVE_REQUEST})
            return {"status": "success" if self._gather_ack() else "error"}
        return {"status": "error", "error": f"unknown request type {t!r}"}

    def close(self) -> None:
        self._rank0.close(linger=0)
        self._trainer.close(linger=0)


def spawn(ipc_name: str, tcp_port: int) -> "subprocess.Popen":
    """Launch the coordinator as a detached child process. Returns the ``Popen`` for teardown.

    The child imports only ``zmq`` + :mod:`wbridge.backend.control_channel` (no torch), so startup is
    cheap. It self-exits if this parent dies (see :func:`_parent_death_watchdog`).
    """
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "wbridge.backend.coordinator",
            "--ipc",
            ipc_name,
            "--tcp-port",
            str(tcp_port),
        ]
    )


def _parent_death_watchdog() -> None:
    """Exit if our parent (the spawning rank-0 process) dies — avoids leaking coordinators across runs."""
    ppid = os.getppid()
    while True:
        time.sleep(2.0)
        if os.getppid() != ppid:
            logger.warning("wbridge coordinator: parent %d gone, exiting", ppid)
            os._exit(0)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[wbridge-coord] %(message)s")
    ap = argparse.ArgumentParser(
        description="WeightBridge standalone control-plane coordinator"
    )
    ap.add_argument(
        "--ipc",
        required=True,
        help="rank0-facing IPC address produced by coordinator_ipc(port)",
    )
    ap.add_argument(
        "--tcp-port", type=int, required=True, help="Trainer-facing TCP port"
    )
    args = ap.parse_args()
    threading.Thread(
        target=_parent_death_watchdog, name="wbridge-coord-watchdog", daemon=True
    ).start()
    wrc = WeightReceiverController(
        ipc_name=args.ipc, tcp_endpoint=f"tcp://*:{args.tcp_port}"
    )
    logger.info("wbridge coordinator: up (ipc=%s, tcp=*:%d)", args.ipc, args.tcp_port)
    try:
        wrc.serve()
    except KeyboardInterrupt:
        pass
    finally:
        wrc.close()


if __name__ == "__main__":
    main()
