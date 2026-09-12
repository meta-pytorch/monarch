# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""ZMQ coordinator relay (:class:`~wbridge.backend.coordinator.WeightReceiverController`).

Torch/GPU-free: drives the coordinator's relay loop in-process with a mock receiver rank 0 and a mock
Trainer, over IPC (the trainer-facing ROUTER binds any endpoint string, so no TCP port is needed here).
Covers: rank 0's WORKER_REGISTER -> world query, and connect/receive forwarding + ack round-trip.
"""

import json
import tempfile
import threading

import pytest

pytest.importorskip("zmq")
import zmq  # noqa: E402
from wbridge.backend.control_channel import (  # noqa: E402
    CONNECT,
    CONNECT_REQUEST,
    RECEIVE,
    RECEIVE_REQUEST,
    WORKER_REGISTER,
    WORLD_QUERY,
)
from wbridge.backend.coordinator import (  # noqa: E402
    _coordinator_register_timeout_s,
    COORDINATOR_REGISTER_TIMEOUT_ENV,
    WeightReceiverController,
)


def _ipc(suffix: str) -> str:
    return f"ipc://{tempfile.NamedTemporaryFile(delete=False, suffix=suffix).name}"


def test_coordinator_register_timeout_env(monkeypatch):
    monkeypatch.delenv(COORDINATOR_REGISTER_TIMEOUT_ENV, raising=False)
    assert _coordinator_register_timeout_s() == 600.0

    monkeypatch.setenv(COORDINATOR_REGISTER_TIMEOUT_ENV, "7200")
    assert _coordinator_register_timeout_s() == 7200.0

    monkeypatch.setenv(COORDINATOR_REGISTER_TIMEOUT_ENV, "0")
    with pytest.raises(ValueError, match="must be positive"):
        _coordinator_register_timeout_s()


def test_coordinator_relay():
    ctx = zmq.Context.instance()
    wrc = WeightReceiverController(
        ipc_name=_ipc("_r0"), tcp_endpoint=_ipc("_tr"), ctx=ctx
    )

    # Mock receiver rank 0: DEALER (identity worker-0). Registers its worker count, then acks every
    # forwarded control message — mimicking ControlChannel.step's drain+ack.
    rank0 = ctx.socket(zmq.DEALER)
    rank0.setsockopt_string(zmq.IDENTITY, "worker-0")
    rank0.connect(wrc.ipc_name)
    rank0.send_string(json.dumps({"type": WORKER_REGISTER, "num_workers": 2}))

    def _rank0_acker():
        while True:
            msg = json.loads(rank0.recv().decode())
            if msg.get("type") in (CONNECT_REQUEST, RECEIVE_REQUEST):
                rank0.send_string(json.dumps({"status": "ack"}))

    threading.Thread(target=_rank0_acker, daemon=True).start()
    threading.Thread(target=wrc.serve, daemon=True).start()

    tr = ctx.socket(zmq.DEALER)
    tr.connect(wrc._tcp_endpoint)

    def _req(obj: dict) -> dict:
        tr.send_string(json.dumps(obj))
        assert tr.poll(5000), f"no coordinator reply to {obj}"
        return json.loads(tr.recv().decode())

    assert _req({"type": WORLD_QUERY}) == {"status": "success", "world_size": 2}
    assert _req({"type": CONNECT, "rank": 8, "world_size": 12})["status"] == "success"
    assert _req({"type": RECEIVE})["status"] == "success"

    tr.close(linger=0)
    rank0.close(linger=0)
    wrc.close()


if __name__ == "__main__":
    test_coordinator_relay()
    print("test_coordinator_relay passed")
