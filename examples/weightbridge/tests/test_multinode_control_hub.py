# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Regression tests for the cross-host receiver control hub."""

import multiprocessing as mp
import socket
import time

import pytest

pytest.importorskip("zmq")

from wbridge.backend.control_channel import (
    ControlChannel,
    coordinator_ipc,
    multi_node_hub_addr,
)


_UNUSED_CONTROLLER = coordinator_ipc(39999)


def _free_tcp_endpoint() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return f"tcp://127.0.0.1:{sock.getsockname()[1]}"


def _peer(endpoint: str, output) -> None:
    channel = ControlChannel(
        _UNUSED_CONTROLLER,
        1,
        2,
        hub_endpoint=endpoint,
        reg_timeout_s=5.0,
    )
    output.put(channel.poll_decision(timeout_ms=5000))
    channel.close()


def _idle_then_delayed_peer(endpoint: str, output) -> None:
    channel = ControlChannel(
        _UNUSED_CONTROLLER,
        1,
        2,
        hub_endpoint=endpoint,
        reg_timeout_s=5.0,
    )
    output.put(channel.poll_decision(timeout_ms=200))
    time.sleep(0.4)
    output.put(channel.poll_decision(timeout_ms=5000))
    channel.close()


def test_multinode_hub_address_derivation() -> None:
    assert multi_node_hub_addr(15000, "10.1.2.3:15003", 1) is None
    assert multi_node_hub_addr(15000, "10.1.2.3:15003", 2) == "tcp://10.1.2.3:16718"
    assert (
        multi_node_hub_addr(15000, "[2001:db8::1]:15003", 2)
        == "tcp://[2001:db8::1]:16718"
    )


def test_coordinator_ipc_uses_configured_directory(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WBRIDGE_COORDINATOR_IPC_DIR", str(tmp_path))
    assert coordinator_ipc(15000) == f"ipc://{tmp_path}/wbridge_coord_15000.sock"


def test_tcp_hub_registers_remote_peer_and_broadcasts() -> None:
    endpoint = _free_tcp_endpoint()
    spawn = mp.get_context("spawn")
    output = spawn.Queue()
    peer = spawn.Process(target=_peer, args=(endpoint, output))
    peer.start()

    rank0 = ControlChannel(
        _UNUSED_CONTROLLER,
        0,
        2,
        hub_endpoint=endpoint,
        reg_timeout_s=5.0,
    )
    decision = {"type": "connect_request", "epoch": 7}
    rank0.broadcast(decision)
    assert output.get(timeout=10) == decision

    rank0.close()
    peer.join(timeout=10)
    assert peer.exitcode == 0


def test_idle_is_barriered_and_action_waits_for_peer_quiescence() -> None:
    endpoint = _free_tcp_endpoint()
    spawn = mp.get_context("spawn")
    output = spawn.Queue()
    peer = spawn.Process(target=_idle_then_delayed_peer, args=(endpoint, output))
    peer.start()

    rank0 = ControlChannel(
        _UNUSED_CONTROLLER,
        0,
        2,
        hub_endpoint=endpoint,
        reg_timeout_s=5.0,
    )
    idle = {"type": "empty"}
    rank0.broadcast(idle)
    assert output.get(timeout=5) == idle

    decision = {"type": "connect_request", "epoch": 7}
    started = time.monotonic()
    rank0.broadcast(decision)
    elapsed = time.monotonic() - started
    assert elapsed >= 0.3
    assert output.get(timeout=10) == decision

    rank0.close()
    peer.join(timeout=10)
    assert peer.exitcode == 0
