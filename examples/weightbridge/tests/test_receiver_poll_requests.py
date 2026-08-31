# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

from types import MethodType, SimpleNamespace

from wbridge.backend.control_channel import CONNECT_REQUEST, RECEIVE_REQUEST
from wbridge.backend.receiver import WeightReceiver
from wbridge.frontend.adapters import ReceiverAdapter


def _receiver_for(decision: dict | None, calls: list[object]) -> WeightReceiver:
    receiver = WeightReceiver.__new__(WeightReceiver)
    receiver._pending = None

    def is_update_ready(self) -> bool:
        calls.append("poll")
        self._pending = decision
        return decision is not None

    def request_update(self) -> bool:
        calls.append("execute")
        current = self._pending
        self._pending = None
        return current is not None and current.get("type") == RECEIVE_REQUEST

    receiver.is_update_ready = MethodType(is_update_ready, receiver)
    receiver.request_update = MethodType(request_update, receiver)
    return receiver


def test_empty_poll_returns_false_without_executing_or_calling_hook() -> None:
    calls: list[object] = []
    receiver = _receiver_for(None, calls)

    assert receiver.poll_requests(lambda epoch: calls.append(("hook", epoch))) is False
    assert calls == ["poll"]


def test_connect_executes_without_calling_receive_hook() -> None:
    calls: list[object] = []
    receiver = _receiver_for({"type": CONNECT_REQUEST}, calls)

    assert receiver.poll_requests(lambda epoch: calls.append(("hook", epoch))) is False
    assert calls == ["poll", "execute"]


def test_receive_calls_hook_before_execution_and_returns_true() -> None:
    calls: list[object] = []
    receiver = _receiver_for({"type": RECEIVE_REQUEST, "epoch": 7}, calls)

    assert receiver.poll_requests(lambda epoch: calls.append(("hook", epoch))) is True
    assert calls == ["poll", ("hook", 7), "execute"]


def test_receiver_adapter_exposes_only_the_unified_poll_surface() -> None:
    calls: list[object] = []
    adapter = ReceiverAdapter.__new__(ReceiverAdapter)
    adapter.receiver = SimpleNamespace(
        poll_requests=lambda before_receive=None: (
            calls.append(before_receive),
            True,
        )[1]
    )
    hook = lambda _epoch: None

    assert adapter.poll_requests(before_receive=hook) is True
    assert calls == [hook]
    assert not hasattr(ReceiverAdapter, "is_update_ready")
    assert not hasattr(ReceiverAdapter, "request_update")
