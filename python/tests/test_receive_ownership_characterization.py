# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import pickle
from typing import Any, cast

import pytest
from monarch._rust_bindings.monarch_hyperactor.actor import (
    PythonMessage,
    PythonMessageKind,
)
from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer
from monarch._src.actor.actor_mesh import context, RankedPortReceiver
from monarch.actor import Channel


@pytest.mark.timeout(10)
def test_recv_futures_observed_in_reverse_receive_in_observation_order() -> None:
    """A public receive claims its message when observed, not when created."""
    sender, receiver = Channel[int].open()
    sender.send(11)
    sender.send(22)

    first = receiver.recv()
    second = receiver.recv()

    assert second.get(timeout=2) == 11
    assert first.get(timeout=2) == 22


@pytest.mark.timeout(10)
def test_discarded_regular_recv_claims_nothing() -> None:
    """Dropping an unobserved regular receive leaves its message pending."""
    sender, receiver = Channel[int].open()
    sender.send(31)

    discarded = receiver.recv()
    del discarded

    assert receiver.recv().get(timeout=2) == 31


@pytest.mark.timeout(10)
def test_discarded_once_recv_at_public_boundary_claims_nothing() -> None:
    """An unobserved once receive leaves the port unclaimed: the native once
    receive is lazy too, and claims the port only when first observed."""
    sender, receiver = Channel[int].open(once=True)
    sender.send(41)

    discarded = receiver.recv()
    del discarded

    assert receiver.recv().get(timeout=2) == 41


@pytest.mark.timeout(10)
def test_timed_out_recv_keeps_its_receive() -> None:
    """A polling caller retries the same Future without losing its message."""
    sender, receiver = Channel[int].open()
    pending = receiver.recv()
    with pytest.raises(TimeoutError):
        pending.get(timeout=0.05)
    sender.send(5)
    assert pending.get(timeout=2) == 5
    sender.send(6)
    assert receiver.recv().get(timeout=2) == 6


@pytest.mark.timeout(10)
def test_ranked_recv_and_remote_exception() -> None:
    """A ranked receiver returns the rank but raises a sent exception unchanged."""
    instance = context().actor_instance
    mailbox = instance._mailbox
    sender, native_receiver = mailbox.open_port()
    receiver = RankedPortReceiver[int](mailbox, native_receiver)

    def post(kind: Any, value: object) -> None:
        buffer = Buffer()
        buffer.write(pickle.dumps(value))
        sender.send(instance._as_rust(), PythonMessage(kind, buffer.freeze(), []))

    post(cast(Any, PythonMessageKind.Result)(7), 42)
    assert receiver.recv().get(timeout=2) == (7, 42)
    post(cast(Any, PythonMessageKind.Exception)(8), RuntimeError("remote failure"))
    with pytest.raises(RuntimeError, match="^remote failure$"):
        receiver.recv().get(timeout=2)
