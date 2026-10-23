# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import _thread
import pickle
import threading
import time
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


@pytest.mark.timeout(10)
@pytest.mark.parametrize("once", [False, True])
def test_second_reader_shares_the_receive(once: bool) -> None:
    """A second reader of one receive Future, while an untimed get() on another
    thread drives it, reads the same object instead of failing."""
    sender, receiver = Channel[Any].open(once=once)
    fut = receiver.recv()
    results: list[Any] = []
    first = threading.Thread(target=lambda: results.append(fut.get()))
    first.start()
    time.sleep(0.05)
    second = threading.Thread(target=lambda: results.append(fut.get(timeout=5)))
    second.start()
    sender.send([1, 2, 3])
    first.join(timeout=5)
    second.join(timeout=5)
    assert len(results) == 2
    assert results[0] == [1, 2, 3]
    assert results[0] is results[1]


@pytest.mark.timeout(10)
@pytest.mark.parametrize("once", [False, True])
def test_interrupted_get_leaves_the_receive_for_a_later_read(once: bool) -> None:
    """Ctrl-C ends a main-thread get() but not its receive, so a later get()
    reads the message."""
    assert threading.current_thread() is threading.main_thread()
    sender, receiver = Channel[int].open(once=once)
    fut = receiver.recv()
    timer = threading.Timer(0.3, _thread.interrupt_main)
    timer.start()
    with pytest.raises(KeyboardInterrupt):
        fut.get()
    timer.join()
    sender.send(9)
    assert fut.get(timeout=2) == 9


def _decode_off_the_main_thread(error: BaseException, value: object) -> object:
    """Return `value`, but raise `error` on Python's main thread, as a signal
    handler interrupting the decoding there would."""
    if threading.current_thread() is threading.main_thread():
        raise error
    return value


class _MainThreadDecodeRaises:
    """Unpickles as `value`, except on Python's main thread, where unpickling
    raises `error`."""

    def __init__(self, error: BaseException, value: object) -> None:
        self.error = error
        self.value = value

    def __reduce__(self) -> tuple[Any, tuple[BaseException, object]]:
        return (_decode_off_the_main_thread, (self.error, self.value))


@pytest.mark.timeout(10)
@pytest.mark.parametrize("once", [False, True])
def test_interrupted_decode_leaves_the_message_for_a_later_read(once: bool) -> None:
    """Ctrl-C landing while a main-thread get() decodes the message ends that
    get(), and a later get() reads the message."""
    assert threading.current_thread() is threading.main_thread()
    sender, receiver = Channel[Any].open(once=once)
    sender.send(_MainThreadDecodeRaises(KeyboardInterrupt(), "decoded"))
    fut = receiver.recv()
    with pytest.raises(KeyboardInterrupt):
        fut.get()
    assert fut.get(timeout=2) == "decoded"


@pytest.mark.timeout(10)
@pytest.mark.parametrize("once", [False, True])
def test_main_thread_decode_exception_is_the_result(once: bool) -> None:
    """An Exception raised while a main-thread get() decodes the message is
    the receive's result, not an interrupt: a later get() raises it again."""
    assert threading.current_thread() is threading.main_thread()
    sender, receiver = Channel[Any].open(once=once)
    sender.send(_MainThreadDecodeRaises(ValueError("bad payload"), "decoded"))
    fut = receiver.recv()
    with pytest.raises(ValueError, match="^bad payload$") as first:
        fut.get()
    with pytest.raises(ValueError) as second:
        fut.get(timeout=2)
    assert second.value is first.value
