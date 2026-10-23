# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import asyncio
import importlib
import threading
import warnings
from collections.abc import Iterator
from functools import lru_cache
from typing import Any, Literal

import pytest
from monarch._rust_bindings.monarch_hyperactor.endpoint import Remote
from monarch._rust_bindings.monarch_hyperactor.handle import WouldBlockRuntime
from monarch._rust_bindings.monarch_hyperactor.pickle import _current_receiver_instance
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._rust_bindings.monarch_hyperactor.testing import _call_in_tokio_context
from monarch._src.actor.actor_mesh import Channel, context, Instance, Port
from monarch._src.actor.future import Future
from monarch._src.actor.host_mesh import this_host
from monarch.actor import Actor, endpoint


class _ProbeState:
    """Caller-side unpickle acknowledgements, never sent to the remote actor."""

    def __init__(self) -> None:
        self.calls = 0
        self.entered = threading.Event()
        self.release = threading.Event()
        self.lock = threading.Lock()
        self.receivers: list[str] = []


@lru_cache(maxsize=1)
def _probe_state() -> _ProbeState:
    return _ProbeState()


def _rebuild_probe(module: str, behavior: str) -> list[int]:
    # Resolve the caller's module explicitly: cloudpickle must not copy the
    # counter/events into an independent function-global dictionary.
    state = importlib.import_module(module)._probe_state()
    with state.lock:
        state.calls += 1
        attempt = state.calls
        receiver = _current_receiver_instance()
        state.receivers.append(
            str(Instance._as_py(receiver).actor_id)
            if receiver is not None
            else "no receiver"
        )
    state.entered.set()
    if behavior == "gate":
        assert state.release.wait(10), "the test must release the unpickle gate"
    if behavior == "error":
        raise ValueError("reply unpickle failed")
    if behavior == "interrupt_once" and attempt == 1:
        raise KeyboardInterrupt("reply unpickle interrupted")
    return [1, 2, 3]


class _SerializedProbe:
    """Serialize normally; acknowledge only when the caller unpickles the reply."""

    def __init__(self, behavior: str) -> None:
        self.behavior = behavior

    def __reduce__(self) -> tuple[Any, tuple[str, str]]:
        return _rebuild_probe, (__name__, self.behavior)


class _ReplyActor(Actor):
    @endpoint
    def reply(self, behavior: str) -> Any:
        return _SerializedProbe(behavior)

    @endpoint
    def port_and_mesh(self) -> tuple[Port[int], Any]:
        sender, self.receiver = Channel[int].open()
        return sender, context().actor_instance.proc_mesh

    @endpoint
    def received(self) -> int:
        return self.receiver.recv().get(timeout=10)


class _ForwardToActor:
    """Exercise Rust Remote without torch, forwarding its send to a real actor.

    This is not the tensor-engine RemoteImpl: the real actor handles the request,
    while Remote's native call_one/choose paths collect and reconstruct its reply.
    """

    def __init__(self, actor_endpoint: Any) -> None:
        self.actor_endpoint = actor_endpoint

    def _get_extent(self) -> Extent:
        return Extent([], [])

    def _call_name(self) -> str:
        return "forwarded actor reply"

    def _send(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        port: Any,
        selection: str,
        correlation_id: int | None,
    ) -> None:
        self.actor_endpoint._send(args, kwargs, port=port, selection=selection)


@pytest.fixture
def reply_actor() -> Iterator[Any]:
    _probe_state.cache_clear()
    mesh = this_host().spawn_procs(per_host={"rank": 1})
    try:
        yield mesh.spawn("reply_probe", _ReplyActor)
    finally:
        _probe_state().release.set()
        mesh.stop().get(timeout=30)


def _reply(
    actor: Any,
    adverb: Literal["call_one", "choose"],
    behavior: str,
    remote: bool = False,
) -> Future[Any]:
    target = Remote(_ForwardToActor(actor.reply)) if remote else actor.reply
    return getattr(target, adverb)(behavior)


@pytest.mark.timeout(60)
@pytest.mark.parametrize("adverb", ["call_one", "choose"])
@pytest.mark.parametrize("remote", [False, True])
def test_first_observation_reconstructs_one_shared_object(
    reply_actor: Any, adverb: Literal["call_one", "choose"], remote: bool
) -> None:
    result = _reply(reply_actor, adverb, "success", remote)
    first = result.get(timeout=10)
    second = result.get(timeout=10)
    assert first == [1, 2, 3]
    assert first is second
    assert result._as_handle().poll() is first
    assert _probe_state().calls == 1
    assert _probe_state().receivers == [str(context().actor_instance.actor_id)]
    assert _current_receiver_instance() is None, "receiver context must be restored"


@pytest.mark.timeout(60)
@pytest.mark.parametrize("adverb", ["call_one", "choose"])
def test_concurrent_observers_share_one_reconstruction(
    reply_actor: Any, adverb: Literal["call_one", "choose"]
) -> None:
    result = _reply(reply_actor, adverb, "gate")
    values: list[Any] = []
    errors: list[BaseException] = []
    finished = threading.Event()

    def observe() -> None:
        try:
            values.append(result.get(timeout=10))
        except (Exception, KeyboardInterrupt, SystemExit) as error:
            errors.append(error)
        finally:
            finished.set()

    first = threading.Thread(target=observe)
    first.start()
    try:
        assert _probe_state().entered.wait(10), "first observer must own reconstruction"
        assert result._as_handle().poll() is None
        with pytest.raises(TimeoutError):
            result.get(timeout=0.01)
        assert _probe_state().calls == 1
        _probe_state().release.set()
        second = result.get(timeout=10)
        assert finished.wait(10), "first observer must finish after release"
        assert not errors
        assert values[0] is second
        assert _probe_state().calls == 1
    finally:
        _probe_state().release.set()
        first.join(timeout=10)
        assert not first.is_alive(), "observer cleanup must be bounded"


@pytest.mark.timeout(60)
@pytest.mark.parametrize("adverb", ["call_one", "choose"])
@pytest.mark.parametrize("fallback", [False, True])
def test_asyncio_stays_responsive_while_another_observer_unpickles(
    reply_actor: Any, adverb: Literal["call_one", "choose"], fallback: bool
) -> None:
    result = _reply(reply_actor, adverb, "gate")
    finished = threading.Event()
    values: list[Any] = []
    errors: list[BaseException] = []

    def observe() -> None:
        try:
            values.append(result.get(timeout=10))
        except (Exception, KeyboardInterrupt, SystemExit) as error:
            errors.append(error)
        finally:
            finished.set()

    first = threading.Thread(target=observe)
    first.start()
    try:
        assert _probe_state().entered.wait(10), "another observer must own the claim"

        async def await_reply() -> Any:
            loop = asyncio.get_running_loop()
            if fallback:

                def no_reader(fd: Any, callback: Any, *args: Any) -> None:
                    raise NotImplementedError

                loop.add_reader = no_reader
            pending = result.as_asyncio()
            assert not pending.done()
            callback = loop.create_future()
            loop.call_soon(callback.set_result, None)
            await asyncio.wait_for(callback, timeout=5)
            assert not _probe_state().release.is_set(), (
                "loop callback ran before claim release"
            )
            _probe_state().release.set()
            return await asyncio.wait_for(pending, timeout=10)

        second = asyncio.run(await_reply())
        assert finished.wait(10)
        assert not errors
        assert values[0] is second
        assert _probe_state().calls == 1
    finally:
        _probe_state().release.set()
        first.join(timeout=10)
        assert not first.is_alive(), "observer cleanup must be bounded"


@pytest.mark.timeout(60)
@pytest.mark.parametrize("adverb", ["call_one", "choose"])
def test_unpickle_error_is_shared_between_get_and_await(
    reply_actor: Any, adverb: Literal["call_one", "choose"]
) -> None:
    result = _reply(reply_actor, adverb, "error")
    with pytest.raises(ValueError, match="^reply unpickle failed$") as first:
        result.get(timeout=10)

    async def again() -> None:
        with pytest.raises(ValueError, match="^reply unpickle failed$") as second:
            await result
        assert first.value is second.value

    asyncio.run(again())
    assert _probe_state().calls == 1
    assert _current_receiver_instance() is None, (
        "failed unpickling must restore the receiver"
    )


@pytest.mark.timeout(60)
@pytest.mark.parametrize("adverb", ["call_one", "choose"])
def test_main_thread_interrupt_preserves_reply_for_a_later_get(
    reply_actor: Any, adverb: Literal["call_one", "choose"]
) -> None:
    result = _reply(reply_actor, adverb, "interrupt_once")
    with pytest.raises(KeyboardInterrupt, match="^reply unpickle interrupted$"):
        result.get(timeout=10)
    assert result.get(timeout=10) == [1, 2, 3]
    assert _probe_state().calls == 2
    assert _current_receiver_instance() is None, (
        "interrupted unpickling must restore the receiver"
    )


@pytest.mark.timeout(60)
def test_rejected_observations_do_not_reconstruct(reply_actor: Any) -> None:
    result = _reply(reply_actor, "call_one", "success")
    with pytest.raises(ValueError):
        result.get(timeout=-1)
    with pytest.raises(WouldBlockRuntime):
        _call_in_tokio_context(lambda: result.get())
    with pytest.raises(RuntimeError):
        result.as_asyncio()

    async def rejected() -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            with pytest.raises(UserWarning):
                result.get()

    asyncio.run(rejected())
    assert _probe_state().calls == 0
    assert result.get(timeout=10) == [1, 2, 3]


@pytest.mark.timeout(60)
@pytest.mark.parametrize("adverb", ["call_one", "choose"])
def test_port_and_mesh_reply_observed_on_another_thread(
    reply_actor: Any, adverb: Literal["call_one", "choose"]
) -> None:
    result = getattr(reply_actor.port_and_mesh, adverb)()
    errors: list[BaseException] = []
    finished = threading.Event()

    def observe() -> None:
        try:
            sender, mesh = result.get(timeout=10)
            sender.send(73)
            assert mesh._proc_mesh.get(timeout=10) is not None
        except (Exception, KeyboardInterrupt, SystemExit) as error:
            errors.append(error)
        finally:
            finished.set()

    reader = threading.Thread(target=observe)
    reader.start()
    try:
        assert finished.wait(10), "cross-thread reply observation must finish"
        assert not errors
        assert reply_actor.received.call_one().get(timeout=10) == 73
    finally:
        reader.join(timeout=10)
        assert not reader.is_alive(), "cross-thread reader cleanup must be bounded"
