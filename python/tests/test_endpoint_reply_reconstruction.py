# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import asyncio
import gc
import importlib
import re
import threading
import time
import uuid
import warnings
from collections.abc import Iterator
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import psutil
import pytest
from monarch._rust_bindings.monarch_hyperactor.endpoint import Remote
from monarch._rust_bindings.monarch_hyperactor.handle import _after, WouldBlockRuntime
from monarch._rust_bindings.monarch_hyperactor.pickle import _current_receiver_instance
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._rust_bindings.monarch_hyperactor.testing import _call_in_tokio_context
from monarch._src.actor.actor_mesh import ActorError, Channel, context, Instance, Port
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
        self.threads: list[int] = []
        self.runs: list[int] = []
        self.live_partial = 0


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
        state.threads.append(threading.get_ident())
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


class _PartialValue:
    """Account for objects created by a run that a later interrupt discards."""

    def __init__(self) -> None:
        _probe_state().live_partial += 1

    def __del__(self) -> None:
        _probe_state().live_partial -= 1


def _rebuild_run(module: str, rank: int, behavior: str) -> Any:
    state = importlib.import_module(module)._probe_state()
    state.runs.append(rank)
    if behavior == "interrupt" and rank == 1 and state.runs.count(1) == 1:
        raise KeyboardInterrupt("partial ValueMesh interrupted")
    if behavior == "error":
        raise ValueError("earlier run unpickle failed")
    return (
        importlib.import_module(module)._PartialValue()
        if behavior == "interrupt"
        else rank
    )


class _RunProbe:
    def __init__(self, rank: int, behavior: str) -> None:
        self.rank = rank
        self.behavior = behavior

    def __reduce__(self) -> tuple[Any, tuple[str, int, str]]:
        return _rebuild_run, (__name__, self.rank, self.behavior)


class _MeshArgumentError(ValueError):
    """Carry a mesh as an exception argument, not a reply reference table."""


class _ReplyActor(Actor):
    @endpoint
    def reply(self, behavior: str) -> Any:
        return _SerializedProbe(behavior)

    @endpoint
    def port_and_mesh(self) -> tuple[Port[int], Any]:
        sender, self.receiver = Channel[int].open()
        return sender, context().actor_instance.proc_mesh

    @endpoint
    def mesh_reply(self, behavior: str) -> Any:
        return _SerializedProbe(behavior), context().actor_instance.proc_mesh

    @endpoint
    def probed_port_and_mesh(self) -> Any:
        sender, self.receiver = Channel[int].open()
        return _SerializedProbe("success"), sender, context().actor_instance.proc_mesh

    @endpoint
    def ranked_reply(self, behavior: str) -> Any:
        rank = context().message_rank.rank
        if behavior == "repeated":
            return _SerializedProbe("success"), context().actor_instance.proc_mesh
        if behavior == "errors" and rank > 0:
            raise ValueError(f"remote rank {rank}")
        if behavior == "earlier_error" and rank > 0:
            raise _MeshArgumentError(
                f"remote rank {rank}", context().actor_instance.proc_mesh
            )
        probe = _RunProbe(rank, "error" if behavior == "earlier_error" else behavior)
        if behavior in ("lazy", "earlier_error") or (behavior == "mixed" and rank == 1):
            return probe
        return probe, context().actor_instance.proc_mesh

    @endpoint
    def prepare_reply_gate(self) -> Port[int]:
        sender, self.reply_gate = Channel[int].open()
        return sender

    @endpoint
    def delayed_mesh_reply(
        self,
        started: Port[int],
        behavior: str,
        error_message: str = "ignored call remote error",
    ) -> Any:
        started.send(1)
        assert self.reply_gate.recv().get(timeout=10) == 1
        if behavior == "remote_error":
            raise ValueError(error_message)
        return _SerializedProbe(behavior), context().actor_instance.proc_mesh

    @endpoint
    def received(self) -> int:
        return self.receiver.recv().get(timeout=10)


class _ForwardToActor:
    """Exercise Rust Remote without torch, forwarding its send to a real actor.

    This is not the tensor-engine RemoteImpl: the real actor handles the request,
    while Remote's native adverb paths collect and reconstruct its reply.
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
    adverb: Literal["call", "call_one", "choose"],
    behavior: str,
    remote: bool = False,
) -> Future[Any]:
    actor_endpoint = actor.mesh_reply if adverb == "call" else actor.reply
    target = Remote(_ForwardToActor(actor_endpoint)) if remote else actor_endpoint
    return getattr(target, adverb)(behavior)


def _probe_value(value: Any, adverb: str) -> Any:
    """call returns a ValueMesh whose ref-bearing element includes the probe."""
    return value.get(0)[0] if adverb == "call" else value


@pytest.mark.timeout(60)
@pytest.mark.parametrize("adverb", ["call", "call_one", "choose"])
@pytest.mark.parametrize("remote", [False, True])
def test_first_observation_reconstructs_one_shared_object(
    reply_actor: Any, adverb: Literal["call", "call_one", "choose"], remote: bool
) -> None:
    result = _reply(reply_actor, adverb, "success", remote)
    first = result.get(timeout=10)
    second = result.get(timeout=10)
    assert _probe_value(first, adverb) == [1, 2, 3]
    assert first is second
    assert result._as_handle().poll() is first
    assert _probe_state().calls == 1
    assert _probe_state().receivers == [str(context().actor_instance.actor_id)]
    assert _current_receiver_instance() is None, "receiver context must be restored"


@pytest.mark.timeout(60)
@pytest.mark.parametrize("adverb", ["call", "call_one", "choose"])
def test_concurrent_observers_share_one_reconstruction(
    reply_actor: Any, adverb: Literal["call", "call_one", "choose"]
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
@pytest.mark.parametrize("adverb", ["call", "call_one", "choose"])
@pytest.mark.parametrize("fallback", [False, True])
def test_asyncio_stays_responsive_while_another_observer_unpickles(
    reply_actor: Any, adverb: Literal["call", "call_one", "choose"], fallback: bool
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
@pytest.mark.parametrize("adverb", ["call", "call_one", "choose"])
def test_unpickle_error_is_shared_between_get_and_await(
    reply_actor: Any, adverb: Literal["call", "call_one", "choose"]
) -> None:
    result = _reply(reply_actor, adverb, "error")
    with pytest.raises(ValueError, match="^reply unpickle failed$") as first:
        result.get(timeout=10)

    async def again() -> None:
        with pytest.raises(ValueError, match="^reply unpickle failed$") as second:
            await result
        assert first.value is second.value

    asyncio.run(again())
    handle = result._as_handle()
    with pytest.raises(ValueError, match="^reply unpickle failed$") as readiness:
        _after([handle], handle).get(timeout=10)
    assert first.value is readiness.value
    assert _probe_state().calls == 1
    assert _current_receiver_instance() is None, (
        "failed unpickling must restore the receiver"
    )


@pytest.mark.timeout(60)
@pytest.mark.parametrize("adverb", ["call", "call_one", "choose"])
def test_main_thread_interrupt_preserves_reply_for_a_later_get(
    reply_actor: Any, adverb: Literal["call", "call_one", "choose"]
) -> None:
    result = _reply(reply_actor, adverb, "interrupt_once")
    with pytest.raises(KeyboardInterrupt, match="^reply unpickle interrupted$"):
        result.get(timeout=10)
    assert _probe_value(result.get(timeout=10), adverb) == [1, 2, 3]
    assert _probe_state().calls == 2
    assert _current_receiver_instance() is None, (
        "interrupted unpickling must restore the receiver"
    )


@pytest.mark.timeout(60)
@pytest.mark.parametrize("adverb", ["call", "call_one", "choose"])
def test_rejected_observations_do_not_reconstruct(
    reply_actor: Any, adverb: Literal["call", "call_one", "choose"]
) -> None:
    result = _reply(reply_actor, adverb, "success")
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
    assert _probe_value(result.get(timeout=10), adverb) == [1, 2, 3]


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


@pytest.mark.timeout(60)
@pytest.mark.parametrize("other_thread", [False, True])
def test_call_reconstructs_port_and_mesh_on_the_observing_thread(
    reply_actor: Any, other_thread: bool
) -> None:
    result = reply_actor.probed_port_and_mesh.call()
    errors: list[BaseException] = []

    def observe() -> None:
        try:
            value = result.get(timeout=10)
            probe, sender, mesh = value.get(0)
            assert probe == [1, 2, 3]
            assert _probe_state().threads == [threading.get_ident()]
            assert _probe_state().calls == 1
            assert result.get(timeout=10) is value
            assert mesh._proc_mesh.get(timeout=10) is not None
            sender.send(73)
        except (Exception, KeyboardInterrupt, SystemExit) as error:
            errors.append(error)

    if other_thread:
        reader = threading.Thread(target=observe)
        reader.start()
        reader.join(timeout=15)
        assert not reader.is_alive(), (
            "cross-thread reconstruction cleanup must be bounded"
        )
    else:
        observe()
    assert not errors
    assert reply_actor.received.call_one().get(timeout=10) == 73
    assert _current_receiver_instance() is None


@pytest.fixture
def ranked_reply_actor() -> Iterator[Any]:
    _probe_state.cache_clear()
    mesh = this_host().spawn_procs(per_host={"rank": 3})
    try:
        yield mesh.spawn("ranked_reply_probe", _ReplyActor)
    finally:
        _probe_state().release.set()
        mesh.stop().get(timeout=30)


@pytest.mark.timeout(90)
def test_call_leaves_ref_free_elements_lazy_and_cached(ranked_reply_actor: Any) -> None:
    result = ranked_reply_actor.ranked_reply.call("lazy")
    value = result.get(timeout=20)
    assert len(value) == 3
    assert _probe_state().runs == [], (
        "building a ref-free ValueMesh must not unpickle cells"
    )
    assert value.get(1) == 1
    assert _probe_state().runs == [1]
    assert value.get(1) == 1
    assert _probe_state().runs == [1], "a resolved cell must be cached"
    assert list(value.values()) == [0, 1, 2]
    assert _probe_state().runs == [1, 0, 2]
    assert result.get(timeout=10) is value


@pytest.mark.timeout(90)
def test_call_mixed_refs_decodes_every_run_in_rank_order(
    ranked_reply_actor: Any,
) -> None:
    value = ranked_reply_actor.ranked_reply.call("mixed").get(timeout=20)
    assert len(value) == 3
    assert _probe_state().runs == [0, 1, 2]
    assert value.get(0)[0] == 0
    assert value.get(1) == 1
    assert value.get(2)[0] == 2
    assert _probe_state().runs == [0, 1, 2], (
        "access must not repeat completed reconstruction"
    )


@pytest.mark.timeout(90)
def test_call_repeated_run_shares_its_reconstructed_object(
    ranked_reply_actor: Any,
) -> None:
    value = ranked_reply_actor.ranked_reply.call("repeated").get(timeout=20)
    assert len(value) == 3
    assert value.get(0) is value.get(1)
    assert value.get(1) is value.get(2)
    assert _probe_state().calls == 1, "one repeated run must be decoded once"


@pytest.mark.timeout(90)
def test_call_interrupted_partial_build_retries_retained_batch(
    ranked_reply_actor: Any,
) -> None:
    result = ranked_reply_actor.ranked_reply.call("interrupt")
    with pytest.raises(KeyboardInterrupt, match="partial ValueMesh interrupted"):
        result.get(timeout=20)
    gc.collect()
    assert _probe_state().runs == [0, 1]
    assert _probe_state().live_partial == 0, (
        "discard objects from the incomplete attempt"
    )
    assert _current_receiver_instance() is None
    value = result.get(timeout=20)
    assert len(value) == 3
    assert _probe_state().runs == [0, 1, 0, 1, 2]
    assert _probe_state().live_partial == 3
    assert _current_receiver_instance() is None
    del value, result
    gc.collect()
    assert _probe_state().live_partial == 0, "the completed mesh releases its values"


@pytest.mark.timeout(90)
def test_call_ref_free_error_batch_does_not_unpickle_earlier_result(
    ranked_reply_actor: Any,
) -> None:
    # Port.exception() sends no out-of-band references, even when the
    # exception contains a mesh. The earlier Result must remain pickled.
    with pytest.raises(ActorError) as error:
        ranked_reply_actor.ranked_reply.call("earlier_error").get(timeout=20)
    assert isinstance(error.value.exception, _MeshArgumentError)
    assert error.value.exception.args[0] == "remote rank 1"
    assert _probe_state().runs == [], "the ref-free Result must not be unpickled"
    assert _current_receiver_instance() is None


@pytest.mark.timeout(90)
def test_call_remote_exceptions_keep_rank_order(ranked_reply_actor: Any) -> None:
    with pytest.raises(ActorError) as error:
        ranked_reply_actor.ranked_reply.call("errors").get(timeout=20)
    assert isinstance(error.value.exception, ValueError)
    assert str(error.value.exception) == "remote rank 1"
    assert _probe_state().runs == [0], (
        "batch refs require decoding the preceding Result"
    )


def _caller_trace_log() -> Path:
    """Locate this test process's actual open tracing file.

    Inspect the sink's descriptor rather than guessing its user, suffix or
    environment-dependent directory. Peer processes have separate fd tables.
    """
    paths: set[Path] = set()
    for opened in psutil.Process().open_files():
        path = Path(opened.path)
        if path.name.startswith(("monarch_log", "dedicated_log_monarch")) and (
            path.suffix == ".log"
        ):
            paths.add(path)
    assert len(paths) == 1, f"expected one caller tracing file, found {paths}"
    return paths.pop()


@pytest.mark.timeout(60)
def test_ignored_call_error_is_logged_after_its_observer_is_dropped(
    reply_actor: Any,
) -> None:
    # Glog emits one header per event; its message may contain newlines.
    # Require this call's marker in the Handle's background-error event,
    # not in an unrelated event or the peer's own exception report.
    event_header = re.compile(r"(?m)^\[[^\]\n]*\][EWIDT]\d{4} ")
    with _caller_trace_log().open(encoding="utf-8") as log:
        release = reply_actor.prepare_reply_gate.call_one().get(timeout=10)
        released = False
        try:
            started, receiver = Channel[int].open()
            message = f"ignored call remote error {uuid.uuid4().hex}"
            result = reply_actor.delayed_mesh_reply.call(
                started, "remote_error", message
            )
            assert receiver.recv().get(timeout=10) == 1, (
                "the peer must wait until its observer has been dropped"
            )
            del result
            gc.collect()
            log.seek(0, 2)
            release.send(1)
            released = True
            output = ""
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                output += log.read()
                if any(
                    "src/handle.rs:" in event
                    and "background task errored" in event
                    and message in event
                    for event in event_header.split(output)
                ):
                    return
                time.sleep(0.01)
        finally:
            if not released:
                release.send(1)
    pytest.fail(f"the caller did not log its unobserved batch error: {message}")


@pytest.mark.timeout(60)
@pytest.mark.parametrize("fallback", [False, True])
@pytest.mark.parametrize("behavior", ["success", "error", "interrupt_once"])
def test_call_pending_asyncio_reconstruction_is_terminal_before_delivery(
    reply_actor: Any, fallback: bool, behavior: str
) -> None:
    release = reply_actor.prepare_reply_gate.call_one().get(timeout=10)
    started, receiver = Channel[int].open()
    result = reply_actor.delayed_mesh_reply.call(started, behavior)
    assert receiver.recv().get(timeout=10) == 1, (
        "the peer must be waiting at its reply gate"
    )

    async def observe() -> None:
        loop = asyncio.get_running_loop()
        if fallback:

            def no_reader(fd: Any, callback: Any, *args: Any) -> None:
                raise NotImplementedError

            loop.add_reader = no_reader
        pending = result.as_asyncio()
        assert not pending.done(), "register observation before the peer may reply"
        release.send(1)
        if behavior == "success":
            value = await asyncio.wait_for(pending, timeout=10)
            assert value.get(0)[0] == [1, 2, 3]
            assert await result is value
        else:
            error_type = ValueError if behavior == "error" else KeyboardInterrupt
            # asyncio must receive a terminal failure, not a notification while Raw.
            with pytest.raises(error_type) as delivered:
                await asyncio.wait_for(pending, timeout=10)
            with pytest.raises(error_type) as stored:
                result._as_handle().poll()
            assert delivered.value is stored.value
        assert _probe_state().calls == 1

    asyncio.run(observe())


@pytest.mark.timeout(60)
def test_call_later_lazy_cell_error_does_not_fail_future_readiness(
    reply_actor: Any,
) -> None:
    result = reply_actor.reply.call("error")
    value = result.get(timeout=10)
    handle = result._as_handle()
    assert _after([handle], handle).get(timeout=10) is value
    with pytest.raises(ValueError, match="reply unpickle failed"):
        value.get(0)
    assert result.get(timeout=10) is value
    assert _after([handle], handle).get(timeout=10) is value


@pytest.mark.timeout(60)
def test_call_available_raw_batch_reconstructs_on_the_asyncio_loop(
    reply_actor: Any,
) -> None:
    result = _reply(reply_actor, "call", "interrupt_once")
    with pytest.raises(KeyboardInterrupt, match="reply unpickle interrupted"):
        result.get(timeout=10)
    # The interrupted claim proves arrival and leaves the batch Raw for the loop.
    assert _probe_state().calls == 1

    async def observe() -> None:
        pending = result.as_asyncio()
        assert pending.done(), "an available raw batch is reconstructed on the loop"
        value = await pending
        assert value.get(0)[0] == [1, 2, 3]
        assert _probe_state().threads[-1] == threading.get_ident()
        assert await result is value

    asyncio.run(observe())
    assert _probe_state().calls == 2
