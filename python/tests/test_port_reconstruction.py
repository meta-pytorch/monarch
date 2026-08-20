# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import time
from typing import Callable, List, Tuple, TypeVar

import pytest
from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    PortId,
    PortRef,
    UndeliverableMessageEnvelope,
)
from monarch._rust_bindings.monarch_hyperactor.proc import ActorAddr
from monarch._src.actor.actor_mesh import (
    _client_context,
    Channel,
    context,
    Port,
    PortReceiver,
)
from monarch._src.actor.host_mesh import this_host
from monarch._src.actor.proc_mesh import ProcMesh
from monarch.actor import Actor, ActorError, endpoint


class PortOwner(Actor):
    """Owns channels and hands their sending ``Port`` to another actor."""

    def __init__(self) -> None:
        self._recv: PortReceiver[int]

    @endpoint
    async def make_port(self) -> Port[int]:
        send, self._recv = Channel[int].open()
        return send

    @endpoint
    async def received(self) -> int:
        return await self._recv.recv()

    @endpoint
    async def make_doomed_port(self) -> Port[int]:
        """A ``Port`` addressed at a proc that does not exist.

        Sending through it cannot be delivered, so the envelope is returned to
        whichever instance posted it. That is what makes the *sender* -- and
        therefore the instance the ``Port`` was reconstructed with -- directly
        observable, without a getter on ``Port``.
        """
        port_id = PortId(
            actor_id=ActorAddr(addr="local:0", proc_name="bogus", actor_name="bogus"),
            port=1234,
        )
        return Port(PortRef(port_id), context().actor_instance._as_rust(), None)


class _CallerMixin:
    """Captures the sender of any message this actor failed to deliver."""

    def _handle_undeliverable_message(
        self, message: UndeliverableMessageEnvelope
    ) -> bool:
        # pyrefly: ignore [missing-attribute]
        self._undeliverable.append(str(message.sender()))
        return True


class AsyncCaller(_CallerMixin, Actor):
    """Obtains a ``Port`` as a ``call_one`` return value from an async endpoint.

    Sync and async endpoints cannot be mixed on one actor -- actor-mesh
    construction rejects it -- so the sync half lives in :class:`SyncCaller`.
    """

    def __init__(self, owner: PortOwner) -> None:
        self._owner = owner
        self._undeliverable: List[str] = []

    @endpoint
    async def use(self, value: int) -> bool:
        port = await self._owner.make_port.call_one()
        port.send(value)
        return _client_context._val is not None

    @endpoint
    async def send_doomed(self) -> None:
        port = await self._owner.make_doomed_port.call_one()
        port.send(1)

    @endpoint
    async def collect_identity(self) -> Tuple[str, str]:
        observed = self._undeliverable[0] if self._undeliverable else ""
        return observed, str(context().actor_instance.actor_id)


class SyncCaller(_CallerMixin, Actor):
    def __init__(self, owner: PortOwner) -> None:
        self._owner = owner
        self._undeliverable: List[str] = []

    @endpoint
    def use(self, value: int) -> bool:
        port = self._owner.make_port.call_one().get()
        port.send(value)
        return _client_context._val is not None

    @endpoint
    def send_doomed(self) -> None:
        port = self._owner.make_doomed_port.call_one().get()
        port.send(1)

    @endpoint
    def collect_identity(self) -> Tuple[str, str]:
        observed = self._undeliverable[0] if self._undeliverable else ""
        return observed, str(context().actor_instance.actor_id)


class PortReconstructionBug(Exception):
    """The meta-pytorch/monarch#4658 failure, translated from ``ActorError``.

    Expecting a bare ``ActorError`` would also swallow an unrelated setup or
    cleanup failure and report it as the known bug. Only the target call, and
    only when it carries the signature below, becomes this exception; anything
    else propagates and fails the test.
    """


T = TypeVar("T")


def _expect_4658(call: Callable[[], T]) -> T:
    """Run a Port-returning call, translating only the #4658 failure.

    The reply decode has no Monarch context, so ``_reconstruct_port`` ->
    ``context()`` bootstraps a client, whose ``block_on`` panics inside the
    Tokio runtime; the remote error reaches the caller as a closed channel.
    """
    try:
        return call()
    except ActorError as e:
        if "channel closed" not in str(e):
            raise
        raise PortReconstructionBug(str(e)) from e


CallerT = TypeVar("CallerT", AsyncCaller, SyncCaller)


def _spawn_owner_and_caller(
    caller_class: type[CallerT],
) -> Tuple[ProcMesh, PortOwner, CallerT]:
    """One two-proc mesh; owner on rank 0, caller on rank 1.

    The ProcMesh is sliced *before* spawning. Spawning first and slicing the
    resulting ActorMesh would place both actor classes on both ranks and then
    merely address one of each.
    """
    pm = this_host().spawn_procs(per_host={"gpus": 2})
    owner = pm.slice(gpus=0).spawn("owner", PortOwner)
    caller = pm.slice(gpus=1).spawn("caller", caller_class, owner)
    return pm, owner, caller


def _await_observed_sender(caller: AsyncCaller | SyncCaller) -> Tuple[str, str]:
    """Poll for the bounced envelope.

    Collected through a second endpoint rather than inside ``send_doomed`` so a
    *sync* caller does not block its own actor loop waiting for the return.
    """
    for _ in range(200):
        observed, actor_id = caller.collect_identity.call_one().get()
        if observed:
            return observed, actor_id
        time.sleep(0.05)
    return "", ""


@pytest.mark.timeout(120)
@pytest.mark.xfail(
    strict=True,
    raises=PortReconstructionBug,
    reason=(
        "meta-pytorch/monarch#4658: a Port returned by call_one is decoded on a Tokio worker with no Monarch context, so _reconstruct_port -> context() bootstraps a client inside the caller actor process. Remove this mark with the fix."
    ),
)
def test_port_returned_to_async_actor_does_not_bootstrap_client() -> None:
    """A Port returned by one remote actor to another must be reconstructed
    with the *caller actor's* instance, not by bootstrapping a client.

    Currently red: the reply is decoded by a ``PythonTask::new`` collector on a
    Tokio worker with no Monarch context, so ``Port._reconstruct_port`` ->
    ``context()`` takes the client-bootstrap branch inside a worker process and
    ``_init_client_context`` calls ``block_on`` from inside the runtime.
    """
    pm, owner, caller = _spawn_owner_and_caller(AsyncCaller)
    try:
        bootstrapped = _expect_4658(lambda: caller.use.call_one(42).get())
        assert 42 == owner.received.call_one().get()
        assert not bootstrapped, (
            "a client context was bootstrapped inside the caller actor process"
        )
    finally:
        pm.stop().get()


@pytest.mark.timeout(120)
def test_port_returned_to_sync_actor_does_not_bootstrap_client() -> None:
    """The sync-caller control.

    ``Future.get()`` drives the collector via ``block_on`` on the calling
    thread, where the actor context *is* set. If this ever fails too, the
    defect is not reply-decode context propagation and the analysis is wrong.
    """
    pm, owner, caller = _spawn_owner_and_caller(SyncCaller)
    try:
        bootstrapped = caller.use.call_one(43).get()
        assert 43 == owner.received.call_one().get()
        assert not bootstrapped, (
            "a client context was bootstrapped inside the caller actor process"
        )
    finally:
        pm.stop().get()


@pytest.mark.timeout(120)
def test_port_reconstruction_binds_exact_sync_caller_instance() -> None:
    """Reconstruction must use the caller's *own* ``Instance``.

    Asserting only that the Port can carry a sentinel is too weak: any instance
    able to post would satisfy it. The bounced envelope names the actor that
    actually posted, so comparing it against the caller's own actor id pins the
    exact instance.
    """
    pm, _owner, caller = _spawn_owner_and_caller(SyncCaller)
    try:
        caller.send_doomed.call_one().get()
        observed, caller_actor_id = _await_observed_sender(caller)
        assert observed, "no undeliverable envelope returned to the caller"
        assert observed == caller_actor_id, (
            f"Port bound to {observed}, expected the caller {caller_actor_id}"
        )
    finally:
        pm.stop().get()


@pytest.mark.timeout(120)
@pytest.mark.xfail(
    strict=True,
    raises=PortReconstructionBug,
    reason=(
        "meta-pytorch/monarch#4658: a Port returned by call_one is decoded on a Tokio worker with no Monarch context, so _reconstruct_port -> context() bootstraps a client inside the caller actor process. Remove this mark with the fix."
    ),
)
def test_port_reconstruction_binds_exact_async_caller_instance() -> None:
    """The async counterpart of the exact-identity proof. Currently red."""
    pm, _owner, caller = _spawn_owner_and_caller(AsyncCaller)
    try:
        _expect_4658(lambda: caller.send_doomed.call_one().get())
        observed, caller_actor_id = _await_observed_sender(caller)
        assert observed, "no undeliverable envelope returned to the caller"
        assert observed == caller_actor_id, (
            f"Port bound to {observed}, expected the caller {caller_actor_id}"
        )
    finally:
        pm.stop().get()
