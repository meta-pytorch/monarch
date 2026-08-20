# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import time
from typing import List, Tuple, TypeVar

import pytest
from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    PortId,
    PortRef,
    UndeliverableMessageEnvelope,
)
from monarch._rust_bindings.monarch_hyperactor.pickle import (
    _get_mesh_pop_count,
    _reset_mesh_pop_count,
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
from monarch.actor import Actor, endpoint


class PortOwner(Actor):
    """Owns channels and hands their sending ``Port`` to another actor."""

    def __init__(self, extra: "PortOwner | None" = None) -> None:
        self._recv: PortReceiver[int]
        self._extra = extra

    @endpoint
    async def make_port(self) -> Port[int]:
        send, self._recv = Channel[int].open()
        return send

    @endpoint
    async def make_port_and_mesh(self) -> Tuple[Port[int], "PortOwner"]:
        """A reply carrying both a ``Port`` and a mesh reference.

        The mesh drives ``collect_valuemesh`` down its *eager* ref-reuniting
        branch, a different decode site from ``value_collector``.
        """
        send, self._recv = Channel[int].open()
        extra = self._extra
        assert extra is not None
        return send, extra

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
def test_port_returned_to_async_actor_does_not_bootstrap_client() -> None:
    """A Port returned by one remote actor to another must be reconstructed
    with the *caller actor's* instance, not by bootstrapping a client.

    The reply is decoded by a ``PythonTask::new`` collector on a Tokio worker
    with no Monarch context, so the collector installs the caller instance as
    the decode receiver and ``_reconstruct_port`` never reaches the
    client-bootstrap branch of ``context()``.
    """
    pm, owner, caller = _spawn_owner_and_caller(AsyncCaller)
    try:
        bootstrapped = caller.use.call_one(42).get()
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
def test_port_reconstruction_binds_exact_async_caller_instance() -> None:
    """The async counterpart of the exact-identity proof."""
    pm, _owner, caller = _spawn_owner_and_caller(AsyncCaller)
    try:
        caller.send_doomed.call_one().get()
        observed, caller_actor_id = _await_observed_sender(caller)
        assert observed, "no undeliverable envelope returned to the caller"
        assert observed == caller_actor_id, (
            f"Port bound to {observed}, expected the caller {caller_actor_id}"
        )
    finally:
        pm.stop().get()


class StreamCaller(_CallerMixin, Actor):
    """Obtains a ``Port`` through ``.stream()``, a different eager collector."""

    def __init__(self, owner: PortOwner) -> None:
        self._owner = owner
        self._undeliverable: List[str] = []

    @endpoint
    async def send_doomed_via_stream(self) -> None:
        for fut in self._owner.make_doomed_port.stream():
            port = await fut
            port.send(1)

    @endpoint
    async def collect_identity(self) -> Tuple[str, str]:
        observed = self._undeliverable[0] if self._undeliverable else ""
        return observed, str(context().actor_instance.actor_id)


class RefBearingCaller(_CallerMixin, Actor):
    """Receives a reply carrying a mesh reference alongside the ``Port``."""

    def __init__(self, owner: PortOwner) -> None:
        self._owner = owner
        self._undeliverable: List[str] = []

    @endpoint
    async def use_call_with_refs(self, value: int) -> Tuple[bool, int]:
        _reset_mesh_pop_count()
        vm = await self._owner.make_port_and_mesh.call()
        port, mesh = vm.item()
        assert mesh is not None
        port.send(value)
        return _client_context._val is not None, _get_mesh_pop_count()


@pytest.mark.timeout(120)
def test_port_reconstruction_through_stream_binds_caller_instance() -> None:
    """``.stream()`` has its own collector, which must install the receiver too."""
    pm = this_host().spawn_procs(per_host={"gpus": 2})
    try:
        owner = pm.slice(gpus=0).spawn("owner", PortOwner)
        caller = pm.slice(gpus=1).spawn("caller", StreamCaller, owner)
        caller.send_doomed_via_stream.call_one().get()
        observed, caller_actor_id = _await_observed_sender(caller)
        assert observed, "no undeliverable envelope returned to the caller"
        assert observed == caller_actor_id, (
            f"Port bound to {observed}, expected the caller {caller_actor_id}"
        )
    finally:
        pm.stop().get()


@pytest.mark.timeout(120)
def test_ref_bearing_call_decodes_port_and_mesh_eagerly() -> None:
    """A reply carrying a ``Port`` *and* a mesh takes the eager valuemesh branch.

    A nonzero mesh-pop count in the caller's process proves the out-of-band
    reference was reunited during that eager decode, so this exercises the
    ref-bearing path rather than the lazy one.
    """
    pm = this_host().spawn_procs(per_host={"gpus": 2})
    try:
        extra = pm.slice(gpus=0).spawn("extra", PortOwner)
        owner = pm.slice(gpus=0).spawn("owner", PortOwner, extra)
        caller = pm.slice(gpus=1).spawn("caller", RefBearingCaller, owner)

        bootstrapped, mesh_pops = caller.use_call_with_refs.call_one(44).get()
        assert 44 == owner.received.call_one().get()
        assert not bootstrapped, (
            "a client context was bootstrapped inside the caller actor process"
        )
        assert mesh_pops > 0, "the eager mesh-reference decode path did not run"
    finally:
        pm.stop().get()
