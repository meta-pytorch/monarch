# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import pickle
from typing import (
    Any,
    Callable,
    cast,
    final,
    Generic,
    Iterable,
    Type,
    TYPE_CHECKING,
    TypeVar,
)

from monarch._rust_bindings.monarch_hyperactor.actor import (
    ActorKind,
    MethodSpecifier,
    PythonMessage,
    PythonMessageKind,
)
from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer, FrozenBuffer
from monarch._rust_bindings.monarch_hyperactor.handle import Handle
from monarch._rust_bindings.monarch_hyperactor.host_mesh import HostMesh as HyHostMesh
from monarch._rust_bindings.monarch_hyperactor.pickle import (
    PendingMessage,
    pickle as monarch_pickle,
)
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._src.actor.host_mesh import this_host

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.actor import Actor, PortProtocol

from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    Mailbox,
    PortReceiver,
    PortRef,
)
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh
from monarch._src.actor.actor_mesh import context, Instance


def _to_frozen_buffer(data: bytes) -> FrozenBuffer:
    """Helper to convert bytes to FrozenBuffer."""
    buf = Buffer()
    buf.write(data)
    return buf.freeze()


S = TypeVar("S")
U = TypeVar("U")


@final
class Reducer(Generic[U]):
    def __init__(
        self,
        reduce_f: Callable[[U, U], U],
    ) -> None:
        self._reduce_f: Callable[[U, U], U] = reduce_f

    def __call__(self, left: PythonMessage, right: PythonMessage) -> PythonMessage:
        l: U = cast(U, left.decode())
        r: U = cast(U, right.decode())
        result: U = self._reduce_f(l, r)
        return PythonMessage(left.kind, _to_frozen_buffer(pickle.dumps(result)), [])


@final
class Accumulator(Generic[S, U]):
    def __init__(
        self,
        initial_state: S,
        accumulate_f: Callable[[S, U], S],
        reduce_f: Callable[[U, U], U] | None,
    ) -> None:
        self._initial_state: S = initial_state
        self._accumulate_f: Callable[[S, U], S] = accumulate_f
        self._reducer: Reducer[U] | None = (
            Reducer(reduce_f) if reduce_f is not None else None
        )

    def __call__(self, state: PythonMessage, update: PythonMessage) -> PythonMessage:
        s: S = cast(S, state.decode())
        u: U = cast(U, update.decode())
        result: S = self._accumulate_f(s, u)
        return PythonMessage(state.kind, _to_frozen_buffer(pickle.dumps(result)), [])

    @property
    def initial_state(self) -> PythonMessage:
        return PythonMessage(
            cast(Any, PythonMessageKind.Result)(None),
            _to_frozen_buffer(pickle.dumps(self._initial_state)),
            [],
        )

    @property
    def reducer(self) -> Reducer[U] | None:
        return self._reducer


def allocate() -> Handle[ProcMesh]:
    return HyHostMesh.spawn_nonblocking(
        this_host()._hy_host_mesh,
        context().actor_instance._as_rust(),
        "test",
        Extent(["replicas"], [1]),
    )


async def test_accumulator() -> None:
    ins: Instance = context().actor_instance
    mailbox: Mailbox = ins._mailbox

    def my_accumulate(state: str, update: int) -> str:
        return f"{state}+{update}"

    accumulator = Accumulator("init", my_accumulate, None)
    receiver: PortReceiver
    handle, receiver = mailbox.open_accum_port(accumulator)
    port_ref: PortRef = handle.bind()

    def post_message(value: int) -> None:
        port_ref.send(
            ins._as_rust(),
            PythonMessage(
                # pyrefly: ignore [bad-argument-type]
                PythonMessageKind.CallMethod(
                    # pyrefly: ignore [bad-argument-count]
                    MethodSpecifier.ReturnsResponse("test_accumulator"),
                    None,
                ),
                _to_frozen_buffer(pickle.dumps(value)),
                [],
            ),
        )

    async def recv_message() -> str:
        return cast(str, await asyncio.wait_for(receiver.recv(False), 5))

    post_message(1)
    # Receive the first message
    assert await recv_message() == "init+1"

    post_message(2)
    post_message(3)
    post_message(4)
    # Receive the 2nd to 4th messages
    assert await recv_message() == "init+1+2+3+4"


class MyActor:
    async def handle(
        self,
        ctx: Any,
        method: MethodSpecifier,
        message: bytes,
        panic_flag: Any,
        local_state: Iterable[Any],
        mesh_references: Iterable[Any],
        response_port: "PortProtocol[Any]",
        correlation_id: int | None = None,
    ) -> None:
        match method:
            # pyrefly: ignore [invalid-pattern]
            case MethodSpecifier.Init():
                # Handle init message - response_port may be None
                if response_port is not None:
                    response_port.send(None)
                return None
            case _:
                response_port.send(pickle.loads(message))
                for i in range(100):
                    response_port.send(f"msg{i}")


async def test_reducer() -> None:
    proc_mesh_task = allocate()

    # Create an explicit init message
    init_state = monarch_pickle(None)
    init_message = PendingMessage(
        # pyrefly: ignore [bad-argument-count, bad-argument-type]
        PythonMessageKind.CallMethod(MethodSpecifier.Init(), None),
        init_state,
    )

    # Use spawn_async with the explicit init message
    actor_mesh = ProcMesh.spawn_async(
        proc_mesh_task,
        context().actor_instance._as_rust(),
        "test",
        cast(Type["Actor"], MyActor),
        init_message,
        ActorKind.ASYNC,
        False,  # emulated
    )

    ins = context().actor_instance

    def my_accumulate(state: str, update: str) -> str:
        return state + update

    def my_reduce(state: str, update: str) -> str:
        return f"[reduced]({state}+{update})"

    accumulator = Accumulator("", my_accumulate, my_reduce)
    receiver: PortReceiver
    handle, receiver = ins._mailbox.open_accum_port(accumulator)
    port_ref = handle.bind()

    state = monarch_pickle("start")
    actor_mesh.cast_unresolved(
        PendingMessage(
            # pyrefly: ignore [bad-argument-type]
            PythonMessageKind.CallMethod(
                # pyrefly: ignore [bad-argument-count]
                MethodSpecifier.ReturnsResponse("echo"),
                port_ref,
            ),
            state,
        ),
        "all",
        ins._as_rust(),
    )

    value = await asyncio.wait_for(receiver.recv(False), 5)
    assert "[reduced](start+msg0)" in value

    #  Note: occasionally test would hang without this stop
    proc_mesh = await proc_mesh_task
    await proc_mesh.stop_nonblocking(ins._as_rust(), "test cleanup")
