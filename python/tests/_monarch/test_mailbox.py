# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import pickle
from typing import (
    Any,
    Callable,
    cast,
    Coroutine,
    final,
    Generic,
    Iterable,
    TYPE_CHECKING,
    TypeVar,
)

from monarch._rust_bindings.monarch_hyperactor.actor import (
    MethodSpecifier,
    PanicFlag,
    PythonMessage,
    PythonMessageKind,
)
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._src.actor.allocator import LocalAllocator

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.actor import PortProtocol

from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints, AllocSpec
from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    Mailbox,
    PortReceiver,
    PortRef,
)
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh
from monarch._src.actor.actor_mesh import context, Instance


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
        l: U = cast(U, pickle.loads(left.message))
        r: U = cast(U, pickle.loads(right.message))
        result: U = self._reduce_f(l, r)
        return PythonMessage(left.kind, pickle.dumps(result))


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
        s: S = cast(S, pickle.loads(state.message))
        u: U = cast(U, pickle.loads(update.message))
        result: S = self._accumulate_f(s, u)
        return PythonMessage(state.kind, pickle.dumps(result))

    @property
    def initial_state(self) -> PythonMessage:
        return PythonMessage(
            PythonMessageKind.CallMethod(
                MethodSpecifier.ReturnsResponse(" @Accumulator.initial_state"), None
            ),
            pickle.dumps(self._initial_state),
        )

    @property
    def reducer(self) -> Reducer[U] | None:
        return self._reducer


async def allocate() -> ProcMesh:
    spec = AllocSpec(AllocConstraints(), replica=1)
    allocator = LocalAllocator()
    alloc = await allocator.allocate_nonblocking(spec)
    return await ProcMesh.allocate_nonblocking(
        context().actor_instance._as_rust(), alloc, "test"
    )


def _python_task_test(
    fn: Callable[[], Coroutine[Any, Any, None]],
) -> Callable[[], None]:
    """
    Wrapper for tests that use the internal tokio event loop
    APIs and need to run on that event loop.
    """
    return lambda: PythonTask.from_coroutine(fn()).block_on()


@_python_task_test
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
                PythonMessageKind.CallMethod(
                    MethodSpecifier.ReturnsResponse("test_accumulator"), None
                ),
                pickle.dumps(value),
            ),
        )

    async def recv_message() -> str:
        messge = await receiver.recv_task().with_timeout(seconds=5)
        value = pickle.loads(messge.message)
        return cast(str, value)

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
        panic_flag: PanicFlag,
        local_state: Iterable[Any],
        response_port: "PortProtocol[Any]",
    ) -> None:
        response_port.send(pickle.loads(message))
        for i in range(100):
            response_port.send(f"msg{i}")


@_python_task_test
async def test_reducer() -> None:
    proc_mesh = await allocate()
    actor_mesh = await proc_mesh.spawn_nonblocking(
        context().actor_instance._as_rust(), "test", MyActor
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

    actor_mesh.cast(
        PythonMessage(
            PythonMessageKind.CallMethod(
                MethodSpecifier.ReturnsResponse("echo"), port_ref
            ),
            pickle.dumps("start"),
        ),
        "all",
        ins._as_rust(),
    )

    m = await receiver.recv_task().with_timeout(seconds=5)
    value = pickle.loads(m.message)
    assert "[reduced](start+msg0)" in value

    #  Note: occasionally test would hang without this stop
    await proc_mesh.stop_nonblocking(
        context().actor_instance._as_rust(), "test cleanup"
    )
