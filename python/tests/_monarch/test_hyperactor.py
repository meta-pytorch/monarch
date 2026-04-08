# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import multiprocessing
import os
import signal
import time
from typing import Any, Callable, cast, Coroutine, Iterable, Type, TYPE_CHECKING

from monarch._rust_bindings.monarch_hyperactor.actor import (
    MethodSpecifier,
    PanicFlag,
    PythonMessageKind,
)
from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer
from monarch._rust_bindings.monarch_hyperactor.pickle import (
    PendingMessage,
    pickle as monarch_pickle,
)
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._rust_bindings.monarch_hyperactor.proc_mesh import ProcMesh
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._src.actor.host_mesh import HostMesh, this_host
from monarch._src.actor.pickle import flatten, unflatten
from monarch.actor import context


if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.actor import Actor, PortProtocol


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
        match method:
            case MethodSpecifier.Init():
                # Handle init message - response_port may be None
                if response_port is not None:
                    response_port.send(None)
                return None
            case _:
                raise NotImplementedError()


def test_import() -> None:
    try:
        import monarch._rust_bindings  # noqa
    except ImportError as e:
        raise ImportError(f"monarch._rust_bindings failed to import: {e}")


def test_actor_id() -> None:
    actor_id = ActorId(addr="local:0", proc_name="test", actor_name="actor")
    assert actor_id.pid == 0
    assert str(actor_id) == "local:0,test,actor[0]"


def _shutdown_test_fn() -> None:
    import monarch._rust_bindings  # noqa
    import torch  # noqa

    time.sleep(100)


def test_no_hang_on_shutdown() -> None:
    proc = multiprocessing.Process(target=_shutdown_test_fn)
    proc.start()
    pid = proc.pid
    assert pid is not None

    os.kill(pid, signal.SIGTERM)
    time.sleep(2)
    pid, code = os.waitpid(pid, os.WNOHANG)
    assert pid > 0
    assert code == signal.SIGTERM, code


def _python_task_test(
    fn: Callable[[], Coroutine[Any, Any, None]],
) -> Callable[[], None]:
    """
    Wrapper for tests that use the internal tokio event loop
    APIs and need to run on that event loop.
    """
    return lambda: PythonTask.from_coroutine(fn()).block_on()


@_python_task_test
async def test_proc_mesh() -> None:
    host_mesh: HostMesh = this_host()

    async def task() -> ProcMesh:
        hy_host_mesh = await host_mesh._hy_host_mesh
        return await hy_host_mesh.spawn_nonblocking(
            context().actor_instance._as_rust(),
            "proc_mesh",
            Extent(["replicas"], [2]),
        )

    proc_mesh = await PythonTask.from_coroutine(task()).spawn()
    # v1 has a different repr format
    assert "ProcMesh" in str(proc_mesh)


@_python_task_test
async def test_actor_mesh() -> None:
    host_mesh: HostMesh = this_host()

    async def task() -> ProcMesh:
        hy_host_mesh = await host_mesh._hy_host_mesh
        return await hy_host_mesh.spawn_nonblocking(
            context().actor_instance._as_rust(),
            "test",
            Extent(["replicas"], [2]),
        )

    proc_mesh_task: Shared[ProcMesh] = PythonTask.from_coroutine(task()).spawn()

    # Create an explicit init message
    init_state = monarch_pickle(None)
    init_message = PendingMessage(
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
        False,  # emulated
    )

    await actor_mesh.initialized()

    # assert isinstance(actor_mesh.client, Mailbox)


def test_buffer_read_write() -> None:
    b = Buffer()
    b.write(b"yellow")
    assert b.freeze().read() == b"yellow"


def test_pickle_to_buffer() -> None:
    x = [bytes(100000)]
    args, b = flatten(x, lambda x: False)
    y = unflatten(b.freeze(), args)
    assert x == y
