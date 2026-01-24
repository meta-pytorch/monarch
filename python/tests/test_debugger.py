# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import asyncio
import functools
import importlib.resources
import os
import re
import shutil
import signal
import subprocess
import sys
from typing import cast, List, Optional, Tuple, TypeVar
from unittest.mock import AsyncMock, patch

import cloudpickle
import monarch
import pytest
import torch
from monarch._src.actor.actor_mesh import (
    Actor,
    ActorError,
    context,
    current_rank,
    IN_PAR,
)
from monarch._src.actor.debugger.debug_command import (
    Attach,
    Cast,
    Continue,
    DebugCommand,
    Help,
    ListCommand,
    Quit,
)
from monarch._src.actor.debugger.debug_controller import DebugController
from monarch._src.actor.debugger.debug_io import DebugStdIO
from monarch._src.actor.debugger.debug_session import (
    DebugSession,
    DebugSessionInfo,
    DebugSessions,
)
from monarch._src.actor.endpoint import endpoint, Extent
from monarch._src.actor.host_mesh import create_local_host_mesh, this_host
from monarch._src.actor.proc_mesh import (
    get_or_spawn_controller,
    proc_mesh as proc_mesh_v0,
    ProcMesh,
)
from monarch._src.actor.source_loader import SourceLoaderController
from monarch.config import configure
from monarch.tools.debug_env import (
    _MONARCH_DEBUG_SERVER_HOST_ENV_VAR,
    _MONARCH_DEBUG_SERVER_PORT_ENV_VAR,
)
from pyre_extensions import none_throws


TActor = TypeVar("TActor", bound=Actor)


def proc_mesh(
    gpus: int = 1,
    hosts: int = 1,
) -> ProcMesh:
    return create_local_host_mesh(extent=Extent(["hosts"], [hosts])).spawn_procs(
        per_host={"gpus": gpus}
    )


needs_cuda = pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="CUDA not available",
)


debug_env = {
    _MONARCH_DEBUG_SERVER_HOST_ENV_VAR: "0.0.0.0",
    _MONARCH_DEBUG_SERVER_PORT_ENV_VAR: "0",
}


def isolate_in_subprocess(test_fn=None, *, env=None):
    if test_fn is None:
        return functools.partial(isolate_in_subprocess, env=env)

    if env is None:
        env = {}

    def sync_test_fn():
        asyncio.run(test_fn())

    sync_test_fn_name = f"sync_{test_fn.__name__}"
    setattr(sys.modules[__name__], sync_test_fn_name, sync_test_fn)

    env.update(os.environ.copy())

    def wrapper():
        if IN_PAR:
            assert (
                subprocess.call(
                    [
                        str(
                            importlib.resources.files("monarch.python.tests").joinpath(
                                "run_test_bin"
                            )
                        ),
                        sync_test_fn_name,
                    ],
                    env=env,
                )
                == 0
            )
        else:
            assert (
                subprocess.call(
                    [
                        sys.executable,
                        "-c",
                        f"import tests.test_debugger; tests.test_debugger.{sync_test_fn_name}()",
                    ],
                    env=env,
                )
                == 0
            )

    return wrapper


def run_test_from_name():
    getattr(sys.modules[__name__], sys.argv[1])()


cli_bin = (
    str(importlib.resources.files("monarch.python.tests").joinpath("cli_bin"))
    if IN_PAR
    else ""
)


def _bad_rank():
    raise ValueError("bad rank")


def _debugee_actor_internal(rank):
    if rank == 0:
        breakpoint()  # noqa
        rank += 1
        rank += 1
        return rank
    elif rank == 1:
        breakpoint()  # noqa
        rank += 2
        rank += 2
        return rank
    elif rank == 2:
        breakpoint()  # noqa
        rank += 3
        rank += 3
        _bad_rank()
    elif rank == 3:
        breakpoint()  # noqa
        rank += 4
        rank += 4
        return rank


class DebugeeActor(Actor):
    @endpoint
    async def to_debug(self):
        rank = current_rank().rank
        return _debugee_actor_internal(rank)

    @endpoint
    async def name(self) -> str:
        return context().actor_instance.actor_id.actor_name

    @endpoint
    async def nested(self) -> "DebugeeActor":
        return (
            this_host()
            .spawn_procs(per_host={"hosts": 2, "gpus": 2})
            .spawn("debugee_nested", DebugeeActor)
        )


class DebugControllerForTesting(DebugController):
    def __init__(self):
        super().__init__()
        self._debug_io = DebugStdIO()

    @endpoint
    async def blocking_enter(self):
        async with self._task_lock:
            assert self._task is None
            await self._enter()

    @endpoint
    async def server_port(self):
        server: asyncio.Server = await self._server
        if len(server.sockets) > 0:
            return server.sockets[0].getsockname()[1]


async def _wait_for_breakpoints(
    debug_controller, n_breakpoints, timeout_sec=20
) -> List[DebugSessionInfo]:
    breakpoints: List[DebugSessionInfo] = []
    for _ in range(timeout_sec):
        await asyncio.sleep(1)
        breakpoints = await debug_controller.list.call_one(print_output=False)
        if len(breakpoints) == n_breakpoints:
            return breakpoints
    raise RuntimeError("timed out waiting for breakpoints")


async def _test_debug(nested: bool) -> None:
    if not nested:
        proc = proc_mesh(hosts=2, gpus=2)
        debugee = proc.spawn("debugee", DebugeeActor)
    else:
        proc = create_local_host_mesh(extent=Extent(["hosts"], [1])).spawn_procs()
        debugee = proc.spawn("debugee", DebugeeActor).nested.choose().get()
    name = debugee.name.choose().get()

    input_mock = AsyncMock()
    input_mock.side_effect = [
        f"attach {name} 1",
        "n",
        "n",
        "n",
        "n",
        "detach",
        f"attach {name} 1",
        "detach",
        "quit",
        f"cast {name} ranks(0,3) n",
        f"cast {name} ranks(0,3) n",
        # Attaching to 0 and 3 ensures that when we call "list"
        # the next time, their function/lineno info will be
        # up-to-date.
        f"attach {name} 0",
        "detach",
        f"attach {name} 3",
        "detach",
        "quit",
        f"attach {name} 2",
        "c",
        "detach",
        "quit",
        f"attach {name} 2",
        "bt",
        "c",
        "quit",
        "continue",
        "quit",
    ]

    outputs = []

    def _patch_output(msg):
        nonlocal outputs
        outputs.append(msg)

    output_mock = AsyncMock()
    output_mock.side_effect = _patch_output

    with (
        patch("monarch._src.actor.debugger.debug_io.DebugStdIO.input", new=input_mock),
        patch(
            "monarch._src.actor.debugger.debug_io.DebugStdIO.output", new=output_mock
        ),
    ):
        debug_controller = await get_or_spawn_controller(
            "debug_controller", DebugControllerForTesting
        )

        fut = debugee.to_debug.call()
        await debug_controller.wait_pending_session.call_one()
        # This can take a while during stress testing with many instances of the test
        # running in parallel, so the timeout is set to 60 seconds.
        breakpoints = await _wait_for_breakpoints(debug_controller, 4, timeout_sec=60)

        initial_linenos = {}
        for i in range(len(breakpoints)):
            info = breakpoints[i]
            initial_linenos[info.rank] = info.lineno
            assert info.rank == i
            assert info.coords == {"hosts": info.rank // 2, "gpus": info.rank % 2}
            assert info.function == "test_debugger._debugee_actor_internal"
            assert info.lineno == cast(int, breakpoints[0].lineno) + 5 * info.rank

        await debug_controller.blocking_enter.call_one()

        # Check that when detaching and re-attaching to a session, the last portion of the output is repeated
        expected_last_output = [
            r"--Return--",
            r"\n",
            r"> (/.*/)+test_debugger.py\(\d+\)to_debug\(\)->5\n-> return _debugee_actor_internal\(rank\)",
            r"\n",
            r"\(Pdb\) ",
        ]
        output_len = len(expected_last_output)
        rev_outputs = outputs[::-1]
        last_return = rev_outputs.index("--Return--")
        second_to_last_return = rev_outputs.index("--Return--", last_return + 1)
        last_return = len(rev_outputs) - last_return - 1
        second_to_last_return = len(rev_outputs) - second_to_last_return - 1
        assert (
            outputs[second_to_last_return : second_to_last_return + output_len]  # noqa
            == outputs[last_return : last_return + output_len]  # noqa
        )
        for real_output, expected_output in zip(
            outputs[last_return : last_return + output_len],  # noqa
            expected_last_output,
        ):
            assert re.match(expected_output, real_output) is not None

        breakpoints = await debug_controller.list.call_one(print_output=False)
        for i in range(len(breakpoints)):
            if i == 1:
                assert breakpoints[i].function == "test_debugger.to_debug"
            else:
                assert (
                    breakpoints[i].function == "test_debugger._debugee_actor_internal"
                )
                assert breakpoints[i].lineno == initial_linenos[i]

        await debug_controller.blocking_enter.call_one()

        breakpoints = await debug_controller.list.call_one(print_output=False)
        for i in range(len(breakpoints)):
            if i == 1:
                assert breakpoints[i].function == "test_debugger.to_debug"
            elif i in (0, 3):
                assert (
                    breakpoints[i].function == "test_debugger._debugee_actor_internal"
                )
                assert breakpoints[i].lineno == initial_linenos[i] + 2
            else:
                assert (
                    breakpoints[i].function == "test_debugger._debugee_actor_internal"
                )
                assert breakpoints[i].lineno == initial_linenos[i]

        await debug_controller.blocking_enter.call_one()

        breakpoints = await debug_controller.list.call_one(print_output=False)
        assert len(breakpoints) == 4
        # Expect post-mortem debugging for rank 2
        assert breakpoints[2].function == "test_debugger._bad_rank"

        await debug_controller.blocking_enter.call_one()

        expected_last_output = [
            r"\s*(/.*/)+test_debugger.py\(\d+\)_debugee_actor_internal\(\)\n-> _bad_rank\(\)",
            r"\n",
            r'> (/.*/)+test_debugger.py\(\d+\)_bad_rank\(\)\n-> raise ValueError\("bad rank"\)',
            r"\n",
            r"\(Pdb\) ",
        ]

        rev_outputs = outputs[::-1]
        output_index = len(outputs) - (
            rev_outputs.index("(Pdb) ") + len(expected_last_output)
        )

        for output, expected_output in zip(
            outputs[output_index : output_index + len(expected_last_output)],  # noqa
            expected_last_output,
        ):
            assert re.match(expected_output, output) is not None

        breakpoints = await debug_controller.list.call_one(print_output=False)
        assert len(breakpoints) == 3
        for i, rank in enumerate((0, 1, 3)):
            assert breakpoints[i].rank == rank

        await debug_controller.blocking_enter.call_one()
        await _wait_for_breakpoints(debug_controller, 0)

        with pytest.raises(
            monarch._src.actor.actor_mesh.ActorError, match="ValueError: bad rank"
        ):
            await fut


# We have to run this test in a separate process because there is only one
# debug controller per process, and we don't want this to interfere with
# the other tests that access the debug controller.
@isolate_in_subprocess(env=debug_env)
@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Not enough GPUs, this test requires at least 2 GPUs",
)
@pytest.mark.timeout(60)
async def test_debug():
    await _test_debug(nested=False)


# See earlier comment.
@isolate_in_subprocess(env=debug_env)
@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Not enough GPUs, this test requires at least 2 GPUs",
)
@pytest.mark.timeout(60)
async def test_debug_nested():
    await _test_debug(nested=True)


# See earlier comment
@isolate_in_subprocess(env=debug_env)
@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Not enough GPUs, this test requires at least 2 GPUs",
)
@pytest.mark.timeout(60)
async def test_debug_multi_actor() -> None:
    proc = proc_mesh(hosts=2, gpus=2)
    debugee_1 = proc.spawn("debugee_1", DebugeeActor)
    debugee_2 = proc.spawn("debugee_2", DebugeeActor)
    name_1 = debugee_1.name.choose().get()
    name_2 = debugee_2.name.choose().get()

    input_mock = AsyncMock()
    input_mock.side_effect = [
        f"attach {name_2} 2",
        "n",
        "detach",
        f"attach {name_1} 1",
        "n",
        "detach",
        "quit",
        f"cast {name_1} ranks(:) c",
        f"cast {name_2} ranks(:) c",
        f"attach {name_2} 2",
        "c",
        "quit",
        "continue",
        "quit",
    ]

    with patch(
        "monarch._src.actor.debugger.debug_io.DebugStdIO.input", side_effect=input_mock
    ):
        debug_controller = await get_or_spawn_controller(
            "debug_controller", DebugControllerForTesting
        )

        fut_1 = debugee_1.to_debug.call()
        fut_2 = debugee_2.to_debug.call()
        await debug_controller.wait_pending_session.call_one()

        breakpoints = await _wait_for_breakpoints(debug_controller, 8)

        initial_linenos = {}
        for i in range(len(breakpoints)):
            info = breakpoints[i]
            initial_linenos[info.rank] = info.lineno
            assert info.rank == i % 4
            assert info.actor_name == name_1 if i < 4 else name_2
            assert info.coords == {"hosts": info.rank // 2, "gpus": info.rank % 2}
            assert info.function == "test_debugger._debugee_actor_internal"
            assert info.lineno == cast(int, breakpoints[0].lineno) + 5 * info.rank

        await debug_controller.blocking_enter.call_one()

        breakpoints = await _wait_for_breakpoints(debug_controller, 8)
        for i in range(len(breakpoints)):
            if i == 1:
                assert breakpoints[i].actor_name == name_1
                assert breakpoints[i].rank == 1
                assert breakpoints[i].lineno == initial_linenos[breakpoints[i].rank] + 1
            elif i == 6:
                assert breakpoints[i].actor_name == name_2
                assert breakpoints[i].rank == 2
                assert breakpoints[i].lineno == initial_linenos[breakpoints[i].rank] + 1
            else:
                assert breakpoints[i].actor_name == name_1 if i < 4 else name_2
                assert breakpoints[i].rank == i % 4
                assert breakpoints[i].lineno == initial_linenos[breakpoints[i].rank]

        await debug_controller.blocking_enter.call_one()

        breakpoints = await _wait_for_breakpoints(debug_controller, 1)
        with pytest.raises(ActorError, match="ValueError: bad rank"):
            await fut_2
        assert breakpoints[0].actor_name == name_1
        assert breakpoints[0].rank == 2
        assert breakpoints[0].function == "test_debugger._bad_rank"

        await debug_controller.blocking_enter.call_one()

        breakpoints = await _wait_for_breakpoints(debug_controller, 0)
        with pytest.raises(ActorError, match="ValueError: bad rank"):
            await fut_1


async def test_debug_sessions_insert_get_remove() -> None:
    mock_sessions = []
    for actor_name in ("actor_a", "actor_b"):
        for rank in range(2):
            mock_session = DebugSession(rank, {}, "", actor_name)
            mock_sessions.append(mock_session)

    debug_sessions = DebugSessions()

    with pytest.raises(ValueError, match="No debug sessions for actor actor_a"):
        debug_sessions.get("actor_a", 0)
    debug_sessions.insert(mock_sessions[0])
    assert debug_sessions.get("actor_a", 0) is mock_sessions[0]
    assert ("actor_a", 0) in debug_sessions
    with pytest.raises(
        ValueError, match="Debug session for rank 0 already exists for actor actor_a"
    ):
        debug_sessions.insert(mock_sessions[0])

    with pytest.raises(
        ValueError, match="No debug session for rank 1 for actor actor_a"
    ):
        debug_sessions.get("actor_a", 1)
    debug_sessions.insert(mock_sessions[1])
    assert debug_sessions.get("actor_a", 1) is mock_sessions[1]
    assert ("actor_a", 1) in debug_sessions
    with pytest.raises(
        ValueError, match="Debug session for rank 1 already exists for actor actor_a"
    ):
        debug_sessions.insert(mock_sessions[1])

    with pytest.raises(ValueError, match="No debug sessions for actor actor_b"):
        debug_sessions.get("actor_b", 0)
    debug_sessions.insert(mock_sessions[2])
    assert debug_sessions.get("actor_b", 0) is mock_sessions[2]
    assert ("actor_b", 0) in debug_sessions
    with pytest.raises(
        ValueError, match="Debug session for rank 0 already exists for actor actor_b"
    ):
        debug_sessions.insert(mock_sessions[2])

    with pytest.raises(
        ValueError, match="No debug session for rank 1 for actor actor_b"
    ):
        debug_sessions.get("actor_b", 1)
    debug_sessions.insert(mock_sessions[3])
    assert debug_sessions.get("actor_b", 1) is mock_sessions[3]
    assert ("actor_b", 1) in debug_sessions
    with pytest.raises(
        ValueError, match="Debug session for rank 1 already exists for actor actor_b"
    ):
        debug_sessions.insert(mock_sessions[3])

    assert len(debug_sessions) == 4

    assert debug_sessions.remove("actor_a", 0) is mock_sessions[0]
    assert len(debug_sessions) == 3
    assert ("actor_a", 0) not in debug_sessions
    with pytest.raises(
        ValueError, match="No debug session for rank 0 for actor actor_a"
    ):
        debug_sessions.remove("actor_a", 0)

    assert debug_sessions.remove("actor_a", 1) is mock_sessions[1]
    assert len(debug_sessions) == 2
    assert ("actor_a", 1) not in debug_sessions
    with pytest.raises(ValueError, match="No debug sessions for actor actor_a"):
        debug_sessions.remove("actor_a", 1)

    assert debug_sessions.remove("actor_b", 0) is mock_sessions[2]
    assert len(debug_sessions) == 1
    assert ("actor_b", 0) not in debug_sessions
    with pytest.raises(
        ValueError, match="No debug session for rank 0 for actor actor_b"
    ):
        debug_sessions.remove("actor_b", 0)

    assert debug_sessions.remove("actor_b", 1) is mock_sessions[3]
    assert len(debug_sessions) == 0
    assert ("actor_b", 1) not in debug_sessions
    with pytest.raises(ValueError, match="No debug sessions for actor actor_b"):
        debug_sessions.remove("actor_b", 1)


async def test_debug_sessions_iter() -> None:
    debug_sessions = DebugSessions()
    mock_sessions = []

    for actor_name in ("actor_a", "actor_b"):
        for host in range(3):
            for gpu in range(8):
                rank = host * 8 + gpu
                mock_session = DebugSession(
                    rank, {"hosts": host, "gpus": gpu}, "", actor_name
                )
                mock_sessions.append(mock_session)
                debug_sessions.insert(mock_session)

    # Single rank
    for i, actor_name in enumerate(("actor_a", "actor_b")):
        sessions = list(debug_sessions.iter((actor_name, 2)))
        assert len(sessions) == 1
        assert sessions[0] is mock_sessions[i * 24 + 2]

    # List of ranks
    ranks = [1, 3, 5]
    for i, actor_name in enumerate(("actor_a", "actor_b")):
        sessions = sorted(
            debug_sessions.iter((actor_name, ranks)), key=lambda s: s.get_info()
        )
        assert len(sessions) == 3
        for j in range(3):
            assert sessions[j] is mock_sessions[i * 24 + ranks[j]]

    # Range of ranks
    ranks = range(2, 24, 3)
    for i, actor_name in enumerate(("actor_a", "actor_b")):
        sessions = sorted(
            debug_sessions.iter((actor_name, ranks)), key=lambda s: s.get_info()
        )
        ranks = list(ranks)
        assert len(sessions) == len(ranks)
        for j in range(len(ranks)):
            assert sessions[j] is mock_sessions[i * 24 + ranks[j]]

    # All ranks
    for i, actor_name in enumerate(("actor_a", "actor_b")):
        sessions = sorted(
            debug_sessions.iter((actor_name, None)), key=lambda s: s.get_info()
        )
        assert len(sessions) == 24
        for j in range(24):
            assert sessions[j] is mock_sessions[i * 24 + j]

    # All ranks, all actors
    sessions = sorted(debug_sessions.iter(None), key=lambda s: s.get_info())
    assert len(sessions) == 48
    for i in range(48):
        assert sessions[i] is mock_sessions[i]

    # Dimension filtering with a single value
    for i, actor_name in enumerate(("actor_a", "actor_b")):
        sessions = sorted(
            debug_sessions.iter((actor_name, {"hosts": 1})), key=lambda s: s.get_info()
        )
        assert len(sessions) == 8
        for j in range(8):
            assert sessions[j] is mock_sessions[i * 24 + 8 + j]

    # Dimension filtering with a list
    for i, actor_name in enumerate(("actor_a", "actor_b")):
        sessions = sorted(
            debug_sessions.iter((actor_name, {"hosts": [0, 2]})),
            key=lambda s: s.get_info(),
        )
        assert len(sessions) == 16
        j = 0
        for host in (0, 2):
            for gpu in range(8):
                assert sessions[j] is mock_sessions[i * 24 + host * 8 + gpu]
                j += 1

    # Dimension filtering with a range
    for i, actor_name in enumerate(("actor_a", "actor_b")):
        sessions = sorted(
            debug_sessions.iter((actor_name, {"gpus": range(5, 8)})),
            key=lambda s: s.get_info(),
        )
        assert len(sessions) == 9
        j = 0
        for host in range(3):
            for gpu in range(5, 8):
                assert sessions[j] is mock_sessions[i * 24 + host * 8 + gpu]
                j += 1

    # Multiple dimension filters
    for i, actor_name in enumerate(("actor_a", "actor_b")):
        sessions = sorted(
            debug_sessions.iter(
                (actor_name, {"hosts": [1, 3], "gpus": range(0, sys.maxsize, 3)})
            ),
            key=lambda s: s.get_info(),
        )
        assert len(sessions) == 3
        j = 0
        for gpu in range(0, 8, 3):
            assert sessions[j] is mock_sessions[i * 24 + 8 + gpu]
            j += 1

    # Non-existent dimension
    for actor_name in ("actor_a", "actor_b"):
        sessions = sorted(
            debug_sessions.iter((actor_name, {"hosts": 0, "gpus": 0, "foo": 0})),
            key=lambda s: s.get_info(),
        )
        assert len(sessions) == 0


@pytest.mark.parametrize(
    ["user_input", "expected_output"],
    [
        ("attach debugee 1", Attach("debugee", 1)),
        ("a my_awesome_actor-123_DBG 100", Attach("my_awesome_actor-123_DBG", 100)),
        ("list", ListCommand()),
        ("l", ListCommand()),
        ("help", Help()),
        ("h", Help()),
        ("quit", Quit()),
        ("q", Quit()),
        ("continue", Continue()),
        ("c", Continue()),
        (
            "cast debugee ranks(123) b 25",
            Cast(actor_name="debugee", ranks=123, command="b 25"),
        ),
        (
            "cast my_awesome_actor ranks(12,34,56) b 25",
            Cast(actor_name="my_awesome_actor", ranks=[12, 34, 56], command="b 25"),
        ),
        (
            "cast debugee ranks(:) b 25",
            Cast(actor_name="debugee", ranks=range(0, sys.maxsize), command="b 25"),
        ),
        (
            "cast debugee ranks(:123) b 25",
            Cast(actor_name="debugee", ranks=range(0, 123), command="b 25"),
        ),
        (
            "cast debugee ranks(123:) b 25",
            Cast(actor_name="debugee", ranks=range(123, sys.maxsize), command="b 25"),
        ),
        (
            "cast debugee ranks(123:456) b 25",
            Cast(actor_name="debugee", ranks=range(123, 456), command="b 25"),
        ),
        (
            "cast debugee ranks(::) b 25",
            Cast(actor_name="debugee", ranks=range(0, sys.maxsize), command="b 25"),
        ),
        (
            "cast debugee ranks(::123) b 25",
            Cast(
                actor_name="debugee", ranks=range(0, sys.maxsize, 123), command="b 25"
            ),
        ),
        (
            "cast debugee ranks(123::) b 25",
            Cast(actor_name="debugee", ranks=range(123, sys.maxsize), command="b 25"),
        ),
        (
            "cast debugee ranks(:123:) b 25",
            Cast(actor_name="debugee", ranks=range(0, 123), command="b 25"),
        ),
        (
            "cast debugee ranks(:456:123) b 25",
            Cast(actor_name="debugee", ranks=range(0, 456, 123), command="b 25"),
        ),
        (
            "cast debugee ranks(456::123) b 25",
            Cast(
                actor_name="debugee", ranks=range(456, sys.maxsize, 123), command="b 25"
            ),
        ),
        (
            "cast debugee ranks(123:456:) b 25",
            Cast(actor_name="debugee", ranks=range(123, 456), command="b 25"),
        ),
        (
            "cast debugee ranks(456:789:123) b 25",
            Cast(actor_name="debugee", ranks=range(456, 789, 123), command="b 25"),
        ),
        (
            "cast debugee ranks(dim1=123) up 2",
            Cast(actor_name="debugee", ranks={"dim1": 123}, command="up 2"),
        ),
        (
            "cast debugee ranks(dim1=123, dim2=(12,34,56), dim3=15::2) up 2",
            Cast(
                actor_name="debugee",
                ranks={
                    "dim1": 123,
                    "dim2": [12, 34, 56],
                    "dim3": range(15, sys.maxsize, 2),
                },
                command="up 2",
            ),
        ),
    ],
)
async def test_debug_command_parser_valid_inputs(user_input, expected_output):
    assert await DebugCommand.parse(DebugStdIO(), user_input) == expected_output


@pytest.mark.parametrize(
    "invalid_input",
    [
        "",
        "a",
        "attach",
        "a actor",
        "attach actor",
        "attacha actor 1attch actor 1",
        "attach actor 1abc",
        "attach actor 1 a",
        "cast ranks(123) b 25",
        "cast   ranks(123) b 25",
        "castactor ranks(123) b 25",
        "cast actor rnks(123) b 25",
        "cast actor ranks() b 25",
        "cast actor ranks(1ab) b 25",
        "cast actor ranks(1,a,3) b 25",
        "cast actor ranks(a:2:4) b 25",
        "cast actor ranks(1,2,3",
        "cast actor ranks(1,2,3)) b 25",
        "cast actor ranks(1,) b 25",
        "cast actor ranks(1,2,) b 25",
        "cast actor ranks(,1,2) b 25",
        "cast actor ranks(1,,2) b 25",
        "cast actor ranks(:::) b 25",
        "cast actor ranks(:123::) b 25",
        "cast actor ranks(1:2:3,4) b 25",
        "cast actor ranks(dim1=) b 25",
        "cast actor ranks(dim1=123, dim2=) b 25",
        "cast actor ranks(dim1=123, dim2=(12,34,56) b 25",
        "cast actor ranks(dim1=123, dim2=(,12,34,56) b 25",
        "cast actor ranks(dim1=123, dim2=(12,,34,56) b 25",
        "cast actor ranks(dim1=123, dim2=(12,34,56), dim3=15::2 b 25",
        "cast actor ranks(dim1=123,) b 25",
    ],
)
async def test_debug_command_parser_invalid_inputs(invalid_input):
    assert await DebugCommand.parse(DebugStdIO(), invalid_input) is None


# See earlier comment
@isolate_in_subprocess(env={"MONARCH_CLI_BIN": cli_bin, **debug_env})
@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Not enough GPUs, this test requires at least 2 GPUs",
)
@pytest.mark.timeout(60)
async def test_debug_cli():
    proc = proc_mesh(hosts=2, gpus=2)
    debugee = proc.spawn("debugee", DebugeeActor)
    name = debugee.name.choose().get()
    debug_controller = get_or_spawn_controller(
        "debug_controller", DebugControllerForTesting
    ).get()

    fut = debugee.to_debug.call()
    # Stupidly high timeout because when CI tries to run many instances of this
    # test in parallel, it can take a long time for breakpoints to actually show
    # up.
    breakpoints = await _wait_for_breakpoints(debug_controller, 4, timeout_sec=180)

    initial_linenos = {}
    for i in range(len(breakpoints)):
        info = breakpoints[i]
        initial_linenos[info.rank] = info.lineno
        assert info.rank == i
        assert info.coords == {"hosts": info.rank // 2, "gpus": info.rank % 2}
        assert info.function == "test_debugger._debugee_actor_internal"
        assert info.lineno == cast(int, breakpoints[0].lineno) + 5 * info.rank

    port = debug_controller.server_port.call_one().get()

    async def create_debug_cli_proc() -> Tuple[
        Optional[asyncio.subprocess.Process],
        asyncio.StreamWriter,
        asyncio.StreamReader,
    ]:
        cmd = None
        if IN_PAR:
            cmd = [
                os.environ["MONARCH_CLI_BIN"],
                "debug",
                "--host",
                os.environ[_MONARCH_DEBUG_SERVER_HOST_ENV_VAR],
                "--port",
                str(port),
            ]
        elif any(shutil.which(nc_cmd) for nc_cmd in ["ncat", "nc", "netcat"]):
            cmd = [
                "monarch",
                "debug",
                "--host",
                os.environ[_MONARCH_DEBUG_SERVER_HOST_ENV_VAR],
                "--port",
                str(port),
            ]
        if cmd:
            debug_cli_proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
            )
            debug_cli_stdin = none_throws(debug_cli_proc.stdin)
            debug_cli_stdout = none_throws(debug_cli_proc.stdout)
            return debug_cli_proc, debug_cli_stdin, debug_cli_stdout
        else:
            # Netcat isn't available in our github CI environment, so we can't
            # run the monarch.debug_cli module
            reader, writer = await asyncio.open_connection(
                os.environ[_MONARCH_DEBUG_SERVER_HOST_ENV_VAR], port
            )
            return None, writer, reader

    (
        debug_cli_proc,
        debug_cli_stdin,
        debug_cli_stdout,
    ) = await create_debug_cli_proc()

    debug_cli_stdin.writelines(
        [
            f"attach {name} 1\n".encode(),
            b"n\n",
            b"n\n",
            b"n\n",
            b"n\n",
            b"detach\n",
            f"attach {name} 1\n".encode(),
            b"print('test separator')\n",
            b"detach\n",
        ]
    )
    await debug_cli_stdin.drain()

    # Check that when detaching and re-attaching to a session, the last portion of the output is repeated
    expected_last_output = (
        r"--Return--\n"
        r"> (?:/.*/)+test_debugger.py\(\d+\)to_debug\(\)->5\n"
        r"-> return _debugee_actor_internal\(rank\)\n"
        r"\(Pdb\) "
    )

    outputs = (await debug_cli_stdout.readuntil(b"test separator")).decode()
    assert len(re.findall(expected_last_output, outputs)) == 2
    assert outputs[0] == outputs[1]

    breakpoints = await debug_controller.list.call_one(print_output=False)
    for i in range(len(breakpoints)):
        if i == 1:
            assert breakpoints[i].function == "test_debugger.to_debug"
        else:
            assert breakpoints[i].function == "test_debugger._debugee_actor_internal"
            assert breakpoints[i].lineno == initial_linenos[i]

    debug_cli_stdin.write(b"quit\n")
    await debug_cli_stdin.drain()
    # Yield and wait so that the debug controller has a chance to process the
    # input before we close stdin.
    await asyncio.sleep(1)
    debug_cli_stdin.close()
    await debug_cli_stdin.wait_closed()
    if debug_cli_proc:
        assert await debug_cli_proc.wait() == 0

    (
        debug_cli_proc,
        debug_cli_stdin,
        debug_cli_stdout,
    ) = await create_debug_cli_proc()

    debug_cli_stdin.writelines(
        [
            f"cast {name} ranks(0,3) n\n".encode(),
            f"cast {name} ranks(0,3) n\n".encode(),
            # Attaching to 0 and 3 ensures that when we call "list"
            # the next time, their function/lineno info will be
            # up-to-date.
            f"attach {name} 0\n".encode(),
            b"detach\n",
            f"attach {name} 3\n".encode(),
            b"detach\n",
        ]
    )
    await debug_cli_stdin.drain()

    # Make sure we have run all the commands before killing the CLI, otherwise
    # the commands may not actually be sent to the debug controller.
    await debug_cli_stdout.readuntil(
        f"Detached from debug session for {name} 3".encode()
    )
    if debug_cli_proc:
        # Even if we kill the proc using a signal, we should be able to reconnect
        # without issue.
        debug_cli_proc.send_signal(signal.SIGINT)
        assert await debug_cli_proc.wait() != 0
    else:
        debug_cli_stdin.close()
        await debug_cli_stdin.wait_closed()

    breakpoints = await debug_controller.list.call_one(print_output=False)
    for i in range(len(breakpoints)):
        if i == 1:
            assert breakpoints[i].function == "test_debugger.to_debug"
        elif i in (0, 3):
            assert breakpoints[i].function == "test_debugger._debugee_actor_internal"
            assert breakpoints[i].lineno == initial_linenos[i] + 2
        else:
            assert breakpoints[i].function == "test_debugger._debugee_actor_internal"
            assert breakpoints[i].lineno == initial_linenos[i]

    (
        debug_cli_proc,
        debug_cli_stdin,
        debug_cli_stdout,
    ) = await create_debug_cli_proc()

    debug_cli_stdin.writelines([f"attach {name} 2\n".encode(), b"c\n"])
    await debug_cli_stdin.drain()

    # Make sure we have run all the commands before killing the CLI, otherwise
    # the commands may not actually be sent to the debug controller.
    await debug_cli_stdout.readuntil(b"raise ValueError")
    if debug_cli_proc:
        # Even if we kill the proc using a signal while the debugger is attached to
        # a specific rank, we should be able to reconnect to that rank later without
        # issue.
        debug_cli_proc.send_signal(signal.SIGINT)
        assert await debug_cli_proc.wait() != 0
    else:
        debug_cli_stdin.close()
        await debug_cli_stdin.wait_closed()

    breakpoints = await debug_controller.list.call_one(print_output=False)
    assert len(breakpoints) == 4
    # Expect post-mortem debugging for rank 2
    assert breakpoints[2].function == "test_debugger._bad_rank"

    (
        debug_cli_proc,
        debug_cli_stdin,
        debug_cli_stdout,
    ) = await create_debug_cli_proc()

    debug_cli_stdin.writelines([f"attach {name} 2\n".encode(), b"bt\n", b"c\n"])
    await debug_cli_stdin.drain()

    expected_output = (
        r"(?:/.*/)+test_debugger.py\(\d+\)_debugee_actor_internal\(\)\n-> _bad_rank\(\)\n"
        r'> (?:/.*/)+test_debugger.py\(\d+\)_bad_rank\(\)\n-> raise ValueError\("bad rank"\)\n'
        r"\(Pdb\)"
    )

    output = (
        await debug_cli_stdout.readuntil(
            f"Detached from debug session for {name} 2".encode()
        )
    ).decode()
    assert len(re.findall(expected_output, output)) == 1

    debug_cli_stdin.writelines([b"quit\n"])
    await debug_cli_stdin.drain()
    debug_cli_stdin.close()
    # Yield and wait so that the debug controller has a chance to process the
    # input before we close stdin.
    await asyncio.sleep(1)
    await debug_cli_stdin.wait_closed()
    if debug_cli_proc:
        assert await debug_cli_proc.wait() == 0

    breakpoints = await debug_controller.list.call_one(print_output=False)
    assert len(breakpoints) == 3
    for i, rank in enumerate((0, 1, 3)):
        assert breakpoints[i].rank == rank

    debug_cli_proc, debug_cli_stdin, _ = await create_debug_cli_proc()
    debug_cli_stdin.writelines([b"continue\n", b"quit\n"])
    await debug_cli_stdin.drain()
    # Yield and wait so that the debug controller has a chance to process the
    # input before we close stdin.
    await asyncio.sleep(1)
    debug_cli_stdin.close()
    await debug_cli_stdin.wait_closed()
    if debug_cli_proc:
        assert await debug_cli_proc.wait() == 0

    breakpoints = await _wait_for_breakpoints(debug_controller, 0)
    assert len(breakpoints) == 0

    with pytest.raises(
        monarch._src.actor.actor_mesh.ActorError, match="ValueError: bad rank"
    ):
        await fut


class_closure_source = """class ClassClosure:
    def __init__(self, arg):
        self.arg = arg

    def closure(self):
        arg = self.arg

        class Internal:
            def __init__(self):
                self.arg = arg
# noqa
            def get_arg(self):
                breakpoint()
                return self.arg

        return Internal
"""

function_closure_source = """def func_closure(arg, bp):
    def func(internal):
        if bp:
            breakpoint()
        return internal().get_arg() + arg
    return func
"""


def load_class_closure():
    pickled = b'\x80\x05\x95\xc7\x03\x00\x00\x00\x00\x00\x00\x8c\x17cloudpickle.cloudpickle\x94\x8c\x14_make_skeleton_class\x94\x93\x94(\x8c\x08builtins\x94\x8c\x04type\x94\x93\x94\x8c\x08Internal\x94h\x03\x8c\x06object\x94\x93\x94\x85\x94}\x94\x8c\n__module__\x94\x8c\rclass_closure\x94s\x8c 0f63369d5845486db9033c9f3c3253d5\x94Nt\x94R\x94h\x00\x8c\x0f_class_setstate\x94\x93\x94h\x0f}\x94(\x8c\x07__doc__\x94N\x8c\x08__init__\x94h\x00\x8c\x0e_make_function\x94\x93\x94(h\x00\x8c\r_builtin_type\x94\x93\x94\x8c\x08CodeType\x94\x85\x94R\x94(K\x01K\x00K\x00K\x01K\x02K\x13C\n\x88\x00|\x00_\x00d\x00S\x00\x94N\x85\x94\x8c\x03arg\x94\x85\x94\x8c\x04self\x94\x85\x94\x8c"/tmp/monarch_test/class_closure.py\x94\x8c\x08__init__\x94K\tC\x02\n\x01\x94h\x1e\x85\x94)t\x94R\x94}\x94(\x8c\x0b__package__\x94\x8c\x00\x94\x8c\x08__name__\x94h\x0c\x8c\x08__file__\x94h"uNNh\x00\x8c\x10_make_empty_cell\x94\x93\x94)R\x94\x85\x94t\x94R\x94h\x00\x8c\x12_function_setstate\x94\x93\x94h2}\x94}\x94(h+\x8c\x08__init__\x94\x8c\x0c__qualname__\x94\x8c/ClassClosure.closure.<locals>.Internal.__init__\x94\x8c\x0f__annotations__\x94}\x94\x8c\x0e__kwdefaults__\x94N\x8c\x0c__defaults__\x94Nh\x0bh\x0c\x8c\x07__doc__\x94N\x8c\x0b__closure__\x94h\x00\x8c\n_make_cell\x94\x93\x94K\n\x85\x94R\x94\x85\x94\x8c\x17_cloudpickle_submodules\x94]\x94\x8c\x0b__globals__\x94}\x94u\x86\x94\x86R0\x8c\n__module__\x94h\x0c\x8c\x07get_arg\x94h\x16(h\x1b(K\x01K\x00K\x00K\x01K\x01KSC\x0ct\x00\x83\x00\x01\x00|\x00j\x01S\x00\x94h\x1d\x8c\nbreakpoint\x94h\x1e\x86\x94h \x85\x94h"\x8c\x07get_arg\x94K\x0cC\x04\x06\x01\x06\x01\x94))t\x94R\x94h(NNNt\x94R\x94h4hU}\x94}\x94(h+\x8c\x07get_arg\x94h8\x8c.ClassClosure.closure.<locals>.Internal.get_arg\x94h:}\x94h<Nh=Nh\x0bh\x0ch>Nh?NhE]\x94hG}\x94u\x86\x94\x86R0u}\x94\x86\x94\x86R0.'
    # Unpickle `ClassClosure(10).closure()``
    return cloudpickle.loads(pickled)


def load_func_closure():
    pickled = b"\x80\x05\x95\xd9\x02\x00\x00\x00\x00\x00\x00\x8c\x17cloudpickle.cloudpickle\x94\x8c\x0e_make_function\x94\x93\x94(h\x00\x8c\r_builtin_type\x94\x93\x94\x8c\x08CodeType\x94\x85\x94R\x94(K\x01K\x00K\x00K\x01K\x02K\x13C\x18\x88\x01r\x05t\x00\x83\x00\x01\x00|\x00\x83\x00\xa0\x01\xa1\x00\x88\x00\x17\x00S\x00\x94N\x85\x94\x8c\nbreakpoint\x94\x8c\x07get_arg\x94\x86\x94\x8c\x08internal\x94\x85\x94\x8c%/tmp/monarch_test/function_closure.py\x94\x8c\x04func\x94K\x02C\x06\x04\x01\x06\x01\x0e\x01\x94\x8c\x03arg\x94\x8c\x02bp\x94\x86\x94)t\x94R\x94}\x94(\x8c\x0b__package__\x94\x8c\x00\x94\x8c\x08__name__\x94\x8c\x10function_closure\x94\x8c\x08__file__\x94h\x0fuNNh\x00\x8c\x10_make_empty_cell\x94\x93\x94)R\x94h\x1e)R\x94\x86\x94t\x94R\x94h\x00\x8c\x12_function_setstate\x94\x93\x94h#}\x94}\x94(h\x1a\x8c\x04func\x94\x8c\x0c__qualname__\x94\x8c\x1afunc_closure.<locals>.func\x94\x8c\x0f__annotations__\x94}\x94\x8c\x0e__kwdefaults__\x94N\x8c\x0c__defaults__\x94N\x8c\n__module__\x94h\x1b\x8c\x07__doc__\x94N\x8c\x0b__closure__\x94h\x00\x8c\n_make_cell\x94\x93\x94K\x05\x85\x94R\x94h3\x88\x85\x94R\x94\x86\x94\x8c\x17_cloudpickle_submodules\x94]\x94\x8c\x0b__globals__\x94}\x94u\x86\x94\x86R0h\x02(h\x16h\x17NNh\x1e)R\x94h\x1e)R\x94\x86\x94t\x94R\x94h%hB}\x94}\x94(h\x1a\x8c\x04func\x94h)\x8c\x1afunc_closure.<locals>.func\x94h+}\x94h-Nh.Nh/h\x1bh0Nh1h3K\x05\x85\x94R\x94h3\x89\x85\x94R\x94\x86\x94h9]\x94h;}\x94u\x86\x94\x86R0\x86\x94."
    # Unpickle `(func(5, True), func(5, False))`
    return cloudpickle.loads(pickled)


class SourceLoaderControllerWithMockedSource(SourceLoaderController):
    @endpoint
    def get_source(self, filename: str) -> str:
        if filename == "/tmp/monarch_test/class_closure.py":
            return class_closure_source
        elif filename == "/tmp/monarch_test/function_closure.py":
            return function_closure_source
        else:
            raise ValueError(f"Test should not have requested source for {filename}")


class ClosureDebugeeActor(Actor):
    @endpoint
    def debug_class_closure(self, class_closure) -> int:
        return class_closure().get_arg()

    @endpoint
    def debug_func(self, func, class_closure) -> int:
        return func(class_closure)

    @endpoint
    def name(self) -> str:
        return context().actor_instance.actor_id.actor_name


# We have to run this test in a subprocess because it requires a special
# instantiation of the debug controller singleton.
@isolate_in_subprocess(env=debug_env)
@pytest.mark.timeout(60)
async def test_debug_with_pickle_by_value():
    """
    This test tests debugger functionality when there are breakpoints in
    code that has been pickled by value (as opposed to pickling by reference,
    where the pickled representation is essentially just "from <module> import
    <code>"). Cloudpickle will pickle by value for a few reasons, the primary
    among them being:
      - The function, class, etc. was defined in the __main__ module
      - The function, class, etc. is a closure
      - The function is a lambda
    When code that was pickled by value hits a breakpoint, if the original file
    that the code came from doesn't exist on the host, we need to do some special
    handling inside `monarch._src.actor.debugger.pdb_wrapper` to make all the pdb
    commands work as expected.

    For this test, I created two files: /tmp/monarch_test/class_closure.py and
    /tmp/monarch_test/function_closure.py. Their source code is contained in
    the variables `class_closure_source` and `function_closure_source`,
    respectively, above. The functions `load_class_closure` and `load_func_closure`
    above contain `cloudpickle.dumps(ClassClosure(10).closure())`, and
    `cloudpickle.dumps((func(5, True), func(5, False)))`, respectively.

    The test unpickles these and sends them to an actor endpoint, in which
    breakpoints will be hit and we can test the special pdb handling logic.
    """
    pm = proc_mesh(gpus=1, hosts=1)
    debugee = pm.spawn("debugee", ClosureDebugeeActor)
    name = debugee.name.choose().get()

    input_mock = AsyncMock()
    input_mock.side_effect = [
        f"attach {name} 0",
        "c",
        "quit",
        f"attach {name} 0",
        "bt",
        "c",
        "quit",
        f"attach {name} 0",
        "b /tmp/monarch_test/class_closure:10",
        "c",
        "detach",
        "quit",
        f"attach {name} 0",
        "c",
        "detach",
        "quit",
        "c",
        "quit",
    ]

    outputs = []

    def _patch_output(msg):
        nonlocal outputs
        outputs.append(msg)

    output_mock = AsyncMock()
    output_mock.side_effect = _patch_output

    with (
        patch("monarch._src.actor.debugger.debug_io.DebugStdIO.input", new=input_mock),
        patch(
            "monarch._src.actor.debugger.debug_io.DebugStdIO.output", new=output_mock
        ),
    ):
        debug_controller = get_or_spawn_controller(
            "debug_controller", DebugControllerForTesting
        ).get()

        # Spawn a special source loader that knows how to retrieve the source code
        # for /tmp/monarch_test/class_closure.py and
        # /tmp/monarch_test/function_closure.py
        get_or_spawn_controller(
            "source_loader", SourceLoaderControllerWithMockedSource
        ).get()

        class_closure = load_class_closure()
        func_bp_true, func_bp_false = load_func_closure()

        fut = debugee.debug_class_closure.call_one(class_closure)
        breakpoints = await _wait_for_breakpoints(debug_controller, 1)
        assert breakpoints[0].function == "class_closure.get_arg"
        assert breakpoints[0].lineno == 14

        debug_controller.blocking_enter.call_one().get()

        assert (
            "> /tmp/monarch_test/class_closure.py(14)get_arg()\n-> return self.arg"
            in outputs
        )

        await fut

        fut = debugee.debug_func.call_one(func_bp_false, class_closure)
        breakpoints = await _wait_for_breakpoints(debug_controller, 1)
        assert breakpoints[0].function == "class_closure.get_arg"
        assert breakpoints[0].lineno == 14

        debug_controller.blocking_enter.call_one().get()

        expected_backtrace = [
            (
                "  /tmp/monarch_test/function_closure.py(5)func()\n"
                "-> return internal().get_arg() + arg"
            ),
            "\n",
            "> /tmp/monarch_test/class_closure.py(14)get_arg()\n-> return self.arg",
            "\n",
            "(Pdb) ",
        ]
        start = outputs.index(expected_backtrace[0])
        assert expected_backtrace == outputs[start : start + len(expected_backtrace)]  # noqa

        await fut

        fut = debugee.debug_func.call_one(func_bp_true, class_closure)
        breakpoints = await _wait_for_breakpoints(debug_controller, 1)
        assert breakpoints[0].function == "function_closure.func"
        assert breakpoints[0].lineno == 5

        debug_controller.blocking_enter.call_one().get()

        assert (
            "> /tmp/monarch_test/function_closure.py(5)func()\n-> return internal().get_arg() + arg"
            in outputs
        )
        assert "Breakpoint 1 at /tmp/monarch_test/class_closure.py:10" in outputs
        assert (
            "> /tmp/monarch_test/class_closure.py(10)__init__()\n-> self.arg = arg"
            in outputs
        )

        breakpoints = await _wait_for_breakpoints(debug_controller, 1)
        assert breakpoints[0].function == "class_closure.__init__"
        assert breakpoints[0].lineno == 10

        debug_controller.blocking_enter.call_one().get()

        breakpoints = await _wait_for_breakpoints(debug_controller, 1)
        assert breakpoints[0].function == "class_closure.get_arg"
        assert breakpoints[0].lineno == 14

        debug_controller.blocking_enter.call_one().get()

        await _wait_for_breakpoints(debug_controller, 0)

        await fut
        await pm.stop()
