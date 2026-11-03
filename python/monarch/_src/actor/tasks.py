# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Actor-owned tasks shared by endpoint and raw protocol libraries."""

import asyncio
import weakref
from dataclasses import dataclass, field
from traceback import TracebackException
from typing import Any, Coroutine, TypeVar

T = TypeVar("T")


@dataclass
class _TaskRecord:
    cancel_for_cleanup: bool = False


@dataclass
class _TaskState:
    records: dict[asyncio.Task[Any], _TaskRecord] = field(default_factory=dict)


_states: weakref.WeakKeyDictionary[Any, _TaskState] = weakref.WeakKeyDictionary()


def _fail_actor(actor_instance: Any, error: BaseException) -> None:
    reason = "".join(TracebackException.from_exception(error).format())
    try:
        actor_instance.kill(reason)
    except Exception:
        pass


def spawn_actor_task(
    actor: Any,
    coroutine: Coroutine[Any, Any, T],
    *,
    name: str | None = None,
) -> asyncio.Task[T]:
    """Own a task until completion and cancel/await it before user cleanup.

    Uncaught errors fail the actor through supervision. Protocols may handle
    errors and explicitly send replies. Tasks interleave state updates at awaits.
    This requires the actor's running asyncio loop; sync actors have no such loop.
    """
    return _spawn_actor_task(actor, coroutine, name=name)


def _spawn_actor_task(
    actor: Any,
    coroutine: Coroutine[Any, Any, T],
    *,
    name: str | None = None,
    _record: _TaskRecord | None = None,
    _instrument: bool = True,
) -> asyncio.Task[T]:
    from monarch._src.actor.actor_mesh import context

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        coroutine.close()
        raise
    state = _states.setdefault(actor, _TaskState())
    record = _TaskRecord() if _record is None else _record
    instance = context().actor_instance
    started = False

    async def run() -> T:
        nonlocal started
        started = True
        token = (
            instance._execution_start(name or coroutine.__qualname__)
            if _instrument
            else 0
        )
        try:
            return await coroutine
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            if not record.cancel_for_cleanup:
                _fail_actor(instance, error)
            raise
        finally:
            instance._execution_finish(token)

    owned = run()
    try:
        task = loop.create_task(owned, name=name)
    except BaseException:
        owned.close()
        coroutine.close()
        raise
    state.records[task] = record

    def finish(done: asyncio.Task[T]) -> None:
        state.records.pop(done, None)
        if not started:
            coroutine.close()
        if not done.cancelled():
            done.exception()

    task.add_done_callback(finish)
    return task


async def _cancel_tasks(actor: Any) -> None:
    state = _states.get(actor)
    if state is None:
        return
    while state.records:
        tasks = tuple(state.records)
        for task in tasks:
            record = state.records.get(task)
            if record is not None and not task.done():
                record.cancel_for_cleanup = True
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
