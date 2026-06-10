# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import functools
import inspect
from typing import Any, Awaitable, Callable, cast, Coroutine, Type, TypeVar

from monarch._src.actor.endpoint import EndpointProperty

ActorClass = TypeVar("ActorClass", bound=Type[Any])
_TASKS_ATTR = "_monarch_concurrent_actor_tasks"
_WRAPPER_ATTR = "_monarch_concurrent_actor_wrapper"


def _is_user_override(method: Any) -> bool:
    return method is not None and not getattr(method, "_monarch_doc_stub", False)


def _tasks(instance: Any) -> set[asyncio.Task[None]]:
    tasks = instance.__dict__.setdefault(_TASKS_ATTR, set())
    return cast(set[asyncio.Task[None]], tasks)


def _spawn_task(instance: Any, coro: Coroutine[Any, Any, None]) -> None:
    tasks = _tasks(instance)
    task = asyncio.create_task(coro)
    tasks.add(task)

    def discard_task(done: asyncio.Task[None]) -> None:
        tasks.discard(done)

    task.add_done_callback(discard_task)


async def _await_tasks(instance: Any) -> None:
    tasks = _tasks(instance)
    while tasks:
        await asyncio.gather(*tuple(tasks))


def _explicit_response_signature(
    method: Callable[..., Any], *, already_explicit: bool
) -> inspect.Signature:
    signature = inspect.signature(method)
    if already_explicit:
        return signature

    parameters = list(signature.parameters.values())
    if not parameters:
        return signature

    name = "_monarch_response_port"
    while name in signature.parameters:
        name = f"{name}_"

    port_kind = inspect.Parameter.POSITIONAL_OR_KEYWORD
    if any(
        parameter.kind is inspect.Parameter.POSITIONAL_ONLY
        for parameter in parameters[1:]
    ):
        port_kind = inspect.Parameter.POSITIONAL_ONLY

    return signature.replace(
        parameters=[
            parameters[0],
            inspect.Parameter(name, port_kind),
            *parameters[1:],
        ],
        return_annotation=None,
    )


def _wrap_endpoint(
    endpoint_property: EndpointProperty[Any, Any],
) -> EndpointProperty[Any, Any]:
    method = endpoint_property._method
    if getattr(method, _WRAPPER_ATTR, False):
        return endpoint_property
    if not inspect.iscoroutinefunction(method):
        raise ValueError("concurrent_actor can only wrap async endpoints")

    wrapper: Callable[..., Awaitable[None]]
    if endpoint_property._explicit_response_port:

        @functools.wraps(method)
        async def explicit_port_wrapper(
            self: Any, port: Any, *args: Any, **kwargs: Any
        ) -> None:
            async def run() -> None:
                try:
                    await method(self, port, *args, **kwargs)
                except Exception as e:
                    port.exception(e)

            _spawn_task(self, run())

        wrapper = explicit_port_wrapper
    else:

        @functools.wraps(method)
        async def returns_response_wrapper(
            self: Any, port: Any, *args: Any, **kwargs: Any
        ) -> None:
            async def run() -> None:
                try:
                    port.send(await method(self, *args, **kwargs))
                except Exception as e:
                    port.exception(e)

            _spawn_task(self, run())

        wrapper = returns_response_wrapper

    # pyre-ignore[16]: function attributes are dynamic
    wrapper.__signature__ = _explicit_response_signature(
        method, already_explicit=endpoint_property._explicit_response_port
    )
    setattr(wrapper, _WRAPPER_ATTR, True)
    return EndpointProperty(
        wrapper,
        propagator=endpoint_property._propagator,
        explicit_response_port=True,
        instrument=endpoint_property._instrument,
    )


def _wrap_cleanup(Class: Type[Any]) -> None:
    cleanup = getattr(Class, "__cleanup__", None)
    cleanup_fn: Callable[[Any, Exception | None], Awaitable[None]] | None = None
    if _is_user_override(cleanup):
        if not inspect.iscoroutinefunction(cleanup):
            raise ValueError("concurrent_actor requires async __cleanup__")
        cleanup_fn = cast(
            Callable[[Any, Exception | None], Awaitable[None]],
            cleanup,
        )

    async def cleanup_wrapper(self: Any, exc: Exception | None) -> None:
        await _await_tasks(self)
        if cleanup_fn is not None:
            await cleanup_fn(self, exc)

    Class.__cleanup__ = cleanup_wrapper


def concurrent_actor(Class: ActorClass) -> ActorClass:
    """Run every async endpoint on ``Class`` as an ``asyncio`` background task.

    The decorator rewrites endpoints to use explicit response ports. Each
    endpoint wrapper schedules the original async body as a task, forwards the
    result or exception through the response port, and returns immediately so
    that the next queued message can start.
    """
    for attr_name in dir(Class):
        attr_value = getattr(Class, attr_name, None)
        if isinstance(attr_value, EndpointProperty):
            setattr(Class, attr_name, _wrap_endpoint(attr_value))
    _wrap_cleanup(Class)
    return Class
