# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import functools
import inspect
import logging
import weakref
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, cast

from monarch._rust_bindings.monarch_hyperactor.logging import log_endpoint_exception
from monarch._src.actor.actor_mesh import ActorError, context
from monarch._src.actor.endpoint import endpoint, EndpointProperty, Propagator
from monarch._src.actor.telemetry import span

_WRAPPER_ATTR = "_monarch_concurrent_endpoint_wrapper"
_CLEANUP_WRAPPER_ATTR = "_monarch_concurrent_endpoint_cleanup_wrapper"
logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class _TaskRecord:
    cancel_for_cleanup: bool = False


@dataclass
class _ConcurrentState:
    records: dict[asyncio.Task[None], _TaskRecord] = field(default_factory=dict)


_states: weakref.WeakKeyDictionary[Any, _ConcurrentState] = weakref.WeakKeyDictionary()


def _state(instance: Any) -> _ConcurrentState:
    state = _states.get(instance)
    if state is None:
        state = _ConcurrentState()
        _states[instance] = state
    return state


def _send_exception(port: Any, actor_error: ActorError) -> None:
    try:
        port.exception(actor_error)
    except Exception:
        pass


def _log_explicit_response_port_exception(
    actor_name: str, method_name: str, exception: BaseException
) -> None:
    logger.warning(
        "concurrent explicit response-port endpoint raised without forwarding its own exception: %s.%s",
        actor_name,
        method_name,
        exc_info=(type(exception), exception, exception.__traceback__),
    )


async def _run_endpoint(
    actor_instance: Any,
    port: Any,
    call: Callable[[], Awaitable[None]],
    *,
    method_name: str,
    should_instrument: bool,
    forwards_exception: bool,
    record: _TaskRecord | None = None,
) -> None:
    actor_name = actor_instance.name
    actor_id = actor_instance.actor_id
    token = actor_instance._execution_start(method_name)
    try:
        try:
            if should_instrument:
                with span(method_name):
                    await call()
            else:
                await call()
        except asyncio.CancelledError as e:
            if record is not None and record.cancel_for_cleanup:
                return
            if forwards_exception:
                actor_error = ActorError(
                    e,
                    f"Actor call {actor_name}.{method_name} failed with BaseException.",
                )
                _send_exception(port, actor_error)
            raise
        except Exception as e:
            if forwards_exception:
                log_endpoint_exception(e, method_name, actor_id)
                actor_error = ActorError(
                    e,
                    f"Actor call {actor_name}.{method_name} failed.",
                )
                _send_exception(port, actor_error)
            else:
                _log_explicit_response_port_exception(actor_name, method_name, e)
        except BaseException as e:  # noqa: B036
            actor_error = ActorError(
                e,
                f"Actor call {actor_name}.{method_name} failed with BaseException.",
            )
            if forwards_exception:
                _send_exception(port, actor_error)
            else:
                _log_explicit_response_port_exception(actor_name, method_name, e)
    finally:
        actor_instance._execution_finish(token)


def _spawn_task(
    instance: Any,
    port: Any,
    call: Callable[[], Awaitable[None]],
    *,
    method_name: str,
    should_instrument: bool,
    forwards_exception: bool,
) -> None:
    _install_cleanup_wrapper(instance)
    state = _state(instance)
    record = _TaskRecord()
    actor_instance = context().actor_instance
    task = asyncio.create_task(
        _run_endpoint(
            actor_instance,
            port,
            call,
            method_name=method_name,
            should_instrument=should_instrument,
            forwards_exception=forwards_exception,
            record=record,
        )
    )
    state.records[task] = record

    def finish_task(done: asyncio.Task[None]) -> None:
        state.records.pop(done, None)

    task.add_done_callback(finish_task)


async def _cancel_tasks(instance: Any) -> None:
    state = _states.get(instance)
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


def _has_attr(method: Any, name: str) -> bool:
    return bool(getattr(method, name, False))


def _install_cleanup_wrapper(instance: Any) -> None:
    cleanup = getattr(instance, "__cleanup__", None)
    if _has_attr(cleanup, _CLEANUP_WRAPPER_ATTR):
        return

    cleanup_fn: Callable[[Exception | None], Awaitable[None]] | None = None
    if cleanup is not None and inspect.iscoroutinefunction(cleanup):
        cleanup_fn = cast(Callable[[Exception | None], Awaitable[None]], cleanup)

    async def cleanup_wrapper(exc: Exception | None) -> None:
        await _cancel_tasks(instance)
        if cleanup_fn is not None:
            await cleanup_fn(exc)

    setattr(cleanup_wrapper, _CLEANUP_WRAPPER_ATTR, True)
    instance.__cleanup__ = cleanup_wrapper


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


class _ConcurrentEndpointProperty(EndpointProperty[Any, Any]):
    def __init__(
        self,
        endpoint_property: EndpointProperty[Any, Any],
    ) -> None:
        self._name = endpoint_property._method.__name__
        method = endpoint_property._method
        has_explicit_response_port = endpoint_property._explicit_response_port

        @functools.wraps(method)
        async def wrapper(
            actor_self: Any,
            port: Any,
            *args: Any,
            **kwargs: Any,
        ) -> None:
            async def call() -> None:
                if has_explicit_response_port:
                    await method(actor_self, port, *args, **kwargs)
                else:
                    port.send(await method(actor_self, *args, **kwargs))

            _spawn_task(
                actor_self,
                port,
                call,
                method_name=self._name,
                should_instrument=endpoint_property._instrument,
                forwards_exception=not has_explicit_response_port,
            )

        # pyre-ignore[16]: function attributes are dynamic
        wrapper.__signature__ = _explicit_response_signature(
            method, already_explicit=has_explicit_response_port
        )
        setattr(wrapper, _WRAPPER_ATTR, True)
        super().__init__(
            wrapper,
            propagator=endpoint_property._propagator,
            explicit_response_port=True,
            instrument=False,
        )

    def __set_name__(self, owner: Any, name: str) -> None:
        self._name = name


def _wrap_endpoint(
    endpoint_property: EndpointProperty[Any, Any],
) -> EndpointProperty[Any, Any]:
    method = endpoint_property._method
    if getattr(method, _WRAPPER_ATTR, False):
        return endpoint_property
    if not inspect.iscoroutinefunction(method):
        raise ValueError("concurrent_endpoint can only wrap async endpoints")
    return _ConcurrentEndpointProperty(endpoint_property)


def concurrent_endpoint(
    method: Any = None,
    *,
    propagate: Propagator = None,
    explicit_response_port: bool = False,
    instrument: bool = True,
) -> Any:
    """Run one async actor endpoint as an ``asyncio`` background task.

    The decorator accepts the same endpoint options as ``@endpoint``. It
    rewrites the endpoint to use an explicit response port, schedules the
    original async body as a task, and returns immediately so that the actor can
    handle another queued message.

    If two messages from the same source actor call ``@concurrent_endpoint``
    methods on the same target actor, the first endpoint body starts before the
    second. This is only a start-order guarantee: the first endpoint runs until
    its first ``await``, not to completion, before the second starts.

    If you mix ``@concurrent_endpoint`` with normal ``@endpoint`` methods, a
    normal endpoint that follows a concurrent endpoint may run before the
    concurrent endpoint body has started.

    Outstanding tasks created by this decorator are cancelled and awaited
    before user ``__cleanup__`` runs. This is a cleanup-time guarantee only:
    meshes owned by the actor may already have been stopped by the core actor
    lifecycle before these tasks are cancelled. General pending tasks on the
    actor's asyncio loop are cancelled later, after user ``__cleanup__``
    completes.

    More complex protocols can still use ``@endpoint(explicit_response_port=True)``
    and manage response ports manually.
    """
    if method is None:
        return functools.partial(
            concurrent_endpoint,
            propagate=propagate,
            explicit_response_port=explicit_response_port,
            instrument=instrument,
        )
    if isinstance(method, EndpointProperty):
        raise ValueError(
            "concurrent_endpoint does not wrap @endpoint; pass endpoint options to @concurrent_endpoint"
        )
    endpoint_property = cast(
        EndpointProperty[Any, Any],
        endpoint(
            method,
            propagate=propagate,
            explicit_response_port=explicit_response_port,
            instrument=instrument,
        ),
    )
    return _wrap_endpoint(endpoint_property)
