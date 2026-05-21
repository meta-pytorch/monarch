# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import functools
import inspect
import logging
import warnings
from typing import Callable, Dict, Optional, overload, Sequence, TypeVar

import opentelemetry.metrics as metrics  # @manual=fbsource//third-party/pypi/opentelemetry-api:opentelemetry-api
import opentelemetry.trace as trace  # @manual=fbsource//third-party/pypi/opentelemetry-api:opentelemetry-api
from monarch._rust_bindings.monarch_hyperactor.proc import ActorAddr
from monarch._rust_bindings.monarch_hyperactor.telemetry import (  # @manual=//monarch/monarch_extension:monarch_extension
    forward_to_tracing,
    PyCounter,
    PyHistogram,
    PySpan,
    PyUpDownCounter,
)
from opentelemetry.context import Context
from opentelemetry.metrics import CallbackT
from opentelemetry.util.types import Attributes
from typing_extensions import ParamSpec


def _current_actor_id() -> ActorAddr | None:
    from monarch._src.actor.actor_mesh import _context

    ctx = _context.get(None)
    return None if ctx is None else ctx.actor_instance.actor_id


def span(name: str) -> PySpan:
    return PySpan(name, _current_actor_id())


_P = ParamSpec("_P")
_R = TypeVar("_R")


@overload
def traced(fn: Callable[_P, _R]) -> Callable[_P, _R]: ...


@overload
def traced(*, name: str) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]: ...


def traced(
    fn: Callable[_P, _R] | None = None, *, name: str | None = None
) -> Callable[_P, _R] | Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Decorator that wraps a function in a telemetry span.

    Works with both sync and async functions. The span is automatically
    associated with the current actor context, if any. When no name is
    provided, the function's ``__name__`` is used as the span name.

    Usage::

        @traced
        async def do_work():
            ...

        @traced(name="custom_name")
        def compute():
            ...
    """

    def decorator(fn: Callable[_P, _R]) -> Callable[_P, _R]:
        span_name: str = name if name is not None else fn.__name__

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            # pyrefly: ignore [bad-return]
            async def async_wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
                with span(span_name):
                    return await fn(*args, **kwargs)  # type: ignore[misc]

            return async_wrapper  # type: ignore[return-value]
        else:

            @functools.wraps(fn)
            # pyrefly: ignore [bad-return]
            def sync_wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
                with span(span_name):
                    return fn(*args, **kwargs)

            return sync_wrapper

    if fn is not None:
        return decorator(fn)
    return decorator


def log_with_tracing(
    level: int,
    msg: object,
    *args: object,
    stack_info: bool = False,
    stacklevel: int = 1,
    extra: Dict[str, object] | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Emit a log record through the normal `logging` handler chain and also
    forward it directly to Rust tracing.

    Use this in library code that must guarantee the event reaches the
    tracing backend even when no `TracingForwarder` handler is attached to
    the calling process's loggers (e.g., a non-actor driver process).
    `stacklevel=1` attributes the record's pathname/lineno/funcName to the
    immediate caller of `log_with_tracing`.
    """
    logger = logger or logging.getLogger(__name__)

    fn, lno, func, sinfo = logger.findCaller(
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
    )

    record = logger.makeRecord(
        logger.name,
        level,
        fn,
        lno,
        msg,
        args,
        None,
        func,
        extra=extra,
        sinfo=sinfo,
    )
    logger.handle(record)
    forward_to_tracing(record)


class TracingForwarder(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        # Try to add actor_id from the current context to the logging record
        try:
            actor_id = _current_actor_id()
            if actor_id is not None:
                # Add actor_id as an attribute to the logging record
                record.actor_id = str(actor_id)  # type: ignore[attr-defined]
        except Exception:
            # If we can't get the context or actor_id for any reason, just continue
            # without adding the actor_id field
            pass

        forward_to_tracing(record)


class Counter(metrics.Counter):
    inner: PyCounter

    def __init__(self, name: str) -> None:
        # pyrefly: ignore [missing-attribute]
        super().__init__(name)
        self.inner = PyCounter(name)

    def add(
        self,
        amount: int | float,
        attributes: Optional[Attributes] = None,
        context: Optional[Context] = None,
    ) -> None:
        rust_attributes = None
        if attributes:
            rust_attributes = {str(k): str(v) for k, v in attributes.items()}
        return self.inner.add(int(amount), attributes=rust_attributes)


class UpDownCounter(metrics.UpDownCounter):
    inner: PyUpDownCounter

    def __init__(self, name: str) -> None:
        # pyrefly: ignore [missing-attribute]
        super().__init__(name)
        self.inner = PyUpDownCounter(name)

    def add(
        self,
        amount: int | float,
        attributes: Optional[Attributes] = None,
        context: Optional[Context] = None,
    ) -> None:
        rust_attributes = None
        if attributes:
            rust_attributes = {str(k): str(v) for k, v in attributes.items()}
        self.inner.add(int(amount), attributes=rust_attributes)


class Histogram(metrics.Histogram):
    inner: PyHistogram

    def __init__(self, name: str) -> None:
        # pyrefly: ignore [missing-attribute]
        super().__init__(name)
        self.inner = PyHistogram(name)

    def record(
        self,
        amount: int | float,
        attributes: Optional[Attributes] = None,
        context: Optional[Context] = None,
    ) -> None:
        rust_attributes = None
        if attributes:
            rust_attributes = {str(k): str(v) for k, v in attributes.items()}
        self.inner.record(amount, attributes=rust_attributes)


class Meter(metrics.Meter):
    def create_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> metrics.Counter:
        return Counter(name)

    def create_up_down_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> metrics.UpDownCounter:
        return UpDownCounter(name)

    def create_observable_counter(
        self,
        name: str,
        callbacks: Optional[Sequence[CallbackT]] = None,
        unit: str = "",
        description: str = "",
    ) -> metrics.ObservableCounter:
        raise NotImplementedError()

    def create_histogram(
        self,
        name: str,
        unit: str = "",
        description: str = "",
        *,
        explicit_bucket_boundaries_advisory: Optional[Sequence[float]] = None,
    ) -> metrics.Histogram:
        return Histogram(name)

    def create_gauge(  # type: ignore # pylint: disable=no-self-use
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> metrics._Gauge:  # pyright: ignore[reportReturnType]
        warnings.warn(
            "create_gauge() is not implemented and will be a no-op", stacklevel=2
        )
        raise NotImplementedError()

    def create_observable_gauge(
        self,
        name: str,
        callbacks: Optional[Sequence[CallbackT]] = None,
        unit: str = "",
        description: str = "",
    ) -> metrics.ObservableGauge:
        raise NotImplementedError()

    def create_observable_up_down_counter(
        self,
        name: str,
        callbacks: Optional[Sequence[CallbackT]] = None,
        unit: str = "",
        description: str = "",
    ) -> metrics.ObservableUpDownCounter:
        raise NotImplementedError()


class MeterProvider(metrics.MeterProvider):
    def get_meter(
        self,
        name: str,
        version: Optional[str] = None,
        schema_url: Optional[str] = None,
        attributes: Optional[Attributes] = None,
    ) -> metrics.Meter:
        return Meter(name, version, schema_url)


_TRACER: trace.Tracer = trace.NoOpTracer()


def get_monarch_tracer() -> trace.Tracer:
    """
    Return a no-op OTEL tracer for compatibility with older call sites.
    Prefer `span()` for new code.
    """

    return _TRACER


_INSTALLED = False

METER: metrics.Meter = Meter("monarch")


def install() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    metrics.set_meter_provider(MeterProvider())

    global METER
    METER = metrics.get_meter("monarch")
    _INSTALLED = True
