# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import functools
import time
from abc import ABC, abstractmethod
from typing import (
    Any,
    Awaitable,
    Callable,
    cast,
    Concatenate,
    Coroutine,
    Dict,
    Generator,
    Generic,
    List,
    Literal,
    Optional,
    overload,
    ParamSpec,
    Tuple,
    TYPE_CHECKING,
    TypeVar,
    Union,
)

from monarch._rust_bindings.monarch_hyperactor.actor import MethodSpecifier
from monarch._rust_bindings.monarch_hyperactor.shape import Extent

from monarch._src.actor.future import Future
from monarch._src.actor.telemetry import METER
from monarch._src.actor.tensor_engine_shim import _cached_propagation, fake_call

from opentelemetry.metrics import Histogram

# Histogram for measuring endpoint call latency
endpoint_call_latency_histogram: Histogram = METER.create_histogram(
    name="endpoint_call_latency.us",
    description="Latency of endpoint call operations in microseconds",
)

# Histogram for measuring endpoint call_one latency
endpoint_call_one_latency_histogram: Histogram = METER.create_histogram(
    name="endpoint_call_one_latency.us",
    description="Latency of endpoint call_one operations in microseconds",
)

# Histogram for measuring endpoint stream latency per yield
endpoint_stream_latency_histogram: Histogram = METER.create_histogram(
    name="endpoint_stream_latency.us",
    description="Latency of endpoint stream operations per yield in microseconds",
)

T = TypeVar("T")


def _measure_latency(
    coro: Coroutine[Any, Any, T],
    start_time: int,
    histogram: Histogram,
    method_name: str,
    actor_count: int,
) -> Coroutine[Any, Any, T]:
    """
    Decorator to measure and record latency of an async operation.

    Args:
        coro: The coroutine to measure
        histogram: The histogram to record metrics to
        method_name: Name of the method being called
        actor_count: Number of actors involved in the call

    Returns:
        A wrapped coroutine that records latency metrics
    """

    async def _wrapper() -> T:
        try:
            return await coro
        finally:
            duration_us = int((time.monotonic_ns() - start_time) / 1_000)
            histogram.record(
                duration_us,
                attributes={
                    "method": method_name,
                    "actor_count": actor_count,
                },
            )

    return _wrapper()


if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.mailbox import (
        OncePortReceiver as HyOncePortReceiver,
        PortReceiver as HyPortReceiver,
    )
    from monarch._src.actor.actor_mesh import ActorMesh, Port, PortReceiver, ValueMesh

P = ParamSpec("P")
R = TypeVar("R")

Selection = Literal["all", "choose"]


Propagator = Union[None, Literal["cached", "inspect", "mocked"], Callable[..., Any]]


class Endpoint(ABC, Generic[P, R]):
    def __init__(self, propagator: Propagator) -> None:
        self._propagator_arg = propagator
        self._cache: Optional[Dict[Any, Any]] = None

    def _get_method_name(self) -> str:
        """
        Extract method name from this endpoint's method specifier.

        Returns:
            The method name, or "unknown" if not available
        """
        method_specifier = self._call_name()
        if hasattr(method_specifier, "name"):
            # pyre-ignore[16]: MethodSpecifier subclasses ReturnsResponse and ExplicitPort have .name
            return method_specifier.name
        return "unknown"

    @abstractmethod
    def _send(
        self,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        port: "Optional[Port[R]]" = None,
        selection: Selection = "all",
    ) -> Extent:
        """
        Implements sending a message to the endpoint. The return value of the endpoint will
        be sent to port if provided. If port is not provided, the return will be dropped,
        and any exception will cause the actor to fail.

        The return value is the (multi-dimension) size of the actors that were sent a message.
        For ActorEndpoints this will be the actor_meshes size. For free-function endpoints,
        this will be the size of the currently active proc_mesh.
        """
        pass

    def _port(self, once: bool = False) -> "Tuple[Port[R], PortReceiver[R]]":
        from monarch._src.actor.actor_mesh import Channel

        return Channel[R].open(once)

    @abstractmethod
    def _call_name(self) -> MethodSpecifier:
        """
        Something to use in InputChecker to represent calling this thingy.
        """
        pass

    def _supervise(
        self, r: "HyPortReceiver | HyOncePortReceiver"
    ) -> "HyPortReceiver | HyOncePortReceiver":
        return r

    # the following are all 'adverbs' or different ways to handle the
    # return values of this endpoint. Adverbs should only ever take *args, **kwargs
    # of the original call. If we want to add syntax sugar for something that needs additional
    # arguments, it should be implemented as function indepdendent of endpoint like `send`
    # and `Accumulator`
    def choose(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        """
        Load balanced sends a message to one chosen actor and awaits a result.

        Load balanced RPC-style entrypoint for request/response messaging.
        """

        p, r = self._port(once=True)
        # pyre-ignore[6]: ParamSpec kwargs is compatible with Dict[str, Any]
        self._send(args, kwargs, port=p, selection="choose")
        return r.recv()

    def call_one(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        p, r_port = self._port(once=True)
        r: PortReceiver[R] = r_port
        start_time: int = time.monotonic_ns()
        # pyre-ignore[6]: ParamSpec kwargs is compatible with Dict[str, Any]
        extent = self._send(args, kwargs, port=p, selection="choose")
        if extent.nelements != 1:
            raise ValueError(
                f"Can only use 'call_one' on a single Actor but this actor has shape {extent}"
            )

        method_name = self._get_method_name()

        async def process() -> R:
            result = await r.recv()
            return result

        measured_coro = _measure_latency(
            process(),
            start_time,
            endpoint_call_one_latency_histogram,
            method_name,
            1,
        )
        return Future(coro=measured_coro)

    def call(self, *args: P.args, **kwargs: P.kwargs) -> "Future[ValueMesh[R]]":
        from monarch._src.actor.actor_mesh import RankedPortReceiver, ValueMesh

        p, unranked = self._port()
        r: RankedPortReceiver[R] = unranked.ranked()
        start_time: int = time.monotonic_ns()
        # pyre-ignore[6]: ParamSpec kwargs is compatible with Dict[str, Any]
        extent: Extent = self._send(args, kwargs, port=p)

        method_name = self._get_method_name()

        async def process() -> "ValueMesh[R]":
            from monarch._rust_bindings.monarch_hyperactor.shape import Shape
            from monarch._src.actor.shape import NDSlice

            results: List[R] = [None] * extent.nelements  # pyre-fixme[9]
            for _ in range(extent.nelements):
                rank, value = await r._recv()
                results[rank] = value
            call_shape = Shape(
                extent.labels,
                NDSlice.new_row_major(extent.sizes),
            )
            return ValueMesh(call_shape, results)

        measured_coro = _measure_latency(
            process(),
            start_time,
            endpoint_call_latency_histogram,
            method_name,
            extent.nelements,
        )
        return Future(coro=measured_coro)

    def stream(
        self, *args: P.args, **kwargs: P.kwargs
    ) -> Generator[Future[R], None, None]:
        """
        Broadcasts to all actors and yields their responses as a stream / generator.

        This enables processing results from multiple actors incrementally as
        they become available. Returns an async generator of response values.
        """
        p, r_port = self._port()
        start_time: int = time.monotonic_ns()
        # pyre-ignore[6]: ParamSpec kwargs is compatible with Dict[str, Any]
        extent: Extent = self._send(args, kwargs, port=p)
        r: "PortReceiver[R]" = r_port

        method_name: str = self._get_method_name()

        def _stream() -> Generator[Future[R], None, None]:
            for _ in range(extent.nelements):
                measured_coro = _measure_latency(
                    r._recv(),
                    start_time,
                    endpoint_stream_latency_histogram,
                    method_name,
                    extent.nelements,
                )
                yield Future(coro=measured_coro)

        return _stream()

    def broadcast(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """
        Fire-and-forget broadcast to all actors without waiting for actors to
        acknowledge receipt.

        In other words, the return of this method does not guarrantee the
        delivery of the message.
        """
        from monarch._src.actor.actor_mesh import send

        # pyre-ignore[6]: ParamSpec kwargs is compatible with Dict[str, Any]
        send(self, args, kwargs)

    @abstractmethod
    def _rref(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> R: ...

    def rref(self, *args: P.args, **kwargs: P.kwargs) -> R:
        # pyre-ignore[6]: ParamSpec kwargs is compatible with Dict[str, Any]
        return self._rref(args, kwargs)

    def _propagate(
        self,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        fake_args: Tuple[Any, ...],
        fake_kwargs: Dict[str, Any],
    ) -> Any:
        if self._propagator_arg is None or self._propagator_arg == "cached":
            if self._cache is None:
                self._cache = {}
            resolvable = getattr(self, "_resolvable", None)
            if resolvable is None:
                raise NotImplementedError(
                    "Cached propagation is not implemented for actor endpoints."
                )
            return _cached_propagation(self._cache, resolvable, args, kwargs)
        elif self._propagator_arg == "inspect":
            return None
        elif self._propagator_arg == "mocked":
            raise NotImplementedError("mocked propagation")
        else:
            return fake_call(self._propagator_arg, *fake_args, **fake_kwargs)

    def _fetch_propagate(
        self,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        fake_args: Tuple[Any, ...],
        fake_kwargs: Dict[str, Any],
    ) -> Any:
        if self._propagator_arg is None:
            return  # no propgator provided, so we just assume no mutations
        return self._propagate(args, kwargs, fake_args, fake_kwargs)

    def _pipe_propagate(
        self,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        fake_args: Tuple[Any, ...],
        fake_kwargs: Dict[str, Any],
    ) -> Any:
        if not callable(self._propagator_arg):
            raise ValueError("Must specify explicit callable for pipe")
        return self._propagate(args, kwargs, fake_args, fake_kwargs)


class EndpointProperty(Generic[P, R]):
    @overload
    def __init__(
        self,
        method: Callable[Concatenate[Any, P], Awaitable[R]],
        propagator: Propagator,
        explicit_response_port: bool,
        instrument: bool = False,
    ) -> None: ...

    @overload
    def __init__(
        self,
        method: Callable[Concatenate[Any, P], R],
        propagator: Propagator,
        explicit_response_port: bool,
        instrument: bool = False,
    ) -> None: ...

    def __init__(
        self,
        method: Any,
        propagator: Propagator,
        explicit_response_port: bool,
        instrument: bool = False,
    ) -> None:
        self._method = method
        self._propagator = propagator
        self._explicit_response_port = explicit_response_port
        self._instrument = instrument

    def __get__(self, instance: Any, owner: Any) -> Endpoint[P, R]:
        # this is a total lie, but we have to actually
        # recognize this was defined as an endpoint,
        # and also lookup the method
        return cast(Endpoint[P, R], self)


class NotAnEndpoint:
    """
    Used as the dynamic value of functions on an ActorMesh that were not marked as endpoints.
    This is used both to give a better error message (since we cannot prevent the type system from thinking they are methods),
    and to provide the oppurtunity for someone to do endpoint(x.foo) on something that wasn't marked as an endpoint.
    """

    def __init__(self, ref: "ActorMesh[Any]", name: str) -> None:
        self._ref = ref
        self._name = name

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError(
            f"Actor {self._ref._class}.{self._name} is not annotated as an endpoint. To call it as one, add a @endpoint decorator to it, or directly wrap it in one as_endpoint(obj.method).call(...)"
        )


# This can't just be Callable because otherwise we are not
# allowed to use type arguments in the return value.
class EndpointIfy:
    @overload
    def __call__(
        self, function: Callable[Concatenate[Any, P], Awaitable[R]]
    ) -> Endpoint[P, R]: ...

    @overload
    def __call__(
        self, function: Callable[Concatenate[Any, P], R]
    ) -> Endpoint[P, R]: ...

    def __call__(self, function: Any) -> Any:
        pass


class PortedEndpointIfy:
    @overload
    def __call__(
        self,
        function: Callable[Concatenate[Any, "Port[R]", P], Awaitable[None]],
    ) -> Endpoint[P, R]: ...

    @overload
    def __call__(
        self, function: Callable[Concatenate[Any, "Port[R]", P], None]
    ) -> Endpoint[P, R]: ...

    def __call__(self, function: Any) -> Any:
        pass


@overload
def endpoint(
    method: Callable[Concatenate[Any, P], Awaitable[R]],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[False] = False,
    instrument: bool = False,
) -> EndpointProperty[P, R]: ...


@overload
def endpoint(
    method: Callable[Concatenate[Any, P], R],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[False] = False,
    instrument: bool = False,
) -> EndpointProperty[P, R]: ...


@overload
def endpoint(
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[False] = False,
    instrument: bool = False,
) -> EndpointIfy: ...


@overload
def endpoint(
    method: Callable[Concatenate[Any, "Port[R]", P], Awaitable[None]],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[True],
    instrument: bool = False,
) -> EndpointProperty[P, R]: ...


@overload
def endpoint(
    method: Callable[Concatenate[Any, "Port[R]", P], None],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[True],
    instrument: bool = False,
) -> EndpointProperty[P, R]: ...


@overload
def endpoint(
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[True],
    instrument: bool = False,
) -> PortedEndpointIfy: ...


def endpoint(
    method: Any = None,
    *,
    propagate: Any = None,
    explicit_response_port: bool = False,
    instrument: bool = False,
) -> Any:
    if method is None:
        return functools.partial(
            endpoint,
            propagate=propagate,
            explicit_response_port=explicit_response_port,
            instrument=instrument,
        )
    return EndpointProperty(
        method,
        propagator=propagate,
        explicit_response_port=explicit_response_port,
        instrument=instrument,
    )
