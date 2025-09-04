# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import functools
from abc import ABC, abstractmethod
from operator import mul
from typing import (
    Any,
    Awaitable,
    Callable,
    cast,
    Concatenate,
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
)

from monarch._rust_bindings.monarch_hyperactor.shape import Extent

from monarch._src.actor.future import Future
from monarch._src.actor.tensor_engine_shim import _cached_propagation, fake_call

if TYPE_CHECKING:
    from monarch._src.actor.actor_mesh import (
        ActorMesh,
        HyOncePortReceiver,
        HyPortReceiver,
        Port,
        PortReceiver,
        ValueMesh,
    )

P = ParamSpec("P")
R = TypeVar("R")

Selection = Literal["all", "choose"]


Propagator = Any


class Endpoint(ABC, Generic[P, R]):
    def __init__(self, propagator: Propagator) -> None:
        self._propagator_arg = propagator
        self._cache: Optional[dict] = None

    @abstractmethod
    def _send(
        self,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        port: "Optional[Port]" = None,
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
    def _call_name(self) -> Any:
        """
        Something to use in InputChecker to represent calling this thingy.
        """
        pass

    def _supervise(self, r: "HyPortReceiver | HyOncePortReceiver") -> Any:
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
        # pyre-ignore
        self._send(args, kwargs, port=p, selection="choose")
        return r.recv()

    def call_one(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        p, r = self._port(once=True)
        # pyre-ignore
        extent = self._send(args, kwargs, port=p, selection="choose")
        if extent.nelements != 1:
            raise ValueError(
                f"Can only use 'call_one' on a single Actor but this actor has shape {extent}"
            )
        return r.recv()

    def call(self, *args: P.args, **kwargs: P.kwargs) -> "Future[ValueMesh[R]]":
        from monarch._src.actor.actor_mesh import ValueMesh

        p, unranked = self._port()
        r = unranked.ranked()
        # pyre-ignore
        extent = self._send(args, kwargs, port=p)

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

        return Future(coro=process())

    def stream(
        self, *args: P.args, **kwargs: P.kwargs
    ) -> Generator[Future[R], None, None]:
        """
        Broadcasts to all actors and yields their responses as a stream / generator.

        This enables processing results from multiple actors incrementally as
        they become available. Returns an async generator of response values.
        """
        p, r = self._port()
        # type: ignore
        extent = self._send(args, kwargs, port=p)

        def _stream():
            for _ in range(extent.nelements):
                yield r.recv()

        return _stream()

    def broadcast(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """
        Fire-and-forget broadcast to all actors without waiting for actors to
        acknowledge receipt.

        In other words, the return of this method does not guarrantee the
        delivery of the message.
        """
        from monarch._src.actor.actor_mesh import send

        # pyre-ignore
        send(self, args, kwargs)

    @abstractmethod
    def _rref(self, args, kwargs) -> Any: ...

    def rref(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self._rref(args, kwargs)

    def _propagate(self, args, kwargs, fake_args, fake_kwargs):
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

    def _fetch_propagate(self, args, kwargs, fake_args, fake_kwargs):
        if self._propagator_arg is None:
            return  # no propgator provided, so we just assume no mutations
        return self._propagate(args, kwargs, fake_args, fake_kwargs)

    def _pipe_propagate(self, args, kwargs, fake_args, fake_kwargs):
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
    ) -> None: ...

    @overload
    def __init__(
        self,
        method: Callable[Concatenate[Any, P], R],
        propagator: Propagator,
        explicit_response_port: bool,
    ) -> None: ...

    def __init__(
        self, method: Any, propagator: Propagator, explicit_response_port: bool
    ) -> None:
        self._method = method
        self._propagator = propagator
        self._explicit_response_port = explicit_response_port

    def __get__(self, instance, owner) -> Endpoint[P, R]:
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

    def __init__(self, ref: "ActorMesh", name: str):
        self._ref = ref
        self._name = name

    def __call__(self, *args, **kwargs) -> None:
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

    def __call__(self, function: Any):
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

    def __call__(self, function: Any):
        pass


@overload
def endpoint(
    method: Callable[Concatenate[Any, P], Awaitable[R]],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[False] = False,
) -> EndpointProperty[P, R]: ...


@overload
def endpoint(
    method: Callable[Concatenate[Any, P], R],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[False] = False,
) -> EndpointProperty[P, R]: ...


@overload
def endpoint(
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[False] = False,
) -> EndpointIfy: ...


@overload
def endpoint(
    method: Callable[Concatenate[Any, "Port[R]", P], Awaitable[None]],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[True],
) -> EndpointProperty[P, R]: ...


@overload
def endpoint(
    method: Callable[Concatenate[Any, "Port[R]", P], None],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[True],
) -> EndpointProperty[P, R]: ...


@overload
def endpoint(
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[True],
) -> PortedEndpointIfy: ...


def endpoint(method=None, *, propagate=None, explicit_response_port: bool = False):
    if method is None:
        return functools.partial(
            endpoint,
            propagate=propagate,
            explicit_response_port=explicit_response_port,
        )
    return EndpointProperty(
        method, propagator=propagate, explicit_response_port=explicit_response_port
    )
