# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import functools
from typing import (
    Any,
    Awaitable,
    Callable,
    cast,
    Concatenate,
    Dict,
    Generic,
    Iterator,
    Literal,
    Optional,
    overload,
    ParamSpec,
    Protocol,
    Tuple,
    TYPE_CHECKING,
    TypeVar,
    Union,
)

from monarch._rust_bindings.monarch_hyperactor.shape import (
    Extent,  # noqa: F401 re-exports
)
from monarch._src.actor.future import Future
from monarch._src.actor.tensor_engine_shim import _cached_propagation, fake_call

T = TypeVar("T")


if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.mailbox import OncePortRef, PortRef
    from monarch._src.actor.actor_mesh import ActorMesh, Port, ValueMesh
    from monarch._src.actor.future import Future

P = ParamSpec("P")
R = TypeVar("R")

Selection = Literal["all", "choose"]


Propagator = Union[None, Literal["cached", "inspect", "mocked"], Callable[..., Any]]


class Endpoint(Protocol[P, R]):
    def _send(
        self,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        port: "Optional[PortRef | OncePortRef]" = None,
        selection: Selection = "all",
    ) -> None:
        """
        Implements sending a message to the endpoint. The return value of the endpoint will
        be sent to port if provided. If port is not provided, the return will be dropped,
        and any exception will cause the actor to fail.

        The return value is the (multi-dimension) size of the actors that were sent a message.
        For ActorEndpoints this will be the actor_meshes size. For free-function endpoints,
        this will be the size of the currently active proc_mesh.
        """
        ...

    def _call_name(self) -> Any:
        """
        Something to use in InputChecker to represent calling this thingy.
        """
        ...

    def choose(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        """
        Load balanced sends a message to one chosen actor and awaits a result.

        Load balanced RPC-style entrypoint for request/response messaging.
        """
        ...

    def call_one(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]: ...

    def call(self, *args: P.args, **kwargs: P.kwargs) -> "Future[ValueMesh[R]]": ...

    def stream(self, *args: P.args, **kwargs: P.kwargs) -> Iterator[Future[R]]:
        """
        Broadcasts to all actors and yields their responses as a stream.

        This enables processing results from multiple actors incrementally as
        they become available. Returns an iterator of Future values.
        """
        ...

    def broadcast(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """
        Fire-and-forget broadcast to all actors without waiting for actors to
        acknowledge receipt.

        In other words, the return of this method does not guarrantee the
        delivery of the message.
        """
        ...

    def rref(self, *args: P.args, **kwargs: P.kwargs) -> R: ...


def _do_propagate(
    propagator_arg: Propagator,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    fake_args: Tuple[Any, ...],
    fake_kwargs: Dict[str, Any],
    cache: Dict[Any, Any],
    resolvable: Optional[Any] = None,
) -> Any:
    if propagator_arg is None or propagator_arg == "cached":
        if resolvable is None:
            raise NotImplementedError(
                "Cached propagation is not implemented for actor endpoints."
            )
        return _cached_propagation(cache, resolvable, args, kwargs)
    elif propagator_arg == "inspect":
        return None
    elif propagator_arg == "mocked":
        raise NotImplementedError("mocked propagation")
    else:
        return fake_call(propagator_arg, *fake_args, **fake_kwargs)


class EndpointProperty(Generic[P, R]):
    @overload
    def __init__(
        self,
        method: Callable[Concatenate[Any, P], Awaitable[R]],
        propagator: Propagator,
        explicit_response_port: bool,
        instrument: bool = True,
    ) -> None: ...

    @overload
    def __init__(
        self,
        method: Callable[Concatenate[Any, P], R],
        propagator: Propagator,
        explicit_response_port: bool,
        instrument: bool = True,
    ) -> None: ...

    def __init__(
        self,
        method: Any,
        propagator: Propagator,
        explicit_response_port: bool,
        instrument: bool = True,
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
    instrument: bool = True,
) -> EndpointProperty[P, R]: ...


@overload
def endpoint(
    method: Callable[Concatenate[Any, P], R],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[False] = False,
    instrument: bool = True,
) -> EndpointProperty[P, R]: ...


@overload
def endpoint(
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[False] = False,
    instrument: bool = True,
) -> EndpointIfy: ...


@overload
def endpoint(
    method: Callable[Concatenate[Any, "Port[R]", P], Awaitable[None]],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[True],
    instrument: bool = True,
) -> EndpointProperty[P, R]: ...


@overload
def endpoint(
    method: Callable[Concatenate[Any, "Port[R]", P], None],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[True],
    instrument: bool = True,
) -> EndpointProperty[P, R]: ...


@overload
def endpoint(
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[True],
    instrument: bool = True,
) -> PortedEndpointIfy: ...


def endpoint(
    method: Any = None,
    *,
    propagate: Any = None,
    explicit_response_port: bool = False,
    instrument: bool = True,
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
