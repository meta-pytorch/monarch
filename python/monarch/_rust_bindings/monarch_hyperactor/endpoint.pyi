# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
from typing import (
    Any,
    final,
    Generator,
    Generic,
    Iterator,
    Literal,
    Optional,
    TYPE_CHECKING,
    TypeVar,
)

from monarch._rust_bindings.monarch_hyperactor.actor import MethodSpecifier
from monarch._rust_bindings.monarch_hyperactor.actor_mesh import ActorMeshProtocol
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.mailbox import OncePortRef, PortRef
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Shape
from monarch._rust_bindings.monarch_hyperactor.value_mesh import ValueMesh
from typing_extensions import ParamSpec

P = ParamSpec("P")
R = TypeVar("R")

Selection = Literal["all", "choose"]

_T = TypeVar("_T")

class Future(Generic[_T]):
    def get(self, timeout: Optional[float] = None) -> _T: ...
    def as_asyncio(self) -> "asyncio.Future[_T]": ...
    def __await__(self) -> Generator[Any, Any, _T]: ...

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.value_mesh import ValueMesh

@final
class ValueStream(Iterator[Future[Any]]):
    """
    An iterator that yields Futures for streaming endpoint responses.

    Replies arrive and queue eagerly. Each item takes and unpickles one queued
    reply only when its Future is first observed, in observation order.
    Each __next__ uses one iteration slot. Dropping an item unobserved leaves
    one reply unread after the slots are exhausted; observe every item taken
    if all replies must be read.
    """

    def __iter__(self) -> "ValueStream": ...
    def __next__(self) -> Future[Any]:
        """
        Reserve one iteration slot and return a Future for a queued reply.
        Its collector starts only when the Future is first observed.

        Returns:
            A Future that resolves to the next response value.

        Raises:
            StopIteration: After one iteration slot per actor has been used,
                whether or not those items have been observed.
        """
        ...

@final
class ActorEndpoint(Generic[P, R]):
    def __init__(
        self,
        actor_mesh: ActorMeshProtocol,
        method: MethodSpecifier,
        shape: Shape,
        mesh_name: str,
        signature: Any | None = None,
        proc_mesh: Any | None = None,
        propagator: Any | None = None,
    ) -> None: ...
    def _call_name(self) -> Any:
        """Something to use in InputChecker to represent calling this thingy."""
        ...
    def call(self, *args: P.args, **kwargs: P.kwargs) -> "Future[ValueMesh[R]]":
        """Send to all actors and start collecting their replies into a ValueMesh.

        The Future observes completion; dropping it does not stop collection.
        """
        ...
    def choose(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        """
        Send to a randomly selected actor and start collecting its reply.
        The Future observes completion; dropping it does not stop collection.

        Each call independently selects an actor uniformly at random. Selection
        does not account for actor load, so calls are balanced only across many
        calls. Use ``call_one`` on a slice of the mesh when placement must be
        deterministic.
        """
        ...
    def call_one(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        """Require exactly one actor, send now and start collecting its reply.

        The Future observes completion; dropping it does not stop collection.
        """
        ...
    def stream(self, *args: P.args, **kwargs: P.kwargs) -> ValueStream:
        """
        Send to all actors now and return an iterator of Futures.

        Replies arrive and queue eagerly. Each item takes and unpickles one
        queued reply only when first observed. Each next() uses one iteration
        slot; dropping an item unobserved leaves one reply unread after the
        slots are exhausted. Observe every item taken to read every reply.
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
    def _propagate(
        self, args: Any, kwargs: Any, fake_args: Any, fake_kwargs: Any
    ) -> Any: ...
    def _fetch_propagate(
        self, args: Any, kwargs: Any, fake_args: Any, fake_kwargs: Any
    ) -> Any: ...
    def _pipe_propagate(
        self, args: Any, kwargs: Any, fake_args: Any, fake_kwargs: Any
    ) -> Any: ...
    def _send(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        port: PortRef | OncePortRef | None = None,
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
    @property
    def _name(self) -> MethodSpecifier: ...
    @property
    def _signature(self) -> Any: ...
    @property
    def _actor_mesh(self) -> ActorMeshProtocol: ...

@final
class Remote(Generic[P, R]):
    def __init__(self, remote: Any) -> None: ...
    def _call_name(self) -> Any:
        """Something to use in InputChecker to represent calling this thingy."""
        ...
    @property
    def _maybe_resolvable(self) -> Any: ...
    @property
    def _resolvable(self) -> Any: ...
    @property
    def _remote_impl(self) -> Any: ...
    def _fetch_propagate(
        self, args: Any, kwargs: Any, fake_args: Any, fake_kwargs: Any
    ) -> Any: ...
    def _propagate(
        self, args: Any, kwargs: Any, fake_args: Any, fake_kwargs: Any
    ) -> Any: ...
    def _pipe_propagate(
        self, args: Any, kwargs: Any, fake_args: Any, fake_kwargs: Any
    ) -> Any: ...
    def call(self, *args: P.args, **kwargs: P.kwargs) -> "Future[ValueMesh[R]]":
        """Send to all actors and start collecting their replies into a ValueMesh.

        The Future observes completion; dropping it does not stop collection.
        """
        ...
    def choose(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        """
        Send to a randomly selected actor and start collecting its reply.
        The Future observes completion; dropping it does not stop collection.

        Each call independently selects an actor uniformly at random. Selection
        does not account for actor load, so calls are balanced only across many
        calls. Use ``call_one`` on a slice of the mesh when placement must be
        deterministic.
        """
        ...
    def call_one(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        """Require exactly one actor, send now and start collecting its reply.

        The Future observes completion; dropping it does not stop collection.
        """
        ...
    def stream(self, *args: P.args, **kwargs: P.kwargs) -> ValueStream:
        """
        Send to all actors now and return an iterator of Futures.

        Replies arrive and queue eagerly. Each item takes and unpickles one
        queued reply only when first observed. Each next() uses one iteration
        slot; dropping an item unobserved leaves one reply unread after the
        slots are exhausted. Observe every item taken to read every reply.
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
    def _send(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        port: PortRef | OncePortRef | None = None,
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
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...
