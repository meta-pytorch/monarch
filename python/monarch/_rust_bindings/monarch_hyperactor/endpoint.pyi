# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

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
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Shape
from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionMonitor
from monarch._rust_bindings.monarch_hyperactor.value_mesh import ValueMesh
from typing_extensions import ParamSpec

P = ParamSpec("P")
R = TypeVar("R")

Selection = Literal["all", "choose"]

_T = TypeVar("_T")

class Future(Generic[_T]):
    def get(self, timeout: Optional[float] = None) -> _T: ...
    def __await__(self) -> Generator[Any, Any, _T]: ...

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.value_mesh import ValueMesh

def valuemesh_collector(
    extent: Extent,
    method_name: str,
    supervision_monitor: SupervisionMonitor | None,
    instance: Instance,
    qualified_endpoint_name: str | None,
) -> tuple[PortRef, PythonTask[ValueMesh[Any]]]:
    """
    Create a collector for multiple responses into a ValueMesh.

    Args:
        extent: The extent describing the shape of expected responses.
        method_name: The name of the method being called (for telemetry).
        supervisor: Optional supervisor for monitoring actor health.
        instance: Actor instance for opening ports.
        qualified_endpoint_name: Optional full endpoint name for error messages.
        adverb: The type of endpoint operation (Call, CallOne, Choose) for metrics.

    Returns:
        A tuple of (PortRef, PythonTask) where:
        - PortRef: The port reference to send to actors for responses.
        - PythonTask: A task that awaits all responses and returns a ValueMesh.
    """
    ...

def value_collector(
    method_name: str,
    supervision_monitor: SupervisionMonitor | None,
    instance: Instance,
    qualified_endpoint_name: str | None,
    adverb: Literal["call_one", "choose"],
) -> tuple[PortRef, PythonTask[Any]]:
    """
    Create a collector for a single response.

    Args:
        method_name: The name of the method being called (for telemetry).
        supervision_monitor: Optional supervision monitor for monitoring actor health.
        instance: Actor instance for opening ports.
        qualified_endpoint_name: Optional full endpoint name for error messages.
        adverb: The type of endpoint operation ("call_one", "choose") for metrics.

    Returns:
        A tuple of (PortRef, PythonTask) where:
        - PortRef: The port reference to send to an actor for the response.
        - PythonTask: A task that awaits the single response and returns the unpickled value.
    """
    ...
@final
class ValueStream(Iterator[Future[Any]]):
    """
    An iterator that yields Futures for streaming endpoint responses.

    Each call to __next__ returns a Future that resolves to the next
    response value from the stream.
    """

    def __iter__(self) -> "ValueStream": ...
    def __next__(self) -> Future[Any]:
        """
        Get the next response as a Future.

        Returns:
            A Future that resolves to the next response value.

        Raises:
            StopIteration: When all expected responses have been received.
        """
        ...

def stream_collector(
    extent: Extent,
    method_name: str,
    supervision_monitor: SupervisionMonitor | None,
    instance: Instance,
    qualified_endpoint_name: str | None,
) -> tuple[PortRef, ValueStream]:
    """
    Create a streaming collector for multiple responses.

    Args:
        extent: The extent describing the shape of expected responses.
        method_name: The name of the method being called (for telemetry).
        supervisor: Optional supervisor for monitoring actor health.
        instance: Actor instance for opening ports.
        qualified_endpoint_name: Optional full endpoint name for error messages.

    Returns:
        A tuple of (PortRef, ValueStream) where:
        - PortRef: The port reference to send to actors for responses.
        - ValueStream: An iterator that yields PythonTasks for each response.
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
    def call(self, *args: P.args, **kwargs: P.kwargs) -> "Future[ValueMesh[R]]": ...
    def choose(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        """
        Load balanced sends a message to one chosen actor and awaits a result.

        Load balanced RPC-style entrypoint for request/response messaging.
        """
        ...
    def call_one(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]: ...
    def stream(self, *args: P.args, **kwargs: P.kwargs) -> ValueStream:
        """
        Broadcasts to all actors and yields their responses as a stream / generator.

        This enables processing results from multiple actors incrementally as
        they become available. Returns an async generator of response values.
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
