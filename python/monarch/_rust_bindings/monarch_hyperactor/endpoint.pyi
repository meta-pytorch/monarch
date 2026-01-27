# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, final

from monarch._rust_bindings.monarch_hyperactor.actor import MethodSpecifier
from monarch._rust_bindings.monarch_hyperactor.actor_mesh import ActorMeshProtocol
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.mailbox import PortRef
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._rust_bindings.monarch_hyperactor.shape import Extent, Shape
from monarch._rust_bindings.monarch_hyperactor.supervision import Supervisor
from monarch._rust_bindings.monarch_hyperactor.value_mesh import ValueMesh

@final
class EndpointAdverb:
    """
    The type of endpoint operation being performed.

    Used to select the appropriate telemetry metrics for each operation type.
    """

    Call: "EndpointAdverb"
    """Broadcast call to all actors, collecting responses into a ValueMesh"""

    CallOne: "EndpointAdverb"
    """Call to exactly one actor"""

    Choose: "EndpointAdverb"
    """Load-balanced call to a single chosen actor"""

    Stream: "EndpointAdverb"
    """Streaming call to all actors, yielding responses incrementally"""

def valuemesh_collector(
    extent: Extent,
    method_name: str,
    instance: Instance,
    adverb: EndpointAdverb,
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
    instance: Instance,
    adverb: EndpointAdverb,
) -> tuple[PortRef, PythonTask[Any]]:
    """
    Create a collector for a single response.

    Args:
        method_name: The name of the method being called (for telemetry).
        supervisor: Optional supervisor for monitoring actor health.
        instance: Actor instance for opening ports.
        qualified_endpoint_name: Optional full endpoint name for error messages.
        adverb: The type of endpoint operation (Call, CallOne, Choose) for metrics.

    Returns:
        A tuple of (PortRef, PythonTask) where:
        - PortRef: The port reference to send to an actor for the response.
        - PythonTask: A task that awaits the single response and returns the unpickled value.
    """
    ...
@final
class ValueStream:
    """
    An iterator that yields Future objects for streaming endpoint responses.

    Each call to __next__ returns a Future object that can be awaited to get
    the next response value from the stream.
    """

    def __iter__(self) -> "ValueStream":
        """Return self as an iterator."""
        ...

    def __next__(self) -> Any:
        """
        Get the next response as a Future.

        Returns:
            A Future object that resolves to the next response value.

        Raises:
            StopIteration: When all expected responses have been received.
        """
        ...

def stream_collector(
    extent: Extent,
    method_name: str,
    instance: Instance,
) -> tuple[PortRef, ValueStream]:
    """
    Create a streaming collector for multiple responses.

    Args:
        extent: The extent describing the shape of expected responses.
        method_name: The name of the method being called (for telemetry).
        instance: Actor instance for opening ports.

    Returns:
        A tuple of (PortRef, ValueStream) where:
        - PortRef: The port reference to send to actors for responses.
        - ValueStream: An iterator that yields Future objects for each response.
    """
    ...
@final
class ActorEndpoint:
    """
    Rust implementation of ActorEndpoint.

    This replaces the Python ActorEndpoint class with a Rust implementation
    that reduces Python-Rust boundary crossings for endpoint calls.

    The endpoint is created from a PythonActorMesh and MethodSpecifier,
    and provides the adverb methods (choose, call, call_one, stream, broadcast)
    that create collectors, messages, and cast to the actor mesh.
    """

    def __init__(
        self,
        actor_mesh: ActorMeshProtocol,
        method: MethodSpecifier,
        shape: Shape,
        mesh_name: str,
        signature: Any | None = None,
        proc_mesh: Any | None = None,
    ) -> None:
        """
        Create a new ActorEndpoint.

        Args:
            actor_mesh: The ActorMeshProtocol to send messages to
            method: The MethodSpecifier for the endpoint
            shape: The shape of the actor mesh
            mesh_name: The name of the mesh
            signature: Optional Python inspect.Signature for argument validation
            proc_mesh: Optional ProcMesh reference for pending pickle support
        """
        ...

    def _get_extent(self) -> Extent:
        """Get the extent of the actor mesh."""
        ...

    def _get_method_name(self) -> str:
        """Get the method name."""
        ...

    def call(self, *args: Any, **kwargs: Any) -> Any:
        """
        Call the endpoint on all actors and collect all responses into a ValueMesh.

        Returns a Future that resolves to a ValueMesh containing all responses.
        """
        ...

    def choose(self, *args: Any, **kwargs: Any) -> Any:
        """
        Call the endpoint on one randomly chosen actor.

        Returns a Future that resolves to the single response.
        """
        ...

    def call_one(self, *args: Any, **kwargs: Any) -> Any:
        """
        Call the endpoint on exactly one actor (the mesh must have exactly one actor).

        Returns a Future that resolves to the single response.
        Raises an error if the mesh has more than one actor.
        """
        ...

    def stream(self, *args: Any, **kwargs: Any) -> ValueStream:
        """
        Call the endpoint on all actors and return an iterator of Futures.

        Returns a ValueStream that yields Futures for each response as they arrive.
        """
        ...

    def broadcast(self, *args: Any, **kwargs: Any) -> None:
        """Send a message to all actors without waiting for responses (fire-and-forget)."""
        ...
