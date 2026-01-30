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

from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.mailbox import PortRef
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._rust_bindings.monarch_hyperactor.supervision import Supervisor
from monarch._rust_bindings.monarch_hyperactor.value_mesh import ValueMesh

_T = TypeVar("_T")

class Future(Generic[_T]):
    def get(self, timeout: Optional[float] = None) -> _T: ...
    def __await__(self) -> Generator[Any, Any, _T]: ...

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.value_mesh import ValueMesh

def valuemesh_collector(
    extent: Extent,
    method_name: str,
    supervisor: Supervisor | None,
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
    supervisor: Supervisor | None,
    instance: Instance,
    qualified_endpoint_name: str | None,
    adverb: Literal["call_one", "choose"],
) -> tuple[PortRef, PythonTask[Any]]:
    """
    Create a collector for a single response.

    Args:
        method_name: The name of the method being called (for telemetry).
        supervisor: Optional supervisor for monitoring actor health.
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
    supervisor: Supervisor | None,
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
