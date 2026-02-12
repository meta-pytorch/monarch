# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Literal

from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.mailbox import Mailbox, PortRef
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionMonitor
from monarch._rust_bindings.monarch_hyperactor.value_mesh import ValueMesh

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
