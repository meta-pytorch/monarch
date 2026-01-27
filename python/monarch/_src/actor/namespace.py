# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Namespace API for discovering and connecting to remote meshes.

This module provides functions to configure the global namespace and
load meshes (actor, proc, host) that have been registered by name.
"""

from enum import Enum
from typing import Any, cast, overload, Type, TypeVar, Union

from monarch._rust_bindings.monarch_hyperactor.actor_mesh import PythonActorMesh
from monarch._rust_bindings.monarch_hyperactor.namespace import (
    create_in_memory_namespace,
    MeshKind,
    Namespace,
)
from monarch._src.actor.actor_mesh import ActorMesh
from monarch._src.actor.future import Future

# Re-export for type checking
__all__ = [
    "configure_namespace",
    "get_global_namespace",
    "is_namespace_configured",
    "load",
    "MeshKind",
    "Namespace",
    "NamespacePersistence",
]

T = TypeVar("T")


class NamespacePersistence(Enum):
    """NamespacePersistence types for namespace configuration."""

    IN_MEMORY = "in_memory"


# Global namespace instance
_global_namespace: Namespace | None = None


def is_namespace_configured() -> bool:
    """
    Check if the global namespace has been configured.

    Returns:
        True if the namespace is configured, False otherwise
    """
    return _global_namespace is not None


def get_global_namespace() -> Namespace | None:
    """
    Get the global namespace.

    Returns:
        The global Namespace instance if configured, None otherwise.
    """
    return _global_namespace


def configure_namespace(
    persistence: NamespacePersistence = NamespacePersistence.IN_MEMORY,
    name: str = "monarch",
    **kwargs: object,
) -> None:
    """
    Configure the global namespace.

    Args:
        persistence: The namespace persistence to use. Currently supported:
            - NamespacePersistence.IN_MEMORY: In-memory namespace for testing
        name: The namespace name (e.g., "monarch" or "my.namespace")
        **kwargs: Additional configuration options for the persistence.

    Raises:
        RuntimeError: If the namespace has already been configured.
        ValueError: If an unknown persistence is specified.
    """
    global _global_namespace
    if _global_namespace is not None:
        raise RuntimeError("Global namespace has already been configured")

    if persistence == NamespacePersistence.IN_MEMORY:
        _global_namespace = create_in_memory_namespace(name)
    else:
        raise ValueError(f"Unknown namespace persistence: {persistence}")


@overload
def load(
    kind: MeshKind,
    name: str,
    actor_class: Type[T],
) -> "Future[ActorMesh[T]]": ...


@overload
def load(
    kind: MeshKind,
    name: str,
) -> "Future[object]": ...


def load(
    kind: MeshKind,
    name: str,
    actor_class: "Type[T] | None" = None,
) -> "Future[Any]":
    """
    Load a mesh from the namespace.

    This function looks up a registered mesh by its kind and name,
    and returns the appropriate mesh type.

    Args:
        kind: The type of mesh to load (MeshKind.Actor, MeshKind.Proc, or MeshKind.Host)
        name: The name of the mesh.
        actor_class: Required for MeshKind.Actor - the actor class type for type checking
                     and endpoint discovery.

    Returns:
        A Future that resolves to:
        - ActorMesh[T] for MeshKind.Actor
        - ProcMesh for MeshKind.Proc
        - HostMesh for MeshKind.Host

    Raises:
        RuntimeError: If the namespace is not configured.
        KeyError: If the mesh is not found.
        ValueError: If actor_class is not provided for MeshKind.Actor.

    Example:
        >>> from monarch._src.actor import namespace
        >>> from monarch._src.actor.namespace import MeshKind
        >>>
        >>> # Configure the namespace (typically done once at startup)
        >>> namespace.configure_namespace()
        >>>
        >>> # Load an actor mesh
        >>> my_actors = namespace.load(MeshKind.Actor, "my_actors", MyActor).get()
        >>> result = my_actors.some_endpoint.call().get()
        >>>
        >>> # Load a proc mesh
        >>> my_procs = namespace.load(MeshKind.Proc, "my_procs").get()
    """
    if kind == MeshKind.Actor and actor_class is None:
        raise ValueError("actor_class is required when loading an actor mesh")
    elif kind != MeshKind.Actor and actor_class is not None:
        raise ValueError("actor_class is not supported for non-actor meshes")

    async def _load() -> Union[ActorMesh[T], object]:
        ns = get_global_namespace()
        if ns is None:
            raise RuntimeError("Global namespace is not configured")

        task = ns.get(kind, name)
        inner = await task

        if kind == MeshKind.Actor:
            # Wrap PythonActorMesh in ActorMesh for endpoint access
            assert actor_class is not None
            actor_mesh = cast(PythonActorMesh, inner)
            region = actor_mesh.region
            shape = region.as_shape()
            return ActorMesh(actor_class, name, actor_mesh, shape, None)
        else:
            # For Proc and Host meshes, return the Rust type directly
            # These are already usable without additional wrapping
            return inner

    return Future(coro=_load())
