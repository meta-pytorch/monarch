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
from typing import Any, cast, overload, Type, TYPE_CHECKING, TypeVar, Union

from monarch._rust_bindings.monarch_hyperactor.actor_mesh import PythonActorMesh
from monarch._rust_bindings.monarch_hyperactor.namespace import (
    configure_in_memory_namespace,
    get_global_namespace as _rust_get_global_namespace,
    is_namespace_configured as _rust_is_namespace_configured,
    MeshKind,
    Namespace,
)

if TYPE_CHECKING:
    from monarch._src.actor.host_mesh import HostMesh
    from monarch._src.actor.proc_mesh import ProcMesh

# SMC namespace is only available in fbcode builds
try:
    from monarch._rust_bindings.monarch_hyperactor.namespace import (
        configure_smc_namespace as _configure_smc_namespace,
    )
except ImportError:

    def _configure_smc_namespace(name: str, tier: str | None = None) -> None:
        raise NotImplementedError(
            "SMC namespace is not available in this build. "
            "It is only available in fbcode builds."
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
    SMC = "smc"


def is_namespace_configured() -> bool:
    """
    Check if the global namespace has been configured.

    Returns:
        True if the namespace is configured, False otherwise
    """
    return _rust_is_namespace_configured()


def get_global_namespace() -> Namespace | None:
    """
    Get the global namespace.

    Returns:
        The global Namespace instance if configured, None otherwise.
    """
    return _rust_get_global_namespace()


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
            - NamespacePersistence.SMC: SMC-backed namespace for production (fbcode only)
        name: The namespace name (e.g., "monarch" or "my.namespace")
        **kwargs: Additional configuration options for the persistence.
            For SMC backend:
                - tier: Optional SMC tier name. If not provided, uses the
                        default tier from configuration.

    Raises:
        RuntimeError: If the namespace has already been configured.
        ValueError: If an unknown persistence is specified.
    """
    if _rust_is_namespace_configured():
        raise RuntimeError("Global namespace has already been configured")

    if persistence == NamespacePersistence.IN_MEMORY:
        configure_in_memory_namespace(name)
    elif persistence == NamespacePersistence.SMC:
        tier = kwargs.get("tier")
        if tier is not None and not isinstance(tier, str):
            raise ValueError("tier must be a string")
        _configure_smc_namespace(name, tier)
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
) -> "Future[Union[HostMesh, ProcMesh]]": ...


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
        elif kind == MeshKind.Host:
            # Wrap HyHostMesh in HostMesh for Python API access
            from monarch._rust_bindings.monarch_hyperactor.host_mesh import (
                HostMesh as HyHostMesh,
            )
            from monarch._src.actor.host_mesh import HostMesh

            hy_host_mesh = cast(HyHostMesh, inner)
            return HostMesh.from_ref(hy_host_mesh)
        elif kind == MeshKind.Proc:
            # Wrap HyProcMesh in ProcMesh for Python API access
            # First, load the parent host mesh if available
            from monarch._rust_bindings.monarch_hyperactor.host_mesh import (
                HostMesh as HyHostMesh,
            )
            from monarch._rust_bindings.monarch_hyperactor.proc_mesh import (
                ProcMesh as HyProcMesh,
            )
            from monarch._src.actor.host_mesh import HostMesh
            from monarch._src.actor.proc_mesh import ProcMesh

            hy_proc_mesh = cast(HyProcMesh, inner)

            # Get the parent host mesh name and load it
            host_mesh_name = hy_proc_mesh.host_mesh_name
            if host_mesh_name is not None:
                # Load the parent host mesh from namespace
                host_task = ns.get(MeshKind.Host, host_mesh_name)
                hy_host_mesh = cast(HyHostMesh, await host_task)
                host_mesh = HostMesh.from_ref(hy_host_mesh)
            else:
                # No parent host mesh - this shouldn't happen for normal proc meshes
                # but we handle it gracefully by raising an error
                raise RuntimeError(
                    f"ProcMesh '{name}' has no associated HostMesh. "
                    "Cannot create a fully functional ProcMesh wrapper."
                )

            return ProcMesh.from_ref(hy_proc_mesh, host_mesh)
        else:
            # Unknown kind - return as-is
            return inner

    return Future(coro=_load())
