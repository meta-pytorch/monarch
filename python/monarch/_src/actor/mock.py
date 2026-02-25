# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
from abc import ABC, abstractmethod
from functools import wraps
from types import TracebackType
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    Tuple,
    Type,
    TYPE_CHECKING,
    TypeVar,
)

if TYPE_CHECKING:
    from monarch._src.actor.actor_mesh import Actor
    from monarch._src.actor.proc_mesh import DeviceMesh, ProcMesh

# Type alias for tensor engine factory functions
TensorEngineFactory = Callable[["ProcMesh"], "DeviceMesh"]

T = TypeVar("T")


# ============================================================================
# Base Patch Class
# ============================================================================


class _BasePatch(ABC, Generic[T]):
    """
    Base class for patching utilities that provides context manager and decorator support.

    This eliminates code duplication between patch_actor and patch_tensor_engine by
    sharing the common __enter__, __exit__, __aenter__, __aexit__, and __call__ logic.
    """

    def __init__(self, target: Type["Actor"], value: T) -> None:
        self._target = target
        self._value = value
        self._previous: Optional[T] = None

    @abstractmethod
    def _get_registry(self) -> Dict[Type["Actor"], T]:
        """Get the registry to use for this patch."""
        pass

    def __enter__(self) -> T:
        # Save previous value (if any) to support nested patching
        self._previous = self._get_registry().get(self._target)
        self._get_registry()[self._target] = self._value
        return self._value

    async def __aenter__(self) -> T:
        return self.__enter__()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self._previous is not None:
            self._get_registry()[self._target] = self._previous
        else:
            self._get_registry().pop(self._target, None)

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        return self.__exit__(exc_type, exc_val, exc_tb)

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                async with self:
                    return await func(*args, **kwargs)

            return async_wrapper
        else:

            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                with self:
                    return func(*args, **kwargs)

            return sync_wrapper


# ============================================================================
# Generic Mock Registry Restorer
# ============================================================================

StateT = TypeVar("StateT")


class _MockRegistryRestorer(ABC, Generic[StateT]):
    """
    Base class for mock registry restorers that handles serialization.

    Uses __reduce_ex__ for custom pickling. When pickled and sent to a remote
    process, this callable will restore the mock registry state when called.

    Subclasses must implement _set_state() to define how to restore the registry.
    """

    def __init__(self, state: StateT) -> None:
        self._state = state

    @abstractmethod
    def _set_state(self, state: StateT) -> None:
        """Set the registry state. Must be implemented by subclasses."""
        pass

    def __call__(self) -> None:
        self._set_state(self._state)

    # pyre-ignore[14]: intentionally using int for pickle protocol
    def __reduce_ex__(self, protocol: int) -> Tuple[Any, ...]:
        return (type(self), (self._state,))


# ============================================================================
# Actor Mock Registry
# ============================================================================

# Module-level registry mapping original Actor classes to their mocks.
# Uses the class object itself as the key (not __name__) to handle
# classes with the same name defined in different scopes.
_actor_registry: Dict[Type["Actor"], Type["Actor"]] = {}

# Flag to track if the mock startup function has been registered
_registered: bool = False


def get_actor_class(cls: Type["Actor"]) -> Type["Actor"]:
    """Get the mock class for an actor, or the original if not mocked."""
    return _actor_registry.get(cls, cls)


def get_actor_mock_registry_state() -> Dict[Type["Actor"], Type["Actor"]]:
    """Get the current actor mock registry state for propagation to remote processes."""
    return dict(_actor_registry)


def set_actor_mock_registry_state(
    state: Dict[Type["Actor"], Type["Actor"]],
) -> None:
    """Set the actor mock registry state on a remote process."""
    _actor_registry.update(state)


class _ActorMockRegistryRestorer(
    _MockRegistryRestorer[Dict[Type["Actor"], Type["Actor"]]]
):
    """Restorer for actor mock registry state."""

    def _set_state(self, state: Dict[Type["Actor"], Type["Actor"]]) -> None:
        set_actor_mock_registry_state(state)


def _get_actor_mock_restorer() -> Optional[Callable[[], None]]:
    """
    Get a callable that restores the actor mock registry on remote processes.

    Returns None if there are no mocks registered.
    """
    state = get_actor_mock_registry_state()
    if not state:
        return None
    return _ActorMockRegistryRestorer(state)


def _ensure_actor_registered() -> None:
    """
    Register the actor mock startup function lazily on first use.

    This is called when someone first uses patch_actor, combining the
    registration with the logic for whether we need to do startup at all.
    """
    global _registered
    if _registered:
        return
    _registered = True

    from monarch._src.actor.proc_mesh import SetupActor

    SetupActor.register_startup_function(_get_actor_mock_restorer)


class patch_actor(_BasePatch[Type["Actor"]]):
    """
    Actor patching utility, similar to unittest.mock.patch.

    When an actor class is patched, any call to spawn() with that actor class
    will instead spawn the mock actor class.

    Usage:
    ```
        # As context manager
        with patch_actor(RealActor, MockActor):
            train()

        # As decorator
        @patch_actor(RealActor, MockActor)
        def train_with_mocks():
            train()

        # Multiple patches
        @patch_actor(Actor1, Mock1)
        @patch_actor(Actor2, Mock2)
        def train_with_mocks():
            train()
    ```
    """

    def __init__(self, original: Type["Actor"], mock: Type["Actor"]) -> None:
        super().__init__(original, mock)
        # Keep original and mock attributes for backwards compatibility
        self.original: Type["Actor"] = original
        self.mock: Type["Actor"] = mock

    def _get_registry(self) -> Dict[Type["Actor"], Type["Actor"]]:
        return _actor_registry

    def __enter__(self) -> Type["Actor"]:
        # Lazy registration: register the startup function on first use
        _ensure_actor_registered()
        return super().__enter__()


# ============================================================================
# Tensor Engine Mock Registry
# ============================================================================

# Module-level registry mapping Actor classes to tensor engine factories.
# When an actor's tensor engine is patched, the factory function is called
# to create a mock DeviceMesh instead of a real one.
_tensor_engine_registry: Dict[Type["Actor"], TensorEngineFactory] = {}

# Flag to track if the tensor engine mock startup function has been registered
_tensor_engine_registered: bool = False


def get_tensor_engine_factory(
    cls: Type["Actor"],
) -> Optional[TensorEngineFactory]:
    """Get the tensor engine factory for an actor class, or None if not registered."""
    return _tensor_engine_registry.get(cls)


def get_tensor_engine_mock_registry_state() -> Dict[Type["Actor"], TensorEngineFactory]:
    """Get the current tensor engine mock registry state for propagation to remote processes."""
    return dict(_tensor_engine_registry)


def set_tensor_engine_mock_registry_state(
    state: Dict[Type["Actor"], TensorEngineFactory],
) -> None:
    """Set the tensor engine mock registry state on a remote process."""
    _tensor_engine_registry.update(state)


class _TensorEngineMockRegistryRestorer(
    _MockRegistryRestorer[Dict[Type["Actor"], TensorEngineFactory]]
):
    """Restorer for tensor engine mock registry state."""

    def _set_state(self, state: Dict[Type["Actor"], TensorEngineFactory]) -> None:
        set_tensor_engine_mock_registry_state(state)


def _get_tensor_engine_mock_restorer() -> Optional[Callable[[], None]]:
    """
    Get a callable that restores the tensor engine mock registry on remote processes.

    Returns None if there are no mocks registered.
    """
    state = get_tensor_engine_mock_registry_state()
    if not state:
        return None
    return _TensorEngineMockRegistryRestorer(state)


def _ensure_tensor_engine_registered() -> None:
    """
    Register the tensor engine mock startup function lazily on first use.

    This is called when someone first uses patch_tensor_engine, combining the
    registration with the logic for whether we need to do startup at all.
    """
    global _tensor_engine_registered
    if _tensor_engine_registered:
        return
    _tensor_engine_registered = True

    from monarch._src.actor.proc_mesh import SetupActor

    SetupActor.register_startup_function(_get_tensor_engine_mock_restorer)


def _create_simulated_tensor_engine(proc_mesh: "ProcMesh") -> "DeviceMesh":
    """
    Create a simulated tensor engine using Monarch Simulator.

    This is the default tensor engine factory used when no custom factory is provided.
    """
    # Extract the actual mesh dimensions from the proc_mesh
    hosts = proc_mesh.sizes.get("hosts", 1)
    gpus = proc_mesh.sizes.get("gpus", 1)

    # Create simulator with the same dimensions as the real proc_mesh
    from monarch.simulator.interface import Simulator  # pyre-ignore[21, 16]

    simulator = Simulator(  # pyre-ignore[16]
        hosts=hosts, gpus=gpus, trace_mode="stream_only", build_ir=True
    )
    return simulator.mesh


class patch_tensor_engine(_BasePatch[TensorEngineFactory]):
    """
    Tensor engine patching utility, similar to unittest.mock.patch.

    When a tensor engine is patched for an actor class, any call to activate
    the tensor engine for that actor will use the mock factory instead of
    creating a real tensor engine.

    Usage:
    ```
        # As context manager - uses default simulated tensor engine
        with patch_tensor_engine(SomeActor):
            train()

        # As decorator
        @patch_tensor_engine(SomeActor)
        def train_with_mocks():
            train()

        # Multiple patches
        @patch_tensor_engine(Actor1)
        @patch_tensor_engine(Actor2)
        def train_with_mocks():
            train()

        # Custom tensor engine factory
        def my_custom_tensor_engine(proc_mesh):
            return create_my_custom_device_mesh(proc_mesh)

        with patch_tensor_engine(SomeActor, my_custom_tensor_engine):
            train()
    ```
    """

    def __init__(
        self,
        actor_class: Type["Actor"],
        mock_factory: Optional[TensorEngineFactory] = None,
    ) -> None:
        factory = mock_factory or _create_simulated_tensor_engine
        super().__init__(actor_class, factory)
        # Keep actor_class and mock_factory attributes for backwards compatibility
        self.actor_class: Type["Actor"] = actor_class
        self.mock_factory: TensorEngineFactory = factory

    def _get_registry(self) -> Dict[Type["Actor"], TensorEngineFactory]:
        return _tensor_engine_registry

    def __enter__(self) -> TensorEngineFactory:
        # Lazy registration: register the startup function on first use
        _ensure_tensor_engine_registered()
        return super().__enter__()
