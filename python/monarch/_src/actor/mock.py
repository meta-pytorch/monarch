# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
from functools import wraps
from types import TracebackType
from typing import Any, Callable, Dict, Optional, Tuple, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from monarch._src.actor.actor_mesh import Actor


# Module-level registry mapping original Actor classes to their mocks.
# Uses the class object itself as the key (not __name__) to handle
# classes with the same name defined in different scopes.
_registry: Dict[Type["Actor"], Type["Actor"]] = {}

# Flag to track if the mock startup function has been registered
_registered: bool = False


def get_actor_class(cls: Type["Actor"]) -> Type["Actor"]:
    """Get the mock class for an actor, or the original if not mocked."""
    return _registry.get(cls, cls)


def get_mock_registry_state() -> Dict[Type["Actor"], Type["Actor"]]:
    """Get the current mock registry state for propagation to remote processes."""
    return dict(_registry)


def set_mock_registry_state(state: Dict[Type["Actor"], Type["Actor"]]) -> None:
    """Set the mock registry state on a remote process."""
    _registry.update(state)


class _MockRegistryRestorer:
    """
    A callable class that captures mock registry state for serialization.

    Uses __reduce_ex__ for custom pickling. When pickled and sent to a remote
    process, this callable will restore the mock registry state when called.
    """

    def __init__(self, state: Dict[Type["Actor"], Type["Actor"]]) -> None:
        self._state = state

    def __call__(self) -> None:
        set_mock_registry_state(self._state)

    # pyre-ignore[14]: intentionally using int for pickle protocol
    def __reduce_ex__(self, protocol: int) -> Tuple[Any, ...]:
        return (_MockRegistryRestorer, (self._state,))


def _get_mock_restorer() -> Optional[Callable[[], None]]:
    """
    Get a callable that restores the mock registry on remote processes.

    Returns None if there are no mocks registered.
    """
    state = get_mock_registry_state()
    if not state:
        return None
    return _MockRegistryRestorer(state)


def _ensure_registered() -> None:
    """
    Register the mock startup function lazily on first use.

    This is called when someone first uses patch_actor, combining the
    registration with the logic for whether we need to do startup at all.
    """
    global _registered
    if _registered:
        return
    _registered = True

    from monarch._src.actor.proc_mesh import SetupActor

    SetupActor.register_startup_function(_get_mock_restorer)


class patch_actor:
    """
    Actor patching utility, similar to unittest.mock.patch.

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
        self.original: Type["Actor"] = original
        self.mock: Type["Actor"] = mock
        self._previous: Optional[Type["Actor"]] = None

    def __enter__(self) -> Type["Actor"]:
        # Lazy registration: register the startup function on first use
        _ensure_registered()
        # Save previous value (if any) to support nested patching
        self._previous = _registry.get(self.original)
        _registry[self.original] = self.mock
        return self.mock

    async def __aenter__(self) -> Type["Actor"]:
        return self.__enter__()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self._previous is not None:
            _registry[self.original] = self._previous
        else:
            _registry.pop(self.original, None)

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
