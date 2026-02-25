# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, List

from monarch._rust_bindings.monarch_hyperactor.actor import PythonMessageKind
from monarch._rust_bindings.monarch_hyperactor.buffers import FrozenBuffer
from monarch._rust_bindings.monarch_hyperactor.pytokio import Shared

class PicklingState:
    """
    Result of a pickling operation.

    Contains the pickled bytes and any tensor engine references or pending
    pickles that were collected during serialization.
    """

    def __init__(
        self,
        buffer: FrozenBuffer,
        tensor_engine_references: List[Any] | None = None,
    ) -> None:
        """
        Create a new PicklingState from a buffer and optional tensor engine references.

        This is used for unpickling received messages that may contain tensor engine
        references that need to be restored during deserialization.

        Args:
            buffer: The pickled bytes as a FrozenBuffer.
            tensor_engine_references: Optional list of tensor engine references
                to restore during unpickling.
        """
        ...

    def tensor_engine_references(self) -> List[Any]:
        """
        Get a copy of all tensor engine references from this pickling state.

        Returns a list containing copies of the tensor engine references.
        """
        ...

    def buffer(self) -> FrozenBuffer:
        """
        Get the buffer from this pickling state.

        Returns a FrozenBuffer containing the pickled bytes.
        This does not consume the PicklingState.
        """
        ...

    def unpickle(self) -> Any:
        """
        Unpickle the buffer contents.

        This consumes the PicklingState. It will fail if there are any pending
        pickles that haven't been resolved.
        """
        ...

class PendingMessage:
    """
    A message that is pending resolution of async values before it can be sent.

    Contains a PythonMessageKind and a PicklingState. The PicklingState may contain
    pending pickles (unresolved async values) that must be resolved before the message
    can be converted into a PythonMessage.
    """

    def __init__(self, kind: PythonMessageKind, state: PicklingState) -> None:
        """
        Create a new PendingMessage from a kind and pickling state.

        Note: This takes ownership of the PicklingState's inner state.
        """
        ...

    @property
    def kind(self) -> PythonMessageKind:
        """Get the message kind."""
        ...

def pickle(
    obj: Any,
    allow_pending_pickles: bool = True,
    allow_tensor_engine_references: bool = True,
) -> PicklingState:
    """
    Pickle an object with support for pending pickles and tensor engine references.

    Creates a PicklingState and calls cloudpickle.dumps with an active
    thread-local pickling state, allowing __reduce__ implementations to push
    tensor engine references and pending pickles.

    Args:
        obj: The Python object to pickle
        allow_pending_pickles: If true, allow PyShared values to be registered as pending
        allow_tensor_engine_references: If true, allow tensor engine references to be registered

    Returns:
        A PicklingState containing the pickled bytes and any registered references/pending pickles
    """
    ...

def push_tensor_engine_reference_if_active(obj: Any) -> bool:
    """
    Push a tensor engine reference to the active pickling state if one is active.

    Called from Python during pickling when a tensor engine object
    is encountered that needs special handling.

    Returns:
        False if there is no active pickling state.
        True if the reference was successfully pushed.

    Raises:
        RuntimeError: If tensor engine references are not allowed in the
            current pickling context.
    """
    ...

def pop_tensor_engine_reference() -> Any:
    """
    Pop a tensor engine reference from the active pickling state.

    Called from Python during unpickling to retrieve tensor engine
    objects in the order they were pushed.

    Raises:
        RuntimeError: If there is no active pickling state or no references remaining.
    """
    ...

def pop_pending_pickle() -> Shared[Any]:
    """
    Pop a pending pickle from the active pickling state.

    Called from Python during unpickling to retrieve the PyShared
    object that was deferred during pickling.

    Raises:
        RuntimeError: If there is no active pickling state or no pending pickles remaining.
    """
    ...
