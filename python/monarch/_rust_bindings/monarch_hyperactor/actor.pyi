# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum
from typing import (
    Any,
    final,
    Generic,
    Iterable,
    List,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer, FrozenBuffer
from monarch._rust_bindings.monarch_hyperactor.mailbox import OncePortRef, PortRef
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId, Proc, Serialized
from monarch._rust_bindings.monarch_hyperactor.pytokio import PendingPickleState

class PythonMessageKind:
    @classmethod
    @property
    def Result(cls) -> "Type[Result]": ...
    @classmethod
    @property
    def Exception(cls) -> "Type[Exception]": ...
    @classmethod
    @property
    def CallMethod(cls) -> "Type[CallMethod]": ...
    @classmethod
    @property
    def Uninit(cls) -> "Type[Uninit]": ...
    @classmethod
    @property
    def CallMethodIndirect(cls) -> "Type[CallMethodIndirect]": ...

class Result(PythonMessageKind):
    def __init__(self, rank: Optional[int]) -> None: ...
    @property
    def rank(self) -> int | None: ...

class Exception(PythonMessageKind):
    def __init__(self, rank: Optional[int]) -> None: ...
    @property
    def rank(self) -> int | None: ...

class CallMethod(PythonMessageKind):
    def __init__(
        self, name: MethodSpecifier, response_port: PortRef | OncePortRef | None
    ) -> None: ...
    @property
    def name(self) -> MethodSpecifier: ...
    @property
    def response_port(self) -> PortRef | OncePortRef | None: ...

class MethodSpecifier:
    @classmethod
    @property
    def ReturnsResponse(cls) -> "Type[ReturnsResponse]": ...
    @classmethod
    @property
    def ExplicitPort(cls) -> "Type[ExplicitPort]": ...
    @classmethod
    @property
    def Init(cls) -> "Type[Init]": ...
    @property
    def name(self) -> str: ...

class ReturnsResponse(MethodSpecifier):
    def __init__(self, name: str) -> None: ...

class ExplicitPort(MethodSpecifier):
    def __init__(self, name: str) -> None: ...

class Init(MethodSpecifier):
    pass

class UnflattenArg(Enum):
    Mailbox = 0
    PyObject = 1

class CallMethodIndirect(PythonMessageKind):
    def __init__(
        self,
        name: MethodSpecifier,
        broker_id: Tuple[str, int],
        id: int,
        unflatten_args: List[UnflattenArg],
    ) -> None: ...
    @property
    def name(self) -> MethodSpecifier: ...
    @property
    def broker_id(self) -> Tuple[str, int]: ...
    @property
    def id(self) -> int: ...
    @property
    def unflatten_args(self) -> List[UnflattenArg]: ...

class Uninit(PythonMessageKind):
    pass

@final
class PythonMessage:
    """
    A message that carries a python method and a pickled message that contains
    the arguments to the method.
    """
    def __init__(
        self,
        kind: PythonMessageKind,
        message: Union[Buffer, bytes],
        pending_pickle_state: Optional[PendingPickleState] = None,
    ) -> None: ...
    @property
    def message(self) -> FrozenBuffer:
        """The pickled arguments."""
        ...
    @property
    def kind(self) -> PythonMessageKind: ...

@final
class PythonActorHandle:
    """
    A python wrapper around hyperactor ActorHandle. It represents a handle to an
    actor.

    Arguments:
    - `inner`: The inner actor handle.
    """

    def send(self, message: PythonMessage) -> None:
        """
        Send a message to the actor.

        Arguments:
        - `message`: The message to send.
        """
        ...

    def bind(self) -> ActorId:
        """
        Bind this actor. The returned actor id can be used to reach the actor externally.
        """
        ...

@final
class PanicFlag:
    """
    A mechanism to notify the hyperactor runtime that a panic has occurred in an
    asynchronous Python task. See [Panics in async endpoints] for more details.
    """

    def signal_panic(self, ex: BaseException) -> None:
        """
        Signal that a panic has occurred in an asynchronous Python task.
        """
        ...

R = TypeVar("R")

class PortProtocol(Generic[R], Protocol):
    def send(self, obj: R) -> None: ...
    def exception(self, obj: Any) -> None: ...

class Actor(Protocol):
    async def handle(
        self,
        ctx: Any,
        method: MethodSpecifier,
        message: FrozenBuffer,
        panic_flag: PanicFlag,
        local_state: Iterable[Any],
        response_port: PortProtocol[Any],
    ) -> None: ...

@final
class QueuedMessage:
    """
    A message sent through the queue in queue-dispatch mode.
    Contains pre-resolved components ready for Python consumption.
    """

    @property
    def context(self) -> Any:
        """The PyContext for this message."""
        ...

    @property
    def method(self) -> MethodSpecifier:
        """The method specifier for this message."""
        ...

    @property
    def bytes(self) -> FrozenBuffer:
        """The serialized message bytes."""
        ...

    @property
    def local_state(self) -> Any:
        """The local state for this message."""
        ...

    @property
    def response_port(self) -> PortProtocol[Any]:
        """The response port for this message."""
        ...
