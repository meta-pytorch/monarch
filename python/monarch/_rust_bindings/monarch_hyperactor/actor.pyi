# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
from enum import Enum

from typing import Any, final, Iterable, List, Optional, Protocol, Tuple, Type

from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    Mailbox,
    OncePortRef,
    PortRef,
)
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId, Proc, Serialized
from monarch._rust_bindings.monarch_hyperactor.shape import Shape

@final
class PickledMessage:
    """
    A message that can be sent to PickledMessage{,Client}Actor. It is a wrapper around
    a serialized message and the sender's actor id.

    Arguments:
    - `sender_actor_id`: The actor id of the sender.
    - `message`: The pickled message.
    """

    def __init__(self, *, sender_actor_id: ActorId, message: bytes) -> None: ...
    @property
    def sender_actor_id(self) -> ActorId:
        """The actor id of the sender."""
        ...

    @property
    def message(self) -> bytes:
        """The pickled message."""
        ...

    def serialize(self) -> Serialized:
        """Serialize the message into a Serialized object."""
        ...

@final
class PickledMessageClientActor:
    """
    A python based detached actor that can be used to send messages to other
    actors and recieve PickledMessage objects from them.

    Arguments:
    - `proc`: The proc the actor is a part of.
    - `actor_name`: Name of the actor.
    """

    def __init__(self, proc: Proc, actor_name: str) -> None: ...
    def send(self, actor_id: ActorId, message: Serialized) -> None:
        """
        Send a message to the actor with the given actor id.

        Arguments:
        - `actor_id`: The actor id of the actor to send the message to.
        - `message`: The message to send.
        """
        ...

    def get_next_message(
        self, *, timeout_msec: int | None = None
    ) -> PickledMessage | None:
        """
        Get the next message sent to the actor. If the timeout is reached
        before a message is received, None is returned.

        Arguments:
        - `timeout_msec`: Number of milliseconds to wait for a message.
                None means wait forever.
        """
        ...

    def stop_worlds(self, world_names: List[str]) -> None:
        """Stop the system."""
        ...

    def drain_and_stop(self) -> list[PickledMessage]:
        """Stop the actor and drain all messages."""
        ...

    def world_status(self) -> dict[str, str]:
        """Get the world status from the system."""
        ...

    @property
    def actor_id(self) -> ActorId:
        """The actor id of the actor."""
        ...

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
        self, name: str, response_port: PortRef | OncePortRef | None
    ) -> None: ...
    @property
    def name(self) -> str: ...
    @property
    def response_port(self) -> PortRef | OncePortRef | None: ...

class UnflattenArg(Enum):
    Mailbox = 0
    PyObject = 1

class CallMethodIndirect(PythonMessageKind):
    def __init__(
        self,
        name: str,
        broker_id: Tuple[str, int],
        id: int,
        unflatten_args: List[UnflattenArg],
    ) -> None: ...

class Init(PythonMessageKind):
    def __init__(self, response_port: PortRef | OncePortRef | None) -> None: ...
    @property
    def response_port(self) -> PortRef | OncePortRef | None: ...

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
        message: bytes,
    ) -> None: ...
    @property
    def message(self) -> bytes:
        """The pickled arguments."""
        ...
    @property
    def kind(self) -> PythonMessageKind: ...

class UndeliverableMessageEnvelope:
    """
    An envelope representing a message that could not be delivered.

    This object is opaque; its contents are not accessible from Python.
    """

    ...

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

class PortProtocol(Protocol):
    def send(self, obj: Any) -> None: ...
    def exception(self, obj: Any) -> None: ...

class Actor(Protocol):
    async def handle(
        self,
        mailbox: Mailbox,
        rank: int,
        shape: Shape,
        method: str,
        message: bytes,
        panic_flag: PanicFlag,
        local_state: Iterable[Any],
        response_port: PortProtocol,
    ) -> None: ...
