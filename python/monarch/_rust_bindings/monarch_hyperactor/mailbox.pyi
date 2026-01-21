# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import final, Protocol

from monarch._rust_bindings.monarch_hyperactor.actor import PythonMessage
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask

@final
class PortId:
    def __init__(self, *, actor_id: ActorId, port: int) -> None:
        """
        Create a new port id given an actor id and a port index.
        """
        ...
    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    @property
    def actor_id(self) -> ActorId:
        """
        The ID of the actor that owns the port.
        """
        ...

    @property
    def index(self) -> int:
        """
        The actor-relative index of the port.
        """
        ...

    @staticmethod
    def from_string(port_id_str: str) -> PortId:
        """
        Parse a port id from the provided string.
        """
        ...

@final
class PortHandle:
    """
    A handle to a port over which PythonMessages can be sent.
    """

    def send(self, instance: Instance, message: PythonMessage) -> None:
        """Send a message to the port's receiver."""

    def bind(self) -> PortRef:
        """Bind this port. The returned port ref can be used to reach the port externally."""
        ...

@final
class PortRef:
    """
    A reference to a remote port over which PythonMessages can be sent.
    """

    def __init__(self, port_id: PortId) -> None:
        """
        Create a new port ref given a port id.
        """
        ...

    def send(self, instance: Instance, message: PythonMessage) -> None:
        """Send a single message to the port's receiver."""
        ...

    @property
    def return_undeliverable(self) -> bool:
        """Whether to return undeliverable messages to the sender. By default, this is true"""
        ...

    @return_undeliverable.setter
    def return_undeliverable(self, value: bool) -> None: ...
    @property
    def port_id(self) -> PortId: ...
    def __repr__(self) -> str: ...

class PortReceiverBase:
    """
    A receiver to which PythonMessages are sent.
    """
    def recv_task(self) -> "PythonTask[PythonMessage]":
        """Receive a PythonMessage from the port's sender, returns a tokio Future for the completion."""
        ...

@final
class PortReceiver(PortReceiverBase):
    pass

@final
class UndeliverablePortReceiver:
    """
    A receiver to which undeliverable message envelopes are sent.
    """
    async def recv(self) -> UndeliverableMessageEnvelope:
        """Receive a single undeliverable message from the port's sender."""
        ...
    def blocking_recv(self) -> UndeliverableMessageEnvelope:
        """Receive a single undeliverable message from the port's sender."""
        ...

@final
class OncePortHandle:
    """
    A variant of PortHandle that can only send a single message.
    """

    def send(self, instance: Instance, message: PythonMessage) -> None:
        """Send a single message to the port's receiver."""
        ...

    def bind(self) -> OncePortRef:
        """Bind this port. The returned port ID can be used to reach the port externally."""
        ...

@final
class OncePortRef:
    """
    A reference to a remote once port over which a single PythonMessages can be sent.
    """

    def send(self, instance: Instance, message: PythonMessage) -> None:
        """Send a single message to the port's receiver."""
        ...

    @property
    def return_undeliverable(self) -> bool:
        """Whether to return undeliverable messages to the sender. By default, this is true."""
        ...

    @return_undeliverable.setter
    def return_undeliverable(self, value: bool) -> None: ...
    @property
    def port_id(self) -> PortId: ...
    def __repr__(self) -> str: ...

@final
class OncePortReceiver(PortReceiverBase):
    pass

@final
class Mailbox:
    """
    A mailbox from that can receive messages.
    """
    def open_port(self) -> tuple[PortHandle, PortReceiver]:
        """Open a port to receive `PythonMessage` messages."""
        ...

    def open_once_port(self) -> tuple[OncePortHandle, OncePortReceiver]:
        """Open a port to receive a single `PythonMessage` message."""
        ...

    def open_accum_port(
        self, accumulator: Accumulator
    ) -> tuple[PortHandle, PortReceiver]:
        """Open a accum port."""
        ...

    def post(self, dest: ActorId, message: PythonMessage) -> None:
        """
        Post a message to the provided destination. If the destination is an actor id,
        the message is sent to the default handler for `PythonMessage` on the actor.
        Otherwise, it is sent to the port directly.
        """
        ...

    @property
    def actor_id(self) -> ActorId: ...

class Accumulator(Protocol):
    """
    Define the Python interface for its `trait Accumulator` counterpart in
    Monarch Rust backend. It enables users to implement accumulators in python
    natively.
    """
    def __call__(
        self, state: PythonMessage, update: PythonMessage
    ) -> PythonMessage: ...
    """
    Accumulate an `update` into the current `state`.

    This method's Rust counterpart is `Accumulator::accumulate`.
    """
    @property
    def initial_state(self) -> PythonMessage: ...
    """
    Define the initial state of this accumulator.
    """
    @property
    def reducer(self) -> Reducer | None: ...
    """
    The reducer associated with this accumulator.
    """

class Reducer(Protocol):
    """
    Define the Python interface for its `trait CommReducer` counterpart in
    Monarch Rust backend. It enables users to implement reducers in python
    natively.
    """

    def __call__(self, left: PythonMessage, right: PythonMessage) -> PythonMessage: ...
    """
    Reduce 2 updates into a single update.

    This method's Rust counterpart is `CommReducer::reduce`.
    """

class UndeliverableMessageEnvelope:
    """
    An envelope representing a message that could not be delivered.
    """

    def __repr__(self) -> str: ...
    def sender(self) -> ActorId:
        """
        The actor id of the sender.
        """
        ...

    def dest(self) -> PortId:
        """
        The port id of the destination.
        """
        ...

    def error_msg(self) -> str:
        """
        The error message describing why the message could not be delivered.
        """
        ...
