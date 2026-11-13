# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from enum import Enum
from typing import Any, final, Protocol

from monarch._rust_bindings.monarch_hyperactor.actor import PythonMessage
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.handle import Handle
from monarch._rust_bindings.monarch_hyperactor.proc import ActorAddr

@final
class PortId:
    def __init__(self, *, actor_id: ActorAddr, port: int) -> None:
        """
        Create a new port id given an actor id and a port index.
        """
        ...
    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    @property
    def actor_id(self) -> ActorAddr:
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
    def recv(self, ranked: bool) -> Handle[Any]:
        """Return a Handle for one message, received and decoded when first
        observed.

        A ranked receive yields ``(rank, payload)``. Exception messages raise
        their sent exception when observed. A started receive stops, and
        releases its port receiver, once nobody can observe it.
        """
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

    def post(self, dest: ActorAddr, message: PythonMessage) -> None:
        """
        Post a message to the provided destination. If the destination is an actor id,
        the message is sent to the default handler for `PythonMessage` on the actor.
        Otherwise, it is sent to the port directly.
        """
        ...

    @property
    def actor_id(self) -> ActorAddr: ...

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

class DeliveryFailureKind(Enum):
    """The category of a delivery failure."""

    InvalidReference = 0
    Undeliverable = 1
    Expired = 2

class InvalidReferenceReason(Enum):
    """Why a destination reference is invalid."""

    ProcNotExist = 0
    RouteNotExist = 1
    ActorNotExist = 2
    HandlerNotBound = 3
    ActorStopped = 4
    ActorFailed = 5
    PortNeverAllocated = 6
    ProtocolMismatch = 7
    WrongMailboxOwner = 8

class UndeliverableReason(Enum):
    """A transport failure or a port whose recipient is gone."""

    Transport = 0
    PortGone = 1

class TransportFailureReason(Enum):
    """Why transport could not deliver a message."""

    ChannelClosed = 0
    AckTimedOut = 1
    DialFailed = 2
    NoRoute = 3
    OversizedFrame = 4
    LinkUnavailable = 5
    ForwarderUnavailable = 6

@final
class DeliveryFailure:
    """Structured details of a Rust delivery failure."""
    @property
    def kind(self) -> DeliveryFailureKind: ...
    @property
    def invalid_reference_reason(self) -> InvalidReferenceReason | None:
        """The invalid-reference reason, or None for another failure kind."""
        ...
    @property
    def undeliverable_reason(self) -> UndeliverableReason | None:
        """The undeliverable reason, or None for another failure kind."""
        ...
    @property
    def transport_failure_reason(self) -> TransportFailureReason | None:
        """The transport reason, or None for another failure kind."""
        ...
    @property
    def target(self) -> str:
        """The delivery target named by the failure."""
        ...
    def __str__(self) -> str: ...

class UndeliverableMessageEnvelope:
    """
    An envelope representing a message that could not be delivered.
    """

    def __repr__(self) -> str: ...
    def sender(self) -> ActorAddr:
        """
        The actor address of the sender.
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
    def root_delivery_failure(self) -> DeliveryFailure | None:
        """The original failure, excluding failures on the return path."""
        ...
    @property
    def has_payload(self) -> bool:
        """Whether the original message was returned with the failure."""
        ...
