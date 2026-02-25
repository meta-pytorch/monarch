# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Dict, final, List, Optional, Protocol, Type

from monarch._rust_bindings.monarch_hyperactor.alloc import Alloc, AllocSpec
from monarch._rust_bindings.monarch_hyperactor.selection import Selection
from monarch._rust_bindings.monarch_hyperactor.shape import Shape

def init_proc(
    *,
    proc_id: str,
    bootstrap_addr: str,
    timeout: int = 5,
    supervision_update_interval: int = 0,
    listen_addr: Optional[str] = None,
) -> Proc:
    """
    Helper function to bootstrap a new Proc.

    Arguments:
    - `proc_id`: String representation of the ProcId eg. `"world_name[0]"`
    - `bootstrap_addr`: String representation of the channel address of the system
        actor. eg. `"tcp![::1]:2345"`
    - `timeout`: Number of seconds to wait to successfully connect to the system.
    - `supervision_update_interval`: Number of seconds between supervision updates.
    - `listen_addr`: String representation of the channel address of the proc
        actor. eg. `"tcp![::1]:2345"`
    """
    ...
@final
class Serialized:
    """
    An opaque wrapper around a message that hhas been serialized in a hyperactor
    friendly manner.
    """

    ...

@final
class ActorId:
    """
    A python wrapper around hyperactor ActorId. It represents a unique reference
    for an actor.

    Arguments:
    - `world_name`: The world the actor belongs in (same as the Proc containing the Actor)
    - `rank`: The rank of the proc containing the actor.
    - `actor_name`: Name of the actor.
    - `pid`: The pid of the actor.
    """

    def __init__(
        self, *, world_name: str, rank: int, actor_name: str, pid: int = 0
    ) -> None: ...
    def __str__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    @property
    def world_name(self) -> str:
        """The world the actor belongs in (same as the Proc containing the Actor)"""
        ...

    @property
    def rank(self) -> int:
        """The rank of the proc containing the actor."""
        ...

    @property
    def actor_name(self) -> str:
        """Name of the actor."""
        ...

    @property
    def pid(self) -> int:
        """The pid of the actor."""
        ...

    @property
    def proc_id(self) -> str:
        """String representation of the ProcId eg. `"world_name[0]"`"""
        ...

    @staticmethod
    def from_string(actor_id_str: str) -> ActorId:
        """
        Create an ActorId from a string representation.

        Arguments:
        - `actor_id_str`: String representation of the actor id.
        """
        ...

class Actor(Protocol):
    async def handle(self, mailbox: Mailbox, message: PythonMessage) -> None:
        """
        Handle a message from the mailbox.

        Arguments:
        - `mailbox`: The actor's mailbox.
        - `message`: The message to handle.
        """
        ...

    async def handle_cast(
        self,
        mailbox: Mailbox,
        rank: int,
        coordinates: list[tuple[str, int]],
        message: PythonMessage,
    ) -> None:
        """
        Handle a message casted to this actor, on a mesh in which this actor
        has the given rank and coordinates.

        Arguments:
        - `mailbox`: The actor's mailbox.
        - `rank`: The rank of the actor in the mesh.
        - `coordinates`: The labeled coordinates of the actor in the mesh.
        - `message`: The message to handle.
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
class PortId:
    def __init__(self, actor_id: ActorId, index: int) -> None:
        """
        Create a new port id given an actor id and an index.
        """
        ...
    def __str__(self) -> str: ...
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
class Proc:
    """
    A python wrapper around hyperactor Proc. This is the root container
    for all actors in the process.
    """

    def __init__(self) -> None:
        """Create a new Proc."""
        ...

    @property
    def world_name(self) -> str:
        """The world the Proc is a part of."""
        ...

    @property
    def rank(self) -> int:
        """Rank of the Proc in that world."""
        ...

    def destroy(self, timeout_in_secs: int) -> list[str]:
        """Destroy the Proc."""
        ...

    async def spawn(self, actor: Type[Actor]) -> PythonActorHandle:
        """
        Spawn a new actor.

        Arguments:
        - `actor_name`: Name of the actor.
        - `actor`: The type of the actor, which
        """
        ...

    def attach(self, name: str) -> Mailbox:
        """
        Attach to this proc.

        Arguments:
        - `name`: Name of the actor.
        """
        ...

@final
class PythonMessage:
    """
    A message that carries a python method and a pickled message that contains
    the arguments to the method.
    """

    def __init__(self, method: str, message: bytes) -> None: ...
    @property
    def method(self) -> str:
        """The method of the message."""
        ...

    @property
    def message(self) -> bytes:
        """The pickled arguments."""
        ...

@final
class PortHandle:
    """
    A handle to a port over which PythonMessages can be sent.
    """

    def send(self, message: PythonMessage) -> None:
        """Send a message to the port's receiver."""

    def bind(self) -> PortId:
        """Bind this port. The returned port ID can be used to reach the port externally."""
        ...

@final
class PortReceiver:
    """
    A receiver to which PythonMessages are sent.
    """
    async def recv(self) -> PythonMessage:
        """Receive a PythonMessage from the port's sender."""
        ...
    def blocking_recv(self) -> PythonMessage:
        """Receive a single PythonMessage from the port's sender."""
        ...

@final
class OncePortHandle:
    """
    A variant of PortHandle that can only send a single message.
    """

    def send(self, message: PythonMessage) -> None:
        """Send a single message to the port's receiver."""
        ...

    def bind(self) -> PortId:
        """Bind this port. The returned port ID can be used to reach the port externally."""
        ...

@final
class OncePortReceiver:
    """
    A variant of PortReceiver that can only receive a single message.
    """
    async def recv(self) -> PythonMessage:
        """Receive a single PythonMessage from the port's sender."""
        ...
    def blocking_recv(self) -> PythonMessage:
        """Receive a single PythonMessage from the port's sender."""
        ...

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

    def post(self, dest: ActorId | PortId, message: PythonMessage) -> None:
        """
        Post a message to the provided destination. If the destination is an actor id,
        the message is sent to the default handler for `PythonMessage` on the actor.
        Otherwise, it is sent to the port directly.
        """
        ...

    def post_cast(
        self, dest: ActorId | PortId, rank: int, shape: Shape, message: PythonMessage
    ) -> None:
        """
        Post a message to the provided actor. It will be handled using the handle_cast
        endpoint as if the destination was `rank` of `shape`.
        """
        ...

    @property
    def actor_id(self) -> ActorId: ...

class ProcessAllocatorBase:
    def __init__(
        self,
        program: str,
        args: Optional[list[str]] = None,
        envs: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Create a new process allocator.

        Arguments:
        - `program`: The program for each process to run. Must be a hyperactor
                    bootstrapped program.
        - `args`: The arguments to pass to the program.
        - `envs`: The environment variables to set for the program.
        """
        ...

    async def allocate_nonblocking(self, spec: AllocSpec) -> Alloc:
        """
        Allocate a process according to the provided spec.

        Arguments:
        - `spec`: The spec to allocate according to.
        """
        ...

    def allocate_blocking(self, spec: AllocSpec) -> Alloc:
        """
        Allocate a process according to the provided spec, blocking until an
        alloc is returned.

        Arguments:
        - `spec`: The spec to allocate according to.
        """
        ...

class LocalAllocatorBase:
    async def allocate_nonblocking(self, spec: AllocSpec) -> Alloc:
        """
        Allocate a process according to the provided spec.

        Arguments:
        - `spec`: The spec to allocate according to.
        """
        ...

    def allocate_blocking(self, spec: AllocSpec) -> Alloc:
        """
        Allocate a process according to the provided spec, blocking until an
        alloc is returned.

        Arguments:
        - `spec`: The spec to allocate according to.
        """
        ...

@final
class ProcMesh:
    @classmethod
    async def allocate_nonblocking(self, alloc: Alloc) -> ProcMesh:
        """
        Allocate a process mesh according to the provided alloc.
        Returns when the mesh is fully allocated.

        Arguments:
        - `alloc`: The alloc to allocate according to.
        """
        ...

    @classmethod
    def allocate_blocking(self, alloc: Alloc) -> ProcMesh:
        """
        Allocate a process mesh according to the provided alloc.
        Blocks until the mesh is fully allocated.

        Arguments:
        - `alloc`: The alloc to allocate according to.
        """
        ...

    async def spawn_nonblocking(self, name: str, actor: Type[Actor]) -> PythonActorMesh:
        """
        Spawn a new actor on this mesh.

        Arguments:
        - `name`: Name of the actor.
        - `actor`: The type of the actor that will be spawned.
        """
        ...

    async def spawn_blocking(self, name: str, actor: Type[Actor]) -> PythonActorMesh:
        """
        Spawn a new actor on this mesh. Blocks until the actor is fully spawned.

        Arguments:
        - `name`: Name of the actor.
        - `actor`: The type of the actor that will be spawned.
        """
        ...

    @property
    def client(self) -> Mailbox:
        """
        A client that can be used to communicate with individual
        actors in the mesh, and also to create ports that can be
        broadcast across the mesh)
        """
        ...
    def __repr__(self) -> str: ...

@final
class PythonActorMesh:
    def cast(self, message: PythonMessage) -> None:
        """
        Cast a message to this mesh.
        """

    def get(self, rank: int) -> ActorId | None:
        """
        Get the actor id for the actor at the given rank.
        """
        ...

    @property
    def client(self) -> Mailbox:
        """
        A client that can be used to communicate with individual
        actors in the mesh, and also to create ports that can be
        broadcast across the mesh)
        """
        ...

    @property
    def shape(self) -> Shape:
        """
        The Shape object that describes how the rank of an actor
        retrieved with get corresponds to coordinates in the
        mesh.
        """
        ...
