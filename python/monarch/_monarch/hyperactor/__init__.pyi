# pyre-strict

from typing import Dict, final, List, Optional, Type

from monarch._monarch.shape import Shape, Slice as NDSlice

@final
class Proc:
    """
    A python wrapper around hyperactor Proc. This is the root container
    for all actors in the process.
    """

    @property
    def world_name(self) -> str:
        """The world the Proc is a part of."""
        ...

    @property
    def rank(self) -> int:
        """Rank of the Proc in that world."""
        ...

    def destroy(self, timeout_in_secs: int) -> List[str]:
        """Destroy the Proc."""
        ...

def init_proc(
    *,
    proc_id: str,
    bootstrap_addr: str,
    timeout: int = 5,
    supervision_update_interval: int = 0,
) -> Proc:
    """
    Helper function to bootstrap a new Proc.

    Arguments:
    - `proc_id`: String representation of the ProcId eg. `"world_name[0]"`
    bootstrap_addr`: String representation of the channel address of the system
        actor. eg. `"tcp![::1]:2345"`
    - `timeout`: Number of seconds to wait to successfully connect to the system.
    """
    ...

def init_asyncio_loop() -> None:
    """
    Sets the asyncio loop used by hyperactor to dispatch actor handlers.
    Must be called before any actor dispatches occur.

    This is a temporary workaround until we change our dispatch logic.
    """
    ...
@final
class Serialized:
    """
    An opaque wrapper around a message that has been serialized in a hyperactor
    friendly manner.
    """

    ...

@final
class PythonMessage:
    """
    A message that can be sent to PythonMessageActor. It is a wrapper
    around a method name and a serialized message.

    Arguments:
    - `method`: The name of the method to call.
    - `message`: The message to send.
    """

    def __init__(self, method: str, message: bytes) -> None: ...
    @property
    def method(self) -> str:
        """The name of the method to call."""
        ...

    @property
    def message(self) -> bytes:
        """The message to send."""
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

class Actor:
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

    @property
    def actor_id(self) -> ActorId: ...

class Alloc:
    """
    An alloc represents an allocation of procs. Allocs are returned by
    one of the allocator implementations, such as `ProcessAllocator` or
    `LocalAllocator`.
    """

@final
class AllocSpec:
    def __init__(self, **kwargs: int) -> None:
        """
        Initialize a shape with the provided dimension-size pairs.
        For example, `AllocSpec(replica=2, host=3, gpu=8)` creates a
        shape with 2 replicas with 3 hosts each, each of which in turn
        has 8 GPUs.
        """
        ...

@final
class ProcessAllocator:
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

    async def allocate(self, spec: AllocSpec) -> Alloc:
        """
        Allocate a process according to the provided spec.

        Arguments:
        - `spec`: The spec to allocate according to.
        """
        ...

@final
class LocalAllocator:
    @classmethod
    async def allocate(cls, spec: AllocSpec) -> Alloc:
        """
        Allocate a process according to the provided spec.

        Arguments:
        - `spec`: The spec to allocate according to.
        """
        ...

@final
class ProcMesh:
    @classmethod
    async def allocate(self, alloc: Alloc) -> ProcMesh:
        """
        Allocate a process mesh according to the provided alloc.
        Returns when the mesh is fully allocated.

        Arguments:
        - `alloc`: The alloc to allocate according to.
        """
        ...

    async def spawn(self, name: str, actor: Type[Actor]) -> PythonActorMesh:
        """
        Spawn a new actor on this mesh.

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
