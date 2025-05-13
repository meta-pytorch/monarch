from typing import Any, ClassVar, Dict, final, List

from monarch._monarch.debugger import DebuggerActionType

from monarch._monarch.hyperactor import ActorId, Proc, Serialized
from monarch._monarch.worker import Ref

class Exception:
    """
    An exception to commanded execution. This class is not used on its own; rather
    it serves as a super class for `Error` and `Failure`.
    """

@final
class Error(Exception):
    """
    A deterministic problem with the user's code. For example, an OOM
    resulting in trying to allocate too much GPU memory, or violating
    some invariant enforced by the various APIs.
    """

    @property
    def seq(self) -> int:
        """The seq of this invocation that errored out."""
        ...

    @property
    def caused_by_seq(self) -> int:
        """The original seq of the invocation that caused the error."""
        ...

    @property
    def actor_id(self) -> ActorId:
        """The actor id of the worker where the error occured."""
        ...

    @property
    def backtrace(self) -> str:
        """The backtrace associated with the error."""
        ...

    @staticmethod
    def new_for_unit_test(
        seq: int, caused_by_seq: int, actor_id: ActorId, backtrace: str
    ) -> "Error": ...

@final
class Failure(Exception):
    """
    A failure is a non-deterministic problem with the underlying worker
    or its infrastructure. For example, a worker may enter a crash loop,
    or its GPU may be lost
    """

    @property
    def actor_id(self) -> ActorId:
        """The actor id of the worker where the failure occured."""
        ...

    @property
    def address(self) -> str:
        """The address of the actor proc."""
        ...

    @property
    def backtrace(self) -> str:
        """The backtrace associated with the error."""
        ...

    @staticmethod
    def new_for_unit_test(actor_id: ActorId) -> "Failure": ...

@final
class WorkerResponse:
    """
    Response that has been sent back from a worker through the controller
    to the client./python/monarch/_monarch/client/__init__.pyi. This does not get created by in python outside of unittests.
    """

    @property
    def seq(self) -> int:
        """
        Seq of the message sent by the client to the worker to which this is a response.
        """
        ...

    def result(self) -> Any:
        """
        The result sent back to the client by the worker. This will be `None` if there is no
        result or if `error` is present. Check `is_error` to see if there is an error first.
        """
        ...

    def exception(self) -> Exception | None:
        """
        The exception transmitted back to the client by the worker. This will be `None` if there
        isn't one. If present `result` will return `None`.
        """
        ...

    def is_exception(self) -> bool:
        """Will return `True` if calling `error` will return an `Exception`."""
        ...

    @staticmethod
    def new_for_unit_test(*, seq: int, response: Any) -> "WorkerResponse": ...

@final
class LogLevel:
    INFO: ClassVar[LogLevel]
    WARNING: ClassVar[LogLevel]
    ERROR: ClassVar[LogLevel]

@final
class LogMessage:
    """
    A message from the controller for the client so that certain
    events in the controller can be surfaced to the user
    """

    @property
    def level(self) -> LogLevel:
        """Warning, Info, or Error"""
        ...

    @property
    def message(self) -> str:
        """What the controller wants to notify the user about"""
        ...

@final
class ProcInfo:
    @property
    def labels(self) -> Dict[str, str]:
        """The labels of the proc."""
        ...

@final
class SystemSnapshotFilter:
    """
    A filter for the system snapshot. All conditions are logically ANDed together.

    Arguments:
    - `worlds`: A list of worlds to filter on. If None, all worlds will be returned.
    - `world_labels`: A dictionary of labels to filter on. If None, all worlds will be returned.
    - `proc_labels`: A dictionary of labels to filter on. If None, all procs will be returned.
    """
    def __init__(
        self,
        worlds: List[str] | None = None,
        world_labels: Dict[str, str] | None = None,
        proc_labels: Dict[str, str] | None = None,
    ) -> None: ...

@final
class WorldState:
    """
    A snapshot state of a world.
    TODO: merge world status.

    """

    @property
    def labels(self) -> Dict[str, str]:
        """Labels attached to this world."""
        ...

    @property
    def procs(self) -> Dict[str, ProcInfo]:
        """The procs in the world."""
        ...

@final
class ClientActor:
    """A python based detached actor that can be used to send messages to other
    actors and recieve WorkerResponse objects from them. This in practise will
    only talk to the controller.

    Arguments:
    - `proc`: The proc the actor is a part of.
    - `actor_name`: Name of the actor.
    """

    def __init__(self, proc: Proc, actor_name: str) -> None: ...
    @staticmethod
    def new_with_parent(proc: Proc, parent_id: ActorId) -> ClientActor:
        """
        Create a new client actor with the given parent id. This is used to create
        a client actor that is a child of another client actor.
        """
        ...

    def attach(self, controller_id: ActorId) -> None:
        """Attach the client to the given controller.

        Arguments:
        - `controller_id`: The actor id of the controller to attach to.
        """
        ...

    def send(self, actor_id: ActorId, message: Serialized) -> None:
        """Send a message to the actor with the given actor id.

        Arguments:
        - `actor_id`: The actor id of the actor to send the message to.
        - `message`: The message to send.
        """
        ...

    def drop_refs(self, controller_id: ActorId, refs: List[Ref]) -> None:
        """
        Mark references as never being used again
        """
        ...

    def get_next_message(
        self, *, timeout_msec: int | None = None
    ) -> LogMessage | WorkerResponse | DebuggerMessage | None:
        """Get the next message sent to the actor.

        Arguments:
        - `timeout_msec`: Number of milliseconds to wait for a message.
                None means wait forever.
        """
        ...

    def stop_worlds(self, world_names: List[str]) -> None:
        """Stop the system."""
        ...

    def drain_and_stop(self) -> List[LogMessage | WorkerResponse | DebuggerMessage]:
        """Stop the actor and drain all messages."""
        ...

    def world_status(
        self, filter: SystemSnapshotFilter | None = None
    ) -> dict[str, str]:
        """Get the world status from the system."""
        ...

    def world_state(
        self, filter: SystemSnapshotFilter | None = None
    ) -> Dict[str, WorldState]:
        """Get worlds info and procs from the system."""
        ...

    @property
    def actor_id(self) -> ActorId:
        """The actor id of the actor."""
        ...

@final
class DebuggerMessage:
    """
    Message for debugger communication between worker and client.
    """

    def __init__(
        self, *, debugger_actor_id: ActorId, action: DebuggerActionType
    ) -> None: ...
    @property
    def debugger_actor_id(self) -> ActorId:
        """Get the actor id of the debugger actor."""
        ...

    @property
    def action(self) -> DebuggerActionType:
        """Get the debugger action."""
        ...
