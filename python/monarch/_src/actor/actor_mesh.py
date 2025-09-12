# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import abc
import collections
import contextvars
import functools
import inspect
import itertools
import logging
import random
import traceback
from abc import abstractmethod, abstractproperty

from dataclasses import dataclass
from pprint import pformat
from textwrap import indent
from traceback import TracebackException
from typing import (
    Any,
    Awaitable,
    Callable,
    cast,
    Concatenate,
    Dict,
    Generator,
    Generic,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    overload,
    ParamSpec,
    Tuple,
    Type,
    TYPE_CHECKING,
    TypeVar,
)

from monarch._rust_bindings.monarch_hyperactor.actor import (
    MethodSpecifier,
    PanicFlag,
    PythonMessage,
    PythonMessageKind,
)
from monarch._rust_bindings.monarch_hyperactor.actor_mesh import (
    PythonActorMesh,
    PythonActorMeshImpl,
)
from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer, FrozenBuffer
from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    Mailbox,
    OncePortReceiver as HyOncePortReceiver,  # noqa: F401
    OncePortRef,
    PortReceiver as HyPortReceiver,  # noqa: F401
    PortRef,
    UndeliverableMessageEnvelope,
)
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.selection import Selection as HySelection
from monarch._rust_bindings.monarch_hyperactor.shape import Point as HyPoint, Shape
from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionError
from monarch._src.actor.allocator import LocalAllocator, ProcessAllocator
from monarch._src.actor.debugger.pdb_wrapper import PdbWrapper
from monarch._src.actor.endpoint import (
    Endpoint,
    EndpointProperty,
    Extent,
    NotAnEndpoint,
    Propagator,
    Selection,
)
from monarch._src.actor.future import DeprecatedNotAFuture, Future
from monarch._src.actor.pickle import flatten, unflatten
from monarch._src.actor.python_extension_methods import rust_struct
from monarch._src.actor.shape import MeshTrait, NDSlice
from monarch._src.actor.sync_state import fake_sync_state
from monarch._src.actor.telemetry import METER
from monarch._src.actor.tensor_engine_shim import actor_rref, actor_send
from typing_extensions import Self

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.actor import PortProtocol
    from monarch._rust_bindings.monarch_hyperactor.actor_mesh import ActorMeshProtocol
    from monarch._rust_bindings.monarch_hyperactor.mailbox import PortReceiverBase
    from monarch._src.actor.proc_mesh import _ControllerController, ProcMesh
from monarch._src.actor.telemetry import get_monarch_tracer

CallMethod = PythonMessageKind.CallMethod

logger: logging.Logger = logging.getLogger(__name__)

TRACER = get_monarch_tracer()

Allocator = ProcessAllocator | LocalAllocator

try:
    from __manifest__ import fbmake  # noqa

    IN_PAR = bool(fbmake.get("par_style"))
except ImportError:
    IN_PAR = False

T1 = TypeVar("T1")
T2 = TypeVar("T2")


class Point(HyPoint, collections.abc.Mapping):
    pass


@rust_struct("monarch_hyperactor::mailbox::Instance")
class Instance(abc.ABC):
    @abstractproperty
    def _mailbox(self) -> Mailbox:
        """
        This can be removed once we fix all the uses of mailbox to just use context instead.
        """
        ...

    @property
    def proc_id(self) -> str:
        """
        The proc_id of the current actor.
        """
        return self.actor_id.proc_id

    @abstractproperty
    def actor_id(self) -> ActorId:
        """
        The actor_id of the current actor.
        """
        ...

    @property
    def proc(self) -> "ProcMesh":
        """
        The singleton proc mesh that corresponds to just this actor.
        """

        return self.proc_mesh.slice(**self.rank)

    """
    Every actor is spawned over some mesh of processes. This identifies the point in that mesh where
    the current actor was spawned. In other words, it is the `monarch.current_rank()` of
    The actors __init__ message.
    """
    rank: Point
    proc_mesh: "ProcMesh"
    _controller_controller: "_ControllerController"

    # this property is used to hold the handles to actors and processes launched by this actor
    # in order to keep them alive until this actor exits.
    _children: "Optional[List[ActorMesh | ProcMesh]]"

    def _add_child(self, child: "ActorMesh | ProcMesh") -> None:
        if self._children is None:
            self._children = [child]
        else:
            self._children.append(child)


@rust_struct("monarch_hyperactor::mailbox::Context")
class Context:
    @property
    def actor_instance(self) -> Instance:
        """
        Information about the actor currently running in this context.
        """
        ...

    @property
    def message_rank(self) -> Point:
        """
        Every message is sent as some broadcast of messages. This call identifies the
        point in this space where the current actor is participating.

        This is not the same self.actor_instance.rank: if the message was sent to some slice of
        actors this identifies where the actor appears in the slice and not the identity of the actor.

        These Point objects always exist. For singletons it will have 0 dimensions.
        """
        ...

    @staticmethod
    def _root_client_context() -> "Context": ...


_context: contextvars.ContextVar[Context] = contextvars.ContextVar(
    "monarch.actor_mesh._context"
)


def context() -> Context:
    c = _context.get(None)
    if c is None:
        c = Context._root_client_context()
        _context.set(c)
        from monarch._src.actor.host_mesh import create_local_host_mesh
        from monarch._src.actor.proc_mesh import _get_controller_controller

        c.actor_instance.proc_mesh, c.actor_instance._controller_controller = (
            _get_controller_controller()
        )
        c.actor_instance.proc_mesh._host_mesh = create_local_host_mesh()
    return c


@dataclass
class DebugContext:
    pdb_wrapper: Optional[PdbWrapper] = None

    @staticmethod
    def get() -> "DebugContext":
        return _debug_context.get()

    @staticmethod
    def set(debug_context: "DebugContext") -> None:
        _debug_context.set(debug_context)


_debug_context: contextvars.ContextVar[DebugContext] = contextvars.ContextVar(
    "monarch.actor_mesh._debug_context"
)

T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")
A = TypeVar("A")

# keep this load balancing deterministic, but
# equally distributed.
_load_balancing_seed = random.Random(4)


class _SingletonActorAdapator:
    def __init__(self, inner: ActorId, shape: Optional[Shape] = None) -> None:
        self._inner: ActorId = inner
        if shape is None:
            shape = singleton_shape
        self._shape = shape

    def cast(
        self,
        message: PythonMessage,
        selection: str,
        mailbox: Mailbox,
    ) -> None:
        mailbox.post(self._inner, message)

    def new_with_shape(self, shape: Shape) -> "ActorMeshProtocol":
        return _SingletonActorAdapator(self._inner, self._shape)

    def supervision_event(self) -> "Optional[Shared[Exception]]":
        return None

    def stop(self) -> "PythonTask[None]":
        raise NotImplementedError("stop()")

    def initialized(self) -> "PythonTask[None]":
        async def empty():
            pass

        return PythonTask.from_coroutine(empty())


# standin class for whatever is the serializable python object we use
# to name an actor mesh. Hacked up today because ActorMesh
# isn't plumbed to non-clients
class _ActorMeshRefImpl:
    def __init__(
        self,
        mailbox: Mailbox,
        hy_actor_mesh: Optional[PythonActorMeshImpl],
        proc_mesh: "Optional[ProcMesh]",
        shape: Shape,
        actor_ids: List[ActorId],
    ) -> None:
        self._mailbox = mailbox
        self._actor_mesh = hy_actor_mesh
        # actor meshes do not have a way to look this up at the moment,
        # so we fake it here
        self._proc_mesh = proc_mesh
        self._shape = shape
        self._please_replace_me_actor_ids = actor_ids

    @staticmethod
    def from_hyperactor_mesh(
        mailbox: Mailbox,
        shape: Shape,
        hy_actor_mesh: PythonActorMeshImpl,
        proc_mesh: "ProcMesh",
    ) -> "_ActorMeshRefImpl":
        return _ActorMeshRefImpl(
            mailbox,
            hy_actor_mesh,
            proc_mesh,
            shape,
            [cast(ActorId, hy_actor_mesh.get(i)) for i in range(len(shape))],
        )

    def __getstate__(
        self,
    ) -> Tuple[Shape, List[ActorId], Mailbox]:
        return self._shape, self._please_replace_me_actor_ids, self._mailbox

    def __setstate__(
        self,
        state: Tuple[Shape, List[ActorId], Mailbox],
    ) -> None:
        self._actor_mesh = None
        self._shape, self._please_replace_me_actor_ids, self._mailbox = state

    def _check_state(self) -> None:
        # This is temporary until we have real cast integration here. We need to actively check
        # supervision error here is because all communication is done through direct mailbox sending
        # and not through comm actor casting.
        # TODO: remove this when casting integration is done.
        if self._actor_mesh is not None:
            if self._actor_mesh.stopped:
                raise SupervisionError(
                    "actor mesh is unhealthy with reason: actor mesh is stopped due to proc mesh shutdown. "
                    "`PythonActorMesh` has already been stopped."
                )

            event = self._actor_mesh.get_supervision_event()
            if event is not None:
                raise SupervisionError(f"actor mesh is unhealthy with reason: {event}")

    def cast(
        self,
        message: PythonMessage,
        selection: str,
        mailbox: Mailbox,
    ) -> None:
        self._check_state()

        # TODO: use the actual actor mesh when available. We cannot currently use it
        # directly because we risk bifurcating the message delivery paths from the same
        # client, since slicing the mesh will produce a reference, which calls actors
        # directly. The reason these paths are bifurcated is that actor meshes will
        # use multicasting, while direct actor comms do not. Separately we need to decide
        # whether actor meshes are ordered with actor references.
        #
        # The fix is to provide a first-class reference into Python, and always call "cast"
        # on it, including for load balanced requests.
        if selection == "choose":
            idx = _load_balancing_seed.randrange(len(self._shape))
            actor_rank = self._shape.ndslice[idx]
            self._mailbox.post(self._please_replace_me_actor_ids[actor_rank], message)
        elif selection == "all":
            # replace me with actual remote actor mesh
            call_shape = Shape(
                self._shape.labels, NDSlice.new_row_major(self._shape.ndslice.sizes)
            )
            for i, rank in enumerate(self._shape.ranks()):
                self._mailbox.post_cast(
                    self._please_replace_me_actor_ids[rank],
                    i,
                    call_shape,
                    message,
                )
        elif isinstance(selection, int):
            try:
                self._mailbox.post(
                    self._please_replace_me_actor_ids[selection], message
                )
            except IndexError:
                raise IndexError(
                    f"Tried to send to an out-of-range rank {selection}: "
                    f"mesh has {len(self._please_replace_me_actor_ids)} elements."
                )
        else:
            raise ValueError(f"invalid selection: {selection}")

    def __len__(self) -> int:
        return len(self._shape)

    @property
    def _name_pid(self):
        actor_id0 = self._please_replace_me_actor_ids[0]
        return actor_id0.actor_name, actor_id0.pid

    @property
    def shape(self) -> Shape:
        return self._shape

    @property
    def proc_mesh(self) -> Optional["ProcMesh"]:
        return self._proc_mesh

    def new_with_shape(self, shape: Shape) -> "_ActorMeshRefImpl":
        return _ActorMeshRefImpl(
            self._mailbox, None, None, shape, self._please_replace_me_actor_ids
        )

    def supervision_event(self) -> "Optional[Shared[Exception]]":
        if self._actor_mesh is None:
            return None
        return self._actor_mesh.supervision_event()

    def stop(self) -> PythonTask[None]:
        async def task():
            if self._actor_mesh is not None:
                self._actor_mesh.stop()

        return PythonTask.from_coroutine(task())

    def initialized(self) -> PythonTask[None]:
        async def task():
            pass

        return PythonTask.from_coroutine(task())


class ActorEndpoint(Endpoint[P, R]):
    def __init__(
        self,
        actor_mesh: "ActorMeshProtocol",
        shape: Shape,
        proc_mesh: "Optional[ProcMesh]",
        name: MethodSpecifier,
        impl: Callable[Concatenate[Any, P], Awaitable[R]],
        mailbox: Mailbox,
        propagator: Propagator,
        explicit_response_port: bool,
    ) -> None:
        super().__init__(propagator)
        self._actor_mesh = actor_mesh
        self._name = name
        self._shape = shape
        self._proc_mesh = proc_mesh
        self._signature: inspect.Signature = inspect.signature(impl)
        self._mailbox = mailbox
        self._explicit_response_port = explicit_response_port

    def _call_name(self) -> Any:
        return self._name

    def _check_arguments(self, args, kwargs):
        if self._explicit_response_port:
            self._signature.bind(None, None, *args, **kwargs)
        else:
            self._signature.bind(None, *args, **kwargs)

    def _send(
        self,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        port: "Optional[Port]" = None,
        selection: Selection = "all",
    ) -> Extent:
        """
        Fire-and-forget broadcast invocation of the endpoint across all actors in the mesh.

        This sends the message to all actors but does not wait for any result.
        """
        self._check_arguments(args, kwargs)
        objects, buffer = flatten((args, kwargs), _is_ref_or_mailbox)
        if all(not hasattr(obj, "__monarch_ref__") for obj in objects):
            message = PythonMessage(
                PythonMessageKind.CallMethod(
                    self._name, None if port is None else port._port_ref
                ),
                buffer,
            )
            self._actor_mesh.cast(message, selection, self._mailbox)
        else:
            actor_send(self, bytes, objects, port, selection)
        shape = self._shape
        return Extent(shape.labels, shape.ndslice.sizes)

    def _port(self, once: bool = False) -> "Tuple[Port[R], PortReceiver[R]]":
        p, r = super()._port(once=once)
        monitor: Optional[Shared[Exception]] = self._actor_mesh.supervision_event()
        r._set_monitor(monitor)
        return (p, r)

    def _rref(self, args, kwargs):
        self._check_arguments(args, kwargs)
        refs, bytes = flatten((args, kwargs), _is_ref_or_mailbox)

        return actor_rref(self, bytes, refs)


@overload
def as_endpoint(
    not_an_endpoint: Callable[P, R],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[False] = False,
) -> Endpoint[P, R]: ...


@overload
def as_endpoint(
    not_an_endpoint: Callable[Concatenate["PortProtocol[R]", P], None],
    *,
    propagate: Propagator = None,
    explicit_response_port: Literal[True],
) -> Endpoint[P, R]: ...


def as_endpoint(
    not_an_endpoint: Any,
    *,
    propagate: Propagator = None,
    explicit_response_port: bool = False,
):
    if not isinstance(not_an_endpoint, NotAnEndpoint):
        raise ValueError("expected an method of a spawned actor")
    kind = (
        MethodSpecifier.ExplicitPort
        if explicit_response_port
        else MethodSpecifier.ReturnsResponse
    )
    return not_an_endpoint._ref._endpoint(
        kind(not_an_endpoint._name),
        getattr(not_an_endpoint._ref, not_an_endpoint._name),
        propagate,
        explicit_response_port,
    )


class Accumulator(Generic[P, R, A]):
    """
    Accumulate the result of a broadcast invocation of an endpoint
    across a sliced mesh.

    Usage:
            >>> counter = Accumulator(Actor.increment, 0, lambda x, y: x + y)
    """

    def __init__(
        self, endpoint: Endpoint[P, R], identity: A, combine: Callable[[A, R], A]
    ) -> None:
        """
        Args:
            endpoint: Endpoint to accumulate the result of.
            identity: Initial value of the accumulated value before the first combine invocation.
            combine: Lambda invoked for combining the result of the endpoint with the accumulated value.
        """
        self._endpoint: Endpoint[P, R] = endpoint
        self._identity: A = identity
        self._combine: Callable[[A, R], A] = combine

    def accumulate(self, *args: P.args, **kwargs: P.kwargs) -> "Future[A]":
        """
        Accumulate the result of the endpoint invocation.

        Args:
            args: Arguments to pass to the endpoint.
            kwargs: Keyword arguments to pass to the endpoint.

        Returns:
            Future that resolves to the accumulated value.
        """
        gen: Generator[Future[R], None, None] = self._endpoint.stream(*args, **kwargs)

        async def impl() -> A:
            value = self._identity
            for x in gen:
                value = self._combine(value, await x)
            return value

        return Future(coro=impl())


class ValueMesh(MeshTrait, Generic[R]):
    """
    A mesh that holds the result of an endpoint invocation.
    """

    def __init__(self, shape: Shape, values: List[R]) -> None:
        self._shape = shape
        self._values = values

    def _new_with_shape(self, shape: Shape) -> "ValueMesh[R]":
        return ValueMesh(shape, self._values)

    def item(self, **kwargs) -> R:
        """
        Get the value at the given coordinates.

        Args:
            kwargs: Coordinates to get the value at.

        Returns:
            Value at the given coordinate.

        Raises:
            KeyError: If invalid coordinates are provided.
        """
        coordinates = [kwargs.pop(label) for label in self._labels]
        if kwargs:
            raise KeyError(f"item has extra dimensions: {list(kwargs.keys())}")

        return self._values[self._ndslice.nditem(coordinates)]

    def items(self) -> Iterable[Tuple[Point, R]]:
        """
        Generator that returns values for the provided coordinates.

        Returns:
            Values at all coordinates.
        """
        extent = self._shape.extent
        for i, rank in enumerate(self._shape.ranks()):
            yield Point(i, extent), self._values[rank]

    def __iter__(self) -> Iterator[Tuple[Point, R]]:
        return iter(self.items())

    def __repr__(self) -> str:
        body = indent(pformat(tuple(self.items())), "  ")
        return f"ValueMesh({self._shape.extent}):\n{body}"

    @property
    def _ndslice(self) -> NDSlice:
        return self._shape.ndslice

    @property
    def _labels(self) -> Iterable[str]:
        return self._shape.labels


def send(
    endpoint: Endpoint[P, R],
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    port: "Optional[Port]" = None,
    selection: Selection = "all",
) -> None:
    """
        Fire-and-forget broadcast invocation of the endpoint across a given selection of the mesh.

        This sends the message to all actors but does not wait for any result. Use the port provided to
        send the response back to the caller.

    Args:
        endpoint: Endpoint to invoke.
        args: Arguments to pass to the endpoint.
        kwargs: Keyword arguments to pass to the endpoint.
        port: Handle to send the response to.
        selection: Selection query representing a subset of the mesh.
    """
    endpoint._send(args, kwargs, port, selection)


class Port(Generic[R]):
    """
    Handle used to send reliable in-order messages through a channel to
    a PortReceiver.
    """

    def __init__(
        self,
        port_ref: PortRef | OncePortRef,
        mailbox: Mailbox,
        rank: Optional[int],
    ) -> None:
        self._port_ref = port_ref
        self._mailbox = mailbox
        self._rank = rank

    def send(self, obj: R) -> None:
        """
            Fire-and-forget send R-typed objects in order
            through a channel to its corresponding PortReceiver.

        Args:
            obj: R-typed object to send.
        """
        self._port_ref.send(
            self._mailbox,
            PythonMessage(PythonMessageKind.Result(self._rank), _pickle(obj)),
        )

    def exception(self, obj: Exception) -> None:
        # we deliver each error exactly once, so if there is no port to respond to,
        # the error is sent to the current actor as an exception.
        self._port_ref.send(
            self._mailbox,
            PythonMessage(PythonMessageKind.Exception(self._rank), _pickle(obj)),
        )


class DroppingPort:
    """
    Used in place of a real port when the message has no response port.
    Makes sure any exception sent to it causes the actor to report an exception.
    """

    def __init__(self):
        pass

    def send(self, obj: Any) -> None:
        pass

    def exception(self, obj: Exception) -> None:
        # we deliver each error exactly once, so if there is no port to respond to,
        # the error is sent to the current actor as an exception.
        raise obj from None


R = TypeVar("R")

T = TypeVar("T")


# advance lower-level API for sending messages. This is intentially
# not part of the Endpoint API because they way it accepts arguments
# and handles concerns is different.
class Channel(Generic[R]):
    """
    An advanced low level API for a communication channel used for message passing
    between actors.

    Provides static methods to create communication channels with port pairs
    for sending and receiving messages of type R.
    """

    @staticmethod
    def open(once: bool = False) -> Tuple["Port[R]", "PortReceiver[R]"]:
        """ """
        mailbox = context().actor_instance._mailbox
        handle, receiver = mailbox.open_once_port() if once else mailbox.open_port()
        port_ref = handle.bind()
        return (
            Port(port_ref, mailbox, rank=None),
            PortReceiver(mailbox, receiver),
        )

    @staticmethod
    def open_ranked(once: bool = False) -> Tuple["Port[R]", "RankedPortReceiver[R]"]:
        send, recv = Channel[R].open()
        return (send, recv.ranked())


class PortReceiver(Generic[R]):
    """
    Receiver for messages sent through a communication channel.

    Handles receiving R-typed objects sent from a corresponding Port.
    Asynchronously message reception with optional supervision
    monitoring for error handling.
    """

    def __init__(
        self,
        mailbox: Mailbox,
        receiver: "PortReceiverBase",
        monitor: "Optional[Shared[Exception]]" = None,
    ) -> None:
        self._mailbox: Mailbox = mailbox
        self._monitor = monitor
        self._receiver = receiver

    async def _recv(self) -> R:
        awaitable = self._receiver.recv_task()
        if self._monitor is None:
            result = await awaitable
        else:
            # type: ignore
            result, i = await PythonTask.select_one([self._monitor.task(), awaitable])
            if i == 0:
                raise result
        return self._process(result)

    def _process(self, msg: PythonMessage) -> R:
        # TODO: Try to do something more structured than a cast here
        payload = cast(R, unflatten(msg.message, itertools.repeat(self._mailbox)))
        match msg.kind:
            case PythonMessageKind.Result():
                return payload
            case PythonMessageKind.Exception():
                raise cast(Exception, payload)
            case _:
                raise ValueError(f"Unexpected message kind: {msg.kind}")

    def recv(self) -> "Future[R]":
        return Future(coro=self._recv())

    def ranked(self) -> "RankedPortReceiver[R]":
        return RankedPortReceiver[R](self._mailbox, self._receiver, self._monitor)

    def _set_monitor(self, monitor: "Optional[Shared[Exception]]"):
        self._monitor = monitor


class RankedPortReceiver(PortReceiver[Tuple[int, R]]):
    def _process(self, msg: PythonMessage) -> Tuple[int, R]:
        rank = getattr(msg.kind, "rank", None)
        if rank is None:
            raise ValueError(
                f"RankedPort receiver got a message without a rank {msg}",
            )
        return rank, super()._process(msg)


singleton_shape = Shape([], NDSlice(offset=0, sizes=[], strides=[]))


# Currently the synchronous function of actors are run on a python thread that has an active event loop.
# Technically it is unsafe for them to block at all because they will block the loop of other
# calls, so all calls to .get() should be failing.
# But in the meantime, to implement get() by reusing async functions,
#  we need to signal to the consumer of the PythonTask object that the thread really isn't in an async context.
# We do this by blanking out the running event loop during the call to the synchronous actor function.

MESSAGES_HANDLED = METER.create_counter("py_mesages_handled")


class _Actor:
    """
    This is the message handling implementation of a Python actor.

    The layering goes:
        Rust `PythonActor` -> `_Actor` -> user-provided `Actor` instance

    Messages are received from the Rust backend, and forwarded to the `handle`
    methods on this class.

    This class wraps the actual `Actor` instance provided by the user, and
    routes messages to it, managing argument serialization/deserialization and
    error handling.
    """

    def __init__(self) -> None:
        self.instance: object | None = None
        # TODO: (@pzhang) remove this with T229200522
        self._saved_error: ActorError | None = None

    async def handle(
        self,
        ctx: Context,
        method: MethodSpecifier,
        message: FrozenBuffer,
        panic_flag: PanicFlag,
        local_state: Iterable[Any],
        response_port: "PortProtocol[Any]",
    ) -> None:
        MESSAGES_HANDLED.add(1)
        # response_port can be None. If so, then sending to port will drop the response,
        # and raise any exceptions to the caller.
        try:
            _context.set(ctx)

            DebugContext.set(DebugContext())

            args, kwargs = unflatten(message, local_state)

            match method:
                case MethodSpecifier.Init():
                    ins = ctx.actor_instance
                    Class, ins.proc_mesh, ins._controller_controller, *args = args
                    ins.rank = ctx.message_rank
                    try:
                        self.instance = Class(*args, **kwargs)
                        self._maybe_exit_debugger()
                    except Exception as e:
                        self._saved_error = ActorError(
                            e, f"Remote actor {Class}.__init__ call failed."
                        )
                        raise e
                    response_port.send(None)
                    return None
                case MethodSpecifier.ReturnsResponse(name=method_name):
                    pass
                case MethodSpecifier.ExplicitPort(name=method_name):
                    args = (response_port, *args)
                    response_port = DroppingPort()

            if self.instance is None:
                # This could happen because of the following reasons. Both
                # indicates a possible bug in the framework:
                # 1. the execution of the previous message for "__init__" failed,
                #    but that error is not surfaced to the caller.
                #      - TODO(T229200522): there is a known bug. fix it.
                # 2. this message is delivered to this actor before the previous
                #    message of "__init__" is delivered. Out-of-order delivery
                #    should never happen. It indicates either a bug in the
                #    message delivery mechanism, or the framework accidentally
                #    mixed the usage of cast and direct send.

                error_message = f"Actor object is missing when executing method {method_name} on actor {ctx.actor_instance.actor_id}."
                if self._saved_error is not None:
                    error_message += (
                        f" This is likely due to an earlier error: {self._saved_error}"
                    )
                raise AssertionError(error_message)

            the_method = getattr(self.instance, method_name)
            if isinstance(the_method, EndpointProperty):
                the_method = functools.partial(the_method._method, self.instance)

            if inspect.iscoroutinefunction(the_method):

                async def instrumented():
                    with TRACER.start_as_current_span(
                        method_name,
                        attributes={"actor_id": str(ctx.actor_instance.actor_id)},
                    ):
                        try:
                            result = await the_method(*args, **kwargs)
                            self._maybe_exit_debugger()
                        except Exception as e:
                            logging.critical(
                                "Unhandled exception in actor endpoint",
                                exc_info=e,
                            )
                            raise e
                    return result

                result = await instrumented()
            else:
                with TRACER.start_as_current_span(
                    method_name,
                    attributes={"actor_id": str(ctx.actor_instance.actor_id)},
                ):
                    with fake_sync_state():
                        result = the_method(*args, **kwargs)
                    self._maybe_exit_debugger()

            response_port.send(result)
        except Exception as e:
            self._post_mortem_debug(e.__traceback__)
            response_port.exception(ActorError(e))
        except BaseException as e:
            self._post_mortem_debug(e.__traceback__)
            # A BaseException can be thrown in the case of a Rust panic.
            # In this case, we need a way to signal the panic to the Rust side.
            # See [Panics in async endpoints]
            try:
                panic_flag.signal_panic(e)
            except Exception:
                # The channel might be closed if the Rust side has already detected the error
                pass
            raise

    def _maybe_exit_debugger(self, do_continue=True) -> None:
        if (pdb_wrapper := DebugContext.get().pdb_wrapper) is not None:
            if do_continue:
                pdb_wrapper.clear_all_breaks()
                pdb_wrapper.do_continue("")
            pdb_wrapper.end_debug_session()
        DebugContext.set(DebugContext())

    def _post_mortem_debug(self, exc_tb) -> None:
        from monarch._src.actor.debugger.debug_controller import debug_controller

        if (pdb_wrapper := DebugContext.get().pdb_wrapper) is not None:
            with fake_sync_state():
                ctx = context()
                msg_rank = ctx.message_rank
                pdb_wrapper = PdbWrapper(
                    msg_rank.rank,
                    {k: msg_rank[k] for k in msg_rank},
                    ctx.actor_instance.actor_id,
                    debug_controller(),
                )
                DebugContext.set(DebugContext(pdb_wrapper))
                pdb_wrapper.post_mortem(exc_tb)
                self._maybe_exit_debugger(do_continue=False)

    def _handle_undeliverable_message(
        self, message: UndeliverableMessageEnvelope
    ) -> bool:
        handle_undeliverable = getattr(
            self.instance, "_handle_undeliverable_message", None
        )
        if handle_undeliverable is not None:
            return handle_undeliverable(message)
        else:
            return False


def _is_mailbox(x: object) -> bool:
    if hasattr(x, "__monarch_ref__"):
        raise NotImplementedError(
            "Sending monarch tensor references directly to a port."
        )
    return isinstance(x, Mailbox)


def _is_ref_or_mailbox(x: object) -> bool:
    return hasattr(x, "__monarch_ref__") or isinstance(x, Mailbox)


def _pickle(obj: object) -> bytes | FrozenBuffer:
    _, buff = flatten(obj, _is_mailbox)
    return buff


class Actor(MeshTrait, DeprecatedNotAFuture):
    @functools.cached_property
    def logger(cls) -> logging.Logger:
        lgr = logging.getLogger(cls.__class__.__name__)
        lgr.setLevel(logging.DEBUG)
        return lgr

    @property
    def _ndslice(self) -> NDSlice:
        raise NotImplementedError(
            "actor implementations are not meshes, but we can't convince the typechecker of it..."
        )

    @property
    def _labels(self) -> Tuple[str, ...]:
        raise NotImplementedError(
            "actor implementations are not meshes, but we can't convince the typechecker of it..."
        )

    def _new_with_shape(self, shape: Shape) -> Self:
        raise NotImplementedError(
            "actor implementations are not meshes, but we can't convince the typechecker of it..."
        )

    @property
    def initialized(self):
        raise NotImplementedError(
            "actor implementations are not meshes, but we can't convince the typechecker of it..."
        )

    def _handle_undeliverable_message(
        self, message: UndeliverableMessageEnvelope
    ) -> bool:
        # Return False to indicate that the undeliverable message was not handled.
        return False


class ActorMesh(MeshTrait, Generic[T], DeprecatedNotAFuture):
    """
    A group of actor instances of the same class.

    Represents a collection of T-typed actor instances spawned at most once per process
    that can be communicated with collectively or individually. Provides
    methods for spawning actors, managing their lifecycle, and creating
    endpoints for method invocation across the mesh.
    """

    def __init__(
        self,
        Class: Type[T],
        inner: "ActorMeshProtocol",
        mailbox: Mailbox,
        shape: Shape,
        proc_mesh: "Optional[ProcMesh]",
    ) -> None:
        self.__name__: str = Class.__name__
        self._class: Type[T] = Class
        self._inner: "ActorMeshProtocol" = inner
        self._mailbox: Mailbox = mailbox
        self._shape = shape
        self._proc_mesh = proc_mesh
        for attr_name in dir(self._class):
            attr_value = getattr(self._class, attr_name, None)
            if isinstance(attr_value, EndpointProperty):
                # Convert string method name to appropriate MethodSpecifier
                kind = (
                    MethodSpecifier.ExplicitPort
                    if attr_value._explicit_response_port
                    else MethodSpecifier.ReturnsResponse
                )
                setattr(
                    self,
                    attr_name,
                    self._endpoint(
                        kind(attr_name),
                        attr_value._method,
                        attr_value._propagator,
                        attr_value._explicit_response_port,
                    ),
                )

    def __getattr__(self, attr: str) -> NotAnEndpoint:
        if attr in dir(self._class):
            return NotAnEndpoint(self, attr)
        raise AttributeError(attr)

    def _endpoint(
        self,
        name: MethodSpecifier,
        impl: Callable[Concatenate[Any, P], Awaitable[R]],
        propagator: Any,
        explicit_response_port: bool,
    ):
        return ActorEndpoint(
            self._inner,
            self._shape,
            self._proc_mesh,
            name,
            impl,
            self._mailbox,
            propagator,
            explicit_response_port,
        )

    @classmethod
    def _create(
        cls,
        Class: Type[T],
        actor_mesh: "PythonActorMesh | PythonActorMeshImpl",
        mailbox: Mailbox,
        shape: Shape,
        proc_mesh: "ProcMesh",
        controller_controller: Optional["_ControllerController"],
        # args and kwargs are passed to the __init__ method of the user defined
        # python actor object.
        *args: Any,
        **kwargs: Any,
    ) -> "ActorMesh[T]":
        if isinstance(actor_mesh, PythonActorMeshImpl):
            actor_mesh = _ActorMeshRefImpl.from_hyperactor_mesh(
                mailbox, shape, actor_mesh, proc_mesh
            )

        mesh = cls(Class, actor_mesh, mailbox, shape, proc_mesh)

        async def null_func(*_args: Iterable[Any], **_kwargs: Dict[str, Any]) -> None:
            return None

        # send __init__ message to the mesh to initialize the user defined
        # python actor object.
        ep = mesh._endpoint(
            MethodSpecifier.Init(),
            null_func,
            None,
            False,
        )
        send(ep, (mesh._class, proc_mesh, controller_controller, *args), kwargs)

        return mesh

    @classmethod
    def from_actor_id(
        cls,
        Class: Type[T],
        actor_id: ActorId,
        mailbox: Mailbox,
    ) -> "ActorMesh[T]":
        return cls(
            Class, _SingletonActorAdapator(actor_id), mailbox, singleton_shape, None
        )

    def __reduce_ex__(self, protocol: ...) -> "Tuple[Type[ActorMesh], Tuple[Any, ...]]":
        return ActorMesh, (self._class, self._inner, self._mailbox, self._shape, None)

    @property
    def _ndslice(self) -> NDSlice:
        return self._shape.ndslice

    @property
    def _labels(self) -> Iterable[str]:
        return self._shape.labels

    def _new_with_shape(self, shape: Shape) -> "ActorMesh[T]":
        sliced = self._inner.new_with_shape(shape)
        return ActorMesh(self._class, sliced, self._mailbox, shape, self._proc_mesh)

    def __repr__(self) -> str:
        return f"ActorMesh(class={self._class}, shape={self._shape}), inner={type(self._inner)})"

    def stop(self) -> "Future[None]":
        return Future(coro=self._inner.stop())

    @property
    def initialized(self) -> Future[None]:
        return Future(coro=self._inner.initialized())


class ActorError(Exception):
    """
    Deterministic problem with the user's code.
    For example, an OOM resulting in trying to allocate too much GPU memory, or violating
    some invariant enforced by the various APIs.
    """

    def __init__(
        self,
        exception: Exception,
        message: str = "A remote actor call has failed.",
    ) -> None:
        self.exception = exception
        # Need to stringify the exception early, because the PyPI package
        # exceptiongroup may monkeypatch the "TracebackException" class for python
        # versions < 3.11. If it gets unpickled in a different scope without
        # using that monkeypatch, it'll have an exception in "format()".
        # Store the traceback string instead which shouldn't change between machines.
        actor_mesh_ref_tb = TracebackException.from_exception(exception).format()
        # Replace any traceback lines to indicate it's a remote call traceback.
        actor_mesh_ref_tb = (
            s.replace(
                "Traceback (most recent call last):",
                "Traceback of where the remote call failed (most recent call last):",
            )
            for s in actor_mesh_ref_tb
        )
        self.exception_formatted = "".join(actor_mesh_ref_tb)
        self.message = message

    def __str__(self) -> str:
        return f"{self.message}\n {self.exception_formatted}"


def current_actor_name() -> str:
    return str(context().actor_instance.actor_id)


def current_rank() -> Point:
    return context().message_rank


def current_size() -> Dict[str, int]:
    r = context().message_rank.extent
    return {k: r[k] for k in r}
