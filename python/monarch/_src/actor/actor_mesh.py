# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
import collections
import contextvars
import functools
import inspect
import itertools
import logging
import threading
import warnings
from abc import abstractproperty

from dataclasses import dataclass

from functools import cache
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
from monarch._rust_bindings.monarch_hyperactor.actor_mesh import PythonActorMesh
from monarch._rust_bindings.monarch_hyperactor.buffers import FrozenBuffer
from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import configure
from monarch._rust_bindings.monarch_hyperactor.context import Instance as HyInstance
from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    Mailbox,
    OncePortRef,
    PortRef,
    UndeliverableMessageEnvelope,
)
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.selection import (
    Selection as HySelection,  # noqa: F401
)
from monarch._rust_bindings.monarch_hyperactor.shape import (
    Point as HyPoint,
    Region,
    Shape,
)
from monarch._rust_bindings.monarch_hyperactor.value_mesh import (
    ValueMesh as HyValueMesh,
)
from monarch._src.actor import config
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
from monarch._src.actor.future import Future
from monarch._src.actor.pickle import flatten, unflatten
from monarch._src.actor.python_extension_methods import rust_struct
from monarch._src.actor.shape import MeshTrait, NDSlice
from monarch._src.actor.sync_state import fake_sync_state
from monarch._src.actor.telemetry import METER

from monarch._src.actor.tensor_engine_shim import actor_rref, actor_send
from opentelemetry.metrics import Counter
from opentelemetry.trace import Tracer
from typing_extensions import Self

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.actor import PortProtocol
    from monarch._rust_bindings.monarch_hyperactor.actor_mesh import ActorMeshProtocol
    from monarch._rust_bindings.monarch_hyperactor.mailbox import PortReceiverBase
    from monarch._src.actor.host_mesh import HostMesh
    from monarch._src.actor.proc_mesh import _ControllerController, ProcMesh
from monarch._src.actor.telemetry import get_monarch_tracer

logger: logging.Logger = logging.getLogger(__name__)

TRACER: Tracer = get_monarch_tracer()

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


@rust_struct("monarch_hyperactor::context::Instance")
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
    name: str  # the name this actor was given on spawn
    class_name: str  # the fully qualified class name of the actor.
    creator: Optional[
        "CreatorInstance"
    ]  # information about the actor who spawned this actor
    # None if this actor is the spawning actor.

    # this property is used to hold the handles to actors and processes launched by this actor
    # in order to keep them alive until this actor exits.
    _children: "Optional[List[ActorMesh[Any] | ProcMesh]]"

    def _add_child(self, child: "ActorMesh[Any] | ProcMesh") -> None:
        if self._children is None:
            self._children = [child]
        else:
            self._children.append(child)

    def _as_rust(self) -> HyInstance:
        return cast(HyInstance, self)

    @staticmethod
    def _as_py(ins: HyInstance) -> "Instance":
        return cast(Instance, ins)

    def _as_creator(self) -> "CreatorInstance":
        return CreatorInstance(
            self.rank,
            self.proc_mesh,
            self.proc,
            self.name,
            self.class_name,
            self.creator,
        )

    def __repr__(self) -> str:
        return _qualified_name(self)


@dataclass
class CreatorInstance:
    """
    An instance that can be serialized so it can be passed around
    to describe the creation hierarchy of an actor instance.
    """

    rank: Point
    proc_mesh: "ProcMesh"
    proc: "ProcMesh"
    name: str
    class_name: Optional[str]
    creator: Optional["CreatorInstance"]

    def __repr__(self) -> str:
        return _qualified_name(self)


def _qualified_name(ins: "CreatorInstance | Instance | None") -> str:
    names = []
    while ins:
        class_prefix = "" if ins.class_name is None else f"{ins.class_name} "
        rank_postfix = str(ins.rank) if len(ins.rank) > 0 else ""
        names.append(f"<{class_prefix}{ins.name}{rank_postfix}>")
        ins = ins.creator
    return ".".join(reversed(names))


@rust_struct("monarch_hyperactor::context::Context")
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


class _ActorFilter(logging.Filter):
    def __init__(self) -> None:
        super().__init__()

    def filter(self, record: Any) -> bool:
        try:
            if not config.prefix_python_logs_with_actor:
                return True
            ctx = _context.get(None)
            if ctx is not None:
                record.msg = f"[actor={ctx.actor_instance}] {record.msg}"
        except Exception as e:
            warnings.warn(
                f"failed to add monarch actor information to python logs: {e}",
                stacklevel=2,
            )
        return True


@cache
def _init_context_log_handler() -> None:
    af: _ActorFilter = _ActorFilter()
    logger = logging.getLogger()
    for handler in logger.handlers:
        handler.addFilter(af)

    _original_addHandler: Any = logging.Logger.addHandler

    def _patched_addHandler(self: logging.Logger, hdlr: logging.Handler) -> None:
        _original_addHandler(self, hdlr)
        if af not in hdlr.filters:
            hdlr.addFilter(af)

    # pyre-ignore[8]: Intentionally monkey-patching Logger.addHandler
    logging.Logger.addHandler = _patched_addHandler


def _set_context(c: Context) -> None:
    _init_context_log_handler()
    _context.set(c)


T = TypeVar("T")


class _Lazy(Generic[T]):
    def __init__(self, init: Callable[[], T]) -> None:
        self._lock = threading.Lock()
        self._val: Optional[T] = None
        self._init = init

    def get(self) -> T:
        with self._lock:
            if not self._val:
                self._val = self._init()
            return self._val

    def try_get(self) -> Optional[T]:
        return self._val


def _init_this_host_for_fake_in_process_host() -> "HostMesh":
    from monarch._src.actor.host_mesh import create_local_host_mesh

    return create_local_host_mesh()


_this_host_for_fake_in_process_host: _Lazy["HostMesh"] = _Lazy(
    _init_this_host_for_fake_in_process_host
)


def _init_root_proc_mesh() -> "ProcMesh":
    from monarch._src.actor.host_mesh import fake_in_process_host

    return fake_in_process_host()._spawn_nonblocking(
        name="root_client_proc_mesh",
        per_host=Extent([], []),
        setup=None,
        _attach_controller_controller=False,  # can't attach the controller controller because it doesn't exist yet
    )


_root_proc_mesh: _Lazy["ProcMesh"] = _Lazy(_init_root_proc_mesh)


def context() -> Context:
    c = _context.get(None)
    if c is None:
        c = Context._root_client_context()
        _set_context(c)

        from monarch._src.actor.host_mesh import create_local_host_mesh
        from monarch._src.actor.proc_mesh import _get_controller_controller
        from monarch._src.actor.v1 import enabled as v1_enabled

        if not v1_enabled:
            c.actor_instance.proc_mesh, c.actor_instance._controller_controller = (
                _get_controller_controller()
            )

            c.actor_instance.proc_mesh._host_mesh = create_local_host_mesh()  # type: ignore
        else:
            c.actor_instance.proc_mesh = _root_proc_mesh.get()

            # This needs to be initialized when the root client context is initialized.
            # Otherwise, it will be initialized inside an actor endpoint running inside
            # a fake in-process host. That will fail with an "unroutable mesh" error,
            # because the hyperactor Proc being used to spawn the local host mesh
            # won't have the correct type of forwarder.
            _this_host_for_fake_in_process_host.get()

            c.actor_instance._controller_controller = _get_controller_controller()[1]
    return c


_transport: Optional[ChannelTransport] = None
_transport_lock = threading.Lock()


def enable_transport(transport: "ChannelTransport | str") -> None:
    """
    Allow monarch to communicate with transport type 'transport'
    This must be called before any other calls in the monarch API.
    If it isn't called, we will implicitly call
    `monarch.enable_transport(ChannelTransport.Unix)` on the first monarch call.

    Currently only one transport type may be enabled at one time.
    In the future we may allow multiple to be enabled.

    For Meta usage, use metatls-hostname
    """
    if isinstance(transport, str):
        transport = {
            "tcp": ChannelTransport.TcpWithHostname,
            "ipc": ChannelTransport.Unix,
            "metatls": ChannelTransport.MetaTlsWithIpV6,
            "metatls-hostname": ChannelTransport.MetaTlsWithHostname,
        }.get(transport)
        if transport is None:
            raise ValueError(f"unknown transport: {transport}")

    if _context.get(None) is not None:
        raise RuntimeError(
            "`enable_transport()` must be called before any other calls in the monarch API. "
            "If it isn't called, we will implicitly call `monarch.enable_transport(ChannelTransport.Unix)` "
            "on the first monarch call."
        )

    global _transport
    with _transport_lock:
        if _transport is not None and _transport != transport:
            raise RuntimeError(
                f"Only one transport type may be enabled at one time. "
                f"Currently enabled transport type is `{_transport}`. "
                f"Attempted to enable transport type `{transport}`."
            )
        _transport = transport
    configure(default_transport=transport)


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


class _SingletonActorAdapator:
    def __init__(self, inner: ActorId, region: Optional[Region] = None) -> None:
        self._inner: ActorId = inner
        if region is None:
            region = singleton_shape.region
        self._region = region

    def cast(
        self,
        message: PythonMessage,
        selection: str,
        instance: HyInstance,
    ) -> None:
        Instance._as_py(instance)._mailbox.post(self._inner, message)

    def new_with_region(self, region: Region) -> "ActorMeshProtocol":
        return _SingletonActorAdapator(self._inner, self._region)

    def supervision_event(self, instance: HyInstance) -> "Optional[Shared[Exception]]":
        return None

    def start_supervision(self, instance: HyInstance) -> None:
        return None

    def stop(self, instance: HyInstance) -> "PythonTask[None]":
        raise NotImplementedError("stop()")

    def initialized(self) -> "PythonTask[None]":
        async def empty() -> None:
            pass

        return PythonTask.from_coroutine(empty())


class ActorEndpoint(Endpoint[P, R]):
    def __init__(
        self,
        actor_mesh: "ActorMeshProtocol",
        shape: Shape,
        proc_mesh: "Optional[ProcMesh]",
        name: MethodSpecifier,
        impl: Callable[Concatenate[Any, P], Awaitable[R]],
        propagator: Propagator,
        explicit_response_port: bool,
    ) -> None:
        super().__init__(propagator)
        self._actor_mesh = actor_mesh
        self._name = name
        self._shape = shape
        self._proc_mesh = proc_mesh
        self._signature: inspect.Signature = inspect.signature(impl)
        self._explicit_response_port = explicit_response_port

    def _call_name(self) -> MethodSpecifier:
        return self._name

    def _check_arguments(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> None:
        if self._explicit_response_port:
            self._signature.bind(None, None, *args, **kwargs)
        else:
            self._signature.bind(None, *args, **kwargs)

    def _send(
        self,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        port: "Optional[Port[R]]" = None,
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
            self._actor_mesh.cast(
                message, selection, context().actor_instance._as_rust()
            )
        else:
            actor_send(self, buffer, objects, port, selection)
        shape = self._shape
        return Extent(shape.labels, shape.ndslice.sizes)

    def _port(self, once: bool = False) -> "Tuple[Port[R], PortReceiver[R]]":
        p, r = super()._port(once=once)
        instance = context().actor_instance._as_rust()
        monitor: Optional[Shared[Exception]] = self._actor_mesh.supervision_event(
            instance
        )
        r._set_monitor(monitor)
        return (p, r)

    def _rref(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> R:
        self._check_arguments(args, kwargs)
        refs, buffer = flatten((args, kwargs), _is_ref_or_mailbox)

        return actor_rref(self, buffer, refs)


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
) -> Any:
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
        self._hy: HyValueMesh = HyValueMesh(shape, values)

    def _new_with_shape(self, shape: Shape) -> "ValueMesh[R]":
        # Build a map from current global ranks -> local indices.
        cur_ranks = list(self._shape.ranks())
        pos = {g: i for i, g in enumerate(cur_ranks)}
        # For each global rank of the target shape, pull from our
        # current local index.
        remapped = [self._hy.get(pos[g]) for g in shape.ranks()]
        return ValueMesh(shape, remapped)

    def item(self, **kwargs: int) -> R:
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

        global_rank = self._ndslice.nditem(coordinates)  # May include offset.
        # Map global -> local (position in this shape's rank order).
        ranks = list(self._shape.ranks())
        try:
            local_idx = ranks.index(global_rank)
        except ValueError:
            # Shouldn't happen if Shape is consistent, but keep a clear
            # error.
            raise IndexError(f"rank {global_rank} not in current shape")
        return self._hy.get(local_idx)

    def items(self) -> Iterable[Tuple[Point, R]]:
        """
        Generator that returns values for the provided coordinates.

        Returns:
            Values at all coordinates.
        """
        extent = self._shape.extent
        for i, _global_rank in enumerate(self._shape.ranks()):
            yield Point(i, extent), self._hy.get(i)

    def values(self) -> Iterable[R]:
        """
        Generator that iterates over just the values in the mesh.

        Returns:
            Values at all coordinates.
        """
        for _, value in self.items():
            yield value

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

    def __getstate__(self) -> Dict[str, Any]:
        return {"shape": self._shape, "values": self._hy.values()}

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self._shape = state["shape"]
        vals = state["values"]
        self._hy = HyValueMesh(self._shape, vals)


def send(
    endpoint: Endpoint[P, R],
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    port: "Optional[Port[R]]" = None,
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
        instance: HyInstance,
        rank: Optional[int],
    ) -> None:
        self._port_ref = port_ref
        self._instance = instance
        self._rank = rank

    def send(self, obj: R) -> None:
        """
            Fire-and-forget send R-typed objects in order
            through a channel to its corresponding PortReceiver.

        Args:
            obj: R-typed object to send.
        """
        self._port_ref.send(
            self._instance,
            PythonMessage(PythonMessageKind.Result(self._rank), _pickle(obj)),
        )

    def exception(self, obj: Exception) -> None:
        # we deliver each error exactly once, so if there is no port to respond to,
        # the error is sent to the current actor as an exception.
        self._port_ref.send(
            self._instance,
            PythonMessage(PythonMessageKind.Exception(self._rank), _pickle(obj)),
        )

    def __reduce__(self) -> Tuple[Any, Tuple[Any, ...]]:
        """
        When Port is sent over the wire, we do not want to send the actor instance
        from the current context. Instead, we want to reconstruct the Port with
        the receiver's context, since that is where the message will be sent
        from through this port.
        """

        def _reconstruct_port(
            port_ref: PortRef | OncePortRef, rank: Optional[int]
        ) -> "Port[R]":
            instance = context().actor_instance._as_rust()
            return Port(port_ref, instance, rank)

        return (
            _reconstruct_port,
            (self._port_ref, self._rank),
        )


class DroppingPort:
    """
    Used in place of a real port when the message has no response port.
    Makes sure any exception sent to it causes the actor to report an exception.
    """

    def __init__(self) -> None:
        pass

    def send(self, obj: Any) -> None:
        pass

    def exception(self, obj: Exception) -> None:
        if isinstance(obj, ActorError):
            obj = obj.exception
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
        actor_instance = context().actor_instance
        mailbox = actor_instance._mailbox
        handle, receiver = mailbox.open_once_port() if once else mailbox.open_port()
        port_ref = handle.bind()
        hy_instance = actor_instance._as_rust()
        port = Port(port_ref, hy_instance, None)
        return (
            port,
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

    def _set_monitor(self, monitor: "Optional[Shared[Exception]]") -> None:
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

MESSAGES_HANDLED: Counter = METER.create_counter("py_mesages_handled")


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
            _set_context(ctx)

            DebugContext.set(DebugContext())

            args, kwargs = unflatten(message, local_state)

            match method:
                case MethodSpecifier.Init():
                    ins = ctx.actor_instance
                    (
                        Class,
                        ins.proc_mesh,
                        ins._controller_controller,
                        ins.name,
                        ins.creator,
                        *args,
                    ) = args
                    ins.rank = ctx.message_rank
                    ins.class_name = f"{Class.__module__}.{Class.__qualname__}"
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
            should_instrument = False

            if isinstance(the_method, EndpointProperty):
                should_instrument = the_method._instrument
                the_method = functools.partial(the_method._method, self.instance)

            if inspect.iscoroutinefunction(the_method):
                try:
                    if should_instrument:
                        with TRACER.start_as_current_span(
                            method_name,
                            attributes={"actor_id": str(ctx.actor_instance.actor_id)},
                        ):
                            result = await the_method(*args, **kwargs)
                    else:
                        result = await the_method(*args, **kwargs)
                    self._maybe_exit_debugger()
                except Exception as e:
                    logging.critical(
                        "Unhandled exception in actor endpoint",
                        exc_info=e,
                    )
                    raise e
            else:
                with fake_sync_state():
                    if should_instrument:
                        with TRACER.start_as_current_span(
                            method_name,
                            attributes={"actor_id": str(ctx.actor_instance.actor_id)},
                        ):
                            result = the_method(*args, **kwargs)
                    else:
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

    def _maybe_exit_debugger(self, do_continue: bool = True) -> None:
        if (pdb_wrapper := DebugContext.get().pdb_wrapper) is not None:
            if do_continue:
                pdb_wrapper.clear_all_breaks()
                pdb_wrapper.do_continue("")
            pdb_wrapper.end_debug_session()
        DebugContext.set(DebugContext())

    def _post_mortem_debug(self, exc_tb: Any) -> None:
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
        self, cx: Context, message: UndeliverableMessageEnvelope
    ) -> bool:
        _set_context(cx)
        handle_undeliverable = getattr(
            self.instance, "_handle_undeliverable_message", None
        )
        if handle_undeliverable is not None:
            return handle_undeliverable(message)
        else:
            return False

    def __supervise__(self, cx: Context, *args: Any, **kwargs: Any) -> object:
        _set_context(cx)
        instance = self.instance
        if instance is None:
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

            error_message = f"Actor object is missing when executing method __supervise__ on actor {cx.actor_instance.actor_id}."
            if self._saved_error is not None:
                error_message += (
                    f" This is likely due to an earlier error: {self._saved_error}"
                )
            raise AssertionError(error_message)

        # Forward a call to supervise on this actor to the user-provided instance.
        if hasattr(instance, "__supervise__"):
            # pyre-fixme[16]: Caller needs to handle the case where instance is None.
            return instance.__supervise__(*args, **kwargs)
        else:
            # If there is no __supervise__ method, the default would be to return
            # None. That means the supervision error is not handled and will be
            # propagated to the next owner.
            return None

    async def __cleanup__(self, cx: Context, exc: str | Exception | None) -> None:
        """Cleans up any resources owned by this Actor before stopping. Automatically
        called even if there is an error"""
        _context.set(cx)
        instance = self.instance
        if instance is None:
            # If there is no instance, there's nothing to clean up, the actor
            # was never constructed
            return None

        # Forward a call to supervise on this actor to the user-provided instance.
        cleanup = getattr(instance, "__cleanup__", None)
        if cleanup is None:
            return None

        if isinstance(exc, str):
            # Wrap the string in an exception object so the main API of __cleanup__
            # is to take an optional exception object.
            # The raw string is used for wider compatibility with other error
            # types for now.
            exc = Exception(exc)

        if inspect.iscoroutinefunction(cleanup):
            return await cleanup(exc)
        else:
            with fake_sync_state():
                return cleanup(exc)

    def __repr__(self) -> str:
        return f"_Actor(instance={self.instance!r})"


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


class Actor(MeshTrait):
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
    def initialized(self) -> Any:
        raise NotImplementedError(
            "actor implementations are not meshes, but we can't convince the typechecker of it..."
        )

    def _handle_undeliverable_message(
        self, message: UndeliverableMessageEnvelope
    ) -> bool:
        # Return False to indicate that the undeliverable message was not handled.
        return False


class ActorMesh(MeshTrait, Generic[T]):
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
        shape: Shape,
        proc_mesh: "Optional[ProcMesh]",
    ) -> None:
        self.__name__: str = Class.__name__
        self._class: Type[T] = Class
        self._inner: "ActorMeshProtocol" = inner
        self._shape = shape
        self._proc_mesh = proc_mesh
        # We don't start the supervision polling loop until the first call to
        # supervision_event, which needs an Instance. Initialize here so events
        # can be collected even without any endpoints being awaited.
        self._inner.start_supervision(context().actor_instance._as_rust())

        async_endpoints = []
        sync_endpoints = []
        async_cleanup = None
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
                if inspect.iscoroutinefunction(attr_value._method):
                    async_endpoints.append(attr_name)
                else:
                    sync_endpoints.append(attr_name)
            if attr_name == "__cleanup__" and attr_value is not None:
                async_cleanup = inspect.iscoroutinefunction(attr_value)

        if sync_endpoints and async_endpoints:
            raise ValueError(
                f"{self._class} mixes both async and sync endpoints."
                "Synchronous endpoints cannot be mixed with async endpoints because they can cause the asyncio loop to deadlock if they wait."
                f"sync: {sync_endpoints} async: {async_endpoints}"
            )
        if sync_endpoints and async_cleanup:
            raise ValueError(
                f"{self._class} has sync endpoints, but an async __cleanup__. Make sure __cleanup__ is also synchronous."
                "Synchronous endpoints cannot be mixed with async endpoints because they can cause the asyncio loop to deadlock if they wait."
                f"sync: {sync_endpoints}"
            )
        # Check for False explicitly because None means there is no cleanup.
        if async_endpoints and async_cleanup is False:
            raise ValueError(
                f"{self._class} has async endpoints, but a synchronous __cleanup__. Make sure __cleanup__ is also async."
                "Synchronous endpoints cannot be mixed with async endpoints because they can cause the asyncio loop to deadlock if they wait."
                f"sync: {sync_endpoints}"
            )

    def __getattr__(self, attr: str) -> NotAnEndpoint:
        if attr in dir(self._class):
            return NotAnEndpoint(self, attr)
        raise AttributeError(attr)

    def _endpoint(
        self,
        name: MethodSpecifier,
        impl: Callable[Concatenate[Any, P], Awaitable[R]],
        propagator: Propagator,
        explicit_response_port: bool,
    ) -> Any:
        return ActorEndpoint(
            self._inner,
            self._shape,
            self._proc_mesh,
            name,
            impl,
            propagator,
            explicit_response_port,
        )

    @classmethod
    def _create(
        cls,
        Class: Type[T],
        name: str,
        actor_mesh: "PythonActorMesh",
        shape: Shape,
        proc_mesh: "ProcMesh",
        controller_controller: Optional["_ControllerController"],
        # args and kwargs are passed to the __init__ method of the user defined
        # python actor object.
        *args: Any,
        **kwargs: Any,
    ) -> "ActorMesh[T]":
        mesh = cls(Class, actor_mesh, shape, proc_mesh)

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
        send(
            ep,
            (
                mesh._class,
                proc_mesh,
                controller_controller,
                name,
                context().actor_instance._as_creator(),
                *args,
            ),
            kwargs,
        )

        return mesh

    @classmethod
    def from_actor_id(
        cls,
        Class: Type[T],
        actor_id: ActorId,
    ) -> "ActorMesh[T]":
        return cls(Class, _SingletonActorAdapator(actor_id), singleton_shape, None)

    def __reduce_ex__(
        self, protocol: Any
    ) -> "Tuple[Type[ActorMesh[T]], Tuple[Any, ...]]":
        return ActorMesh, (self._class, self._inner, self._shape, self._proc_mesh)

    @property
    def _ndslice(self) -> NDSlice:
        return self._shape.ndslice

    @property
    def _labels(self) -> Iterable[str]:
        return self._shape.labels

    def _new_with_shape(self, shape: Shape) -> "ActorMesh[T]":
        sliced = self._inner.new_with_region(shape.region)
        return ActorMesh(self._class, sliced, shape, self._proc_mesh)

    def __repr__(self) -> str:
        return f"ActorMesh(class={self._class}, shape={self._shape}), inner={type(self._inner)})"

    def stop(self) -> "Future[None]":
        instance = context().actor_instance._as_rust()
        return Future(coro=self._inner.stop(instance))

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
        self.exception_formatted: str = "".join(actor_mesh_ref_tb)
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
