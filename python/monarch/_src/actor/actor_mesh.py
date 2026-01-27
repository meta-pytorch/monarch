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
from abc import abstractmethod, abstractproperty
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
from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer, FrozenBuffer
from monarch._rust_bindings.monarch_hyperactor.channel import BindSpec, ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import configure
from monarch._rust_bindings.monarch_hyperactor.context import Instance as HyInstance
from monarch._rust_bindings.monarch_hyperactor.logging import log_endpoint_exception
from monarch._rust_bindings.monarch_hyperactor.mailbox import (
    Mailbox,
    OncePortRef,
    PortRef,
    UndeliverableMessageEnvelope,
)
from monarch._rust_bindings.monarch_hyperactor.proc import ActorId
from monarch._rust_bindings.monarch_hyperactor.pytokio import (
    PendingPickle,
    PendingPickleState,
    PythonTask,
    Shared,
)
from monarch._rust_bindings.monarch_hyperactor.selection import (
    Selection as HySelection,  # noqa: F401
)
from monarch._rust_bindings.monarch_hyperactor.shape import (
    Point as HyPoint,
    Region,
    Shape,
)
from monarch._rust_bindings.monarch_hyperactor.supervision import (
    MeshFailure,
    SupervisionError,
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
from monarch._src.actor.metrics import endpoint_message_size_histogram
from monarch._src.actor.mpsc import (  # noqa: F401 - import runs @rust_struct patching
    Receiver,
)
from monarch._src.actor.pickle import allow_pending_pickle_mesh, flatten, unflatten
from monarch._src.actor.python_extension_methods import rust_struct
from monarch._src.actor.shape import MeshTrait, NDSlice
from monarch._src.actor.sync_state import fake_sync_state
from monarch._src.actor.telemetry import METER
from monarch._src.actor.tensor_engine_shim import actor_rref, create_actor_message
from opentelemetry.metrics import Counter
from opentelemetry.trace import Tracer
from typing_extensions import Self

if TYPE_CHECKING:
    from monarch._rust_bindings.monarch_hyperactor.actor import (
        PortProtocol,
        QueuedMessage,
    )
    from monarch._rust_bindings.monarch_hyperactor.actor_mesh import ActorMeshProtocol
    from monarch._rust_bindings.monarch_hyperactor.mailbox import (
        PortHandle,
        PortReceiverBase,
    )
    from monarch._src.actor.proc_mesh import _ControllerController, DeviceMesh, ProcMesh
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
    # Optional tensor engine factory for mocking. When set, this is used
    # instead of the real tensor engine when spawning device meshes.
    _mock_tensor_engine_factory: Optional[Callable[["ProcMesh"], "DeviceMesh"]] = None

    def spawn_tensor_engine(self, proc_mesh: "ProcMesh") -> "DeviceMesh":
        """
        Spawn a tensor engine for this actor.

        If a mock tensor engine factory is set, use it. Otherwise, use the
        real tensor engine from mesh_controller.
        """
        if self._mock_tensor_engine_factory is not None:
            return self._mock_tensor_engine_factory(proc_mesh)

        # pyre-ignore[21]: mesh_controller may not be visible to pyre in this target
        from monarch.mesh_controller import spawn_tensor_engine as real_spawn

        # pyre-ignore[16]: spawn_tensor_engine is defined in mesh_controller
        return real_spawn(proc_mesh)

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

    @abstractmethod
    def abort(self, reason: Optional[str] = None) -> None:
        """
        Abort the current actor. This will cause the actor to terminate
        with a failure, and a supervision error will propagate to its creator.
        """
        ...


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

    @staticmethod
    def _from_instance(instance: Instance) -> "Context": ...


_context: contextvars.ContextVar[Context] = contextvars.ContextVar(
    "monarch.actor_mesh._context"
)


class _ActorFilter(logging.Filter):
    """
    Logging filter that adds actor context to log messages.

    This filter is automatically added to all logging handlers when code runs
    inside a Monarch actor. It prefixes log messages with the actor's identity,
    e.g. "[actor=<root>.MyActor] my log message".

    We skip empty messages because torch.compile uses them for structured trace
    logging. If we modify those, torch's formatter fails with "expected empty
    string for trace".
    """

    def __init__(self) -> None:
        super().__init__()

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if not config.prefix_python_logs_with_actor:
                return True
            ctx = _context.get(None)
            if ctx is not None:
                actor_prefix = f"[actor={ctx.actor_instance}] "
                record.actor_prefix = actor_prefix  # type: ignore[attr-defined]
                # Skip empty messages (used for structured logging, e.g. torch.compile)
                if record.msg:
                    record.msg = f"{actor_prefix}{record.msg}"
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


def _set_context(c: Context) -> contextvars.Token[Context]:
    _init_context_log_handler()
    return _context.set(c)


def _reset_context(c: contextvars.Token[Context]) -> None:
    _context.reset(c)


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


def _init_client_context() -> Context:
    """
    Create a client context that bootstraps an actor instance running on a real
    local proc mesh on a real local host mesh.
    """
    from monarch._rust_bindings.monarch_hyperactor.host_mesh import bootstrap_host
    from monarch._src.actor.host_mesh import _bootstrap_cmd, HostMesh
    from monarch._src.actor.proc_mesh import ProcMesh

    hy_host_mesh, hy_proc_mesh, hy_instance = bootstrap_host(
        _bootstrap_cmd()
    ).block_on()

    ctx = Context._from_instance(cast(Instance, hy_instance))  # type: ignore
    # Set the context here to avoid recursive context creation:
    token = _set_context(ctx)
    try:
        py_host_mesh = HostMesh._from_rust(hy_host_mesh)
        py_proc_mesh = ProcMesh._from_rust(hy_proc_mesh, py_host_mesh)
    finally:
        _reset_context(token)

    ctx.actor_instance.proc_mesh = py_proc_mesh
    return ctx


_client_context: _Lazy[Context] = _Lazy(_init_client_context)


def shutdown_context() -> "Future[None]":
    """Shutdown global actor context resources.

    This should be called at the end of scripts that use the actor
    system to ensure clean shutdown of background processes.

    Returns:
        Future[None]: A future that completes when shutdown is
                      finished. Call with .get() to wait for
                      completion.
    """
    from monarch._src.actor.future import Future

    try:
        from monarch._rust_bindings.monarch_hyperactor.host_mesh import (
            shutdown_local_host_mesh,
        )

        return Future(coro=shutdown_local_host_mesh())
    except RuntimeError:
        # No local host mesh to shutdown
        pass

    # Nothing to shutdown - return a completed future
    async def noop() -> None:
        pass

    return Future(coro=noop())


def context() -> Context:
    c = _context.get(None)
    if c is None:
        from monarch._src.actor.proc_mesh import _get_controller_controller

        c = _client_context.get()
        _set_context(c)
        _, c.actor_instance._controller_controller = _get_controller_controller()
    return c


_transport: Optional[BindSpec] = None
_transport_lock = threading.Lock()


def enable_transport(transport: "ChannelTransport | str") -> None:
    """
    Allow monarch to communicate with transport type 'transport'
    This must be called before any other calls in the monarch API.
    If it isn't called, we will implicitly call
    `monarch.enable_transport(ChannelTransport.Unix)` on the first monarch call.

    Currently only one transport type may be enabled at one time.
    In the future we may allow multiple to be enabled.

    Supported transport values:
        - ChannelTransport enum: ChannelTransport.Unix, ChannelTransport.TcpWithHostname, etc.
        - string short cuts for the ChannelTransport enum:
            - "tcp": ChannelTransport.TcpWithHostname
            - "ipc": ChannelTransport.Unix
            - "metatls": ChannelTransport.MetaTlsWithIpV6
            - "metatls-hostname": ChannelTransport.MetaTlsWithHostname
        - ZMQ-style URL format string for explicit address, e.g.:
            - "tcp://127.0.0.1:8080"

    For Meta usage, use metatls-hostname
    """
    if isinstance(transport, str):
        # Handle string shortcuts for the ChannelTransport enum,
        resolved = {
            "tcp": ChannelTransport.TcpWithHostname,
            "ipc": ChannelTransport.Unix,
            "metatls": ChannelTransport.MetaTlsWithIpV6,
            "metatls-hostname": ChannelTransport.MetaTlsWithHostname,
        }.get(transport)
        if resolved is not None:
            transport_config = BindSpec(resolved)
        else:
            transport_config = BindSpec(transport)
    else:
        # ChannelTransport enum
        transport_config = BindSpec(transport)

    if _context.get(None) is not None:
        raise RuntimeError(
            "`enable_transport()` must be called before any other calls in the monarch API. "
            "If it isn't called, we will implicitly call `monarch.enable_transport(ChannelTransport.Unix)` "
            "on the first monarch call."
        )

    global _transport
    with _transport_lock:
        if _transport is not None and _transport != transport_config:
            raise RuntimeError(
                f"Only one transport type may be enabled at one time. "
                f"Currently enabled transport type is `{_transport}`. "
                f"Attempted to enable transport type `{transport_config}`."
            )
        _transport = transport_config
    # pyre-ignore[6]: BindSpec is accepted by configure. We just do not expose
    # it in the method's signature since BindSpec is not a public type.
    configure(default_transport=transport_config)


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
        self._region: Region = region

    @property
    def region(self) -> Region:
        return self._region

    def get(self, rank: int) -> Optional[ActorId]:
        if rank == 0:
            return self._inner
        return None

    def cast(
        self,
        message: PythonMessage,
        selection: str,
        instance: HyInstance,
    ) -> None:
        Instance._as_py(instance)._mailbox.post(self._inner, message)

    def new_with_region(self, region: Region) -> "_SingletonActorAdapator":
        return _SingletonActorAdapator(self._inner, self._region)

    def supervision_event(self, instance: HyInstance) -> "Optional[Shared[Exception]]":
        return None

    def start_supervision(
        self, instance: HyInstance, supervision_display_name: str
    ) -> None:
        return None

    def stop(self, instance: HyInstance, reason: str) -> "PythonTask[None]":
        raise NotImplementedError("stop()")

    def initialized(self) -> "PythonTask[None]":
        async def empty() -> None:
            pass

        return PythonTask.from_coroutine(empty())


def _check_endpoint_arguments(
    method_name: MethodSpecifier,
    signature: inspect.Signature,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
) -> None:
    """
    Check that the arguments match the expected signature for the endpoint.

    For Init methods, the message args contain an ActorInitArgs wrapper, so we
    unpack it and validate the actual constructor arguments.
    For ExplicitPort methods, the signature expects (self, port, *args, **kwargs),
    so we bind with two None placeholders for self and port.
    For other methods, the signature expects (self, *args, **kwargs),
    so we bind with one None placeholder for self.
    """
    match method_name:
        case MethodSpecifier.Init():
            # For Init, args[0] is ActorInitArgs which wraps the real constructor args
            if len(args) != 1 or not isinstance(args[0], ActorInitArgs):
                raise TypeError("Init message must contain exactly one ActorInitArgs")
            init_args = args[0]
            # Validate the actual constructor arguments against the signature
            signature.bind(None, *init_args.args, **kwargs)
        case MethodSpecifier.ExplicitPort():
            signature.bind(None, None, *args, **kwargs)
        case _:
            signature.bind(None, *args, **kwargs)


def _create_endpoint_message(
    method_name: MethodSpecifier,
    signature: inspect.Signature,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    port: "Optional[Port[Any]]",
    proc_mesh: "Optional[ProcMesh]",
) -> PythonMessage:
    """
    Create a PythonMessage for sending to an actor endpoint.

    Checks arguments, flattens them, and creates the appropriate message based on
    whether the arguments contain monarch references.

    Returns:
        PythonMessage ready to be sent to the actor mesh
    """
    _check_endpoint_arguments(method_name, signature, args, kwargs)
    with allow_pending_pickle_mesh():
        objects, buffer = flatten((args, kwargs), _is_ref_or_mailbox_or_pending_pickle)

    has_ref = False
    has_pending_pickle = False
    mailbox_or_refs = []
    for obj in objects:
        if isinstance(obj, PendingPickle):
            has_pending_pickle = True
        elif hasattr(obj, "__monarch_ref__"):
            mailbox_or_refs.append(obj)
            has_ref = True
        else:
            mailbox_or_refs.append(obj)

    pending_pickle_state = None
    if has_pending_pickle:
        pending_pickle_state = PendingPickleState(
            objects, _is_ref_or_mailbox_or_pending_pickle
        )

    if not has_ref:
        message = PythonMessage(
            PythonMessageKind.CallMethod(
                method_name, None if port is None else port._port_ref
            ),
            buffer,
            pending_pickle_state,
        )
    else:
        message = create_actor_message(
            method_name, proc_mesh, buffer, objects, port, pending_pickle_state
        )

    return message


class ActorEndpoint(Endpoint[P, R]):
    def __init__(
        self,
        actor_mesh: "ActorMeshProtocol",
        mesh_name: str,
        shape: Shape,
        proc_mesh: "Optional[ProcMesh]",
        name: MethodSpecifier,
        impl: Callable[Concatenate[Any, P], Awaitable[R]],
        propagator: Propagator,
    ) -> None:
        super().__init__(propagator)
        self._actor_mesh = actor_mesh
        self._mesh_name = mesh_name
        self._name = name
        self._shape = shape
        self._proc_mesh = proc_mesh
        self._signature: inspect.Signature = inspect.signature(impl)

    def _call_name(self) -> MethodSpecifier:
        return self._name

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
        message = _create_endpoint_message(
            self._name, self._signature, args, kwargs, port, self._proc_mesh
        )

        # Record the message size with method name attribute
        endpoint_message_size_histogram.record(
            len(message.message), {"method": self._get_method_name()}
        )

        self._actor_mesh.cast(message, selection, context().actor_instance._as_rust())
        shape = self._shape
        return Extent(shape.labels, shape.ndslice.sizes)

    def _full_name(self) -> str:
        return f"{self._mesh_name}.{self._get_method_name()}()"

    def _port(self, once: bool = False) -> "Tuple[Port[R], PortReceiver[R]]":
        p, r = super()._port(once=once)
        instance = context().actor_instance._as_rust()
        monitor: Optional[Shared[Exception]] = self._actor_mesh.supervision_event(
            instance
        )

        r._attach_supervision(monitor, self._full_name())
        return (p, r)

    def _rref(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> R:
        _check_endpoint_arguments(self._name, self._signature, args, kwargs)
        refs, buffer, pending_pickle_state = _flatten_with_pending_pickle(
            (args, kwargs)
        )

        return actor_rref(self, buffer, refs, pending_pickle_state)


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

    ValueMesh is returned when calling `.get()` on a Future from an endpoint
    invocation on an ActorMesh or ProcMesh, or by awaiting the Future directly.
    It contains the return values from all actors in the mesh, organized by
    their coordinates.

    Iteration:
        The most efficient way to iterate over a ValueMesh is using `.items()`,
        which yields (point, value) tuples:

        >>> for point, result in value_mesh.items():
        ...     rank = point["hosts"] * gpus_per_host + point["gpus"]
        ...     print(f"Rank {rank}: {result}")

        You can also iterate over just values:

        >>> for result in value_mesh.values():
        ...     process(result)

    Accessing specific values:
        Use `.item()` to extract a single value from a singleton mesh:

        >>> single_value = value_mesh.slice(hosts=0, gpus=0).item()

        Or with keyword arguments for multi-dimensional access:

        >>> value = value_mesh.item(hosts=0, gpus=0)

    Mesh operations:
        ValueMesh supports the same operations as other MeshTrait types:

        - `.flatten(dimension)`: Flatten to a single dimension
        - `.slice(**coords)`: Select a subset of the mesh

    Examples:
        >>> # Sync API - Get results from all actors
        >>> results = actor_mesh.endpoint.call(arg).get()
        >>> for point, result in results.items():
        ...     print(f"Actor at {point}: {result}")
        >>>
        >>> # Async API - Await the future directly
        >>> results = await actor_mesh.endpoint.call(arg)
        >>> for point, result in results.items():
        ...     print(f"Actor at {point}: {result}")
        >>>
        >>> # Access a specific actor's result
        >>> result_0 = results.item(hosts=0, gpus=0)
        >>>
        >>> # Flatten and iterate
        >>> for point, result in results.flatten("rank").items():
        ...     print(f"Rank {point.rank}: {result}")
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
        _, buffer, pending_pickle_state = _flatten_with_pending_pickle(obj)

        self._port_ref.send(
            self._instance,
            PythonMessage(
                PythonMessageKind.Result(self._rank), buffer, pending_pickle_state
            ),
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

    @property
    def return_undeliverable(self) -> bool:
        """Whether to return undeliverable messages to the sender. By default, this is True.
        Setting to False means that if the user of this port cannot deliver the message, it will
        not be returned to the sender and will be dropped. Use this sparingly as
        it could lead to unexpected hangs instead of errors that propagate back
        to the owner of the actor"""
        return self._port_ref.return_undeliverable

    @return_undeliverable.setter
    def return_undeliverable(self, value: bool) -> None:
        self._port_ref.return_undeliverable = value


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

    @property
    def return_undeliverable(self) -> bool:
        return True

    @return_undeliverable.setter
    def return_undeliverable(self, value: bool) -> None:
        pass


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
        endpoint: Optional[str] = None,
    ) -> None:
        self._mailbox: Mailbox = mailbox
        self._monitor = monitor
        self._receiver = receiver
        self._endpoint = endpoint

    def _tag_supervision_error(self, error: Exception) -> None:
        """Tag supervision error with endpoint name if available."""
        if self._endpoint is not None and isinstance(error, SupervisionError):
            error.endpoint = self._endpoint

    async def _recv(self) -> R:
        awaitable = self._receiver.recv_task()
        if self._monitor is None:
            result = await awaitable
        else:
            try:
                result, i = await PythonTask.select_one(
                    # type: ignore
                    [self._monitor.task(), awaitable]
                )
            except Exception as e:
                self._tag_supervision_error(e)
                raise e
            if i == 0:
                self._tag_supervision_error(result)
                raise result
        return self._process(result)

    def _process(self, msg: PythonMessage) -> R:
        # TODO: Try to do something more structured than a cast here
        payload = cast(R, unflatten(msg.message, itertools.repeat(self._mailbox)))
        match msg.kind:
            case PythonMessageKind.Result():
                return payload
            case PythonMessageKind.Exception():
                e = cast(Exception, payload)
                self._tag_supervision_error(e)
                raise e
            case _:
                raise ValueError(f"Unexpected message kind: {msg.kind}")

    def recv(self) -> "Future[R]":
        return Future(coro=self._recv())

    def ranked(self) -> "RankedPortReceiver[R]":
        return RankedPortReceiver[R](
            self._mailbox, self._receiver, self._monitor, self._endpoint
        )

    def _attach_supervision(
        self, monitor: "Optional[Shared[Exception]]", endpoint: str
    ) -> None:
        """
        Attach supervision monitoring to this port receiver.

        Enables the receiver to detect and report errors on any supervision events.

        Args:
            monitor: Shared exception monitor that signals supervision errors
                from the actor mesh. None if supervision is not enabled.
            endpoint: Full endpoint name
        """
        self._monitor = monitor
        self._endpoint = endpoint


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


@dataclass
class ActorInitArgs:
    Class: Type["Actor"]
    proc_mesh: Optional["ProcMesh"]
    controller_controller: Optional["_ControllerController"]
    name: str
    creator: Optional[CreatorInstance]
    args: Tuple[Any, ...]


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

        # Initialize method_name before try block so it's always defined
        method_name = method.name

        # response_port can be None. If so, then sending to port will drop the response,
        # and raise any exceptions to the caller.

        try:
            _set_context(ctx)

            DebugContext.set(DebugContext())

            args, kwargs = unflatten(message, local_state)

            match method:
                case MethodSpecifier.Init():
                    ins = ctx.actor_instance
                    (args,) = args
                    init_args = cast(ActorInitArgs, args)
                    Class = init_args.Class
                    ins.proc_mesh = cast("ProcMesh", init_args.proc_mesh)
                    ins._controller_controller = cast(
                        "_ControllerController", init_args.controller_controller
                    )
                    ins.name = init_args.name
                    ins.creator = init_args.creator
                    args = init_args.args
                    ins.rank = ctx.message_rank
                    ins.class_name = f"{Class.__module__}.{Class.__qualname__}"
                    try:
                        self.instance = Class(*args, **kwargs)
                        # Check if there's a tensor engine mock registered for this actor class.
                        # If so, set _mock_tensor_engine_factory on the Instance for use by
                        # Instance.spawn_tensor_engine().
                        from monarch._src.actor.mock import get_tensor_engine_factory

                        mock_factory = get_tensor_engine_factory(Class)
                        if mock_factory is not None:
                            ins._mock_tensor_engine_factory = (
                                lambda proc_mesh: mock_factory(proc_mesh)
                            )
                        self._maybe_exit_debugger()
                    except Exception as e:
                        self._saved_error = ActorError(
                            e, f"Actor call {ins.name}.{method_name} failed."
                        )
                        raise
                    response_port.send(None)
                    return
                case MethodSpecifier.ReturnsResponse():
                    pass
                case MethodSpecifier.ExplicitPort():
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

                error_message = f'Actor object is missing when executing method "{method_name}" on actor {ctx.actor_instance.actor_id}.'
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
                if should_instrument:
                    # TODO(T12345): Replace with a lower-overhead tracing solution.
                    # Using TRACER context manager for now to avoid thread-safety
                    # issues with PySpan across async/await boundaries.
                    with TRACER.start_as_current_span(
                        method_name,
                        attributes={"actor_id": str(ctx.actor_instance.actor_id)},
                    ):
                        result = await the_method(*args, **kwargs)
                else:
                    result = await the_method(*args, **kwargs)
                self._maybe_exit_debugger()
            else:
                with fake_sync_state():
                    if should_instrument:
                        # TODO(T12345): Replace with a lower-overhead tracing solution.
                        # Using TRACER context manager for now to avoid thread-safety
                        # issues with PySpan across async/await boundaries.
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
            log_endpoint_exception(e, method_name, ctx.actor_instance.actor_id)
            self._post_mortem_debug(e.__traceback__)
            response_port.exception(
                ActorError(
                    e,
                    f"Actor call {ctx.actor_instance.name}.{method_name} failed.",
                )
            )
            return
        except BaseException as e:
            self._post_mortem_debug(e.__traceback__)
            # A BaseException can be thrown in the case of a Rust panic.
            # In this case, we need a way to signal the panic to the Rust side.
            # See [Panics in async endpoints]
            try:
                panic_flag.signal_panic(
                    ActorError(
                        e,
                        f"Actor call {ctx.actor_instance.name}.{method_name} failed with BaseException.",
                    )
                )
            except Exception:
                # The channel might be closed if the Rust side has already detected the error
                pass
            raise
        return

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

    async def _dispatch_loop(
        self,
        receiver: "Receiver[QueuedMessage]",
        error_port: "PortHandle",
    ) -> None:
        """
        Message loop for queue-dispatch mode. Called from Rust Actor::init.

        Args:
            receiver: Channel receiver for queued messages
            error_port: Port to send errors to for actor supervision
        """
        while True:
            msg = await receiver.recv()
            try:
                await self._handle_queued_message(msg)
            except BaseException as e:
                import cloudpickle

                error_bytes = cloudpickle.dumps(e)
                error_msg = PythonMessage(
                    PythonMessageKind.Exception(rank=None),
                    error_bytes,
                )
                error_port.send(msg.context.actor_instance, error_msg)
                raise

    async def _handle_queued_message(self, msg: "QueuedMessage") -> None:
        """Handle a single queued message."""

        class QueuePanicFlag:
            """Panic flag for queue dispatch mode.

            Unlike the DummyPanicFlag, this one stores the exception so it can
            be re-raised after handle() returns, ensuring proper cleanup.
            """

            def __init__(self) -> None:
                self.panic_exception: BaseException | None = None

            def signal_panic(self, ex: BaseException) -> None:
                self.panic_exception = ex

        panic_flag = QueuePanicFlag()
        await self.handle(
            msg.context,
            msg.method,
            msg.bytes,
            panic_flag,  # pyre-ignore[6]: QueuePanicFlag implements PanicFlag protocol
            msg.local_state,
            msg.response_port,
        )
        # If a panic was signaled, re-raise it after handle() has cleaned up
        if panic_flag.panic_exception is not None:
            raise panic_flag.panic_exception

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


def _is_ref_or_mailbox_or_pending_pickle(x: object) -> bool:
    return _is_ref_or_mailbox(x) or isinstance(x, PendingPickle)


def _flatten_with_pending_pickle(
    x: object,
) -> Tuple[List[Any], Buffer, Optional[PendingPickleState]]:
    with allow_pending_pickle_mesh():
        objs, buff = flatten(x, _is_ref_or_mailbox_or_pending_pickle)
    pending_pickle_state = None
    if any(isinstance(obj, PendingPickle) for obj in objs):
        pending_pickle_state = PendingPickleState(
            objs, _is_ref_or_mailbox_or_pending_pickle
        )
    return objs, buff, pending_pickle_state


def _pickle(obj: object) -> Buffer:
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
        name: str,
        inner: "ActorMeshProtocol",
        shape: Shape,
        proc_mesh: "Optional[ProcMesh]",
    ) -> None:
        # Class name of the actor.
        self.__name__: str = Class.__name__
        # The name user gives when spawning the mesh
        self._mesh_name = name
        self._class: Type[T] = Class
        self._inner: "ActorMeshProtocol" = inner
        self._shape = shape
        self._proc_mesh = proc_mesh

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
    ) -> Any:
        return ActorEndpoint(
            self._inner,
            self._mesh_name,
            self._shape,
            self._proc_mesh,
            name,
            impl,
            propagator,
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
        mesh = cls(Class, name, actor_mesh, shape, proc_mesh)

        # We don't start the supervision polling loop until the first call to
        # supervision_event, which needs an Instance. Initialize here so events
        # can be collected even without any endpoints being awaited.
        instance = context().actor_instance
        # Supervision display name is unused here now, it is set in ProcMesh::spawn.
        mesh._inner.start_supervision(instance._as_rust(), "")

        async def null_func(*_args: Iterable[Any], **_kwargs: Dict[str, Any]) -> None:
            return None

        # send __init__ message to the mesh to initialize the user defined
        # python actor object.
        ep = mesh._endpoint(
            MethodSpecifier.Init(),
            null_func,
            None,
        )
        send(
            ep,
            (
                ActorInitArgs(
                    cast(Type[Actor], mesh._class),
                    proc_mesh,
                    controller_controller,
                    name,
                    context().actor_instance._as_creator(),
                    args,
                ),
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
        return cls(Class, "", _SingletonActorAdapator(actor_id), singleton_shape, None)

    def __reduce_ex__(
        self, protocol: Any
    ) -> "Tuple[Type[ActorMesh[T]], Tuple[Any, ...]]":
        return ActorMesh, (
            self._class,
            self._mesh_name,
            self._inner,
            self._shape,
            self._proc_mesh,
        )

    @property
    def _ndslice(self) -> NDSlice:
        return self._shape.ndslice

    @property
    def _labels(self) -> Iterable[str]:
        return self._shape.labels

    def _new_with_shape(self, shape: Shape) -> "ActorMesh[T]":
        sliced = self._inner.new_with_region(shape.region)
        return ActorMesh(self._class, self._mesh_name, sliced, shape, self._proc_mesh)

    def __repr__(self) -> str:
        return f"ActorMesh(class={self._class}, shape={self._shape}), inner={type(self._inner)})"

    def stop(self, reason: str = "stopped by client") -> "Future[None]":
        instance = context().actor_instance._as_rust()
        return Future(coro=self._inner.stop(instance, reason))

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
        exception: BaseException,
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


class RootClientActor(Actor):
    name: str = "client"

    def __supervise__(self, failure: MeshFailure) -> object:
        from monarch.actor import unhandled_fault_hook  # pyre-ignore

        unhandled_fault_hook(failure)  # pyre-ignore
        return True

    @staticmethod
    def _pickled_init_args() -> Buffer:
        args = (
            ActorInitArgs(RootClientActor, None, None, RootClientActor.name, None, ()),
        )
        kwargs = {}
        _, buffer = flatten((args, kwargs), lambda _: False)
        return buffer
