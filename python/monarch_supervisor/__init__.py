# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import io
import logging
import math
import os
import pickle
import signal
import sys
import time
from abc import ABC, abstractmethod
from collections import deque
from enum import Enum
from functools import cache
from logging import Logger
from pathlib import Path
from threading import Thread
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import torch
import zmq
import zmq.asyncio


logger: Logger = logging.getLogger(__name__)

T = TypeVar("T")

# multiplier (how many heartbeats do we miss before lost)
HEARTBEAT_LIVENESS = float(os.getenv("TORCH_SUPERVISOR_HEARTBEAT_LIVENESS", "5.0"))
# frequency (in seconds) how often to send heartbeat
HEARTBEAT_INTERVAL = float(os.getenv("TORCH_SUPERVISOR_HEARTBEAT_INTERVAL", "1.0"))
TTL_REPORT_INTERVAL = float(os.getenv("TORCH_SUPERVISOR_TTL_REPORT_INTERVAL", "60"))
LOG_INTERVAL = float(os.getenv("TORCH_SUPERVISOR_LOG_INTERVAL", "60"))
DEFAULT_LOGGER_FORMAT = (
    "%(levelname).1s%(asctime)s.%(msecs)03d000 %(process)d "
    "%(pathname)s:%(lineno)d] supervisor: %(message)s"
)
DEFAULT_LOGGER_DATEFORMAT = "%m%d %H:%M:%S"

_State = Enum("_State", ["UNATTACHED", "ATTACHED", "LOST"])
_UNATTACHED: _State = _State.UNATTACHED
_ATTACHED: _State = _State.ATTACHED
_LOST: _State = _State.LOST


def pickle_loads(*args, **kwargs) -> Any:
    # Ensure that any tensors load from CPU via monkeypatching how Storages are
    # loaded.
    old = torch.storage._load_from_bytes
    try:
        torch.storage._load_from_bytes = lambda b: torch.load(
            io.BytesIO(b), map_location="cpu", weights_only=False
        )
        # @lint-ignore PYTHONPICKLEISBAD
        return pickle.loads(*args, **kwargs)
    finally:
        torch.storage._load_from_bytes = old


def pickle_dumps(*args, **kwargs) -> Any:
    # @lint-ignore PYTHONPICKLEISBAD
    return pickle.dumps(*args, **kwargs)


# Hosts vs Connection objects:
# Connections get created when a host manager registers with the supervisor.
#   They represent a live socket between the supervisor and the host manager.
# Hosts get created when the supervisor requests a new Host object.
#   They are what the policy script uses as handles to launch jobs.
# The supervisor then brokers a match between an existing Host and an existing Connection,
# fulfilling the Host's request. Because either object could get created first
# (supervisor is slow to create the Host, or host manager is slow to establish a Connection),
# it is easier to keep them as separate concepts then try to fold it into a single Host object.


class Connection:
    """
    Connections get created when a host manager registers with the supervisor.
    They represent a live socket between the supervisor and the host manager.
    """

    def __init__(self, ctx: "Context", name: bytes, hostname: Optional[str]) -> None:
        self.state: _State = _UNATTACHED
        self.name = name
        self.hostname = hostname
        self.host: "Optional[Host]" = None
        # expiration timestamp when the host will be considered lost
        self.expiry: float = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
        if hostname is None:
            self.lost(ctx, "Connection did not start with a hostname")
        else:
            # let the connection know we exist
            ctx._backend.send_multipart([name, b""])

    def heartbeat(self) -> float:
        """
        Sets new heartbeart. Updates expiry timestamp

        Returns:
            float: the old ttl (timestamp in seconds) for record keeping
        """
        now = time.time()
        ttl = self.expiry - now
        self.expiry = now + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
        return ttl

    def check_alive_at(self, ctx: "Context", t: float) -> None:
        """Checks if host manager alive. if not, mark host as lost and send abort"""
        if self.state is not _LOST and self.expiry < t:
            # host timeout
            elapsed = t - self.expiry + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
            logger.warning(
                "Host %s (%s) has not heartbeated in %s seconds, disconnecting it",
                self.hostname,
                self.name,
                elapsed,
            )
            self.lost(ctx, "Host did not heartbeat")

    def handle_message(self, ctx: "Context", msg: bytes) -> None:
        ctx._heartbeat_ttl(self.heartbeat())
        if self.state is _LOST:
            # got a message from a host that expired, but
            # eventually came back to life
            # At this point we've marked its processes as dead
            # so we are going to tell it to abort so that it gets
            # restarted and can become a new connection.
            logger.info("Host %s that was lost reconnected, sending abort", self.name)
            self.send_abort(ctx, "Supervisor thought host timed out")
            return

        if not len(msg):
            # heartbeat msg, respond with our own
            ctx._backend.send_multipart([self.name, b""])
            return

        if self.state is _UNATTACHED:
            logger.warning(
                "Got message from host %s manager before it was attached.", self.name
            )
            self.lost(ctx, "Host manager sent messages before attached.")
            return

        cmd, proc_id, *args = pickle_loads(msg)
        assert self.host is not None
        receiver = self.host if proc_id is None else self.host._proc_table.get(proc_id)
        if receiver is None:
            # messages from a process might arrive after the user
            # no longer has a handle to the Process object
            # in which case they are ok to just drop
            assert proc_id >= 0 and proc_id < ctx._next_id, "unexpected proc_id"
            logger.warning(
                "Received message %s from process %s after local handle deleted",
                cmd,
                proc_id,
            )
        else:
            getattr(receiver, cmd)(*args)
            receiver = None

    def lost(self, ctx: "Context", with_error: Optional[str]) -> None:
        orig_state = self.state
        if orig_state is _LOST:
            return
        self.state = _LOST
        if orig_state is _ATTACHED:
            assert self.host is not None
            self.host._lost(with_error)
            self.host = None
        self.send_abort(ctx, with_error)

    def send_abort(self, ctx: "Context", with_error: Optional[str]) -> None:
        ctx._backend.send_multipart([self.name, pickle_dumps(("abort", with_error))])


class HostDisconnected(NamedTuple):
    time: float


# TODO: rename to HostHandle to disambiguate with supervisor.host.Host?
class Host:
    """
    Hosts get created when the supervisor requests a new Host object.
    They are what the policy script uses as handles to launch jobs.
    """

    def __init__(self, context: "Context") -> None:
        self._context = context
        self._state: _State = _UNATTACHED
        self._name: Optional[bytes] = None
        self._deferred_sends: List[bytes] = []
        self._proc_table: Dict[int, Process] = {}
        self._hostname: Optional[str] = None
        self._is_lost = False

    def __repr__(self) -> str:
        return f"Host[{self._hostname}]"

    @property
    def hostname(self) -> Optional[str]:
        return self._hostname

    def _lost(self, msg: Optional[str]) -> None:
        orig_state = self._state
        if orig_state is _LOST:
            return
        self._state = _LOST
        if orig_state is _ATTACHED:
            assert self._name is not None
            self._context._name_to_connection[self._name].lost(self._context, msg)
        self._name = None
        self._deferred_sends.clear()
        for p in list(self._proc_table.values()):
            p._lost_host()
        # should be cleared by aborting the processes
        assert len(self._proc_table) == 0
        self._context._produce_message(self, HostDisconnected(time.time()))
        self._is_lost = True

    def _send(self, msg: bytes) -> None:
        if self._state is _ATTACHED:
            self._context._backend.send_multipart([self._name, msg])
        elif self._state is _UNATTACHED:
            self._deferred_sends.append(msg)

    def _launch(self, p: "Process") -> None:
        self._proc_table[p._id] = p
        if self._state is _LOST:
            # launch after we lost connection to this host.
            p._lost_host()
            return
        self._send(
            pickle_dumps(
                (
                    "launch",
                    p._id,
                    p.rank,
                    p.processes_per_host,
                    p.world_size,
                    p.popen,
                    p.name,
                    p.simulate,
                    p.logfile,
                )
            )
        )
        self._context._launches += 1

    @property
    def disconnected(self) -> bool:
        return self._is_lost

    def create_process(
        self,
        args: Sequence[str],
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        name: Optional[str] = None,
        simulate: bool = False,
    ) -> "ProcessList":
        return self._context.create_process_group(
            [self], args=args, env=env, cwd=cwd, name=name, simulate=simulate
        )[0]


class ProcessFailedToStart(Exception):
    pass


class ProcessStarted(NamedTuple):
    pid: int


class ProcessExited(NamedTuple):
    result: Union[int, Exception]


class Process:
    def __init__(
        self,
        context: "Context",
        host: "Host",
        rank: int,
        processes_per_host: int,
        world_size: int,
        popen: Mapping[str, object],
        name: str,
        simulate: bool,
    ) -> None:
        self._id: int = context._next_id
        context._next_id += 1
        self._context = context
        self.host = host
        self.rank = rank
        self.processes_per_host = processes_per_host
        self.world_size = world_size
        self.popen = popen
        self.simulate = simulate
        self.name: str = name.format(rank=str(rank).zfill(len(str(world_size))))
        self.logfile: Optional[str] = (
            None
            if context.log_format is None
            else context.log_format.format(name=self.name)
        )
        self._pid = None
        self._returncode = None
        self._state = "launched"
        self._filter_obj = None

    @property
    def returncode(self) -> Optional[int]:
        return self._returncode

    @property
    def pid(self) -> Optional[int]:
        return self._pid

    def __repr__(self) -> str:
        return f"Process(rank={self.rank}, host={self.host}, pid={self.pid})"

    def _lost_host(self) -> None:
        self._abort(ConnectionAbortedError("Lost connection to process host"))

    def _abort(self, e: Exception) -> None:
        if self._state in ["launched", "running"]:
            self._exit_message(e)
        self._state = "aborted"

    def send(self, msg: object) -> None:
        msg = pickle_dumps(msg)
        self._context._schedule(lambda: self._send(msg))

    def _send(self, msg: bytes) -> None:
        if self._state != "aborted":
            self._context._sends += 1
            self.host._send(pickle_dumps(("send", self._id, msg)))

    def signal(self, signal: int = signal.SIGTERM, group: bool = True) -> None:
        self._context._schedule(lambda: self._signal(signal, group))

    def _signal(self, signal: int, group: bool) -> None:
        if self._state != "aborted":
            self.host._send(pickle_dumps(("signal", self._id, signal, group)))

    def _exited(self, returncode: int) -> None:
        self._state = "exited"
        self._returncode = returncode
        self._exit_message(returncode)
        self._context._exits += 1

    def _exit_message(self, returncode: Union[int, Exception]) -> None:
        self.host._proc_table.pop(self._id)
        self._context._produce_message(self, ProcessExited(returncode))

    def _started(self, pid: Union[str, int]) -> None:
        if isinstance(pid, int):
            self._state = "running"
            self._context._produce_message(self, ProcessStarted(pid))
            self._pid = pid
            self._context._starts += 1
        else:
            self._abort(ProcessFailedToStart(pid))

    def _response(self, msg: bytes) -> None:
        unpickled: NamedTuple = pickle_loads(msg)
        self._context._produce_message(self, unpickled)

    def __del__(self) -> None:
        self._context._proc_deletes += 1


def _get_hostname_if_exists(msg: bytes) -> Optional[str]:
    """
    Get's hostname from zmq message if it exists for logging to Connection
    """
    if not len(msg):
        return None
    try:
        cmd, _, hostname = pickle_loads(msg)
        if cmd != "_hostname" or not isinstance(hostname, str):
            return None
        return hostname
    except Exception:
        return None


class Status(NamedTuple):
    launches: int
    starts: int
    exits: int
    sends: int
    responses: int
    process_deletes: int
    unassigned_hosts: int
    unassigned_connections: int
    poll_percentage: float
    active_percentage: float
    heartbeats: int
    heartbeat_average_ttl: float
    heartbeat_min_ttl: float
    connection_histogram: Dict[str, int]
    avg_event_loop_time: float
    max_event_loop_time: float


class Letter(NamedTuple):
    sender: Union[Host, Process, None]
    message: Any


class HostConnected(NamedTuple):
    hostname: str


class FilteredMessageQueue(ABC):
    def __init__(self) -> None:
        self._client_queue: deque[Letter] = deque()
        self._filter: Optional["Filter"] = None

    @abstractmethod
    def _read_messages(self, timeout: Optional[float]) -> List[Letter]: ...

    def _set_filter_to(self, new) -> None:
        old = self._filter
        if new is old:  # None None or f f
            return
        self._filter = new
        if old is not None:
            self._client_queue.rotate(old._cursor)
        if new is not None:  # None f, or f None, or f f'
            new._cursor = 0

    def _next_message(self, timeout) -> Optional[Letter]:
        # return the first message that passes self._filter, starting at self._filter._cursor
        queue = self._client_queue
        if self._filter is None:
            if queue:
                return queue.popleft()
            messages = self._read_messages(timeout)
            if not messages:
                return None
            head, *rest = messages
            queue.extend(rest)
            return head
        else:
            filter = self._filter
            filter_fn = filter._fn
            for i in range(filter._cursor, len(queue)):
                if filter_fn(queue[0]):
                    filter._cursor = i
                    return queue.popleft()
                queue.rotate(-1)
            if timeout is None:
                while True:
                    messages = self._read_messages(None)
                    for i, msg in enumerate(messages):
                        if filter_fn(msg):
                            filter._cursor = len(queue)
                            queue.extendleft(messages[-1:i:-1])
                            return msg
                        queue.append(msg)
            else:
                t = time.time()
                expiry = t + timeout
                while t <= expiry:
                    messages = self._read_messages(expiry - t)
                    if not messages:
                        break
                    for i, msg in enumerate(messages):
                        if filter_fn(msg):
                            filter._cursor = len(queue)
                            queue.extendleft(messages[-1:i:-1])
                            return msg
                        queue.append(msg)
                    t = time.time()
                filter._cursor = len(queue)
                return None

    def recv(self, timeout: Optional[float] = None, _filter=None) -> Letter:
        self._set_filter_to(_filter)
        msg = self._next_message(timeout)
        if msg is None:
            raise TimeoutError()
        return msg

    def recvloop(self, timeout=None):
        while True:
            yield self.recv(timeout)

    def recvready(self, timeout: Optional[float] = 0, _filter=None) -> List[Letter]:
        self._set_filter_to(_filter)
        result = []
        append = result.append
        next_message = self._next_message
        msg = next_message(timeout)
        while msg is not None:
            append(msg)
            msg = next_message(0)
        return result

    def messagefilter(self, fn):
        if isinstance(fn, (tuple, type)):

            def wrapped(msg):
                return isinstance(msg.message, fn)

        else:
            wrapped = fn
        return Filter(self, wrapped)


class Filter:
    def __init__(
        self, context: FilteredMessageQueue, fn: Callable[[Letter], bool]
    ) -> None:
        self._context = context
        self._fn = fn
        self._cursor = 0

    def recv(self, timeout=None):
        return self._context.recv(timeout, _filter=self)

    def recvloop(self, timeout=None):
        while True:
            yield self.recv(timeout)

    def recvready(self, timeout=0):
        return self._context.recvready(timeout, _filter=self)


def TTL(timeout: Optional[float]) -> Callable[[], float]:
    if timeout is None:
        return lambda: math.inf
    expiry = time.time() + timeout
    return lambda: max(expiry - time.time(), 0)


class ProcessList(tuple):
    def send(self, msg: Any) -> None:
        if not self:
            return
        ctx = self[0]._context
        msg = pickle_dumps(msg)
        ctx._schedule(lambda: self._send(msg))

    def _send(self, msg: bytes) -> None:
        for p in self:
            p._send(msg)

    def __getitem__(self, index):
        result = super().__getitem__(index)
        # If the index is a slice, convert the result to MyTuple
        if isinstance(index, slice):
            return ProcessList(result)
        return result


class Context(FilteredMessageQueue):
    def __init__(
        self,
        port: Optional[int] = None,
        log_format: Optional[str] = None,
        log_interval: float = LOG_INTERVAL,
    ) -> None:
        super().__init__()
        if log_format is not None:
            path = log_format.format(name="supervisor")
            logger.warning(f"Redirect logging to {path}")
            Path(path).parent.mkdir(exist_ok=True, parents=True)
            with open(path, "w") as f:
                os.dup2(f.fileno(), sys.stdout.fileno())
                os.dup2(f.fileno(), sys.stderr.fileno())

        self._log_interval: float = log_interval
        self._context: zmq.Context = zmq.Context(1)

        # to talk to python clients in this process
        self._requests: deque[Callable[[], None]] = deque()
        self._delivered_messages: deque[List[Letter]] = deque()
        self._delivered_messages_entry: List[Letter] = []
        self._requests_ready: zmq.Socket = self._socket(zmq.PAIR)

        self._requests_ready.bind("inproc://doorbell")
        self._doorbell: zmq.Socket = self._socket(zmq.PAIR)
        self._doorbell.connect("inproc://doorbell")
        self._doorbell_poller = zmq.Poller()
        self._doorbell_poller.register(self._doorbell, zmq.POLLIN)

        self._backend: zmq.Socket = self._socket(zmq.ROUTER)
        self._backend.setsockopt(zmq.IPV6, True)
        if port is None:
            # Specify a min and max port range; the default min/max triggers a
            # codepath in zmq that is vulnerable to races between ephemeral port
            # acqusition and last_endpoint being available.
            self.port = self._backend.bind_to_random_port("tcp://*", 49153, 65536)
        else:
            self._backend.bind(f"tcp://*:{port}")
            self.port = port

        self._poller = zmq.Poller()
        self._poller.register(self._backend, zmq.POLLIN)
        self._poller.register(self._requests_ready, zmq.POLLIN)

        self._unassigned_hosts: deque[Host] = deque()
        self._unassigned_connections: deque[Connection] = deque()
        self._name_to_connection: Dict[bytes, Connection] = {}
        self._last_heartbeat_check: float = time.time()
        self._last_logstatus: float = self._last_heartbeat_check
        self._next_id = 0
        self._exits = 0
        self._sends = 0
        self._responses = 0
        self._launches = 0
        self._starts = 0
        self._proc_deletes = 0
        self._reset_heartbeat_stats()

        self._exit_event_loop = False
        self._pg_name = 0
        self.log_format = log_format
        self.log_status = lambda status: None

        self._thread = Thread(target=self._event_loop, daemon=True)
        self._thread.start()

    def _socket(self, kind: int) -> zmq.Socket:
        socket = self._context.socket(kind)
        socket.setsockopt(zmq.SNDHWM, 0)
        socket.setsockopt(zmq.RCVHWM, 0)
        return socket

    def _attach(self) -> None:
        while self._unassigned_connections and self._unassigned_hosts:
            c = self._unassigned_connections[0]
            h = self._unassigned_hosts[0]
            if c.state is _LOST:
                self._unassigned_connections.popleft()
            elif h._state is _LOST:
                self._unassigned_hosts.popleft()
            else:
                self._unassigned_connections.popleft()
                self._unassigned_hosts.popleft()
                c.host = h
                h._name = c.name
                assert c.hostname is not None
                h._context._produce_message(h, HostConnected(c.hostname))
                h._hostname = c.hostname
                h._state = c.state = _ATTACHED
                for msg in h._deferred_sends:
                    self._backend.send_multipart([h._name, msg])
                h._deferred_sends.clear()

    def _event_loop(self) -> None:
        _time_poll = 0
        _time_process = 0
        _time_loop = 0
        _max_time_loop = -1
        _num_loops = 0
        while True:
            time_begin = time.time()
            poll_result = self._poller.poll(timeout=int(HEARTBEAT_INTERVAL * 1000))
            time_poll = time.time()

            for sock, _ in poll_result:
                # known host managers
                if sock is self._backend:
                    f, msg = self._backend.recv_multipart()
                    if f not in self._name_to_connection:
                        hostname = _get_hostname_if_exists(msg)
                        connection = self._name_to_connection[f] = Connection(
                            self, f, hostname
                        )
                        self._unassigned_connections.append(connection)
                        self._attach()
                    else:
                        self._name_to_connection[f].handle_message(self, msg)
                elif sock is self._requests_ready:
                    ttl = TTL(HEARTBEAT_INTERVAL / 2)
                    while self._requests and ttl() > 0:
                        # pyre-ignore[29]
                        self._requests_ready.recv()
                        fn = self._requests.popleft()
                        fn()
                        del fn  # otherwise we hold a handle until
                        # the next time we run a command
            if self._exit_event_loop:
                return
            t = time.time()
            if t - time_begin > HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS:
                logger.warning(
                    f"Main poll took too long! ({t - time_begin} > {HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS} seconds). Host managers will think we are dead."
                )
            elapsed = t - self._last_heartbeat_check
            should_check_heartbeat = elapsed > HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
            if should_check_heartbeat:
                self._last_heartbeat_check = t
                # priority queue would be log(N)
                for connection in self._name_to_connection.values():
                    connection.check_alive_at(self, t)

            # Marking futures ready should always happen at the end of processing events above
            # to unblock anything processing the futures, before we start waiting for more events.
            if self._delivered_messages_entry:
                self._delivered_messages.append(self._delivered_messages_entry)
                self._delivered_messages_entry = []
                self._requests_ready.send(b"")
            time_end = time.time()
            _time_poll += time_poll - time_begin
            _time_process += time_end - time_poll
            _time_loop += time_end - time_begin
            _max_time_loop = max(_max_time_loop, time_end - time_begin)
            _num_loops += 1

            elapsed = t - self._last_logstatus
            if elapsed > self._log_interval:
                self._last_logstatus = t
                self._logstatus(
                    _time_poll / elapsed,
                    _time_process / elapsed,
                    _time_loop / _num_loops,
                    _max_time_loop,
                )
                _time_poll = 0
                _time_process = 0
                _time_loop = 0
                _max_time_loop = -1
                _num_loops = 0

    def _logstatus(
        self,
        poll_fraction: float,
        active_fraction: float,
        avg_event_loop_time: float,
        max_event_loop_time: float,
    ) -> None:
        connection_histogram = {}
        for connection in self._name_to_connection.values():
            state = connection.state.name
            connection_histogram[state] = connection_histogram.setdefault(state, 0) + 1

        status = Status(
            self._launches,
            self._starts,
            self._exits,
            self._sends,
            self._responses,
            self._proc_deletes,
            len(self._unassigned_hosts),
            len(self._unassigned_connections),
            poll_fraction * 100,
            active_fraction * 100,
            self._heartbeats,
            self._heartbeat_ttl_sum / self._heartbeats if self._heartbeats else 0,
            self._heartbeat_min_ttl,
            connection_histogram,
            avg_event_loop_time,
            max_event_loop_time,
        )
        self._reset_heartbeat_stats()

        logger.info(
            "supervisor status: %s process launches, %s starts, %s exits, %s message sends, %s message responses,"
            " %s process __del__, %s hosts waiting for connections, %s connections waiting for handles,"
            " time is %.2f%% polling and %.2f%% active, heartbeats %s, heartbeat_avg_ttl %.4f,"
            " heartbeat_min_ttl %.4f, connections %s, avg_event_loop_time %.4f seconds, max_event_loop_time %.4f seconds",
            *status,
        )
        self.log_status(status)

    def _heartbeat_ttl(self, ttl: float) -> None:
        # Updates heartbeat stats with most recent ttl
        self._heartbeats += 1
        self._heartbeat_ttl_sum += ttl
        self._heartbeat_min_ttl = min(self._heartbeat_min_ttl, ttl)

    def _reset_heartbeat_stats(self) -> None:
        self._heartbeats = 0
        self._heartbeat_ttl_sum = 0
        self._heartbeat_min_ttl = sys.maxsize

    def _schedule(self, fn: Callable[[], None]) -> None:
        self._requests.append(fn)
        self._doorbell.send(b"")

    def request_hosts(self, n: int) -> "Tuple[Host, ...]":
        """
        Request from the scheduler n hosts to run processes on.
        The future is fulfilled when the reservation is made, but
        potenially before all the hosts check in with this API.

        Note: implementations that use existing slurm-like schedulers,
        will immediately full the future because the reservation was
        already made.
        """
        hosts = tuple(Host(self) for i in range(n))
        self._schedule(lambda: self._request_hosts(hosts))
        return hosts

    def _request_host(self, h: Host) -> None:
        self._unassigned_hosts.append(h)
        self._attach()

    def _request_hosts(self, hosts: Sequence[Host]) -> None:
        for h in hosts:
            self._request_host(h)

    def return_hosts(self, hosts: Sequence[Host], error: Optional[str] = None) -> None:
        """
        Processes on the returned hosts will be killed,
        and future processes launches with the host will fail.
        """
        self._schedule(lambda: self._return_hosts(hosts, error))

    def _return_hosts(self, hosts: Sequence[Host], error: Optional[str]) -> None:
        for h in hosts:
            h._lost(error)

    def replace_hosts(self, hosts: Sequence[Host]) -> "Tuple[Host, ...]":
        """
        Request that these hosts be replaced with new hosts.
        Processes on the host will be killed, and future processes
        launches will be launched on the new hosts.
        """
        # if the host is disconnected, return it to the pool of unused hosts
        # and we hope that scheduler has replaced the job
        # if the host is still connected, then send the host a message
        # then cancel is processes and abort with an error to get the
        # the scheduler to reassign the host
        hosts = list(hosts)
        self.return_hosts(hosts, "supervisor requested replacement")
        return self.request_hosts(len(hosts))

    def _shutdown(self) -> None:
        self._exit_event_loop = True
        for connection in self._name_to_connection.values():
            connection.lost(self, None)

    def shutdown(self) -> None:
        self._schedule(self._shutdown)
        self._thread.join()
        self._backend.close()
        self._requests_ready.close()
        self._doorbell.close()
        self._context.term()

    # TODO: other arguments like environment, etc.
    def create_process_group(
        self,
        hosts: Sequence[Host],
        args: Union["_FunctionCall", Sequence[str]],
        processes_per_host: int = 1,
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        name: Optional[str] = None,
        simulate: bool = False,
    ) -> ProcessList:
        world_size = processes_per_host * len(hosts)
        if name is None:
            name = f"pg{self._pg_name}"
            self._pg_name += 1
        logger.info(
            "Starting process group %r with %d processes (%s hosts * %s processes per host)",
            name,
            world_size,
            len(hosts),
            processes_per_host,
        )
        popen = {"args": args, "env": env, "cwd": cwd}
        procs = ProcessList(
            Process(
                self,
                h,
                i * processes_per_host + j,
                processes_per_host,
                world_size,
                popen,
                name,
                simulate,
            )
            for i, h in enumerate(hosts)
            for j in range(processes_per_host)
        )
        self._schedule(lambda: self._launch_processes(procs))
        return procs

    def _launch_processes(self, procs: Sequence[Process]) -> None:
        for p in procs:
            p.host._launch(p)

    def _produce_message(
        self, sender: Union[Host, Process], message: NamedTuple
    ) -> None:
        self._delivered_messages_entry.append(Letter(sender, message))

    def _read_messages(self, timeout: Optional[float]) -> List[Letter]:
        if timeout is not None and not self._doorbell_poller.poll(
            timeout=int(1000 * timeout)
        ):
            return []
        # pyre-ignore[29]
        self._doorbell.recv()
        return self._delivered_messages.popleft()


@cache
def get_message_queue(
    supervisor_ident: Optional[int] = None, supervisor_pipe: Optional[str] = None
) -> "LocalMessageQueue":
    """
    Processes launched on the hosts can use this function to connect
    to the messaging queue of the supervisor.

    Messages send from here can be received by the supervisor using
    `proc.recv()` and messages from proc.send() will appear in this queue.
    """
    if supervisor_ident is None:
        supervisor_ident = int(os.environ["SUPERVISOR_IDENT"])
    if supervisor_pipe is None:
        supervisor_pipe = os.environ["SUPERVISOR_PIPE"]

    return LocalMessageQueue(supervisor_ident, supervisor_pipe)


class LocalMessageQueue(FilteredMessageQueue):
    """
    Used by processes launched on the host to communicate with the supervisor.
    Also used as the pipe between main worker process and pipe process with worker pipes.
    """

    def __init__(self, supervisor_ident: int, supervisor_pipe: str) -> None:
        super().__init__()
        self._ctx = zmq.Context(1)
        self._sock = self._socket(zmq.DEALER)
        proc_id = supervisor_ident.to_bytes(8, byteorder="little")
        self._sock.setsockopt(zmq.IDENTITY, proc_id)
        self._sock.connect(supervisor_pipe)
        self._sock.send(b"")
        self._poller = zmq.Poller()
        self._poller.register(self._sock, zmq.POLLIN)
        self._async_socket: Optional[zmq.asyncio.Socket] = None

    def _socket(self, kind) -> zmq.Socket:
        sock = self._ctx.socket(kind)
        sock.setsockopt(zmq.SNDHWM, 0)
        sock.setsockopt(zmq.RCVHWM, 0)
        return sock

    def _read_messages(self, timeout: Optional[float]) -> List[Letter]:
        if timeout is not None and not self._poller.poll(timeout=int(1000 * timeout)):
            return []
        return [Letter(None, self._sock.recv_pyobj())]

    async def recv_async(self) -> Letter:
        if self._async_socket is None:
            self._async_socket = zmq.asyncio.Socket.from_socket(self._sock)
        return Letter(None, await self._async_socket.recv_pyobj())

    def send(self, message: Any) -> None:
        self._sock.send_pyobj(message)

    def close(self) -> None:
        self._sock.close()
        self._ctx.term()


class _FunctionCall(NamedTuple):
    target: str
    args: Tuple[str]
    kwargs: Dict[str, str]


def FunctionCall(target: str, *args, **kwargs) -> _FunctionCall:
    if target.startswith("__main__."):
        file = sys.modules["__main__"].__file__
        sys.modules["__entry__"] = sys.modules["__main__"]
        target = f"{file}:{target.split('.', 1)[1]}"
    return _FunctionCall(target, args, kwargs)


# [Threading Model]
# The supervisor policy script runs in the main thread,
# and there is a separate _event_loop thread launched by the
# Context object for managing messages from host managers.
# Context, Host, and Process objects get created on the
# main thread, and their public APIs contain read
# only parameters. Private members should only be read/
# written from the event loop for these objects.
# The context._schedule provides a way to schedule
# a function to run on the event loop from the main thread.
# The only observable changes from the main thread go through
# future objects.
# The _event_loop maintains a list of
# futures to be marked finished (_finished_futures_entry) which it will
# be sent to the main thread at the end of one event loop iteration to
# actually mutate the future to be completed, and run callbacks.
# _finished_futures, _request_ready, and Future instances are the only
# objects the main thread should mutate after they are created.

# [zeromq Background]
# We use zeromq to make high-throughput messaging possible despite
# using Python for the event loop. There are a few differences from traditional
# tcp sockets that are important:
# * A traditional 'socket' is a connection that can be read or written
#   and is connected to exactly one other socket. In zeromq sockets
#   can be connected to multiple other sockets. For instance,
#   context._backend is connect to _all_ host managers.
# * To connect sockets traditionally, one side listens on a listener socket
#   for a new connection with 'bind'/'listen', and the other side 'connect's its socket.
#   This creates another socket on the 'bind' side (three total sockets). Bind and listen
#   must happen before 'connect' or the connection will be refused. In zeromq, any socket can
#   bind or connect. A socket that binds can be connected to many others if multiple other sockets connect to it.
#   A socket can also connect itself to multiple other sockets by calling connect multiple times
#   (we do not use this here).
#   Connect can come before bind, zeromq will retry if the bind-ing process is not yet there.
# * When sockets are connected to multiple others, we have to define what it means to send
#   or receive. This is configured when creating a socket. zmq.PAIR asserts there will only
#   be a single other sockets, and behaves like a traditional socket. zmq.DEALER sends data
#   by round robining (dealing) each message to the sockets its connected to. A receive will
#   get a message from any of the incoming sockets. zmq.ROUTER receives a message from one of its
#   connections and _prefixes_ it with the identity of the socket that sent it (recv_multipart) is
#   used to get a list of message parts and find this identity. When sending a message with zmq.ROUTER,
#   the first part must be an identity, and it will send (route) the message to the connection with
#   that identity.
# The zeromq guide has more information, but this implementation is intentially only use the above
# features to make it easier to use a different message broker if needed.
