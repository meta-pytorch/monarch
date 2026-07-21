# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Execution-surface reproducer.

Brings up a small mesh, exposes the mesh-admin HTTP endpoint, and drives
the ``execution`` introspection surface deterministically over a stdin
command / stdout sentinel handshake. The integration test
(``execution.rs``) is the parent process: it writes commands and waits
for sentinels rather than sleeping.

Actors:

  - ``BusyActor`` (direct dispatch): holds an invocation per id until it
    is individually released or raised. Direct dispatch is concurrent, so
    ``control`` runs while ``hold`` awaits.
  - an idle sibling ``BusyActor`` that is never invoked.
  - ``QueueActor`` (queue dispatch): a single held invocation shows the
    framework hook also fires in queue mode. Queue dispatch is serialized,
    so a held invocation is never released through a second endpoint call
    (that would deadlock the dispatch loop); the test asserts
    ``count == 1`` then tears the proc down.
  - ``ConcurrentActor`` (queue dispatch): ``hold`` uses
    ``@concurrent_endpoint``, so multiple held invocations overlap and
    ``control`` can release them while they wait. This covers the
    queue-dispatch-compatible pattern used by resource managers.
  - ``deadlock_actor`` (queue dispatch): a ``BusyActor`` reused under queue
    dispatch. Its blocking plain ``@endpoint`` ``hold`` monopolizes the Python
    dispatch loop, so a later ``control`` release is stuck behind it and can
    never run -- a genuine deadlock (the failure the airport turnaround demo
    hits under the default queue dispatch). ``execution.rs`` drives it to prove
    the mesh-admin API surfaces the wedge (``execution.active_count`` stays
    pinned at 1 on the wedged invocation).

Each proc pins its dispatch mode explicitly via a ``bootstrap`` callable
that sets ``MONARCH_ACTOR_QUEUE_DISPATCH`` and calls
``reload_config_from_env()`` before any actor spawns on it (``0`` for the
direct-dispatch ``busy``/``idle`` procs, ``1`` for the ``queue`` proc), so
the choice is independent of the process-global default. The reload is
required: the Env config layer is materialized once at proc start, so
setting the variable in the bootstrap only takes effect after the reload.

Stdin command protocol, one command per line::

    HOLD <actor> <id>     start a held invocation;      prints EXEC_ACK HOLD <id>
    RELEASE <actor> <id>  let a held invocation return; prints EXEC_SENDING then
                          EXEC_ACK release <id>
    RAISE <actor> <id>    make a held invocation raise; prints EXEC_SENDING then
                          EXEC_ACK raise <id>

``<actor>`` is ``busy``, ``queue``, ``concurrent``, or ``deadlock``. Each ``hold`` signals
"entered" from inside the handler body -- after the framework's
``_execution_start`` has recorded the invocation and before the handler
blocks -- so the signal means "mesh-admin now reports this invocation", not
merely "user code started". The signal travels over a monarch ``Port`` rather
than handler stdout: a spawned proc's stdout reaches the client on a
multi-second aggregation window, too coarse for a handshake. The main task
receives on the port and echoes ``EXEC_ENTERED <id>`` to its own unbuffered
stdout.
"""

import asyncio
import logging
import os
import sys

from monarch._rust_bindings.monarch_hyperactor.config import (  # @manual=//monarch/monarch_extension:monarch_extension
    reload_config_from_env,
)
from monarch._src.actor.actor_mesh import Channel, Port, PortReceiver
from monarch._src.actor.telemetry import TracingForwarder
from monarch.actor import Actor, concurrent_endpoint, context, endpoint
from monarch.job import ProcessJob

logger: logging.Logger = logging.getLogger("execution_workload")
logger.addHandler(TracingForwarder())
logger.setLevel(logging.INFO)


def _enable_queue_dispatch() -> None:
    """Proc bootstrap: force queue dispatch for actors spawned here.

    The reload is required, not cosmetic: the Env config layer is
    materialized once at proc start (before this bootstrap runs), so setting
    the variable only takes effect after ``reload_config_from_env()`` rebuilds
    that layer. Affects only this proc.
    """
    os.environ["MONARCH_ACTOR_QUEUE_DISPATCH"] = "1"
    reload_config_from_env()


def _disable_queue_dispatch() -> None:
    """Proc bootstrap: force direct (concurrent) dispatch for actors here.

    ``BusyActor`` depends on concurrent dispatch -- it holds one ``hold``
    while a second ``hold``/``control`` overlaps -- so this proc is pinned to
    direct dispatch rather than inheriting the process-global
    ``ACTOR_QUEUE_DISPATCH`` default (now queue dispatch). See
    ``_enable_queue_dispatch`` for why the reload is required.
    """
    os.environ["MONARCH_ACTOR_QUEUE_DISPATCH"] = "0"
    reload_config_from_env()


class BusyActor(Actor):
    """Holds invocations until each is individually released or raised.

    ``hold`` creates a per-id event, signals "entered", then blocks on the
    event; ``control`` wakes a held invocation, either to return normally
    (``release``) or to raise (``raise``). Under direct dispatch (the
    default) ``control`` runs concurrently with the blocked ``hold``.
    """

    def __init__(self) -> None:
        self._events: dict[str, asyncio.Event] = {}
        self._raise: dict[str, bool] = {}

    @endpoint
    async def hold(self, id: str, entered: "Port[str]") -> None:
        self._events[id] = asyncio.Event()
        self._raise[id] = False
        logger.info("hold %s", id)
        # Signal "entered" from inside the handler body: the framework has
        # already run _execution_start, so the registry reflects this
        # invocation, and the handler has not yet blocked. Sent over a
        # monarch Port, not stdout (see module docstring for why).
        entered.send(id)
        await self._events[id].wait()
        if self._raise[id]:
            raise RuntimeError("requested")

    @endpoint
    async def control(self, id: str, op: str) -> None:
        logger.info("control %s %s", op, id)
        if op == "release":
            self._events[id].set()
        elif op == "raise":
            self._raise[id] = True
            self._events[id].set()
        else:
            raise ValueError(f"unknown op: {op}")

    @endpoint
    async def whoami(self) -> str:
        return str(context().actor_instance.actor_id)


class QueueActor(Actor):
    """Queue-dispatch sibling. Only ``hold`` is exercised: queue dispatch
    is serialized, so a held invocation is never released (that would
    deadlock the dispatch loop). The test asserts ``count == 1`` then
    tears the proc down."""

    def __init__(self) -> None:
        self._events: dict[str, asyncio.Event] = {}

    @endpoint
    async def hold(self, id: str, entered: "Port[str]") -> None:
        self._events[id] = asyncio.Event()
        logger.info("hold %s", id)
        entered.send(id)
        await self._events[id].wait()

    @endpoint
    async def whoami(self) -> str:
        return str(context().actor_instance.actor_id)


class ConcurrentActor(Actor):
    """Queue-dispatch actor whose held work runs through @concurrent_endpoint."""

    def __init__(self) -> None:
        self._events: dict[str, asyncio.Event] = {}
        self._raise: dict[str, bool] = {}

    @concurrent_endpoint
    async def hold(self, id: str, entered: "Port[str]") -> None:
        self._events[id] = asyncio.Event()
        self._raise[id] = False
        logger.info("concurrent hold %s", id)
        entered.send(id)
        await self._events[id].wait()
        if self._raise[id]:
            raise RuntimeError("requested")

    @endpoint
    async def control(self, id: str, op: str) -> None:
        logger.info("concurrent control %s %s", op, id)
        if op == "release":
            self._events[id].set()
        elif op == "raise":
            self._raise[id] = True
            self._events[id].set()
        else:
            raise ValueError(f"unknown op: {op}")

    @endpoint
    async def whoami(self) -> str:
        return str(context().actor_instance.actor_id)


async def _stdin_reader() -> asyncio.StreamReader:
    """An asyncio reader over this process's stdin."""
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    return reader


async def _hold_task(actor, id: str, entered_port: "Port[str]") -> None:
    """Await one held invocation via ``call_one`` (a real response port).

    ``call_one`` rather than ``broadcast`` is what keeps the actor alive
    when the handler raises: a handler ``Exception`` is routed back to the
    caller as an ``ActorError`` (``_Actor.handle``'s ``except Exception``
    arm), whereas ``broadcast`` resolves through a ``DroppingPort`` that
    re-raises the error inside the actor and aborts it. ``hold`` blocks
    until released, so this runs as a background task.
    """
    try:
        await actor.hold.call_one(id, entered_port)
    except Exception as e:
        # The raise path returns the handler error here as an ActorError;
        # the registry decrement already happened in the framework's
        # finally. Log rather than propagate so the command loop survives.
        logger.debug("hold %s ended with %s", id, type(e).__name__)


async def async_main() -> None:
    job = ProcessJob({"hosts": 1}).enable_telemetry()
    # In-flight held invocations, tracked so they can be cancelled at
    # shutdown.
    holds: dict[str, asyncio.Task] = {}
    drainer: asyncio.Task | None = None
    try:
        state = job.state(cached_path=None)
        host = state.hosts

        busy_proc = host.spawn_procs(
            name="execution_busy", bootstrap=_disable_queue_dispatch
        )
        idle_proc = host.spawn_procs(
            name="execution_idle", bootstrap=_disable_queue_dispatch
        )
        queue_proc = host.spawn_procs(
            name="execution_queue", bootstrap=_enable_queue_dispatch
        )
        concurrent_proc = host.spawn_procs(
            name="execution_concurrent", bootstrap=_enable_queue_dispatch
        )
        deadlock_proc = host.spawn_procs(
            name="execution_deadlock", bootstrap=_enable_queue_dispatch
        )

        busy = busy_proc.spawn("busy_actor", BusyActor)
        idle = idle_proc.spawn("idle_actor", BusyActor)
        queue = queue_proc.spawn("queue_actor", QueueActor)
        concurrent = concurrent_proc.spawn("concurrent_actor", ConcurrentActor)
        # A queue-dispatch BusyActor: its blocking plain @endpoint `hold`
        # monopolizes the serial dispatch loop, so a later `control` release
        # queues behind it and can never run -- a deadlock the admin API surfaces.
        deadlock = deadlock_proc.spawn("deadlock_actor", BusyActor)

        actors = {
            "busy": busy,
            "queue": queue,
            "concurrent": concurrent,
            "deadlock": deadlock,
        }

        busy_ref = await busy.whoami.call_one()
        idle_ref = await idle.whoami.call_one()
        queue_ref = await queue.whoami.call_one()
        concurrent_ref = await concurrent.whoami.call_one()
        deadlock_ref = await deadlock.whoami.call_one()

        # Held invocations send their id over this port from inside the handler
        # body; a background task drains it and echoes EXEC_ENTERED to stdout.
        entered_port: Port[str]
        entered_recv: PortReceiver[str]
        entered_port, entered_recv = Channel[str].open()

        async def drain_entered() -> None:
            while True:
                entered_id = await entered_recv.recv()
                print(f"EXEC_ENTERED {entered_id}", flush=True)

        drainer = asyncio.create_task(drain_entered())

        print(f"Mesh admin server listening on {state.admin_url}", flush=True)
        print(f"  - Busy actor:  {busy_ref}", flush=True)
        print(f"  - Idle actor:  {idle_ref}", flush=True)
        print(f"  - Queue actor: {queue_ref}", flush=True)
        print(f"  - Concurrent actor: {concurrent_ref}", flush=True)
        print(f"  - Deadlock actor: {deadlock_ref}", flush=True)

        reader = await _stdin_reader()
        while True:
            raw = await reader.readline()
            if not raw:
                break  # stdin EOF
            line = raw.decode().strip()
            if not line:
                continue
            parts = line.split()
            cmd = parts[0].upper()
            if cmd == "HOLD":
                _, which, id = parts
                # hold blocks until released, so run it as a background task
                # (via call_one -- see _hold_task). The "entered" signal
                # still arrives mid-handler over entered_port.
                holds[id] = asyncio.create_task(
                    _hold_task(actors[which], id, entered_port)
                )
                print(f"EXEC_ACK HOLD {id}", flush=True)
            elif cmd in ("RELEASE", "RAISE"):
                _, which, id = parts
                op = cmd.lower()
                # Signal dispatch *before* the call: under queue dispatch a
                # release can wedge behind a blocked hold and never return, so
                # this positive sentinel lets the test rule out "never sent".
                print(f"EXEC_SENDING {op} {id}", flush=True)
                await actors[which].control.call_one(id, op)
                print(f"EXEC_ACK {op} {id}", flush=True)
            else:
                print(f"EXEC_ERR unknown command {cmd}", flush=True)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\nShutting down...", flush=True)
        for task in holds.values():
            task.cancel()
        if drainer is not None:
            drainer.cancel()
        # Each ProcessJob worker runs in its own detached session; job.kill()
        # reaps the whole session -- the worker and every proc it spawned.
        job.kill()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
