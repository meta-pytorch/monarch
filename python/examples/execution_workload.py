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

Queue dispatch is forced for the queue proc by a ``bootstrap`` callable
that sets ``MONARCH_ACTOR_QUEUE_DISPATCH=1`` before any actor spawns on
it; ``ACTOR_QUEUE_DISPATCH`` is read from the process-global config at
actor-spawn time.

Stdin command protocol, one command per line::

    HOLD <actor> <id>     start a held invocation;      prints EXEC_ACK HOLD <id>
    RELEASE <actor> <id>  let a held invocation return; prints EXEC_ACK release <id>
    RAISE <actor> <id>    make a held invocation raise; prints EXEC_ACK raise <id>

``<actor>`` is ``busy`` or ``queue``. Each ``hold`` signals "entered"
from inside the handler body -- after the framework's ``_execution_start``
has recorded the invocation and before the handler blocks -- so the
signal means "mesh-admin now reports this invocation", not merely "user
code started". The signal travels over a monarch ``Port`` rather than
handler stdout: a spawned proc's stdout reaches the client on a
multi-second aggregation window, too coarse for a handshake. The main
task receives on the port and echoes ``EXEC_ENTERED <id>`` to its own
unbuffered stdout.
"""

import asyncio
import logging
import os
import sys

from monarch._src.actor.actor_mesh import Channel, Port, PortReceiver
from monarch._src.actor.telemetry import TracingForwarder
from monarch.actor import Actor, context, endpoint
from monarch.job import ProcessJob

logger: logging.Logger = logging.getLogger("execution_workload")
logger.addHandler(TracingForwarder())
logger.setLevel(logging.INFO)


def _enable_queue_dispatch() -> None:
    """Proc bootstrap: force queue dispatch for actors spawned here.

    ``ACTOR_QUEUE_DISPATCH`` is read at actor-spawn time from the
    process-global config, which is backed by ``MONARCH_ACTOR_QUEUE_DISPATCH``.
    Setting it here affects only this proc, not the direct-dispatch procs.
    """
    os.environ["MONARCH_ACTOR_QUEUE_DISPATCH"] = "1"


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
    job = ProcessJob({"hosts": 1}).enable_admin()
    state = job.state(cached_path=None)
    host = state.hosts

    busy_proc = host.spawn_procs(name="execution_busy")
    idle_proc = host.spawn_procs(name="execution_idle")
    queue_proc = host.spawn_procs(
        name="execution_queue", bootstrap=_enable_queue_dispatch
    )

    busy = busy_proc.spawn("busy_actor", BusyActor)
    idle = idle_proc.spawn("idle_actor", BusyActor)
    queue = queue_proc.spawn("queue_actor", QueueActor)

    actors = {"busy": busy, "queue": queue}

    busy_ref = await busy.whoami.call_one()
    idle_ref = await idle.whoami.call_one()
    queue_ref = await queue.whoami.call_one()

    # Held invocations send their id over this port from inside the handler
    # body; a background task drains it and echoes EXEC_ENTERED to stdout.
    entered_port: Port[str]
    entered_recv: PortReceiver[str]
    entered_port, entered_recv = Channel[str].open()

    async def drain_entered() -> None:
        while True:
            entered_id = await entered_recv.recv()
            print(f"EXEC_ENTERED {entered_id}", flush=True)

    drainer = asyncio.ensure_future(drain_entered())

    print(f"Mesh admin server listening on {state.admin_url}", flush=True)
    print(f"  - Busy actor:  {busy_ref}", flush=True)
    print(f"  - Idle actor:  {idle_ref}", flush=True)
    print(f"  - Queue actor: {queue_ref}", flush=True)

    reader = await _stdin_reader()
    # In-flight held invocations, tracked so they can be cancelled at
    # shutdown.
    holds: dict[str, asyncio.Task] = {}
    try:
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
                holds[id] = asyncio.ensure_future(
                    _hold_task(actors[which], id, entered_port)
                )
                print(f"EXEC_ACK HOLD {id}", flush=True)
            elif cmd in ("RELEASE", "RAISE"):
                _, which, id = parts
                op = cmd.lower()
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
        drainer.cancel()
        await busy_proc.stop()
        await idle_proc.stop()
        await queue_proc.stop()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
