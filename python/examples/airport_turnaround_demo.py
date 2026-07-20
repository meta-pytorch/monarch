# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Execution-surface observability demo (airport turnaround).

A run-and-watch companion to the ``execution`` mesh-admin surface, scaled up
from ``execution_demo.py``: a fleet of flights cycles through turnaround phases
(land, taxi, gate, deplane, refuel, baggage, board, depart), contending for a
small pool of gates, runways, fuel trucks, and baggage crews. Point the TUI at
it and watch each flight's active endpoint name turn over while the shared
managers show concurrent in-flight handlers under contention.

Each turnaround phase is a REAL endpoint on ``FlightActor``, driven externally
so the active-handler name turns over in the TUI (a single internal ``run()``
loop would only ever show ``run``). Shared resources are arbitrated by manager
actors (``GateManager`` / ``RunwayController`` / ``FuelTruckPool`` /
``BaggageCrewPool``). Capacity-holding methods use ``@concurrent_endpoint`` so
they can wait on ``asyncio.Semaphore`` without blocking the actor's dispatch
loop; browsing a manager shows ``request_refuel xK`` etc.: the flights
currently blocked on a busy resource, i.e. live contention.

What to watch in the TUI:

  - a ``flight`` actor's Execution pane: phase names turning over.
  - a manager's Execution pane (e.g. ``fuel_truck_pool``): ``request_* xK`` +
    oldest age, i.e. live contention.
  - either actor's Flight Recorder pane: recent phase / grant / release lines.

Usage::

    buck2 run fbcode//monarch/python/examples:airport_turnaround_demo -- --flights 8

Add ``--dashboard`` to also serve the live-telemetry Monarch Dashboard (default
port 8265; on a devvm it serves HTTPS via the Nest Dev Proxy URL printed at
startup)::

    buck2 run fbcode//monarch/python/examples:airport_turnaround_demo -- --dashboard
"""

import argparse
import asyncio
import collections
import logging
import random

from monarch._src.actor.telemetry import TracingForwarder
from monarch.actor import Actor, concurrent_endpoint, context, endpoint
from monarch.job import ProcessJob, TelemetryConfig

logger: logging.Logger = logging.getLogger("airport_turnaround_demo")
logger.addHandler(TracingForwarder())
logger.setLevel(logging.INFO)

# Slight per-flight startup offset so the fleet does not enter in lockstep
# bursts; steady-state spread comes from the randomized phase durations.
_STARTUP_STAGGER_S: float = 0.4


class GateManager(Actor):
    """Assigns gates from a fixed pool. An ``asyncio.Semaphore`` gates capacity
    and a ``deque`` of ids gives deterministic FIFO assignment (gates cycle
    0,1,2,... and the whole pool is visibly exercised, unlike LIFO ``pop()``).

    The gate is held across the turnaround on the *flight* side (``acquire_gate``
    .. ``release_gate``), so this manager's contention shows as ``assign_gate``
    piling up when every gate is taken.
    """

    def __init__(self, num_gates: int) -> None:
        self._sem = asyncio.Semaphore(num_gates)
        self._free: "collections.deque[int]" = collections.deque(range(num_gates))

    @concurrent_endpoint
    async def assign_gate(self) -> int:
        await self._sem.acquire()
        # No await between acquire and popleft: the only cancellation point is
        # the acquire above, so we never hold a slot without handing back an id.
        gate_id = self._free.popleft()
        logger.info("gate %d assigned", gate_id)
        return gate_id

    @endpoint
    async def release_gate(self, gate_id: int) -> None:
        self._free.append(gate_id)
        self._sem.release()
        logger.info("gate %d freed", gate_id)

    @endpoint
    async def whoami(self) -> str:
        return str(context().actor_instance.actor_id)


class RunwayController(Actor):
    """One runway pool shared by landings and takeoffs. The occupancy ``sleep``
    runs inside the endpoint, so the Execution pane shows ``request_landing`` /
    ``request_takeoff`` staying active (and piling up) under contention."""

    def __init__(self, num_runways: int, occupancy_s) -> None:
        self._sem = asyncio.Semaphore(num_runways)
        self._occupancy_s = occupancy_s

    async def _occupy(self, what: str, flight: int) -> None:
        await self._sem.acquire()
        try:
            logger.info("runway %s: flight %d", what, flight)
            await asyncio.sleep(random.uniform(*self._occupancy_s))
        finally:
            # Always release, even if cancelled, so a stray cancellation never
            # leaks a runway slot and wedges the demo.
            self._sem.release()

    @concurrent_endpoint
    async def request_landing(self, flight: int) -> None:
        await self._occupy("landing", flight)

    @concurrent_endpoint
    async def request_takeoff(self, flight: int) -> None:
        await self._occupy("takeoff", flight)

    @endpoint
    async def whoami(self) -> str:
        return str(context().actor_instance.actor_id)


class FuelTruckPool(Actor):
    """Fixed pool of fuel trucks. Service time runs inside ``request_refuel`` so
    the manager's Execution pane shows the live queue under contention."""

    def __init__(self, num_trucks: int, service_s) -> None:
        self._sem = asyncio.Semaphore(num_trucks)
        self._service_s = service_s

    @concurrent_endpoint
    async def request_refuel(self, flight: int) -> None:
        await self._sem.acquire()
        try:
            logger.info("fuel truck -> flight %d", flight)
            await asyncio.sleep(random.uniform(*self._service_s))
        finally:
            self._sem.release()

    @endpoint
    async def whoami(self) -> str:
        return str(context().actor_instance.actor_id)


class BaggageCrewPool(Actor):
    """Fixed pool of baggage crews; same shape as the fuel truck pool."""

    def __init__(self, num_crews: int, service_s) -> None:
        self._sem = asyncio.Semaphore(num_crews)
        self._service_s = service_s

    @concurrent_endpoint
    async def request_baggage_service(self, flight: int) -> None:
        await self._sem.acquire()
        try:
            logger.info("baggage crew -> flight %d", flight)
            await asyncio.sleep(random.uniform(*self._service_s))
        finally:
            self._sem.release()

    @endpoint
    async def whoami(self) -> str:
        return str(context().actor_instance.actor_id)


class FlightActor(Actor):
    """One flight cycling through its turnaround. Every phase is a real endpoint
    so the TUI's Execution pane shows the phase name turning over.

    Local phases just ``sleep``; resource phases call the relevant manager and
    block there. The gate is held from ``acquire_gate`` through ``release_gate``.
    """

    def __init__(self, gates, runways, fuel, baggage, durations) -> None:
        # The manager handles are actor-mesh proxies returned by spawn(), not
        # instances. Left unannotated to match the sibling examples.
        self._gates = gates
        self._runways = runways
        self._fuel = fuel
        self._baggage = baggage
        self._durations = durations
        # Actor-native identity: the spawn-time point in the `flights` mesh,
        # stable under `flights.slice(replica=i)`. NOT current_rank(), which is
        # cast-relative and would rebase to 0 under the slice.
        self._flight: int = int(context().actor_instance.rank["replica"])
        self._gate_id: int | None = None

    async def _wait(self, phase: str) -> None:
        await asyncio.sleep(random.uniform(*self._durations[phase]))

    @endpoint
    async def approach(self) -> None:
        logger.info("flight %d: approach", self._flight)
        await self._wait("approach")

    @endpoint
    async def request_landing(self) -> None:
        logger.info("flight %d: request landing", self._flight)
        await self._runways.request_landing.call_one(self._flight)

    @endpoint
    async def taxi_to_gate(self) -> None:
        logger.info("flight %d: taxi to gate", self._flight)
        await self._wait("taxi")

    @endpoint
    async def acquire_gate(self) -> None:
        self._gate_id = await self._gates.assign_gate.call_one()
        logger.info("flight %d: at gate %d", self._flight, self._gate_id)

    @endpoint
    async def deplane(self) -> None:
        logger.info("flight %d: deplane (gate %s)", self._flight, self._gate_id)
        await self._wait("deplane")

    @endpoint
    async def request_refuel(self) -> None:
        logger.info("flight %d: refuel", self._flight)
        await self._fuel.request_refuel.call_one(self._flight)

    @endpoint
    async def request_baggage_service(self) -> None:
        logger.info("flight %d: baggage", self._flight)
        await self._baggage.request_baggage_service.call_one(self._flight)

    @endpoint
    async def board(self) -> None:
        logger.info("flight %d: board (gate %s)", self._flight, self._gate_id)
        await self._wait("board")

    @endpoint
    async def release_gate(self) -> None:
        gate_id = self._gate_id
        self._gate_id = None
        await self._gates.release_gate.call_one(gate_id)
        logger.info("flight %d: left gate %s", self._flight, gate_id)

    @endpoint
    async def request_takeoff(self) -> None:
        logger.info("flight %d: request takeoff", self._flight)
        await self._runways.request_takeoff.call_one(self._flight)

    @endpoint
    async def depart(self) -> None:
        logger.info("flight %d: depart", self._flight)
        await self._wait("depart")

    @endpoint
    async def whoami(self) -> str:
        return str(context().actor_instance.actor_id)


async def async_main(args: argparse.Namespace) -> None:
    job = ProcessJob({"hosts": 1})
    job.enable_admin()
    if args.dashboard:
        job.enable_telemetry(
            TelemetryConfig(
                include_dashboard=True,
                dashboard_port=args.dashboard_port,
            )
        )
    state = job.state(cached_path=None)
    host = state.hosts

    # Flights are one named proc mesh sliced per replica; each manager is its
    # own named singleton proc mesh — all distinct, findable nodes in the TUI.
    # (Without `name=`, the flight procs show up anonymous, e.g. `anon-1`.)
    flight_procs = host.spawn_procs(per_host={"replica": args.flights}, name="flights")
    gate_proc = host.spawn_procs(name="gate_manager")
    runway_proc = host.spawn_procs(name="runway_controller")
    fuel_proc = host.spawn_procs(name="fuel_truck_pool")
    baggage_proc = host.spawn_procs(name="baggage_crew_pool")

    gates = gate_proc.spawn("gate_manager", GateManager, args.gates)
    runways = runway_proc.spawn(
        "runway_controller", RunwayController, args.runways, tuple(args.runway)
    )
    fuel = fuel_proc.spawn(
        "fuel_truck_pool", FuelTruckPool, args.fuel_trucks, tuple(args.refuel)
    )
    baggage = baggage_proc.spawn(
        "baggage_crew_pool", BaggageCrewPool, args.baggage_crews, tuple(args.baggage)
    )

    durations = {
        "approach": tuple(args.approach),
        "taxi": tuple(args.taxi),
        "deplane": tuple(args.deplane),
        "board": tuple(args.board),
        "depart": tuple(args.depart),
    }
    flights = flight_procs.spawn(
        "flight", FlightActor, gates, runways, fuel, baggage, durations
    )

    gate_ref = await gates.whoami.call_one()
    flight0_ref = await flights.slice(replica=0).whoami.call_one()

    admin_url = state.admin_url
    assert admin_url is not None
    mtls_flags = (
        "--cacert /var/facebook/rootcanal/ca.pem "
        "--cert /var/facebook/x509_identities/server.pem "
        "--key /var/facebook/x509_identities/server.pem "
        if admin_url.startswith("https")
        else ""
    )
    tui = (
        "buck2 run fbcode//monarch/hyperactor_mesh_admin_tui:hyperactor_mesh_admin_tui"
    )
    print(f"\nMesh admin server listening on {admin_url}")
    print(f"  - Mesh tree:  curl {mtls_flags}{admin_url}/v1/tree")
    print(f"  - TUI:        {tui} -- --addr {admin_url}")
    print("\nOpen these nodes in the TUI tree (by label):")
    print(f"  - flight (replicas 0..{args.flights - 1}) -> Execution: phase turnover")
    print("  - gate_manager        -> Execution: assign_gate xK when gates are full")
    print("  - runway_controller   -> Execution: request_landing / request_takeoff xK")
    print("  - fuel_truck_pool     -> Execution: request_refuel xK")
    print("  - baggage_crew_pool   -> Execution: request_baggage_service xK")
    print(f"  raw ids: gate_manager={gate_ref}  flight[0]={flight0_ref}")
    if args.dashboard:
        print(
            f"\nMonarch Dashboard enabled on port {args.dashboard_port}. "
            "On a devvm it serves HTTPS: open the Nest Dev Proxy URL printed "
            "above (plain http://localhost hangs against the TLS socket)."
        )
    print(
        f"\n{args.flights} flights; {args.gates} gates, {args.runways} runways, "
        f"{args.fuel_trucks} fuel trucks, {args.baggage_crews} baggage crews. "
        "Press Ctrl+C to stop.\n",
        flush=True,
    )

    async def run_flight(i: int) -> None:
        f = flights.slice(replica=i)
        await asyncio.sleep(i * _STARTUP_STAGGER_S)
        while True:
            await f.approach.call_one()
            await f.request_landing.call_one()
            await f.taxi_to_gate.call_one()
            await f.acquire_gate.call_one()
            await f.deplane.call_one()
            await f.request_refuel.call_one()
            await f.request_baggage_service.call_one()
            await f.board.call_one()
            await f.release_gate.call_one()
            await f.request_takeoff.call_one()
            await f.depart.call_one()

    # Fail-fast: gather propagates the first loop's failure and tears the demo
    # down, surfacing bugs loudly rather than silently degrading.
    loops = [asyncio.ensure_future(run_flight(i)) for i in range(args.flights)]
    try:
        await asyncio.gather(*loops)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\nShutting down...", flush=True)
        # Fixed order: cancel and drain the drivers, then stop flights, then the
        # managers -- so a flight blocked in a request_* call never has its
        # manager vanish first (which would make shutdown look broken).
        for loop in loops:
            loop.cancel()
        await asyncio.gather(*loops, return_exceptions=True)
        await flight_procs.stop()
        await gate_proc.stop()
        await runway_proc.stop()
        await fuel_proc.stop()
        await baggage_proc.stop()
        # Tear down the host last: ProcessJob spawns a host subprocess
        # (run_worker_loop_forever) that stopping the proc meshes alone leaves
        # orphaned; shutdown() stops the host and every process on it.
        await host.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="execution-surface airport-turnaround observability demo"
    )
    parser.add_argument("--flights", type=int, default=8)
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Launch the Monarch Dashboard for live telemetry",
    )
    parser.add_argument(
        "--dashboard-port",
        type=int,
        default=8265,
        help="Dashboard port (default: 8265); only used with --dashboard",
    )
    parser.add_argument("--gates", type=int, default=3)
    parser.add_argument("--runways", type=int, default=2)
    parser.add_argument("--fuel-trucks", type=int, default=2)
    parser.add_argument("--baggage-crews", type=int, default=2)
    # Per-phase duration ranges, in seconds, given as MIN MAX.
    for flag, lo, hi in (
        ("approach", 1.0, 3.0),
        ("runway", 0.8, 1.5),
        ("taxi", 0.8, 1.5),
        ("deplane", 1.0, 2.0),
        ("refuel", 1.5, 3.0),
        ("baggage", 1.0, 2.5),
        ("board", 1.0, 2.0),
        ("depart", 0.8, 1.5),
    ):
        parser.add_argument(
            f"--{flag}",
            nargs=2,
            type=float,
            metavar=("MIN", "MAX"),
            default=[lo, hi],
        )
    args = parser.parse_args()

    for name, count in (
        ("flights", args.flights),
        ("gates", args.gates),
        ("runways", args.runways),
        ("fuel-trucks", args.fuel_trucks),
        ("baggage-crews", args.baggage_crews),
    ):
        if count < 1:
            parser.error(f"--{name} must be >= 1")
    for name in (
        "approach",
        "runway",
        "taxi",
        "deplane",
        "refuel",
        "baggage",
        "board",
        "depart",
    ):
        lo, hi = getattr(args, name)
        if lo <= 0 or lo > hi:
            parser.error(f"--{name} MIN must be > 0 and <= MAX")

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
