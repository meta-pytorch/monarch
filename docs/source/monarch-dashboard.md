# Monarch Dashboard

The **Monarch Dashboard** is a web-based GUI for monitoring Monarch actor systems
in real time. It connects to the distributed telemetry system and renders the
full mesh topology — hosts, processes, actor meshes, and individual actors —
across five tabs: **Overview** for fleet health and message traffic,
**Executions** for command outcomes, **Topology** for an interactive job graph,
**Explorer** for drilling into a single entity, and **Resources** for host and
GPU utilization.

See the [observability overview](observability) for how the dashboard uses
[distributed telemetry](distributed-telemetry) and complements the
[Mesh Admin TUI](admin-tui).

> **Note** — The Monarch Dashboard is in early development and may change
> significantly between releases.

The dashboard is included in the `torchmonarch` PyPI package. The telemetry
sidecar hosts its query service. Set
`TelemetryConfig(include_dashboard=True)` to advertise the browser UI and print
its URL.

## Quick Start

Start any Monarch application that enables telemetry. The
[**Dining Philosophers**](https://github.com/meta-pytorch/monarch/blob/main/python/examples/dining_philosophers.py)
example is the easiest way to try it — five philosopher actors share chopsticks
around a table, mediated by a waiter actor that prevents deadlock. The
[**Airport Turnaround**](https://github.com/meta-pytorch/monarch/blob/main/python/examples/airport_turnaround_demo.py)
example is a busier alternative — a fleet of flights cycles through turnaround
phases while contending for a small pool of gates, runways, fuel trucks, and
baggage crews. Both accept `--dashboard`.

**Terminal 1** — start the example with the dashboard enabled:

```bash
python python/examples/dining_philosophers.py --dashboard
```

The example prints the dashboard URL on startup:

```text
Monarch Dashboard: http://localhost:8265
```

Open [http://localhost:8265](http://localhost:8265) in your browser.

## Overview

The default tab summarizes the whole job. The header persists across tabs,
carrying the tab switcher, a freshness indicator for the live poll, and a
fleet health chip. A separate execution indicator shows active commands and
commands needing attention in retained history, including when another tab is selected.

```{image} _static/dashboard-summary.png
:alt: Overview tab showing metric cards, a health gauge, message activity, actor status, message traffic by endpoint, the actor fleet, and a topology breakdown
:width: 100%
```

The tab is organized into panels:

- **Metric cards** — host meshes, proc meshes, actors (split into workload and
  system), messages over the retention window, handler success rate, and the
  fleet health status.
- **Fleet Health** — Healthy when all reported live workload states are normal;
  Unhealthy when any workload actor reports Failed. System actors and record-backed command actors are excluded.
- **Message Activity** — throughput across the retention window, annotated with
  the sampled time range. Viewing the dashboard itself adds a little `scan`
  traffic, which the panel notes.
- **Actor Status** — a donut of fleet actor states, with a legend per status.
- **Errors & Failures** — failed fleet actors with their reasons, or an explicit
  "No fleet errors detected" message. An expandable lifecycle history retains
  stopped fleet actors and their stop reasons without counting them as errors.
  Command actors and their lifecycle history remain in the actor graph.
- **Message Traffic** — the handler lifecycle (queued → active →
  completed / failed) and a ranked list of message volume by endpoint. These
  are handler states, not delivery kinds.
- **Actor Fleet** — one chip per workload actor showing its role, proc, and
  live status; click a chip to inspect it. Toggle **Show System** to include
  system actors.
- **Topology Breakdown** — counts and proportions of host, proc, and actor
  meshes.
- **Resources** — a snapshot of physical-host health: fresh host samples,
  weighted CPU, available RAM, and GPU memory used. **View Resources →**
  opens the Resources tab.
- **Executions** — active commands and recent outcomes, with a link to the full
  history. A command failure remains visible after a later successful command.

## Executions

When a job enables telemetry, each `monarch exec` invocation records one
execution grouping its targeted meshes and ranks. Short commands such as `ls`
and long-running SPMD commands use the same model. Overlapping commands appear
independently; none changes the fleet health score.

The Executions tab lists active commands and paginated history. Filter outcomes
or search command text, then select a command to inspect its exit code,
duration, working directory, and per-rank outcomes. Signal termination uses
shell exit codes, for example SIGKILL becomes 137. Output from single-rank
commands is returned to the CLI session. Redirected output stays in the worker
output directory shown in the details; access it directly on the workers or
through a GatherMount that covers that directory. Command history stores exit
results and output paths without copying stdout or stderr into telemetry.

Execution records use the existing distributed telemetry Arrow socket and
in-memory DataFusion tables (`executions`, `execution_status_events`, and
`execution_rank_results`). Invocation metadata, final statuses, and rank
summaries remain for the **sidecar's lifetime**, like actor history.
Completed command history has no age or count limit. CLI disconnects preserve
it; stopping or losing the sidecar process clears it.
`TelemetryConfig.retention_secs` applies to message and trace data and command
heartbeats. Destroying the sidecar, including
`monarch exec --kill`, destroys this in-memory history. Worker output files have
their own lifetime.

Each committed invocation sends a heartbeat every ten seconds. The existing
retention sweep expires old heartbeats using
`TelemetryConfig.retention_secs` (one hour by default). Setting it to zero
disables expiry. A final outcome takes precedence over later heartbeats. If no final outcome exists and heartbeats stop for 45 seconds,
the UI reports **Unknown**: the worker command may still be running.

The header counts each invocation needing attention once: Failed, Unknown,
Interrupted, or an owned runtime actor failure. A successful command
can have a separate runtime fault; details preserve its actual exit result and
show the fault reason. A later `ls` does not erase either an earlier failure or
an overlapping long command.
Attention items accumulate for the sidecar's lifetime; this version has no
acknowledgement action or "since" filter.
Heartbeat expiry runs with the existing message and trace retention sweep.
With default settings, sweeps run every five minutes.

Temporary actor ownership requires the actor-mesh → parent Proc-mesh relation
with the exact `exec_<invocation UUID>` name **and a retained invocation record**.
A matching-looking name alone cannot exclude a fleet failure. Missing schemas
or failed collection keep actors in fleet scope. Reporting is best effort and
lossy, like other telemetry producers: a rank frame that does not fit the
bounded queue or fails to send is dropped and counted, and the next send
reconnects and resends the invocation metadata. Lost rank reports are stated in
the outcome. Reporting cannot change command exits. There is no acknowledgement
protocol; a dropped final frame can leave Unknown.
If a process result was observed, its actual exit is retained even when rank
telemetry is lost; the outcome reports the incomplete capture separately.
An empty result set remains Unknown: the CLI's aggregate defaults to zero, but
that value alone does not establish that any process succeeded.

Raw actors, messages, and topology remain inspectable in Explorer and Topology.
Ordinary actor failures still make fleet health Unhealthy. Automatic recording is
scoped to `monarch exec`; direct `exec_command()` and arbitrary actor/SPMD APIs
keep their actor behavior. Direct `exec_command()` also normalizes negative
subprocess signal results to shell exit codes, for example -9 becomes 137.

Command arguments, directories, and recorded errors are visible to anyone able
to query job telemetry. Avoid putting secrets in arguments. Script recording
stores its path (or stdin label) and a SHA-256 digest of the actual script text,
without storing the script body. This identifies edits at the same path.

### Health coverage and limits

Health has no numeric score or state weights. Created, Initializing, Loading,
Client, Idle, Processing, Saving, and Stopping are normal, healthy states.
Any Failed fleet actor makes health **Unhealthy**, regardless of the number of
healthy actors. Stopped fleet actors remain in lifecycle history and leave the live
workload calculation.

Missing or unrecognized states make health **Unknown** unless a failure is
already reported. A fleet with only stopped actors is **Inactive**. The caption
shows known, unknown, and stopped counts. Message failures are shown separately.

GatherSourceActor and MountHandlerClient can have no status events in a normal
retained DevX session. Mount components start before telemetry connects, and
status transitions emitted before the socket sink attaches are not replayed.
Actors that stay idle can retain topology data without a status event. Their
missing events remain coverage gaps, so Unknown can be a normal steady state.
The backend summary includes infrastructure actors; the frontend retains its
system-role exclusions. Their health classifications and coverage counts can
differ during client or controller shutdown.

Reported actor states do not independently verify process or host liveness.
An unexpected SIGKILL can leave a stale Idle status. Root supervision can
report process loss without changing that actor's telemetry status.

### Retained memory and query cost

Command metadata and rank results accumulate for the sidecar's lifetime.
Their memory use grows with the number of commands and targeted ranks, plus
command text and coordinates. The existing actor graph also retains command
actors and their lifecycle history. With retention enabled, heartbeat rows
remain for the configured window plus the delay until the next sweep. Final
statuses remain for the sidecar's lifetime. Listing commands still scans stored
status rows; pagination bounds the response size.

System roles come from the runtime's `actor_nodes.is_system` snapshot flag,
matched to telemetry by canonical actor id. Host/Proc mesh membership also identifies runtime agents, including processes
that exit between captures. Actor names do not determine roles.
Before an actor has snapshot metadata, it stays visible and its reported state
still counts toward health. Retained actors keep their last recorded role.
Topology also uses the explicit `children.is_system` membership flag.

## Topology

The Topology tab renders the job as an interactive directed graph. Solid edges
are the mesh hierarchy; dashed edges are message flow between actors, rolled up
to whichever nodes are currently visible.

Two view modes trade detail against legibility. Switch between them with the
leftmost toolbar button.

### Actors view

One node per entity — host, proc, and actor — each tagged with its mesh. Use
this to follow individual actors and see which of them talk to each other.

```text
Host → Proc → Actor
```

```{image} _static/dashboard-dag.png
:alt: Topology tab in Actors view, fully expanded, showing a host and a controller above six procs and their philosopher and waiter actors, with dashed message edges
:width: 100%
```

### Meshes view

Each host, proc, and actor mesh collapses to a single node carrying its member
count. This keeps larger and more complex jobs clear: rather than hundreds of
individual actors, the graph shows only the meshes and the relationships
between them.

```{image} _static/dashboard-dag-meshes.png
:alt: Topology tab in Meshes view, fully expanded, showing two host meshes above two proc meshes and two actor meshes, each labeled with its member count
:width: 100%
```

Both modes share the same controls:

- **Expand All** and **Collapse** open or close every level. A collapsed node
  reports how many descendants it hides.
- **Show System** brings in the system actors, which are hidden by default.
- **Top-Down** and **Left-Right** flip the layout direction; **Fit** frames the
  graph. **Auto-fit** re-frames on a structural change but never on a live
  refresh, so it leaves a manual zoom alone.
- **Pan** by dragging the canvas, and **zoom** with the scroll wheel, the zoom
  buttons, or the minimap.
- **Hover** a node for its name, type, status, mesh, and reference. **Click** an
  actor to open its detail drawer; clicking a node that has children expands or
  collapses it instead.
- Nodes are colored by status. The legend keys those colors, the mesh colors,
  and the two kinds of edge.

## Explorer

The Explorer pairs a hierarchy tree with a detail pane. Filter the tree by
name, expand or collapse every level, and toggle **Show System** to include
system actors.

```{image} _static/dashboard-hierarchy.png
:alt: Explorer tab showing the hierarchy tree on the left and an actor detail pane on the right with a breadcrumb, actor info, and a status timeline
:width: 100%
```

```text
Controller (the client host)
Host
  └─ Proc
       └─ Actor
```

Every row is tagged with its mesh and carries a status dot. Hosts and procs
also report how many actors sit below them.

### Entity detail

Selecting a host or proc reports its name, mesh, status, direct child count,
the number of actors below it, and its reference, followed by a rollup of the
statuses below it and a list of its children.

### Actor detail

Selecting an actor shows a breadcrumb of its ancestors and four sections:

- **Py-spy** — capture a live stack trace. Sampling is proc-level, so a dump
  covers every actor sharing that Python process.
- **Actor info** — full name, ID, rank, mesh, current status, and creation
  timestamp.
- **Status timeline** — the most recent status transitions, with timestamps.
- **Messages** — incoming and outgoing messages with peer, endpoint, and
  status. Click a row to expand its status event history.

## Resources

The Resources tab reports physical-host CPU, memory, network, disk, and GPU
utilization from the `host_metrics` and `gpu_metrics` telemetry tables. Use it
to line up actor and message activity with what the machines were doing at the
time. See [Distributed telemetry](distributed-telemetry) to query the same
samples as SQL.

The **host inventory** lists one row per observed physical host, deduplicated
across logical host units, with sortable CPU, RAM pressure, available RAM, GPU
count, and sample-age columns plus host search. The inventory may include the
controller host. Select a host for its recent history:

- **Host detail** — current CPU, RAM pressure, available RAM, swap, and free
  space on `/` and `/tmp`, with five-minute charts for CPU activity, RAM
  pressure, load average, network traffic, disk I/O, and free disk space.
- **GPUs** — one row per NVML-visible device with utilization, memory used and
  total, temperature, and power, plus five-minute charts for each. When a host
  has more than eight devices, the charts cover the first eight by device
  index while the table lists all of them.

Readings are whole-host and include other jobs and the controller. Only fresh
samples are counted; missing or stale samples are excluded rather than shown
as zero. When sampling is disabled or collectors are unavailable, the tab says
so instead of showing an empty table.

## Programmatic Usage

Here is an example of how to enable the dashboard on your job via the [Jobs API](api/monarch.job).

```python
from monarch.job import ProcessJob, TelemetryConfig

job = ProcessJob({"workers": 2}).enable_telemetry(
    TelemetryConfig(include_dashboard=True, dashboard_port=8265)
)
state = job.state(cached_path=None)
print(state.dashboard_url)
```

`enable_telemetry()` also starts mesh admin and periodic introspection
snapshots. Use `state.query_engine_client` to query the same data directly; see
[Distributed Telemetry](distributed-telemetry).
