# Monarch Dashboard

The **Monarch Dashboard** is a web-based GUI for monitoring Monarch actor systems
in real time. It connects to the distributed telemetry system and renders the
full mesh topology — hosts, processes, actor meshes, and individual actors — as
interactive views with live-updating metrics, message traffic analysis, and a
full DAG visualization.

> **Beta** — The Monarch Dashboard is in early development. Features may
> change and rough edges are expected. Feedback is welcome!

The dashboard is included in the `torchmonarch` PyPI package. When you call
`start_telemetry(include_dashboard=True)`, it starts a local web server that
serves the dashboard UI.

## Quick Start

Start any Monarch application that enables telemetry. The **Dining Philosophers**
example is the easiest way to try it — five philosopher actors share chopsticks
around a table, mediated by a waiter actor that prevents deadlock.

**Terminal 1** — start the example with the dashboard enabled:

```bash
python python/examples/dining_philosophers.py --dashboard
```

The example prints the dashboard URL on startup:

```
Monarch Dashboard: http://localhost:8265
```

Open [http://localhost:8265](http://localhost:8265) in your browser.

## Summary View

The default view provides at-a-glance metrics for the entire mesh.

```{image} _static/dashboard-summary.png
:alt: Dashboard summary view showing overview cards, session timeline, status breakdown, error panel, and message traffic
:width: 100%
```

The summary is organized into sections:

- **Overview cards** — host mesh count, proc mesh count, total actors (with
  status count), and total messages (with delivery rate percentage).
- **Session timeline** — a horizontal bar spanning the session lifetime with
  error notches marking when actors failed or stopped.
- **Actor status breakdown** — a segmented bar and legend showing how many
  actors are in each state (Running, Idle, Failed, Stopped, etc.).
- **Errors & failures** — failed actors, stopped actors, and undelivered
  messages, each with the actor name, failure reason, and timestamp.
- **Message traffic** — delivery rate bar segmented by message status, plus
  a ranked bar chart of messages by endpoint name.
- **Hierarchy breakdown** — chip counts of host meshes, proc meshes, and
  actor meshes.

## Hierarchy View

The hierarchy view lets you drill down through the full Monarch mesh tree one
level at a time. Click any row to navigate deeper; use the breadcrumb bar at
the top to jump back to a parent level.

```{image} _static/dashboard-hierarchy.png
:alt: Dashboard hierarchy view showing a breadcrumb bar and a table of actors with name, rank, and status columns
:width: 100%
```

The navigation levels are:

```
Host Meshes
  └─ Host Units (individual hosts)
       └─ Proc Meshes
            └─ Proc Units (individual processes)
                 └─ Actor Meshes
                      └─ Actors
                           └─ Actor Detail
```

### Actor Detail

Selecting an individual actor opens its detail page with three sections:

- **Actor info** — full name, ID, rank, mesh ID, current status, and
  creation timestamp.
- **Status timeline** — chronological list of every status transition with
  timestamp and reason.
- **Messages** — incoming and outgoing message tables showing sender/receiver,
  endpoint name, delivery status, and timestamp. Click any message to expand
  its full status event history (e.g. Sent → Delivered).


## DAG View

The DAG view renders the entire job as an interactive directed graph, helping you to understand your job better.  It also shows message flows between your user actors.

```
Host → Proc → Actor
```

```{image} _static/dashboard-dag.png
:alt: DAG view showing the full mesh topology as an interactive directed graph with color-coded nodes
:width: 100%
```

- **Nodes** are color-coded by status (green = healthy, red = failed,
  gray = stopped).
- **Edges** show parent-child relationships in the mesh hierarchy.
- **Pan** by dragging the canvas; **zoom** with the scroll wheel.
- **Hover** any node for a tooltip with its name, type, and status, and mesh.
- **Click** a node to open its detail panel on the right.

### Programmatic Usage

Here is an example of how to enable the dashboard on your job via the [Jobs API](api/monarch.job).

```python
from monarch.job import LocalJob, ProcessJob, KubernetesJob, SlurmJob, TelemetryConfig

# Provision job - LocalJob, ProcessJob, KubernetesJob, SlurmJob, etc.
# job = ...
dashboard_port = 8265

# Enable admin API and telemetry/dashboard as they work together.
job.enable_admin()
job.enable_telemetry(
    TelemetryConfig(include_dashboard=True, dashboard_port=dashboard_port)
)
```
