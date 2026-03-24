# Mesh Admin TUI

The **Mesh Admin TUI** is an interactive terminal client for inspecting live
Monarch meshes. It connects to the mesh admin HTTP API and renders the full
topology — hosts, processes, and actors — as a navigable tree with a contextual
detail pane.

Use it to observe actor state, browse flight recorder events, run health
diagnostics, and capture Python stack traces with py-spy — all from your
terminal.

## Quick Start

The TUI connects to any running Monarch application that has started a
`MeshAdminAgent`. The easiest way to try it is with the **Dining Philosophers**
example, which models five philosopher actors sharing chopsticks around a table,
mediated by a waiter actor that prevents deadlock.

### Running with the Dining Philosophers

Install Monarch (the TUI binary is included in the wheel):

```bash
pip install torchmonarch
```

Or for development from the repository root:

```bash
pip install -e .
```

**Terminal 1** — start the example:

```bash
python python/examples/dining_philosophers.py
```

The example prints the admin server address on startup:

```
Mesh admin server listening on http://127.0.0.1:1729
```

**Terminal 2** — attach the TUI:

```bash
monarch-tui --addr 127.0.0.1:1729
```

Replace the address with what Terminal 1 printed.

## What You'll See

The TUI has three main areas: a **header bar** with mesh stats, a **topology
tree** on the left, and a **detail pane** on the right.

### Topology Tree View

When you first connect, the tree shows the mesh hierarchy. Press `Tab` to
expand nodes and `j`/`k` to navigate:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Monarch Admin TUI • http://127.0.0.1:1729 • up: 0h 02m • sys:off • ⟳ 2s  │
│ ▸ [Proc] philosophers                                                      │
├──────────────────────────────┬──────────────────────────────────────────────┤
│ Topology                     │ Root Details                                 │
│                              │                                              │
│ ▼ localhost:8080             │   Hosts: 1                                   │
│   ├─ ▶ mesh_admin            │   Started by: user                           │
│   ├─ ▼ philosophers          │   Uptime: 0h 02m 14s                         │
│   │  ├─   Philosopher[0]     │   Started at: 2025-06-15 10:30:00            │
│   │  ├─   Philosopher[1]     │   Data as of: 2s ago                         │
│   │  ├─   Philosopher[2]     │                                              │
│   │  ├─   Philosopher[3]     │     localhost:8080                            │
│   │  └─   Philosopher[4]     │                                              │
│   └─ ▼ waiter                │                                              │
│      └─   Waiter[0]          │                                              │
│                              │                                              │
├──────────────────────────────┴──────────────────────────────────────────────┤
│ j/k:navigate  Tab:expand/collapse  d:diagnostics  p:py-spy  s:system  q:quit│
└─────────────────────────────────────────────────────────────────────────────┘
```

### Actor Detail View

Select an actor to see its status, message stats, and flight recorder events:

```
┌──────────────────────────────┬──────────────────────────────────────────────┐
│ Topology                     │ Actor Details                                │
│                              │                                              │
│   ▼ localhost:8080           │   Status: Running                            │
│     ├─ ▶ mesh_admin          │   Data as of: 1s ago                         │
│     ├─ ▼ philosophers        │   Type: Philosopher                          │
│     │  ├─ ▸ Philosopher[0]   │   Messages: 42                               │
│     │  ├─   Philosopher[1]   │   Processing time: 1.234s                    │
│     │  ├─   Philosopher[2]   │   Created: 2025-06-15 10:30:01               │
│     │  ├─   Philosopher[3]   │   Last handler: request_chopsticks           │
│     │  └─   Philosopher[4]   │   Children: 0                                │
│     └─ ▼ waiter              │ ┌────────────────────────────────────────────┐│
│        └─   Waiter[0]        │ │ Flight Recorder                           ││
│                              │ │                                            ││
│                              │ │ I 10:30:14 thinking for 2s                ││
│                              │ │ I 10:30:16 requesting left chopstick      ││
│                              │ │ I 10:30:16 requesting right chopstick     ││
│                              │ │ I 10:30:17 eating (meal #7)               ││
│                              │ │ I 10:30:19 finished eating, releasing     ││
└──────────────────────────────┴─┴────────────────────────────────────────────┘
```

### Diagnostics Overlay

Press `d` to run a full health check across the mesh. The diagnostics probe
every node in the topology and report pass/slow/fail, separated by failure
domain:

```
┌──────────────────────────────┬──────────────────────────────────────────────┐
│ Topology (dimmed)            │ Diagnostics • completed at 10:32:05          │
│                              │ All 8 checks passed. Admin healthy. Mesh     │
│   ▼ localhost:8080           │ healthy.                                     │
│     ├─ ▶ mesh_admin          │                                              │
│     ├─ ▼ philosophers        │ ── Admin Infra ────────────────────────────  │
│     │  ├─   Philosopher[0]   │  ✓ root 2ms                                 │
│     │  ├─   Philosopher[1]   │  ✓ localhost:8080 3ms  — host agent         │
│     │  ├─   Philosopher[2]   │  ✓ mesh_admin 5ms  — admin service proc     │
│     │  └─ ...                │                                              │
│     └─ ▼ waiter              │ ── Mesh ──────────────────────────────────── │
│        └─   Waiter[0]        │  ✓ philosophers 4ms  — user proc            │
│                              │  ✓ Philosopher[0] 3ms  — user actor         │
│                              │  ✓ Philosopher[1] 2ms  — user actor         │
│                              │  ✓ waiter 3ms  — user proc                  │
│                              │  ✓ Waiter[0] 2ms  — user actor              │
│                              │                                              │
├──────────────────────────────┴──────────────────────────────────────────────┤
│ Esc:dismiss  j/k:scroll                                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Failed Actor View

When an actor fails, it turns red in the tree and failure info propagates up to
parent nodes. Here's what happens if you run with `--kill-waiter-after 10` to
kill the waiter after 10 seconds:

```
┌──────────────────────────────┬──────────────────────────────────────────────┐
│ Topology                     │ Actor Details                                │
│                              │                                              │
│   ▼ localhost:8080           │   Status: failed: actor panicked             │
│     ├─ ▶ mesh_admin          │   Data as of: 0s ago                         │
│     ├─ ▼ philosophers        │   Type: Waiter                               │
│     │  ├─   Philosopher[0]   │   Messages: 23                               │
│     │  └─ ...                │   Processing time: 0.891s                    │
│     └─ ▼ waiter (FAILED)     │   Created: 2025-06-15 10:30:01               │
│        └─   Waiter[0] (red)  │   Last handler: request_chopstick            │
│                              │   Children: 0                                │
│                              │                                              │
│                              │   Error: actor was killed                     │
│                              │   Root cause: Waiter[0]                       │
│                              │   Failed at: 2025-06-15 10:30:11             │
│                              │   Propagated: no                              │
└──────────────────────────────┴──────────────────────────────────────────────┘
```

## Keybindings

| Key | Action |
|-----|--------|
| `j` / `↓` | Move cursor down |
| `k` / `↑` | Move cursor up |
| `g` / `Home` | Jump to top |
| `G` / `End` | Jump to bottom |
| `PgDn` / `Ctrl+D` | Page down |
| `PgUp` / `Ctrl+U` | Page up |
| `Tab` | Expand/collapse selected node (lazy-fetches children on first expand) |
| `c` | Collapse all nodes |
| `s` | Toggle system actor visibility |
| `h` | Toggle stopped actor visibility (failed actors always remain visible) |
| `d` | Run diagnostics overlay |
| `p` | Py-spy stack trace for selected proc or actor |
| `Ctrl+L` | Scroll selected item to top of viewport |
| `Esc` | Dismiss overlay |
| `q` / `Ctrl+C` | Quit |

## CLI Options

```
monarch-tui [OPTIONS] --addr <ADDR>
```

| Flag | Description | Default |
|------|-------------|---------|
| `--addr`, `-a` | Admin server address (`host:port` or `https://host:port`) | required |
| `--refresh-ms` | Auto-refresh interval in milliseconds | `2000` |
| `--theme` | Color theme: `nord` (dark) or `doom-nord-light` (light) | `nord` |
| `--lang` | Display language: `en` or `zh` (Simplified Chinese) | `en` |
| `--diagnose` | Run diagnostics non-interactively, print JSON, and exit | `false` |
| `--tls-ca` | Path to PEM CA certificate for TLS | auto-detected |
| `--tls-cert` | Path to PEM client certificate for mutual TLS | auto-detected |
| `--tls-key` | Path to PEM client key for mutual TLS | — |

### Non-interactive diagnostics

For scripted health checks, use `--diagnose` to get a JSON report on stdout:

```bash
monarch-tui --addr 127.0.0.1:1729 --diagnose
# Exits 0 if healthy, 1 if any check failed.
```

## Features

**Topology exploration.** The tree lazily fetches children on expand, keeping
refresh costs bounded. The auto-refresh (configurable via `--refresh-ms`)
periodically re-walks expanded nodes, preserving your cursor position and
expansion state.

**Flight recorder.** Each actor maintains a ring buffer of recent events
(message sends, receives, state transitions). Select an actor to see its events
with timestamps and log levels in the detail pane.

**Py-spy integration.** Press `p` on any proc or actor to capture a live Python
stack trace via py-spy. The result appears in a scrollable overlay. Each press
fetches a fresh trace.

**Diagnostics.** Press `d` to walk the entire mesh resolution graph, probing
each node for reachability. Results are separated into Admin Infrastructure
(the admin server, host agents, admin service processes) and Mesh (user procs
and actors), with pass/slow/fail status and latency for each probe.

**Failure visibility.** Failed actors are always visible regardless of filter
settings. Failure state propagates up to parent nodes, so you can see at a
glance which branches of the tree have problems.

**Theming.** Two built-in themes: `nord` (dark, default) and `doom-nord-light`
(light). Pass `--theme doom-nord-light` for light terminals.

**Internationalization.** English and Simplified Chinese labels, selected with
`--lang zh`.

## Adding Admin Support to Your Application

Any Monarch application can expose the admin API by spawning a `MeshAdminAgent`.
The dining philosophers example shows how:

```python
from monarch.actor import Actor, endpoint, this_host

class MyActor(Actor):
    @endpoint
    def do_work(self, item: str) -> None:
        ...

async def main():
    procs = this_host().spawn_procs({"gpus": 4})
    workers = procs.spawn("workers", MyActor)

    # The MeshAdminAgent is started automatically by the runtime.
    # Look for the admin address in the startup logs, then attach:
    #   monarch-tui --addr <printed-address>

    workers.do_work.broadcast(item="hello")
```

## Source Code

| Component | Location |
|-----------|----------|
| TUI binary | `hyperactor_mesh/bin/admin_tui/` |
| Admin HTTP API | `hyperactor_mesh/src/mesh_admin.rs` |
| Dining philosophers example | `python/examples/dining_philosophers.py` |
