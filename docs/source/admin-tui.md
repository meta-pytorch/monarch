# Mesh Admin TUI

The **Mesh Admin TUI** (`monarch-tui`) is an interactive terminal client for
inspecting live Monarch meshes. It connects to the mesh admin HTTP API and
renders the full topology — hosts, processes, and actors — as a navigable tree
with a contextual detail pane.

See the [observability overview](observability) for how the TUI complements
[distributed telemetry](distributed-telemetry) and the
[Monarch Dashboard](monarch-dashboard).

The TUI is included in the `torchmonarch` PyPI package. After
`pip install torchmonarch`, the `monarch-tui` command is available on PATH.

## Quick Start

Start any Monarch application that spawns a `MeshAdminAgent`. The
**Dining Philosophers** example is the easiest way to try it — five philosopher
actors share chopsticks around a table, mediated by a waiter actor that prevents
deadlock.

**Terminal 1** — start the example:

```bash
python python/examples/dining_philosophers.py
```

The example prints the admin server address on startup:

```text
Mesh admin server listening on http://127.0.0.1:1729
```

**Terminal 2** — attach the TUI:

```bash
monarch-tui --addr 127.0.0.1:1729
```

## Topology Tree

The main view is a split-pane layout: an expandable **topology tree** on the
left showing hosts, processes, and actors, and a **detail pane** on the right
with contextual information for the selected node.

Navigate with `j`/`k`, expand and collapse nodes with `Tab`. Selecting an
actor shows its status, message count, processing time, last handler, and
flight recorder events.

```{image} _static/tui-tree.png
:alt: Mesh Admin TUI showing the topology tree with philosopher actors expanded and actor details on the right
:width: 100%
```

## Diagnostics

Press `d` to run a full health check across the mesh. The diagnostics overlay
probes every node in the topology and reports pass/slow/fail for each, separated
into **Admin Infrastructure** (admin server, host agents, service procs) and
**Mesh** (user procs and actors). Each probe shows its latency in milliseconds.

```{image} _static/tui-diagnostics.png
:alt: Diagnostics overlay showing health check results for all nodes in the mesh
:width: 100%
```

## Py-spy Stack Traces

Press `p` on any proc or actor to capture a live Python stack trace via
[py-spy](https://github.com/benfred/py-spy). The overlay shows per-thread
stacks with frame-level detail, GIL ownership, and thread names. Each press
fetches a fresh trace — useful for diagnosing hangs in C extensions and CUDA
calls.

`py-spy` is not bundled with `torchmonarch`; it must be resolvable on the
target host — either on `PATH` or via the `PYSPY_BIN` environment variable. If
it cannot be found, the stack-trace request reports that py-spy is unavailable
rather than failing the proc.

```{image} _static/tui-pyspy.png
:alt: Py-spy overlay showing per-thread Python stack traces with native frames
:width: 100%
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
| `Tab` | Expand/collapse selected node |
| `c` | Collapse all nodes |
| `s` | Toggle system actor visibility |
| `h` | Toggle stopped actor visibility (failed actors always remain visible) |
| `d` | Run diagnostics overlay |
| `p` | Py-spy stack trace for selected proc or actor |
| `Ctrl+L` | Scroll selected item to top of viewport |
| `Esc` | Dismiss overlay |
| `q` / `Ctrl+C` | Quit |

## CLI Options

```text
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

## Telemetry Query Proxy

When the admin server starts with distributed telemetry, it also exposes:

| Endpoint | Purpose |
|----------|---------|
| `POST /v1/query` | Run DataFusion SQL and return `{"rows": [...]}` |
| `POST /v1/pyspy_dump/{proc_reference}` | Capture and persist a py-spy dump in telemetry tables |

For example, post `{"sql": "SELECT * FROM actors LIMIT 10"}` to
`/v1/query`. Discover available tables with
`SELECT table_name FROM information_schema.tables`.

The TUI's `p` key and `GET /v1/pyspy/{proc_reference}` perform a one-shot
capture. Use the `pyspy_dump` endpoint when the capture must remain queryable
in `pyspy_dumps`, `pyspy_stack_traces`, `pyspy_frames`, and
`pyspy_local_variables`.

## Source Code

| Component | Location |
|-----------|----------|
| TUI binary | `hyperactor_mesh_admin_tui/bin/admin_tui.rs` |
| TUI library | `hyperactor_mesh_admin_tui/src/` |
| Admin HTTP API | `hyperactor_mesh/src/mesh_admin.rs` |
| Dining philosophers example | `python/examples/dining_philosophers.py` |
