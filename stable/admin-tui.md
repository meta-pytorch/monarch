# Mesh Admin TUI

The **Mesh Admin TUI** (`monarch-tui`) is an interactive terminal client for
inspecting live Monarch meshes. It connects to the mesh admin HTTP API and
renders the full topology -- hosts, processes, and actors -- as a navigable tree
with a contextual detail pane.

The TUI is included in the `torchmonarch` PyPI package. After
`pip install torchmonarch`, the `monarch-tui` command is available on PATH.

## Quick Start

Start any Monarch application that spawns a `MeshAdminAgent`. The
**Dining Philosophers** example is the easiest way to try it -- five philosopher
actors share chopsticks around a table, mediated by a waiter actor that prevents
deadlock.

**Terminal 1** -- start the example:

```
python python/examples/dining_philosophers.py
```

The example prints the admin server address on startup:

```
Mesh admin server listening on http://127.0.0.1:1729
```

**Terminal 2** -- attach the TUI:

```
monarch-tui --addr 127.0.0.1:1729
```

## Topology Tree

The main view is a split-pane layout: an expandable **topology tree** on the
left showing hosts, processes, and actors, and a **detail pane** on the right
with contextual information for the selected node.

Navigate with `j`/`k`, expand and collapse nodes with `Tab`. Selecting an
actor shows its status, message count, processing time, last handler, and
flight recorder events.

[![Mesh Admin TUI showing the topology tree with philosopher actors expanded and actor details on the right](_images/tui-tree.png)](_images/tui-tree.png)

## Diagnostics

Press `d` to run a full health check across the mesh. The diagnostics overlay
probes every node in the topology and reports pass/slow/fail for each, separated
into **Admin Infrastructure** (admin server, host agents, service procs) and
**Mesh** (user procs and actors). Each probe shows its latency in milliseconds.

[![Diagnostics overlay showing health check results for all nodes in the mesh](_images/tui-diagnostics.png)](_images/tui-diagnostics.png)

## Py-spy Stack Traces

Press `p` on any proc or actor to capture a live Python stack trace via
[py-spy](https://github.com/benfred/py-spy). The overlay shows per-thread
stacks with frame-level detail, GIL ownership, and thread names. Each press
fetches a fresh trace -- useful for diagnosing hangs in C extensions and CUDA
calls.

`py-spy` is included as a default dependency of `torchmonarch`.

[![Py-spy overlay showing per-thread Python stack traces with native frames](_images/tui-pyspy.png)](_images/tui-pyspy.png)

## Keybindings

| Key | Action |
| --- | --- |
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

```
monarch-tui [OPTIONS] --addr <ADDR>
```

| Flag | Description | Default |
| --- | --- | --- |
| `--addr`, `-a` | Admin server address (`host:port` or `https://host:port`) | required |
| `--refresh-ms` | Auto-refresh interval in milliseconds | `2000` |
| `--theme` | Color theme: `nord` (dark) or `doom-nord-light` (light) | `nord` |
| `--lang` | Display language: `en` or `zh` (Simplified Chinese) | `en` |
| `--diagnose` | Run diagnostics non-interactively, print JSON, and exit | `false` |
| `--tls-ca` | Path to PEM CA certificate for TLS | auto-detected |
| `--tls-cert` | Path to PEM client certificate for mutual TLS | auto-detected |
| `--tls-key` | Path to PEM client key for mutual TLS | -- |

### Non-interactive diagnostics

For scripted health checks, use `--diagnose` to get a JSON report on stdout:

```
monarch-tui --addr 127.0.0.1:1729 --diagnose
# Exits 0 if healthy, 1 if any check failed.
```

## Source Code

| Component | Location |
| --- | --- |
| TUI binary | `hyperactor_mesh/bin/admin_tui/` |
| Admin HTTP API | `hyperactor_mesh/src/mesh_admin.rs` |
| Dining philosophers example | `python/examples/dining_philosophers.py` |