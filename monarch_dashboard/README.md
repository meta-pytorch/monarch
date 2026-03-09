# Monarch Dashboard

A web dashboard for monitoring Monarch training jobs. Shows real-time actor status, message traffic, and health metrics across the Monarch hierarchy (Meshes > Host Units > Proc Meshes > Procs > Actor Meshes > Actors).

## Quick Start

```bash
cd fbcode/monarch/monarch_dashboard

# Option 1: Shell script (sets up venv + builds frontend automatically)
bash run.sh

# Option 2: Python module (requires deps already installed)
python -m monarch_dashboard
```

Then open http://localhost:5000 in your browser (or use an SSH tunnel, see below).

## Running Modes

### Static Data (default)

Serves the dashboard with a pre-generated SQLite database (`fake_data/fake_data.db`).

```bash
# Shell script
bash run.sh

# Python module
python -m monarch_dashboard
```

### Force Frontend Rebuild

Deletes `frontend/build/` and rebuilds from source before starting.

```bash
# Shell script
bash run.sh --rebuild

# Python module
python -m monarch_dashboard --rebuild
```

### Live Simulator

Launches a background simulator that writes data with real wall-clock timestamps, so the dashboard shows live-updating state. At 4.5 minutes (configurable), a CUDA OOM failure triggers on one host unit with death propagation.

```bash
# Shell script
bash run.sh --simulate

# Python module
python -m monarch_dashboard --simulate
```

### Live Simulator with Custom Failure Time

```bash
# Trigger failure after 30 seconds instead of 4.5 minutes
bash run.sh --simulate --failure-at 30

# Python module equivalent
python -m monarch_dashboard --simulate --failure-at 30
```

### Custom Tick Interval

```bash
# Simulator ticks every 0.5 seconds instead of 1.0
bash run.sh --simulate --interval 0.5

python -m monarch_dashboard --simulate --interval 0.5
```

### Standalone Simulator

Run the simulator by itself (without the Flask server), useful for pre-populating a database.

```bash
python fake_data/simulate.py --db fake_data/fake_data.db --failure-at 270
```

Options:
- `--db PATH` — SQLite database path (default: `fake_data/fake_data.db`)
- `--interval SECONDS` — tick interval (default: 1.0)
- `--failure-at SECONDS` — seconds until failure event (default: 270)

### Filter by Time Range

Restrict the dashboard API to only return data from the last N seconds.

```bash
python -m monarch_dashboard --time-range 60
```

## SSH Tunnel

The dashboard binds to `0.0.0.0:5000` on your devserver. To access it from your laptop:

```bash
ssh -L 5000:localhost:5000 YOUR_DEVSERVER
```

Then open http://localhost:5000 in your local browser.

## All CLI Flags

### `run.sh`

| Flag | Description |
|------|-------------|
| `--rebuild` | Force frontend rebuild before starting |
| `--simulate` | Launch live data simulator alongside server |
| `--failure-at N` | Seconds until simulator failure event (default: 270) |
| `--interval N` | Simulator tick interval in seconds (default: 1.0) |

### `python -m monarch_dashboard`

| Flag | Description |
|------|-------------|
| `--db PATH` | SQLite database path |
| `--host HOST` | Bind address (default: 0.0.0.0) |
| `--port PORT` | Bind port (default: 5000) |
| `--rebuild` | Force frontend rebuild |
| `--simulate` | Launch live data simulator |
| `--failure-at N` | Seconds until simulator failure (default: 270) |
| `--interval N` | Simulator tick interval (default: 1.0) |
| `--time-range N` | Filter API to last N seconds |

### `fake_data/simulate.py`

| Flag | Description |
|------|-------------|
| `--db PATH` | SQLite database path (default: fake_data/fake_data.db) |
| `--interval N` | Tick interval in seconds (default: 1.0) |
| `--failure-at N` | Seconds until failure event (default: 270) |
