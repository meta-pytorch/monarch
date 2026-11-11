# Monarch Dashboard

The dashboard shows actor status, message traffic, execution history, and host
and GPU metrics. Live jobs and saved telemetry use Monarch's DataFusion engine.

## Local development

The standalone launcher generates deterministic Arrow fixtures in memory and
queries them with Monarch's native DataFusion engine:

```bash
python -m monarch.monarch_dashboard --host 127.0.0.1 --port 5000
```

Within fbsource, build and run `fbcode//monarch/python/monarch/monarch_dashboard:__main__`
with Buck. The target includes the frontend and native query engine.

Open http://localhost:5000. The sample contains ten actors across two host
meshes, a five-minute event timeline, and a simulated failure.

## Live simulator

```bash
python -m monarch.monarch_dashboard --simulate --interval 0.5 --failure-at 30
```

The simulator appends Arrow rows with current timestamps in the server process.
It stops when the server exits.

| Option | Default | Meaning |
| --- | --- | --- |
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `5000` | HTTP port |
| `--simulate` | Off | Append simulated telemetry continuously |
| `--interval` | `1.0` | Seconds between simulation ticks; must be positive |
| `--failure-at` | `270.0` | Seconds until the simulated actor failure |

## Real telemetry

Use `monarch dashboard open PATH` to view saved telemetry, or enable the
job's dashboard for live data. See the [persistence guide](../../../docs/source/telemetry-persistence.md)
for configuration, paths, and shutdown behavior.
