# Profiling Actor Activity

`monarch profile` captures a unified Perfetto trace of actor activity across
every host and process in a running job. By default, it captures the next
10 seconds. Use it to inspect remote endpoint calls, handler execution, and
the relationships between them.

See the [observability overview](observability) for telemetry setup and other
diagnostic tools.

## Install the CLI

The `monarch` command is included in the `torchmonarch` package. Follow the
[installation guide](installation).

## Enable telemetry

Enable telemetry before obtaining job state:

```python
from monarch.job import ProcessJob

job = ProcessJob({"workers": 2})

job.enable_telemetry()
```

Use your existing job configuration in place of `ProcessJob`. Keep the job
running and execute the workload during the capture interval.

## Capture a trace

### Active job

If you allocated the job with `monarch apply`, run this in another terminal:

```bash
monarch profile
```

The command uses the active job's telemetry URL.

### Explicit telemetry URL

For a job allocated from Python, print its telemetry URL:

```python
state = job.state()

print(f"Telemetry URL: {state.telemetry_url}")
```

Pass the printed URL to the command. For example:

```bash
monarch profile http://127.0.0.1:12345
```

Run the command from a host that can reach that URL. You can also pass a
mesh-admin URL if that server exposes distributed telemetry.

### Capture duration and output

Override the default capture duration with `--time`:

```bash
monarch profile --time 2s
```

The trace is saved under `/tmp/$USER/monarch_profiles/` by default. Select a
file with `--output`:

```bash
monarch profile --time 2s --output /tmp/actor-activity.pftrace
```

Choose a new output filename for each capture; existing files are not
overwritten. The command prints the path where the trace is available:

```text
/tmp/actor-activity.pftrace
```

Copy the file to your local machine if the command ran on a remote host.
Open [Perfetto](https://ui.perfetto.dev/), select **Open trace file**, and
choose the saved file.

## Read the trace

Tracks are grouped by **Proc** and **Actor**. They show:

- **Caller spans**: remote endpoint calls made by an actor.
- **Handler spans**: endpoint execution on the receiving actors.
- **Flow arrows**: links from each call to its remote handlers.

Select a handler span to find the calling actor. Select a caller span to find
all its remote invocations. A call sent to an actor mesh can link to several
handler spans. Follow these links to compare when the call started with when
each handler ran, and to find handlers that took longer than others.

## Troubleshooting

- If distributed telemetry is not enabled, call `job.enable_telemetry()` before
  allocating the job, then capture from that allocation.
- If the trace has no endpoint activity, run endpoint calls during the capture
  interval or increase `--time`.
- If activity from a host is missing, inspect collector warnings. Distributed
  telemetry collection is best-effort, so failed or slow collectors can reduce
  trace coverage.

## Related documentation

- [Observability overview](observability)
- [Distributed telemetry](distributed-telemetry)
- [Monarch Dashboard](monarch-dashboard)
- [Mesh Admin TUI](admin-tui)
