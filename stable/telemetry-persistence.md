# Telemetry persistence

Save telemetry to query a job after it stops. Use the same Monarch build for
collection and offline reading; schemas are stored in the Arrow IPC data.

## Configure your job

Configure your existing job before calling `state()`:

```
from monarch.job import TelemetryConfig

job.enable_telemetry(
 TelemetryConfig(persist_dir="/tmp/monarch-telemetry")
)
state = job.state(cached_path=None)
print(f"Apply ID: {job.apply_id}")
```

### Python settings

| `TelemetryConfig` setting | Default | Effect |
| --- | --- | --- |
| `persist_dir` | `None` | Persistence directory for collectors; `None` disables it unless the client has an override. |
| `client_persist_dir` | `None` | Overrides the client collector's directory; otherwise it uses `persist_dir`. |
| `retention_secs` | `3600` | Live in-memory retention only; it does not delete persisted data. `0` disables live expiry. |

Choose storage that survives worker allocation cleanup for remote jobs. If the
client cannot access the workers' mount, set `client_persist_dir` to a writable
client directory. Files on a worker's local `/tmp` must be copied before that
storage is discarded.

### Rust writer settings

These `PersistConfig` defaults are internal to the Rust writer. They are
**not exposed as Python or CLI options**; adjusting them requires a code change
and rebuilding Monarch.

| Setting | Default | Effect |
| --- | --- | --- |
| `flush_interval` | `200 ms` | Interval for merging pending batches and flushing file buffers. |
| `sync_interval` | `5 s` | Interval for syncing written data to storage. |
| `segment_bytes` | `256 MiB` | File size that triggers rotation to the next segment. |
| `capacity` | `10,000 batches` | Queue capacity; additional sends can drop batches when full. |
| `coalesce_rows` | `8,192 rows per table` | Pending row count that triggers an immediate write for that table. |

Flush and sync intervals are scheduling intervals, not maximum-loss guarantees.
Fixed format limits are 16 MiB per frame payload and 256 bytes per table name.
Oversized batches are split by rows; a row that cannot fit is dropped.

## Locate and query files

Files are stored under:

```
PERSIST_DIR/APPLY_ID/HOSTNAME-PID/000000.frames
```

`PID` identifies the collector process. Pass the apply-ID directory containing
those collector directories to the CLI. Replace `APPLY_ID` below with the value
printed by your job:

```
MONARCH_LOG_ROOT="/tmp/monarch-telemetry/APPLY_ID"
monarch query --dir "$MONARCH_LOG_ROOT" 'SHOW TABLES'
monarch query --dir "$MONARCH_LOG_ROOT" \
 'SELECT COUNT(*) AS spans FROM spans'
```

Repeat `--dir` to combine separate client and worker roots. Include each
collector's files once to avoid duplicate rows. The offline reader loads the
saved data into a fresh store without live retention or worker connections.

## Open the dashboard

```
monarch dashboard open "$MONARCH_LOG_ROOT" --port 0
```

`--port` defaults to `8265`; `0` selects an available port. Multiple roots can
be passed as positional paths. Open the printed URL and keep the command
running while viewing the dashboard. Ctrl+C stops it; the files can be reopened
later with the matching Monarch build.

## Shutdown and recovery

Use ordinary job shutdown, such as `job.kill()` in a `finally` block, and allow
cleanup to finish before exporting files or releasing storage. Collectors drain
pending batches, write an empty `__end` marker, and sync the files. Fixed close
wait limits are 5 s for the writer and 10 s for the sidecar's client close RPC.
A timeout or storage error can leave the log unclean.

- `clean`: readback recovered the collector's final `__end` marker.
- `unclean`: no final marker was recovered, including after an unexpected stop
or while the collector is still running.
- `torn`: bytes skipped from the first incomplete or undecodable frame to the
end of that segment. Earlier complete frames remain queryable.

A process can die between complete frames, leaving an unclean log with zero
torn bytes. Queued, buffered, or unsynced data may be missing after a crash.
Recovery requires the files to remain accessible. The clean flag describes
collector log closure; capture is best effort and dropped-batch counts are not
stored in the files.