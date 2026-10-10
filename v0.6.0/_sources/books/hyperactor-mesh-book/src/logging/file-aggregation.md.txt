# File aggregation

## What it is

An optional, local file sink that mirrors each child's stdout/stderr into per-proc files on the bootstrap host. It's implemented by `FileAppender` plus a tiny writer task (`file_monitor_task`) that receives lines over a hyperactor channel and appends them to disk.

## When files are created (env gating)

Files are only created when:
- `Env` is not Local, or
- `Env` is `Local` and `HYPERACTOR_FORCE_FILE_LOG=true`
> If the gate returns `None`, file aggregation is simply disabled; streaming to the client and teeing to the bootstrap console still work.

---

## Naming scheme & paths

Files live under the path returned by `log_file_path(env, None)` and are named:
```
{basename}_{host-tag}_{shortuuid}.{stdout|stderr}
```
Where:
- basename — from `log_file_path` (per environment),
- host-tag — the bootstrap host, from hostname::get(),
- shortuuid — `ShortUuid::generate()` per `FileAppender`,
- suffix — "stdout" or "stderr".

```rust
// logging.rs — create_unique_file_writer (abridged)
let (path, filename) = log_file_path(env, None)?;
let mut full_path = PathBuf::from(&path);
let uuid = ShortUuid::generate();
let suffix = match output_target { Stderr => "stderr", Stdout => "stdout" };
full_path.push(format!("{}_{}_{}.{}", filename, file_name_tag, uuid, suffix));
// open(..., create/append) → tokio::fs::File
```

---

## How bytes reach the files

`StreamFwder`'s background `tee(...)` task frames lines and posts them to the file appender's channel as `FileMonitorMessage { lines: Vec<String> }`. The `file_monitor_task` writes each line + newline and flushes.
```rust
// logging.rs — file_monitor_task (abridged)
loop {
    tokio::select! {
        msg = rx.recv() => match msg {
            Ok(FileMonitorMessage { lines }) => {
                for line in &lines {
                    writer.write_all(line.as_bytes()).await?;
                    writer.write_all(b"\n").await?;
                }
                writer.flush().await?;
            }
            Err(e) => { tracing::debug!("channel error: {e}"); break; }
        },
        _ = stop.notified() => { break; }
    }
}
// final flush on exit
```
> Lines may already be truncated to 4 KiB upstream in `tee(...)` (with "… [TRUNCATED]"), and may carry an optional [rank] prefix if `HYPERACTOR_PREFIX_WITH_RANK=true`

## Using FileAppender (bootstrap side)

Creating an appender spins up two writer tasks (stdout/stderr) and exposes one channel address per stream:.
```rust
// logging.rs — FileAppender::new (abridged)
let app = FileAppender::new();                // Option<FileAppender>
let stdout_addr = app.as_ref().map(|a| a.addr_for(OutputTarget::Stdout));
let stderr_addr = app.as_ref().map(|a| a.addr_for(OutputTarget::Stderr));

// StreamFwder::start_with_writer(...) receives `file_monitor_addr` per stream.
// The tee task dials once and posts FileMonitorMessage batches as it frames lines.
```

Dropping the appender triggers a graceful stop & flush:
```rust
impl Drop for FileAppender {
    fn drop(&mut self) {
        self.stop.notify_waiters();           // writer tasks flush and exit
    }
}
```

---

## Rotation semantics & guarantees
    •   Rotation: not implemented (today each `FileAppender` creates fresh, uniquely-named files).
    •   Sync & durability: every batch is `flush()`ed by the writer task; a final `flush` happens on task exit and on drop.
    •   Line ordering: per-stream (stdout vs stderr are independent).
    •   Failure tolerance: if the writer channel closes, the task logs and exits; other paths (streaming, console tee) continue.

---

## Related
- **[Stream forwarders](stream-forwarders.md)** — where lines are framed and posted to the appender
- **[Client actor](client.md)** — aggregation/printing on the Python side
- **[Config & env](config.md)** — `HYPERACTOR_FORCE_FILE_LOG`, `HYPERACTOR_PREFIX_WITH_RANK`, `MAX_LINE_SIZE`
