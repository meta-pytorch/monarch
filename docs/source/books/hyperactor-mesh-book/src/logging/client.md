# Client Actor

## What it does
Aggregates or prints log lines from all remote procs (stdout/stderr), optionally buckets similar lines within a time window, and participates in a versioned sync-flush barrier so the caller can deterministically wait until “all logs up to here” are delivered.

> **Placement:** There is exactly one `LogClientActor`, and it runs inside the Python/driver process (e.g., your notebook/REPL). All per-proc `LogForwardActor`s send to this single client.

## Message types
- **Data (from forwarders)**
  - `LogMessage::Log { hostname, pid, output_target, payload }`  — payload is `Serialized` of `Vec<Vec<u8>>` lines (or `String` fallback).
  - `LogMessage::Flush { sync_version: Option<u64> }` — `None` = heartbeat; `Some(v)` = barrier marker.
- **Control (to client actor)**
  - `LogClientMessage::SetAggregate { aggregate_window_sec: Option<u64> }`
  - `LogClientMessage::StartSyncFlush { expected_procs, reply: OncePortRef<()> , version: OncePortRef<u64> }`

## Lifecycle & fields
- **Fields**
  - `aggregate_window_sec: Option<u64>` (default `Some(5)`)
  - `aggregators: HashMap<OutputTarget, Aggregator>`
  - `last_flush_time: SystemTime`
  - `next_flush_deadline: Option<SystemTime>`
  - `current_flush_version: u64`
  - `current_flush_port: Option<OncePortRef<()>>`
  - `current_unflushed_procs: usize`
- **Lifecycle**
  - `new()` initializes aggregators and timers.
  - `drop` prints any buffered aggregates (final flush).

## Data path: `LogMessage::Log`
1. Deserialize `payload` to `Vec<Vec<String>>` (lines).
2. If `aggregate_window_sec == None`: print each line immediately with `[hostname pid]` prefix.
3. Else: `Aggregator::add_line` per line; set/adjust `next_flush_deadline`; on deadline, flush aggregates to stdout/stderr.

## Aggregation: `Aggregator` + `LogLine`
- `Aggregator` holds `Vec<LogLine>` and a `start_time`.
- Similarity via normalized Levenshtein; default threshold ≈ `0.15`.
- `add_line` merges into the closest group under threshold; else starts a new group.
- `Display` prints a time-window header and grouped counts: `"[N similar log lines] …"`.

## Barrier protocol: sync flush
1. Caller requests flush → `StartSyncFlush` allocates `version = ++current_flush_version`, records `expected_procs`, returns `version`.
2. Python tells each forwarder `ForceSyncFlush { version }` (they inject `Flush(Some(version))` in-band).
3. Client actor receives `Flush(Some(v))` from each forwarder, decrements `current_unflushed_procs`.
4. When it reaches zero: print pending aggregates, send `reply`.

## Teardown & failures
- `drop`: prints remaining aggregates.
- Mismatched versions are logged and ignored (stale `Flush(Some(v))`).
- If a sync `Flush(Some(v))` arrives with no active barrier, error is raised.
- Heartbeats (`Flush(None)`) are benign; they just help liveness.

## Quick reference (defaults & knobs)
- Placement: **single** `LogClientActor` in the Python/driver process; **one** `LogForwardActor` per remote proc.
- Aggregation window: **5s** by default (`DEFAULT_AGGREGATE_WINDOW_SEC`).
- Similarity threshold: **0.15** (merge if normalized edit distance < 0.15).
- Line truncation: **4 KiB** per line (`MAX_LINE_SIZE`) with `"… [TRUNCATED]"` suffix.
- Printing prefix: `"[{hostname} {pid}] "` before each line (non-aggregated path).
- Flush barrier: versioned; completes when **all** forwarders ack `Flush(Some(v))`.

## Data path: LogMessage::Log (source excerpt)

> **Where it runs:** a single `LogClientActor` in the **Python/driver process** receives log data from all remote procs.
> **Upstream:** per-proc `LogForwardActor` (in each remote proc) forwards `LogMessage::Log` if streaming is enabled.

> **Type legend:**
> `LogClientActor` — client-side coordinator (single instance)
> `Serialized` — opaque bytes; expected to be `Vec<Vec<u8>>` (lines) or `String`
> `OutputTarget` — `Stdout | Stderr`

### 1) Deserializing payload into lines
**File:** `logging.rs`
**Item:** free function `deserialize_message_lines(...)`

```rust
fn deserialize_message_lines(
    serialized_message: &Serialized,
) -> anyhow::Result<Vec<Vec<String>>> {
    // Prefer Vec<Vec<u8>> → Vec<Vec<String>>
    if let Ok(message_bytes) = serialized_message.deserialized::<Vec<Vec<u8>>>() {
        let mut result = Vec::new();
        for bytes in message_bytes {
            let message_str = String::from_utf8(bytes)?;
            let lines: Vec<String> = message_str.lines().map(|s| s.to_string()).collect();
            result.push(lines);
        }
        return Ok(result);
    }
    // Fallback: plain String → wrap
    if let Ok(message) = serialized_message.deserialized::<String>() {
        let lines: Vec<String> = message.lines().map(|s| s.to_string()).collect();
        return Ok(vec![lines]);
    }
    anyhow::bail!("failed to deserialize message as either Vec<Vec<u8>> or String")
}
```

### 2) Immediate print helper (non-aggregated path)

**File:** logging.rs
**Type/impl:** `impl LogClientActor`
**Item:** `print_log_line(...)`
```rust
fn print_log_line(hostname: &str, pid: u32, output_target: OutputTarget, line: String) {
    let message = format!("[{} {}] {}", hostname, pid, line);
    #[cfg(test)] crate::logging::test_tap::push(&message);
    match output_target {
        OutputTarget::Stdout => println!("{}", message),
        OutputTarget::Stderr => eprintln!("{}", message),
    }
}
```

### 3) The handler: aggregate or print, and schedule flush if needed

**File:** logging.rs
**Type/impl:** `impl LogMessageHandler for LogClientActor`
**Handler:** `log(...)` (abridged)
```rust
async fn log(
    &mut self,
    cx: &Context<Self>,
    hostname: String,
    pid: u32,
    output_target: OutputTarget,
    payload: Serialized,
) -> Result<(), anyhow::Error> {
    let message_line_groups = deserialize_message_lines(&payload)?;     // Vec<Vec<String>>
    let hostname = hostname.as_str();
    let message_lines: Vec<String> = message_line_groups.into_iter().flatten().collect();

    match self.aggregate_window_sec {
        // --- no aggregation: print immediately ---
        None => {
            for line in message_lines {
                Self::print_log_line(hostname, pid, output_target, line);
            }
            self.last_flush_time = RealClock.system_time_now();
        }

        // --- aggregate within a time window, then flush ---
        Some(window) => {
            for line in message_lines {
                if let Some(agg) = self.aggregators.get_mut(&output_target) {
                    // levenshtein-based bucketing, may start a new group
                    if let Err(e) = agg.add_line(&line) {
                        tracing::error!("error adding log line: {}", e);
                        // fail-open: print line immediately
                        Self::print_log_line(hostname, pid, output_target, line);
                    }
                } else {
                    tracing::error!("unknown output target: {:?}", output_target);
                    Self::print_log_line(hostname, pid, output_target, line);
                }
            }

            // compute (or tighten) the next flush deadline and self-schedule
            let new_deadline = self.last_flush_time + Duration::from_secs(window);
            let now = RealClock.system_time_now();
            if new_deadline <= now {
                self.flush_internal(); // prints and resets aggregators
            } else {
                let delay = new_deadline.duration_since(now)?;
                match self.next_flush_deadline {
                    None => {
                        self.next_flush_deadline = Some(new_deadline);
                        cx.self_message_with_delay(LogMessage::Flush { sync_version: None }, delay)?;
                    }
                    Some(deadline) if new_deadline < deadline => {
                        self.next_flush_deadline = Some(new_deadline);
                        cx.self_message_with_delay(LogMessage::Flush { sync_version: None }, delay)?;
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}
```
### Notes
- Line truncation (4 KiB + "… [TRUNCATED]") happens earlier in the bootstrap's `tee(...)`; the client assumes safe UTF-8 after `deserialize_message_lines`.
- Aggregation runs per target (`Stdout` vs `Stderr`) with the default 5s window and ~0.15 similarity threshold.

---

## Barrier protocol: sync flush (source excerpts)

> **Where it runs:** the kickoff happens in **Python** via `LoggingMeshClient`, which triggers a barrier across *all per-proc* `LogForwardActor`s. The **single** `LogClientActor` in the Python process accounts arrivals of `Flush(Some(v))` and unblocks when all procs have acknowledged.





> **Type legend:**
>
> `ActorHandle<LogClientActor>` — handle for the client actor (runs in the Python/driver proc)
> `ActorMeshRef<LogForwardActor>` — reference to per-proc forwarders (run in remote procs)
> `OncePortRef<T>` — one-shot reply port for sync responses


### 1) Kickoff from Python: `LoggingMeshClient::flush_internal`
**File:** `logging.rs`
**Type/impl:** `impl LoggingMeshClient`
**Method:** `async fn flush_internal(...)`

```rust
async fn flush_internal(
    cx: &impl context::Actor,
    client_actor: ActorHandle<LogClientActor>,
    forwarder_mesh: ActorMeshRef<LogForwardActor>,
) -> Result<(), anyhow::Error> {
    let (reply_tx, reply_rx) = cx.instance().open_once_port::<()>();
    let (version_tx, version_rx) = cx.instance().open_once_port::<u64>();

    // 1) Ask the client actor to start a barrier and return a version.
    client_actor.send(LogClientMessage::StartSyncFlush {
        expected_procs: forwarder_mesh.region().num_ranks(),
        reply: reply_tx.bind(),       // OncePortRef<()>
        version: version_tx.bind(),   // OncePortRef<u64>
    })?;

    let version = version_rx.recv().await?;

    // 2) Tell every forwarder to inject the barrier marker in-band.
    forwarder_mesh.cast(cx, LogForwardMessage::ForceSyncFlush { version })?;

    // 3) Block until the client actor acks that all procs reported.
    reply_rx.recv().await?;
    Ok(())
}
```
Why in-band? Each `LogForwardActor` posts `Flush(Some(version))` on the same ordered channel as its `Log` data, so the barrier marker is guaranteed to come after all prior lines.

### 2) Forwarder injects the barrier marker (in-band)

**File:** logging.rs
**Type/impl:** `impl LogForwardMessageHandler for LogForwardActor`
**Handler:** `force_sync_flush(...)`

```rust
async fn force_sync_flush(
    &mut self,
    _cx: &Context<Self>,
    version: u64,
) -> Result<(), anyhow::Error> {
    // Post the barrier marker into the same ordered stream as data.
    self.flush_tx
        .lock()
        .await
        .send(LogMessage::Flush { sync_version: Some(version) })
        .await
        .map_err(anyhow::Error::from)
}
```
**... and when the forwarder later reads that marker from its rx, it acks the client:**

**Type/impl:** `impl LogForwardMessageHandler for LogForwardActor`
**Handler:** `forward(...)` (excerpt)
```rust
match self.rx.recv().await {
    Ok(LogMessage::Flush { sync_version: Some(version) }) => {
        // All prior logs on this channel are now observed; ack the client.
        self.logging_client_ref.flush(ctx, version).await?;
    }
    // ...
    _ => { /* other cases */ }
}
```
### 3) Client actor tracks & releases the barrier
**File:** logging.rs
**Type/impl:** `impl LogClientMessageHandler for LogClientActor`
**Handler:** `start_sync_flush(...)`

```rust
// logging.rs :: LogClientMessageHandler for LogClientActor — StartSyncFlush
async fn start_sync_flush(
    &mut self,
    cx: &Context<Self>,
    expected_procs_flushed: usize,
    reply: OncePortRef<()>,
    version: OncePortRef<u64>,
) -> Result<(), anyhow::Error> {
    if self.current_unflushed_procs > 0 || self.current_flush_port.is_some() {
        tracing::warn!(
            "found unfinished ongoing flush: version {}; {} unflushed procs",
            self.current_flush_version,
            self.current_unflushed_procs,
        );
    }

    self.current_flush_version += 1;
    tracing::debug!("start sync flush with version {}", self.current_flush_version);
    self.current_flush_port = Some(reply.clone());
    self.current_unflushed_procs = expected_procs_flushed;
    version
        .send(cx, self.current_flush_version)
        .map_err(anyhow::Error::from)?;
    Ok(())

```
**Type/impl:** `impl LogMessageHandler for LogClientActor`
**Handler:** `flush(...)` (counts acks, then releases)
```rust
// logging.rs :: LogMessageHandler for LogClientActor — Flush(Some(v)) branch
async fn flush(
    &mut self,
    cx: &Context<Self>,
    sync_version: Option<u64>,
) -> Result<(), anyhow::Error> {
    match sync_version {
        None => {
            self.flush_internal();
        }
        Some(version) => {
            if version != self.current_flush_version {
                tracing::error!(
                    "found mismatched flush versions: got {}, expect {}; this can happen if some previous flush didn't finish fully",
                    version,
                    self.current_flush_version
                );
                return Ok(());
            }

            if self.current_unflushed_procs == 0 || self.current_flush_port.is_none() {
                anyhow::bail!("found no ongoing flush request");
            }
            self.current_unflushed_procs -= 1;

            tracing::debug!(
                "ack sync flush: version {}; remaining procs: {}",
                self.current_flush_version,
                self.current_unflushed_procs
            );

            if self.current_unflushed_procs == 0 {
                self.flush_internal();
                let reply = self.current_flush_port.take().unwrap();
                self.current_flush_port = None;
                reply.send(cx, ()).map_err(anyhow::Error::from)?;
            }
        }
    }

    Ok(())
}
```
### Summary:
  1. Python asks the client actor to start a barrier → gets version v.
  2. Python tells every forwarder to in-band post `Flush(Some(v))`.
  3. The client actor `decrements current_unflushed_procs` as each arrives; when it hits zero, it prints aggregates and replies to Python.
  4. Heartbeats `Flush(None)` are unrelated to the barrier; they just maintain liveness.
