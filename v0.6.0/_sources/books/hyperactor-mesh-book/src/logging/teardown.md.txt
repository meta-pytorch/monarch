# Teardown

- Always kick off a barrier flush before stopping the mesh.
- On child exit, tee observes EOF, forwards any partial line, and posts one last Flush(None).
- `StreamFwder::abort()` stops the tee task and returns a peek buffer for diagnostics.
- `FileAppender` tasks self-flush on drop; `LogClientActor` prints any buffered aggregates on drop.

---

## End-to-end shutdown (happy path)
  1. Python calls `LoggingMeshClient.flush(...)` → barrier across all forwarders (see Client & Ordering pages).
  2. Python proceeds to stop the mesh/procs.
  3. Each child exits → bootstrap side pipes hit EOF → tee:
    • flushes local std writer,
    • forwards final partial line (if any),
    • calls `log_sender.flush()` `(Flush(None))`.
  4. Bootstrap calls `StreamFwder::abort()` to stop the tee tasks and join them.
  5. `FileAppender` drops → its writer tasks `flush` and exit.
  6. Single `LogClientActor` in the Python process prints any remaining aggregates and returns from the barrier.

> There is one `LogClientActor` and it runs in the Python/driver process.

---

## Component responsibilities on teardown

### Stream forwarder (bootstrap side)

- EOF handling in tee:
  - forwards final partial line (if present),
  - posts `Flush(None)` once,
  - returns `Ok(())`.

```rust
// logging.rs — tee(..) tail (abridged)
std_writer.flush().await?;
if !line_buffer.is_empty() {
    // forward final partial line to sender + file monitor
}
if let Some(sender) = log_sender.as_mut() {
    let _ = sender.flush(); // posts LogMessage::Flush { sync_version: None }
}
Ok(())
```
- Controller stop/join:
```rust
// logging.rs — StreamFwder::abort (abridged)
pub async fn abort(self) -> (Vec<String>, Result<(), anyhow::Error>) {
    self.stop.notify_waiters();
    let lines = self.peek().await;         // diagnostic ring buffer
    let teer_result = self.teer.await;     // join spawned tee task
    let result = match teer_result { Ok(inner) => inner.map_err(anyhow::Error::from), Err(e) => Err(e.into()) };
    (lines, result)
}
```

### File aggregation (bootstrap side)
- Dropping `FileAppender` signals its two writer tasks to finish and flush.
```rust
// logging.rs — FileAppender::drop
fn drop(&mut self) {
    self.stop.notify_waiters();
    tracing::debug!("FileMonitor: dropping, stop signal sent, tasks will flush and exit");
}
```

### Forwarder (child side)
- No special teardown handler; when the proc exits, its served channel closes and the bootstrap side EOF path runs.
- Periodic `Flush(None)` heartbeats are generated during runtime (see Forwarder internals); they don't affect teardown.

---

### Failure paths & what you see
- **Child crashed / channel closed early**
`LocalLogSender::send`/`flush` checks `TxStatus`. If inactive, it skips sending and logs a debug line. Teardown proceeds; you may miss the very last batch for that proc.
- **Barrier never completes**
Usually `expected_procs` < actual forwarders mismatch. Re-check the count you passed into `StartSyncFlush` (Python's `flush()` computes this for us). See Client and Ordering.
- **Partial last line**
Safe: `tee` forwards the remaining bytes as a final line on EOF.
- **Interleaved stdout/stderr**
Expected. Ordering is per proc, per stream only (see Ordering).
- **Local files look incomplete**
If running in `Env::Local` and `HYPERACTOR_FORCE_FILE_LOG` is false, file aggregation may be disabled. See Config & env.

## Related
- **[Client actor](client.md)** — aggregation & barriers (source excerpts included)
- **[Forwarder internals](forwarder.md)** — why `Flush(None)` exists
- **[Stream forwarders](stream-forwarders.md)** — where EOF is handled and the final `flush()` is posted
- **[Teardown](teardown.md)** — what teardown does/doesn't guarantee
- **[Config & env](config.md)** — knobs affecting teardown behavior
