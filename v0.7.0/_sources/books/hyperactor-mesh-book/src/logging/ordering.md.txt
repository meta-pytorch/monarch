# Ordering

## What's guaranteed (and what isn't)

- **Per proc, per stream.** Within a single child's **stdout** stream (and separately within **stderr**), lines are read in order by the bootstrap and forwarded in that order. See the tee path in [`stream-forwarders.md`](stream-forwarders.md).
- **No global total order.** There is **no** ordering guarantee:
  - between **stdout** and **stderr** of the same proc (tasks run independently and messages may interleave), or
  - across **different procs**.
- **Barrier scope.** A **sync flush** ensures that, for each proc, the client observes **all log messages that arrived before the barrier marker** on that proc's channel. It does **not** reorder or coalesce logs across procs/streams. See details in [`client.md`](client.md).

---

## Why the barrier is "in-band"

The forwarder injects `Flush(Some(v))` **on the same actor mailbox** that carries data `Log` messages. Since the client only acks the barrier **after** it has read the marker from that mailbox, all previously enqueued `Log` messages for that proc are known to precede the barrier.

> Heartbeats `Flush(None)` are unrelated to ordering; they are for **liveness** (to avoid `recv()` starvation) and come from the **forwarder**. The bootstrap's stream forwarder only flushes at **EOF**. See [`forwarder.md`](forwarder.md) and [`stream-forwarders.md`](stream-forwarders.md).

---

## Minimal source context

**Forwarder reads data and barrier markers from one mailbox**
```rust
// logging.rs (abridged)
// LogForwardActor main loop: data and control share the same rx.
match self.rx.recv().await {
    Ok(LogMessage::Log { hostname, pid, output_target, payload }) => {
        if self.stream_to_client {
            self.logging_client_ref
                .log(ctx, hostname, pid, output_target, payload)
                .await?;
        }
    }
    Ok(LogMessage::Flush { sync_version: Some(version) }) => {
        // Barrier marker observed ⇒ all prior Log on this mailbox precede it.
        self.logging_client_ref.flush(ctx, version).await?;
    }
    Ok(LogMessage::Flush { sync_version: None }) => {
        // Heartbeat for liveness; not an ordering boundary.
        /* reschedule another heartbeat */
    }
    Err(e) => return Err(e.into()),
}
```

**Bootstrap preserves per-stream read order**
```rust
// logging.rs (abridged)
// tee(...) frames by '\n' in the order read from the child pipe.
for &byte in &buf[..n] {
    if byte == b'\n' {
        /* finalize current line → push to completed_lines in arrival order */
    } else {
        line_buffer.push(byte);
    }
}
// completed_lines are then sent as Vec<Vec<u8>> via LocalLogSender.
```

**Practical takeaways**
- Expect stdout/stderr interleaving; only rely on per-stream order per proc.
- A barrier means “client has seen everything before the marker on each proc,” not “logs are globally ordered.”
- If you need stronger semantics, encode your own sequence markers in the workload’s logs at the source.
