# Forwarders

The `LogForwardActor` runs in every remote process of a `ProcMesh`. It is the in-band relay that receives per-proc logs over a hyperactor channel and forwards them to the client's `LogClientActor`, preserving ordering and enabling sync flush barriers.

## What problem it solves

- Receives the lines that the **bootstrap** captured from each child’s `stdout`/`stderr` (via `StreamFwder`).
- Keeps **control** ordered with **data** by using a single in-band channel carrying `LogMessage::{Log, Flush}`.
- Relays data to the client and coordinates versioned **sync-flush** barriers.

**Where it runs & how it connects**

- One `LogForwardActor` runs **inside each remote proc**.
- On startup it **serves** the Unix channel whose address is in `BOOTSTRAP_LOG_CHANNEL` (see `new()`).
- The bootstrap side **dials** that address to send `LogMessage::Log` frames.
- The forwarder also **self-dials** the same address to post `Flush` markers in-band (`flush_tx`), so barrier/control messages are ordered relative to log data.

## Topology and channel negotiation

### Logging - components and ownership
```
[BootstrapProcManager on each host]
  └─ spawns N child procs
     └─ owns each child’s stdout/stderr pipes
        └─ StreamFwder reads the pipes
           ├─ writes back to bootstrap’s stdout/stderr
           ├─ optionally writes to local files via FileAppender
           └─ sends lines on BOOTSTRAP_LOG_CHANNEL (Unix socket)

[Child process]
  └─ LogForwardActor  (serves BOOTSTRAP_LOG_CHANNEL)
     ├─ receives LogMessage::{Log, Flush}
     ├─ forwards to client’s LogClientActor (if streaming enabled)
     └─ participates in sync flush barriers
```

### Logging - wiring and message flow
```
[host/bootstrap proc]                     [child/remote proc]
StreamFwder ──LocalLogSender──►  (unix)  ──► LogForwardActor (serve)
(stdout/stderr)                              ^   ^
  |                                          |   |
  └─ FileAppender (optional)                 |   └─ dial same channel for in-band Flush
                                             |
                                   BOOTSTRAP_LOG_CHANNEL (env)
```
- The bootstrap chooses a channel address and passes it to the child via `BOOTSTRAP_LOG_CHANNEL`
- The child `LogForwardActor` serves that address; the parent `LocalLogSender` dials it.
- The forwarder also dials its own served address to post `Flush` control messages in-band, guaranteeing ordering w.r.t. data.
> If `BOOTSTRAP_LOG_CHANNEL` is absent (tests/local), the forwarder falls back to `ChannelAddr::any(...)`. In that mode no parent is connected; streaming is effectively disabled.

---

## Message Protocol

### Data plane (parent → forwarder)
```rust
enum LogMessage {
  Log {
    hostname: String,
    pid: u32,
    output_target: OutputTarget, // Stdout|Stderr
    payload: Serialized,         // Vec<Vec<u8>> (lines)
  },
  Flush {
    sync_version: Option<u64>,   // None: heartbeat; Some(v): barrier marker
  },
}
```
### Control plane (client → forwarder)

```rust
enum LogForwardMessage {
  Forward,                        // pull-next from rx (drives the loop)
  SetMode { stream_to_client: bool },
  ForceSyncFlush { version: u64 }, // injects in-band Flush(Some(version))
}
```

---

## Lifecycle

### Spawn & init (trimmed)

```rust
impl Actor for LogForwardActor {
  type Params = ActorRef<LogClientActor>;

  async fn new(logging_client_ref: Self::Params) -> Result<Self> {
    let addr: ChannelAddr = match std::env::var(BOOTSTRAP_LOG_CHANNEL) {
      Ok(s) => s.parse()?,                       // expected path
      Err(_) => ChannelAddr::any(ChannelTransport::Unix), // fallback
    };

    // Serve the parent→child log stream.
    let rx = channel::serve(addr.clone())
      .map(|(_, rx)| rx)
      .unwrap_or_else(|_| channel::serve(ChannelAddr::any(ChannelTransport::Unix)).unwrap().1);

    // Dial same addr for in-band Flush postings.
    let flush_tx = Arc::new(Mutex::new(channel::dial::<LogMessage>(addr)?));

    Ok(Self {
      rx,
      flush_tx,
      next_flush_deadline: RealClock.system_time_now(),
      logging_client_ref,
      stream_to_client: true,
    })
  }

  async fn init(&mut self, this: &Instance<Self>) -> Result<()> {
    // Kick the pull loop and seed a heartbeat to avoid starvation.
    this.self_message_with_delay(LogForwardMessage::Forward, Duration::from_secs(0))?;
    self.flush_tx.lock().await
      .send(LogMessage::Flush { sync_version: None }).await?;
    Ok(())
  }
}
```
Why the self-dial? To inject `Flush` over the same ordered stream as data, so the barrier marker sits after all prior logs.

---

## The forward loop (ordered handling)

```rust
impl LogForwardMessageHandler for LogForwardActor {
  async fn forward(&mut self, ctx: &Context<Self>) -> Result<()> {
    match self.rx.recv().await? {
      LogMessage::Log { hostname, pid, output_target, payload } => {
        if self.stream_to_client {
          self.logging_client_ref
            .log(ctx, hostname, pid, output_target, payload)
            .await?;
        }
      }
      LogMessage::Flush { sync_version } => {
        match sync_version {
          None => {
            // Heartbeat to prevent the actor from sitting in recv()
            // forever and starving its mailbox/transport.
            let delay = Duration::from_secs(1);
            if RealClock.system_time_now() >= self.next_flush_deadline {
              self.next_flush_deadline = RealClock.system_time_now() + delay;
              let tx = self.flush_tx.clone();
              tokio::spawn(async move {
                RealClock.sleep(delay).await;
                let _ = tx.lock().await
                  .send(LogMessage::Flush { sync_version: None }).await;
              });
            }
          }
          Some(version) => {
            // In-band barrier marker: all prior data observed ⇒ now ack to client.
            self.logging_client_ref.flush(ctx, version).await?;
          }
        }
      }
    }

    // Tail-call to keep pulling.
    ctx.self_message_with_delay(LogForwardMessage::Forward, Duration::from_secs(0))?;
    Ok(())
  }

  async fn set_mode(&mut self, _ctx: &Context<Self>, stream_to_client: bool) -> Result<()> {
    self.stream_to_client = stream_to_client;
    Ok(())
  }

  async fn force_sync_flush(&mut self, _ctx: &Context<Self>, version: u64) -> Result<()> {
    // Post the barrier marker into the data stream to preserve ordering.
    self.flush_tx.lock().await
      .send(LogMessage::Flush { sync_version: Some(version) })
      .await
      .map_err(anyhow::Error::from)
  }
}
```
Key guarantees
 - Ordering: `Flush(Some(v))` is read after all earlier `Log` on the same channel.
 - Liveness: Heartbeat `Flush(None)` prevents `recv()` starvation and keeps transport flowing.
 - Fan-in: One forwarder per proc; the client actor aggregates from all forwarders.

---

## Relationship to stream forwarders (bootstrap side)

- The bootstrap process owns `StreamFwder` (one per stdout/stderr) which:
  - reads OS pipes from the child,
  - writes to local stdout/stderr,
  - optionally writes to `FileAppender` aggregated files,
  - uses `LocalLogSender` to send `LogMessage::Log` into the child's served channel,
  - occasionally issues `LocalLogSender.flush()` with posts `Flush(None)`
- The forwarder in the child is oblivious to file IO - it just relays in-band messages to the client.

---

## Sync flush barrier end-to-end
  1. Python calls `LoggingMeshClient.flush(...)`.
  2. Client actor allocates a version v, records `expected_procs`, and returns v to the caller.
  3. For each proc, Python tells its forwarder: `ForceSyncFlush { version: v }`.
  4. Each forwarder posts `Flush(Some(v))` into its own data stream and, upon reading it, calls `client.flush(v)`.
  5. Client counts acks; when all `expected_procs` report, it unblocks the caller.

This guarantees all logs prior to the barrier have been observed by the client.
