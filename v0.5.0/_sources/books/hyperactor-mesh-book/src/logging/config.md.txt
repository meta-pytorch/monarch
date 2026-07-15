# Config & env

Quick reference for the knobs that influence logging behavior. Defaults match the source in `logging.rs`.

## Knobs

| Knob | Type & default | How to set | When it takes effect | Affects | Notes |
|---|---|---|---|---|---|
| `HYPERACTOR_READ_LOG_BUFFER` ↯ | `usize`, default **100** | Environment variable (process env) | Read when stream forwarders are started | Stream forwarder read/batch behavior | Upper bound on lines read before a forced flush/resume cycle. Useful to bound memory when a child is chatty. |
| `HYPERACTOR_FORCE_FILE_LOG` | `bool`, default **false** | Environment variable | Read when `FileAppender::new()` runs | Local file aggregation | If **true**, create per-proc stdout/stderr files even in `Env::Local`. If **false** in `Env::Local`, file aggregation is skipped. |
| `HYPERACTOR_PREFIX_WITH_RANK` | `bool`, default **true** | Environment variable | Read when `StreamFwder::start(...)` wires a stream | Line formatting in the **bootstrap** process | Prepends `"[{rank}] "` to each line before shipping/teeing. Toggle requires re-wiring the forwarders to take effect. |
| `DEFAULT_AGGREGATE_WINDOW_SEC` | `u64`, default **5** | Compile-time constant; can be overridden at runtime via Python `set_mode(..., aggregate_window_sec=…)` | Runtime (client actor) | Client aggregation window | If `None`, aggregation is disabled and lines print immediately. When switching from `Some`→`None`, pending aggregates are flushed first. |
| `MAX_LINE_SIZE` | `usize`, default **4 * 1024** | Compile-time constant | Always on | Line truncation in stream forwarder | Lines longer than 4 KiB are truncated and suffixed with `"… [TRUNCATED]"` **before** reaching the client. |

↯ Defined as the `READ_LOG_BUFFER` attr in code (maps to env `HYPERACTOR_READ_LOG_BUFFER`).

## Runtime vs restart

- **Runtime-adjustable:** aggregation window (via Python `LoggingMeshClient.set_mode(..., aggregate_window_sec=…)`).
- **Apply on (re)start/wiring:** `HYPERACTOR_FORCE_FILE_LOG`, `HYPERACTOR_PREFIX_WITH_RANK`, `HYPERACTOR_READ_LOG_BUFFER` — changing these requires recreating the corresponding forwarders/file appenders to take effect.
- **Fixed at build time:** `MAX_LINE_SIZE`, the default value of `DEFAULT_AGGREGATE_WINDOW_SEC` (you can still override the window at runtime via Python).

## Related
- [Client actor](client.md) — aggregation, similarity bucketing, barriers
- [Forwarder internals](forwarder.md) — in-band `LogMessage::{Log, Flush}` and heartbeats
- [Stream forwarders](stream-forwarders.md) — FD capture, `tee`, local files
- [Python control surface](python.md) — `spawn`, `set_mode`, `flush`
