# Telemetry Self-Message Benchmark

This benchmark stresses the public `ProcessJob` telemetry path with actor
self-message loops. It reports actor completion, query-visible `tick` rows,
missing rows, catch-up latency, and message throughput.

Run it with:

```bash
buck run fbcode//monarch/python/benches/telemetry_self_message_benchmark:telemetry_self_message_benchmark -- \
  --procs 16 \
  --messages 50000 \
  --warmup-messages 500 \
  --payload-bytes 128 \
  --inflight 8
```

The implementation is `benchmark.py`; the Buck target is
`fbcode//monarch/python/benches/telemetry_self_message_benchmark:telemetry_self_message_benchmark`.
The benchmark runs one host; `--procs` controls the local worker process count.

Pass `--disable-telemetry` to run the same actor self-message loop without
`ProcessJob` telemetry. That is useful for separating actor/message throughput
from telemetry overhead.

## Bottleneck Sweep

These single-host runs were collected on 2026-07-06. Successful runs all had
`complete=true`, and telemetry-enabled runs had `telemetry_missing_tick_rows=0`.

| Shape | Messages/proc | Payload | Inflight | Telemetry | Result |
|---|---:|---:|---:|---|---|
| 16 procs | 50,000 | 0 B | 8 | enabled | 51.5k msg/s |
| 16 procs | 50,000 | 128 B | 8 | enabled | 50.2k-51.3k msg/s |
| 16 procs | 50,000 | 128 B | 8 | disabled | 52.7k msg/s |
| 16 procs | 20,000 | 16 KiB | 8 | enabled | 49.6k msg/s |
| 16 procs | 5,000 | 64 KiB | 8 | enabled | 44.4k msg/s |
| 16 procs | 3,000 | 256 KiB | 8 | enabled | 37.4k msg/s |
| 16 procs | 1,000 | 1 MiB | 1 | enabled | 19.1k msg/s |
| 16 procs | 1,000 | 1 MiB | 8 | enabled | 28.9k msg/s |
| 16 procs | 1,000 | 1 MiB | 8 | disabled | 29.0k msg/s |
| 16 procs | 1,000 | 1 MiB | 32 | enabled | 16.8k msg/s |

## Learnings

At 16 procs, the small-payload ceiling is mostly actor/message dispatch, not
telemetry. Disabling telemetry improves the 128 B run from about 50.2k msg/s to
52.7k msg/s, and disabling telemetry for the 1 MiB run is effectively unchanged.
Telemetry is therefore measurable overhead for tiny messages, but not the main
bottleneck.

Tiny payloads are dispatch-bound. The 0 B, 128 B, and 16 KiB runs all stay near
50k-51k msg/s. Longer 16-proc runs hold near 51k msg/s, while a shorter
10,000-message run is lower at 44.2k msg/s. That points to fixed startup and
drain costs on short runs, then a steady-state dispatch ceiling around 51k msg/s
for small payloads.

Payload copy cost becomes visible above the tiny-payload range. Throughput drops
from 49.6k msg/s at 16 KiB to 44.4k msg/s at 64 KiB, 37.4k msg/s at 256 KiB,
and 28.9k msg/s at 1 MiB.

The actor self-message window is not the small-payload bottleneck: 16-proc,
128 B runs at inflight 1 and 32 both stay near 50k msg/s. For 1 MiB payloads,
inflight 8 is best in this sweep. Inflight 1 underfills the pipeline at 19.1k
msg/s, while inflight 32 drops to 16.8k msg/s, likely from queued-byte pressure.
