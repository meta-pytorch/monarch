# Running the minimonarch smoke test on MAST

This describes how to launch the multi-host QUIC smoke test on MAST and how to
query its logs. It is written from the debugging workflow used to scale the test
to 64k–128k workers.

## Topology recap

- Every MAST task starts `SMOKE_WORKERS_PER_HOST` worker processes on sequential
  ports (`SMOKE_PORT`, `SMOKE_PORT+1`, …). Each worker serves a `quic://` listener.
- **Task 0 is the root** (pinned via `TW_TASK_ID == "0"` in `mast_bootstrap.py`).
  It additionally dials every worker across the whole job, times connect +
  round-trip, and prints a summary. Because the root is always task 0, its logs
  are fetchable with a single-task query (see below) — critical at scale.

## Launching

`build_mast.py` builds the fbpkg, writes a job spec, and submits it via Thrift.

Full build + launch:

```bash
python3 build_mast.py \
  --hosts 4096 --workers-per-host 32 \
  --cluster CPUTrainingWorkloads \
  --env MM_QUIC_UDP_BUF_BYTES=134217728 \
  --env MM_QUIC_MAX_CONCURRENT_CONNECTS=1024 \
  --launch
```

On success it prints the **fbpkg id**, the **job name** (e.g.
`minimonarch_smoke_zdevito_<epoch>`), and `scheduled OK`.

Re-launch reusing an already-built package (skips the Rust/Python build — fast,
and guarantees the identical binary):

```bash
python3 build_mast.py \
  --hosts 4096 --workers-per-host 32 \
  --cluster CPUTrainingWorkloads \
  --skip-build --package monarch_additional_packages:<fbpkg-id> \
  --env ... \
  --launch
```

Total workers = `hosts * workers-per-host`. `4096 * 32 = 131072`.

### Tuning env vars (pass each with its own `--env KEY=VALUE`)

| Var | Meaning |
|-----|---------|
| `MM_QUIC_UDP_BUF_BYTES` | Total kernel UDP buffer budget across the client endpoint pool (bytes). |
| `MM_QUIC_CLIENT_ENDPOINTS` | Number of client recv endpoints (UDP sockets). Unset ⇒ adaptive. More is **not** better — extra drivers compete on the single thread. |
| `MM_QUIC_MAX_CONCURRENT_CONNECTS` | Cap on simultaneous connect *attempts*. Bounds the connect storm at high fan-out. Unset/`0` ⇒ unlimited. |
| `MM_QUIC_HEARTBEAT_INTERVAL_MS` | Heartbeat send cadence (default 5000). Raise at very high fan-out to cut steady-state heartbeat load. |
| `MM_QUIC_HEARTBEAT_TIMEOUT_MS` | Sever a connection after this long with no frame (default 20000). Keep ≈ 4× the interval. |
| `SMOKE_WORKERS_PER_HOST` | Set automatically from `--workers-per-host`. |

## Checking job status

```bash
# Top-level job state (RUNNING / COMPLETE / DEAD / SHUTTING_DOWN)
mast get-status <job-name>

# Per-task state counts (allocation progress, restarts, preemption)
mast get-status <job-name> --output json | python3 -c "
import sys, json, collections
d = json.load(sys.stdin); c = collections.Counter()
def walk(o):
    if isinstance(o, dict):
        if 'taskInstanceIdentifier' in o and 'state' in o: c[o['state']] += 1
        for v in o.values(): walk(v)
    elif isinstance(o, list):
        [walk(x) for x in o]
walk(d); print(dict(c))
"
```

## Querying logs

**Important:** this cluster ships task logs only to **Logarithm**, not Scribe, so
`tw log` reports "Failed to find log files". Use `mast get-logs`.

**Also important:** a bare `mast get-logs <job>` fans out **one query per task**
across all tasks and instantly trips Logarithm's per-user rate limit
(`API_THROTTLE 429`, ~150 queries/min). Always scope to specific tasks with
`--twjob` (a regex on the task handle, which ends in `/<task-id>`).

### The root (all `[root]` / summary / FAILURE lines) — single query

The root is task 0, so `--twjob ".*/0$"` reads exactly one task:

```bash
# Summary and outcome (stdout = Python output)
mast get-logs <job-name> --file-path stdout --twjob ".*/0$" \
  --regex "joining|all .*connected|summary|failures:|connect:|round-trip|ERROR"

# Every per-connection failure with its diagnostic fields
mast get-logs <job-name> --file-path stdout --twjob ".*/0$" --regex "FAILURE"

# Bucket failures by whether the connection had established
mast get-logs <job-name> --file-path stdout --twjob ".*/0$" --regex "FAILURE" \
  | grep -oE "established=(true|false)" | sort | uniq -c
```

Rust debug instrumentation goes to **stderr**:

```bash
# Command-loop throughput, mpsc backlog, scheduler lag
mast get-logs <job-name> --file-path stderr --twjob ".*/0$" --regex "MM_CTX"

# UDP ingress health: received datagrams + kernel drops (RcvbufErrors/InErrors)
mast get-logs <job-name> --file-path stderr --twjob ".*/0$" --regex "MM_UDP"

# Client endpoint pool size + connect-concurrency cap confirmation
mast get-logs <job-name> --file-path stderr --twjob ".*/0$" \
  --regex "client pool|connect concurrency"
```

### A specific worker host — map hostname → task id, then one query

Failure lines name the peer as `worker-<hostname>-<port>`. To inspect that
worker's send side (e.g. heartbeat sends `MM_HB`), find its task id from
`get-status` and query just that task:

```bash
HOST=twshared46013.01.atn6
TID=$(mast get-status <job-name> --output json 2>/dev/null | python3 -c "
import sys, json, re
d = json.load(sys.stdin); H = '$HOST'
def walk(o):
    if isinstance(o, dict):
        if o.get('hostname') == H and 'taskInstanceIdentifier' in o:
            m = re.search(r'/(\d+):\d+\$', o['taskInstanceIdentifier'])
            if m: print(m.group(1))
        for v in o.values(): walk(v)
    elif isinstance(o, list):
        [walk(x) for x in o]
walk(d)")

# Worker-side heartbeat sends (proves what the sender emitted, tagged by pid)
mast get-logs <job-name> --file-path stderr --twjob ".*/$TID\$" --regex "MM_HB"
```

Workers log their `pid` at startup (`[worker :PORT] serving … pid=PID`), so
`MM_HB pid=…` lines can be mapped back to a worker's port within a task's log.

## Log line reference

| Prefix | Stream | Meaning |
|--------|--------|---------|
| `[bootstrap]` | stdout | Task startup: host, rank0, task_id, host/worker counts. |
| `[root]` | stdout | Root progress, `--- summary ---`, `FAILURE (connect/reply) <who>: <reason>`. |
| `[worker :PORT]` | stdout | Worker serving/connected/`root gone`; includes `pid=`. |
| `MM_CTX` | stderr | Per-second: `cmds/s`, mpsc `backlog`, `sched-lag(max)` (runtime scheduler lag). |
| `MM_UDP` | stderr | Per-second UDP delta: `in +/s`, `in_err +`, `rcvbuf_err +` (kernel ingress drops). |
| `MM_HB` | stderr | One line per heartbeat *sent* by a serving worker: `pid=`, `sent=`, peer addr. |
| `MM_QUIC …` | stderr | Endpoint pool size, per-socket buffer grants, connect-concurrency cap. |

A `FAILURE` reason carries per-reader diagnostics:
`established=<bool>, heartbeats=<n>, commands=<n>, age=<s>, last_read=<s ago|never>`
— i.e. whether the peer's identity handshake completed, how many heartbeats/frames
this side received, connection lifetime, and how long since the last frame.
