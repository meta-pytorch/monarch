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

## Running across multiple regions (`multi_region.py`)

A single MAST job **cannot span regions** — MAST's allocators are per-region and all
task groups of a job must land in one region (multi-region single-job is experimental,
gated behind the `enable_multi_region_jobs` entitlement, which we don't have). So a run
larger than one region's free capacity can't be placed as one job. On CPUTrainingWorkloads
the T1 fleet lives in only 6 regions (pnb, atn, vll, ftw, nha, ncg); pnb/atn/vll each have
the most headroom. Single-region tops out around ~2048 whole hosts for our best-effort
tenant, so ~4096 hosts (131072 workers) will not schedule in one region.

`multi_region.py` works around this by stitching several single-region jobs into one
logical run, orchestrated from the devserver:

1. It schedules N MAST jobs (one per region) in **coordinated mode** (`SMOKE_COORD_PORT`
   set). Each job's rank-0 task serves a minimonarch *coordination* listener
   (`smoke.py --wait-for-addresses PORT`) and waits — it does **not** read the worker
   list from its own env. Every job serves such a root; all but one go unused.
2. It polls `mast get-status` until every job is RUNNING with all task hostnames published.
3. It gathers the union of every job's worker addresses.
4. It locally joins the chosen job's coordination root over minimonarch and sends the full
   address list (split into `<=4MB` pickled chunks). The root acks, spins up the real
   `b"root"` actor, and runs the normal sweep against every worker across all jobs —
   genuinely cross-region.
5. On completion (the root reports back) it kills every job.

Run it under the built minimonarch (it imports the extension); it shells out to `python3`
for `build_mast.py` and to `mast` for status/kill. From the `python/` dir:

```bash
# Small validation run (2 jobs, auto-region):
.venv/bin/python ../mast/multi_region.py --hosts-per-job 2 --workers-per-host 1 --jobs 2

# Full cross-region run: 2 jobs x 2048 hosts x 32 workers = 131072 workers, pnb + atn:
.venv/bin/python ../mast/multi_region.py \
  --hosts-per-job 2048 --workers-per-host 32 --regions pnb,atn \
  --cluster CPUTrainingWorkloads \
  --env MM_QUIC_UDP_BUF_BYTES=134217728 \
  --env MM_QUIC_MAX_CONCURRENT_CONNECTS=1024 \
  --env MM_QUIC_MAX_DIRECT_CHILDREN=362
```

Key options: `--regions r1,r2,...` (one job per region; number of jobs = number of regions;
omit for `--jobs N` auto-region), `--hosts-per-job`, `--workers-per-host`, `--coord-port`
(default 26599, distinct from the worker ports), `--env KEY=VALUE` (forwarded to every job),
`--keep-jobs` (skip cleanup for debugging). Because the run is long, launch it with `nohup
... &` and tail its log; the per-size sweep numbers are in the **chosen root's** MAST logs
(task 0 of the first job), fetchable exactly like any single-job run below.

Notes:
- `MM_QUIC_MAX_DIRECT_CHILDREN` can't be auto-sized per job (no job knows the cross-job
  total), so pass it explicitly via `--env` (e.g. round(sqrt(total workers))).
- Pick regions with capacity (see the capacity note above); a region without ~`hosts-per-job`
  free whole hosts will sit PENDING or hard-reject at enqueue.

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
