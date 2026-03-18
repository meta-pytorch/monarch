# Remoterun on MAST: Setup Guide

This guide covers how to set up and run `remoterun` on MAST with different hardware types.

## Quick Start

Use the automated setup script:

```bash
# Set up conda envs for GB200/GB300 (aarch64)
bash examples/remotemount/setup_conda_env.sh gb200

# Set up conda envs for GrandTeton/H100 (x86)
bash examples/remotemount/setup_conda_env.sh grandteton
```

This creates `~/monarch_conda_envs/client/conda` (x86) and
`~/monarch_conda_envs/worker/conda` (target arch). Then run:

```bash
bash examples/remotemount/remoterun.sh /tmp/myenv my_script.sh
```

Or directly:

```bash
CONDA_PREFIX=~/monarch_conda_envs/worker/conda \
~/monarch_conda_envs/client/conda/bin/python3.12 \
  examples/remotemount/remoterun.py /tmp/myenv my_script.sh \
  --backend mast --host_type gb200

```

---

## Platform Notes

### GrandTeton (x86, H100)

- Default host type is `gtt_any` (8 GPUs per host). Our entitlement
  (`msl_infra_pytorch_dev`) has **0 quota** on GrandTeton — scheduling
  is opportunistic and may take a long time or fail.
- GrandTeton has ibverbs RDMA hardware, so transfers use native RDMA
  (significantly faster than TCP fallback).

### GB200 / GB300 (aarch64, Blackwell)

- GB200 and GB300 use the same aarch64 conda environment and wheel.
- GB200 schedules faster (~3 min) than GB300 (~10 min). Set the host
  type via `MONARCH_HOST_TYPE=gb200` (default) or `MONARCH_HOST_TYPE=gb300`.
- Must pass `--locality_constraints ""` to allow scheduling in all regions.
- Both have ibverbs RDMA hardware (Mellanox ConnectX devices).
  Client-to-worker transfer uses TCP fallback (client has no ibverbs),
  but worker-to-worker fan-out uses native ibverbs.
- The MAST preamble health checks (`toy_ddp`) fail with socket
  timeouts on these hosts (ephemeral port firewall issue). Remoterun
  automatically skips them via `MAST_PRECHECK_SKIP_TIME_CONSUMING_CHECKS=1`.

---

## Job Reuse

By default, `remoterun` does not kill the MAST job when the script
finishes. The next invocation will reconnect to the cached job
instantly (~0.1s) instead of waiting for new allocation:

```bash
# First run: allocates workers (~2 min)
... remoterun.py /tmp/myenv script1.sh --backend mast ...

# Second run: reuses workers (instant)
... remoterun.py /tmp/myenv script2.sh --backend mast ...

# Kill the job when done
... remoterun.py /tmp/myenv script.sh --backend mast --kill_job
```

The job state is cached in `.monarch/job_state.pkl`.

---

## Incremental Update Performance

Remotemount uses block-level hashing for incremental updates.
Block hashes (xxh64, 100 MB blocks) are computed in the Rust packing
step in a single pass — the hash runs over pages still hot in CPU
cache from the file-read pass, so it adds negligible overhead.
On re-open, only blocks whose hash changed are re-transferred.
Unchanged workers skip transfer entirely (metadata + remount only).

### Benchmark setup

- 2 GB200 hosts, 8 parallel TLS streams, Rust TLS receiver (rustls)
- Client-to-worker transfer over TCP (client has no ibverbs),
  worker-to-worker fan-out via native ibverbs RDMA
- Each payload contains 1000 `.py` files (~50 MB total) plus a `data.bin`
  file sized to reach the target payload
- Warm-up step pre-spawns actors so cold start measures transfer time only

### Scenarios

| Scenario | Description |
|----------|-------------|
| Cold start | First transfer to fresh workers |
| No change | Re-open with identical content — hash match, skip transfer |
| Rewrite data.bin | Replace `data.bin` with new random content (same size) |
| Rewrite .py | Rewrite all 1000 `.py` files — ~50 MB of code re-transferred |
| Delete file | Remove one `.py` file — triggers full re-pack |

### Results (8 streams)

| Payload | Cold start | No change | Rewrite data.bin | Rewrite .py | Delete file |
|---------|-----------|-----------|-----------------|-------------|-------------|
| 0 GB    | 12.6s     | 5.0s      | 5.3s            | 5.3s        | 12.8s       |
| 1 GB    | 14.0s     | 5.3s      | 7.2s            | 6.7s        | 15.3s       |
| 2 GB    | 15.4s     | 7.5s      | 9.7s            | 7.4s        | 16.6s       |
| 4 GB    | 18.5s     | 9.7s      | 13.3s           | 8.0s        | 19.9s       |
| 8 GB    | 26.7s     | 14.9s     | 17.9s           | 12.0s       | 28.5s       |

Key observations:

- **No change** is dominated by client-side packing (no network transfer).
  Combined pack+hash in Rust eliminates a second data pass, reducing
  "no change" time by ~27% at 8 GB vs a two-pass approach.
- **Rewrite .py** only re-transfers the ~50 MB code portion; `data.bin`
  blocks are skipped via hash match.
- **Rewrite data.bin** re-transfers the bulk of the payload since every
  block changes.
- **Delete file** triggers a full re-pack (total size changes → stale),
  similar to cold start.
- **Rust TLS receiver** (rustls) eliminates ~4.3s of Python OpenSSL
  setup overhead per transfer. The receiver writes directly into
  anonymous mmap, removing the separate `load_cache_into_storage` step.
- **Parallel packing** distributes file read work round-robin by
  descending size across 16 threads, achieving ~8 GB/s on the client
  (vs ~1.5 GB/s with the naive sequential assignment).
- **Direct buffer send** — dirty blocks are sent directly from the
  client-side staging buffer (the same mmap used for packing and hashing),
  eliminating a second file-read pass that previously doubled I/O on the
  partial transfer path.

To run the benchmark yourself:

```bash
python3 examples/remotemount/bench_incremental.py --sizes 1 --hosts 2 --host_type gb200
```
