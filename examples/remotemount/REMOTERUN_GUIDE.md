# Remoterun on MAST: Setup Guide

This guide covers how to set up and run `remoterun` on MAST with different hardware types.

## Quick Start

Use the automated setup script:

```bash
# Set up conda envs for GB300 (aarch64)
bash examples/remotemount/setup_conda_env.sh gb300

# Set up conda envs for GrandTeton/H100 (x86)
bash examples/remotemount/setup_conda_env.sh grandteton
```

This creates `~/monarch_conda_envs/client/conda` (x86) and
`~/monarch_conda_envs/worker/conda` (target arch). Then run:

```bash
bash examples/remotemount/run_gb300.sh /tmp/myenv my_script.sh
```

Or directly:

```bash
CONDA_PREFIX=~/monarch_conda_envs/worker/conda \
~/monarch_conda_envs/client/conda/bin/python3.12 \
  examples/remotemount/remoterun.py /tmp/myenv my_script.sh \
  --backend mast --host_type gb300

```

---

## Platform Notes

### GrandTeton (x86, H100)

- Default host type is `gtt_any` (8 GPUs per host). Our entitlement
  (`msl_infra_pytorch_dev`) has **0 quota** on GrandTeton — scheduling
  is opportunistic and may take a long time or fail.
- GrandTeton has ibverbs RDMA hardware, so transfers use native RDMA
  (significantly faster than TCP fallback).

### GB300 (aarch64, Blackwell)

- Our entitlement has **17 quota** on GB300 in the `lco` region —
  scheduling takes ~2 minutes.
- Must pass `--locality_constraints ""` to allow scheduling in all regions.
- GB300 has ibverbs RDMA hardware (10 Mellanox ConnectX devices).
  Client-to-worker transfer uses TCP fallback (client has no ibverbs),
  but worker-to-worker fan-out uses native ibverbs.
- The MAST preamble health checks (`toy_ddp`) fail with socket
  timeouts on GB300 (ephemeral port firewall issue). Remoterun
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

Remotemount uses block-level hashing for incremental updates. On
re-open, only 100 MB blocks whose hash changed are re-transferred.
Unchanged workers skip transfer entirely (metadata + remount only).

### Benchmark setup

- 2 GB300 hosts, 8 parallel TLS streams, TCP fallback (no ibverbs from client)
- Each payload contains 1000 `.py` files (~50 MB total) plus a `data.bin`
  file sized to reach the target payload
- `rdma_max_chunk_size_mb=256`
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
| 1 GB    | 22.0s     | 6.3s      | 22.1s           | 14.2s       | 20.1s       |
| 2 GB    | 22.4s     | 9.0s      | 31.7s           | 16.5s       | 23.2s       |
| 4 GB    | 25.9s     | 12.1s     | 53.7s           | 23.7s       | 30.0s       |
| 8 GB    | 40.8s     | 20.0s     | 92.4s           | 31.8s       | 44.6s       |

Key observations:

- **No change** is dominated by client-side packing + hash computation
  (no network transfer). Time scales linearly with payload size.
- **Rewrite .py** only re-transfers the ~50 MB code portion; `data.bin`
  blocks are skipped via hash match.
- **Rewrite data.bin** re-transfers the bulk of the payload since every
  block changes.
- **Delete file** triggers a full re-pack (total size changes → stale),
  similar to cold start.

To run the benchmark yourself:

```bash
python3 examples/remotemount/bench_incremental.py --sizes 1 --num_hosts 2
```
