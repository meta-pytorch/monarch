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
bash examples/remotemount/remoterun.sh /tmp/myenv my_script.sh
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

## Piping Scripts via stdin

You can pipe a script directly into `remoterun` without creating a file:

```bash
CONDA_PREFIX=~/monarch_conda_envs/worker/conda \
~/monarch_conda_envs/client/conda/bin/python3.12 \
  examples/remotemount/remoterun.py /tmp/myenv stdin \
  --backend mast --host_type gb200 <<'EOF'
#!/bin/bash
echo "Hello from $(hostname)"
nvidia-smi -L
python -c "import torch; print(f'torch {torch.__version__}, CUDA {torch.cuda.device_count()} GPUs')"
EOF
```

The keyword `stdin` tells remoterun to read the script from standard input
instead of a file. This is useful for quick one-off commands.

---

## Setting Up a venv with uv

For custom Python environments (e.g. additional pip packages), use `uv` to
create a venv, then pass it as the source directory to remoterun:

```bash
# Create a venv with uv and install packages.
uv venv /tmp/myenv --python 3.12
uv pip install --python /tmp/myenv/bin/python transformers datasets

# Write a script that uses the venv.
cat > /tmp/run.sh <<'EOF'
#!/bin/bash
export PYTHONPATH="/tmp/myenv/lib/python3.12/site-packages:$PYTHONPATH"
python -c "
from transformers import AutoTokenizer
tok = AutoTokenizer.from_pretrained('gpt2')
print(f'Vocab size: {tok.vocab_size}')
"
EOF

# Run on MAST — remoterun transfers /tmp/myenv to workers and mounts it.
CONDA_PREFIX=~/monarch_conda_envs/worker/conda \
~/monarch_conda_envs/client/conda/bin/python3.12 \
  examples/remotemount/remoterun.py /tmp/myenv /tmp/run.sh \
  --backend mast --host_type gb200
```

The venv directory is packed and transferred to workers just like any other
source directory. The FUSE mount makes it available at the mount point, and
you set `PYTHONPATH` to point at the site-packages inside it.

---

## Incremental Update Performance

Remotemount uses block-level hashing (64 MB blocks) for incremental
updates. On re-open, only blocks whose hash changed are re-transferred.
Unchanged workers skip transfer entirely (metadata + remount only).

### Benchmark setup

- 2 GB200 hosts, 8 parallel TLS streams, TCP fallback (no ibverbs from client)
- Each payload contains 1000 small `.py` files (~330 KB total) plus a
  `data.bin` file sized to reach the target payload
- Warm-up step pre-spawns actors so cold start measures transfer time only

### Scenarios

| Scenario | Description |
|----------|-------------|
| Cold start | First transfer to fresh workers |
| No change | Re-open with identical content — hash match, skip transfer |
| Rewrite data.bin | Replace `data.bin` with new random content (same size) |
| Rewrite .py | Rewrite all 1000 `.py` files — only the 64 MB block(s) containing them are re-transferred |
| Delete file | Remove one `.py` file — partial transfer (only affected blocks) |

### Results — 2 hosts (8 streams)

| Payload | Cold start | No change | Rewrite data.bin | Rewrite .py | Delete file |
|---------|-----------|-----------|-----------------|-------------|-------------|
| 1 GB    | 9.6s      | 7.7s      | 10.6s           | 8.4s        | 8.1s        |
| 2 GB    | 12.4s     | 8.3s      | 19.6s           | 9.9s        | 8.1s        |
| 4 GB    | 13.7s     | 8.9s      | 13.7s           | 9.5s        | 8.6s        |
| 8 GB    | 20.1s     | 9.3s      | 20.3s           | 10.9s       | 11.2s       |

### Results — 64 hosts (8 streams, chunked tree fan-out, 4 leaders)

| Payload | Cold start | No change | Rewrite data.bin | Rewrite .py | Delete file |
|---------|-----------|-----------|-----------------|-------------|-------------|
| 8 GB    | 46.2s     | 9.2s      | 45.6s           | 13.8s       | 13.2s       |

Key observations:

- **No change** is dominated by client-side packing + hash computation
  (no network transfer). Time scales sub-linearly with payload size.
- **Rewrite .py** only re-transfers the 64 MB block(s) containing the
  ~330 KB of `.py` files; `data.bin` blocks are skipped via hash match.
  Append-only packing keeps unchanged files at their original offsets,
  minimizing dirty blocks.
- **Rewrite data.bin** re-transfers the bulk of the payload since every
  block changes.
- **Delete file** uses partial transfer — only the block containing the
  deleted file is re-transferred, not the entire payload.

To run the benchmark yourself:

```bash
bash examples/remotemount/setup_conda_env.sh gb200
CONDA_PREFIX=~/monarch_conda_envs/worker/conda \
~/monarch_conda_envs/client/conda/bin/python3.12 \
  examples/remotemount/bench_incremental.py --sizes 1 --hosts 2
```
