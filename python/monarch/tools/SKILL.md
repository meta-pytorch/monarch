---
name: monarch
description:
  'Use this skill when the user wants to run code on remote GPU hosts,
  launch distributed PyTorch training jobs, or manage Monarch worker
  clusters (K8s, SLURM, MAST, SSH).'
apply_to_user_prompt: '(monarch|remote\s?run|run.*remote.*gpu|launch.*worker|distributed.*train|gpu.*cluster|monarch\s+exec|monarch\s+serve)'
apply_to_regex: '.*'
---

# monarch — Run Scripts on Remote GPU Workers

The `monarch` CLI manages GPU worker clusters and executes commands on them.
Two main commands replace the standalone `remoterun` script:

- **`monarch serve <module>`** — Provision workers from a Python module's `serve()` function
- **`monarch exec <cmd>`** — Mount local files and run commands on workers

## Quick Start

```bash
# 1. Provision workers (module must have a serve() function returning a JobTrait)
monarch serve jobs.kubernetes

# 2. Run a command on rank 0 (output streams in real-time)
monarch exec python train.py

# 3. Run on specific ranks (output prefixed with [rank N])
monarch exec --ranks 0,3 python train.py

# 4. Run once per host (e.g. nvidia-smi, environment checks)
monarch exec --per-host nvidia-smi -L

# 5. Run on specific hosts
monarch exec --hosts 0,2 hostname

# 6. Run on ALL ranks, write per-rank logs
monarch exec --all python train.py

# 7. Run a bash script file
monarch exec --script run.sh

# 8. Kill job after command finishes
monarch exec --kill python train.py
```

## Command Reference

### `monarch serve <module_path>`

Imports a Python module, calls its `serve()` function (which must return a
`JobTrait`), applies the job, and caches it to `.monarch/job_state.pkl`.

```
monarch serve <module_path> [-j JOB_PATH]
```

| Arg | Description |
|-----|-------------|
| `module_path` | Dotted Python module path (e.g. `jobs.kubernetes`) |
| `-j, --job` | Path to cache the job pickle (default: `.monarch/job_state.pkl`) |

### `monarch exec [options] <cmd...>`

Loads a cached job, mounts a local directory on workers via remotemount,
and executes a command.

```
monarch exec [options] [--] <command> [args...]
monarch exec --script <file> [options]
```

**Rank targeting** (mutually exclusive):

| Flag | Description | Output |
|------|-------------|--------|
| *(default)* | Run on rank 0 only | Streaming stdout/stderr |
| `--ranks 0,3` | Run on specific flat ranks | Streaming with `[rank N]` prefix |
| `--per-host` | Run once per host (rank 0 on each) | Streaming with `[rank N]` prefix |
| `--hosts 0,2` | Run on specific hosts (rank 0 on each) | Streaming with `[rank N]` prefix |
| `-a, --all` | Run on all ranks | Per-rank logs in `.monarch/logs/<job>/` |

Non-targeted ranks do NOT execute the script (no wasted compute).

**Other flags:**

| Flag | Description | Default |
|------|-------------|---------|
| `-e, --env KEY=VALUE` | Extra environment variables (repeatable) | — |
| `-v, --verbose` | Verbose remotemount logging | off |
| `--source-dir DIR` | Directory to mount on workers | CWD |
| `--mount-point PATH` | Mount point on workers | same as `--source-dir` |
| `--kill` | Kill the job after command finishes | false |
| `--script FILE` | Read a bash script from file (`-` for stdin) instead of cmd args | — |
| `--refresh-mount` | Force-unmount stale FUSE mounts before remounting | false |
| `-j, --job PATH` | Path to cached job pickle | `.monarch/job_state.pkl` |

### Other Commands

| Command | Description |
|---------|-------------|
| `monarch use <name>` | Switch active job (see `monarch serve --name`) |

## Architecture

1. `monarch serve` calls a user module's `serve()` → returns a `JobTrait` → pickled to disk
2. `monarch exec` loads the pickle → connects to workers → mounts source dir via FUSE → runs command via BashActor
3. Workers see the mounted directory as read-only at the mount point
4. Incremental transfers: only changed blocks are re-sent on subsequent runs

## Job Types

| JobTrait subclass | Backend | Notes |
|-------------------|---------|-------|
| `LocalJob` | Local subprocess | For development/testing |
| `SSHJob` | SSH to hosts | Direct host access |
| `SlurmJob` | SLURM scheduler | HPC clusters |
| `MASTJob` | MAST scheduler | Meta internal |
| Custom K8s job | Kubernetes | Via `monarch serve jobs.kubernetes` |

## Usage Patterns

### Iterative development

```bash
# First time: provision workers
monarch serve jobs.kubernetes

# Edit code locally, then run
monarch exec python train.py

# Repeat — only changed files transfer (~3-9s)
monarch exec python train.py

# Done — tear down
monarch exec --kill echo done
```

### Distributed training with custom env

```bash
monarch exec --all \
    -e NCCL_DEBUG=INFO \
    --source-dir /tmp/myenv \
    python train.py
```

### Run a multi-line bash script

```bash
monarch exec --script - <<'EOF'
#!/bin/bash
echo "Host: $(hostname)"
nvidia-smi -L
python -c "import torch; print(torch.cuda.device_count(), 'GPUs')"
EOF
```

## Job Reuse (Important)

**Job allocation is slow** (~2-5 min for GB200). Avoid re-running `monarch serve`
if a job is already running — it creates a NEW allocation.

- `monarch exec` automatically reuses the active job
- Use `monarch use <name>` to switch between named jobs
- Only run `monarch serve` when you need a different host type or count
- Use `--kill` ONLY when you're done with the hosts entirely
- If the mount is stale from a crashed exec, use `--refresh-mount` to recover
  instead of re-provisioning

## Known Limitations

- Mounted directory is **read-only**. Write output to a separate path.
- Default exec (rank 0) streams stdout/stderr in real-time. `--all` writes
  per-rank logs to `.monarch/logs/<job_name>/exec-HHMMSS/`.
- **Mount point must be writable by the worker process.** MAST workers run as
  `nobody` and cannot create directories under `/home/`. Pass
  `--mount-point /tmp/myproject` to use a writable location.
- If a previous exec left processes holding the mount, the next exec fails
  with "path is busy". Use `--refresh-mount` to force-unmount and retry.

## MAST Host Types

| `host_type` | GPU | GPUs per host | Architecture |
|-------------|-----|---------------|--------------|
| `gb200`     | GB200 (189 GB HBM3e) | **2** | aarch64 |
| `gb300`     | GB300 (278 GB HBM3e) | **4** | aarch64 |
| `grandteton` / `gtt_any` | H100 | **8** | x86_64 |

**Important:** GB200 hosts have only 2 GPUs, not 8. GB200/GB300 workers lack
the CUDA toolkit (nvcc) — pre-compile CUDA extensions or cross-compile from
the devserver.
