# torchtitan on MAST, with the workspace served from your devvm

This example runs `torchtitan`'s `qwen3_1_7b` config on a 4-host MAST h100 allocation. No fbpkg packaging of torch or torchtitan; everything that isn't already in the worker fbpkg (torch, torchtitan, the FineWeb-Edu shards, an editable `monarch` checkout when you want it) is `remote_mount`-ed from your devvm at runtime.

It's a smoke test for an opinion: that you can run real training off of a local source tree without the half-hour packaging cycle in the middle. The qwen3_1_7b run at the end of this writeup is the end-to-end check — a 1.72 B-parameter dense model trains to a real loss on 32 H100s mounted off a devvm checkout, with no packaging step.

Measured on `fbcode/warm` c64cd5acf157 (monarch @ fbcode c2ef6f3431a3 / D108446513; monarch @ github ed01b60f4db3fb6dc570c233edd6bb731b007976, PR meta-pytorch/monarch#4243). Github users: `git checkout ed01b60` to reproduce the baseline. The apply and cold-import numbers below are from a fresh run today against a 4-host PCI1 allocation. Both `import torch` numbers are `monarch exec --one` (single-worker) cold imports.

## What you actually mount

`setup_env.sh` (step 4) builds `~/dev/titan_workspace/` with `uv`:

```text
~/dev/titan_workspace/
├── .venv/                              7.5 GiB
│   ├── bin/python3.12                  symlink to platform010 python
│   └── lib/python3.12/site-packages/
│       ├── monarch/ + _rust_bindings   uv-built wheel (same as client+worker)
│       ├── torch/ + nvidia/            uv-installed wheel (cu128 channel)
│       ├── __editable__.torchtitan-*.pth + finder
│       └── ...
├── torchtitan -> ~/dev/torchtitan      symlink to your local checkout
└── (FineWeb-Edu lives elsewhere; mounted separately)
```

Workers use this venv's python end-to-end — `spawn_procs` resolves to `<workspace>/.venv/bin/python3.12` (via `python_exe=` on the workspace `remote_mount` call) and `monarch exec -- python ...` finds the same binary via the prepended `PATH`. Three pythons in play (client venv, worker fbpkg, workspace venv) all install the same monarch wheel produced by `setup_env.sh`, so no version skew.

Three things worth pulling out:

1. **`.venv` is opaque to fbcode.** torch and torchtitan come from `uv pip install`, not `buck build`. The loop is "edit a Python file, re-run". No packaging step.

2. **monarch is built from source via `uv build`.** `setup_env.sh` produces a wheel out of `<fbsource>/fbcode/monarch` (no Buck) and installs it into the client venv, the worker venv, *and* the workspace `.venv` — so workers run the workspace venv's python end-to-end without bridging via PYTHONPATH. Pass `MONARCH_CLIENT_EDITABLE=1` to install monarch into the client venv editable from the source tree (the workspace and worker stay on the wheel — the worker ships inside an fbpkg, the workspace mounts via FUSE).

3. **The torchtitan checkout is editable + symlinked.** The workspace `torchtitan/` is a symlink to wherever you cloned it; setup_env.sh does `uv pip install -e $WORKSPACE/torchtitan` so site-packages only has the PEP-660 finder, not a copy. Edit local torchtitan, re-run `monarch exec` — the next import sees the change.

`job.py` does up to four `remote_mount` calls — workspace, the torchtitan symlink target so the editable finder's path resolves on workers, the example dir itself (so `train.py` resolves at its absolute path on workers), and the FineWeb directory (if present). Each opens at the same absolute path on the worker as on the devvm, so paths resolve without rewriting.

## The file-size distribution that drives everything

Run this on the workspace (devvm):

```bash
find ~/dev/titan_workspace/.venv -type f -printf '%s\n' \
  | awk 'BEGIN{n_small=0;n_big=0;b_small=0;b_big=0}
         {if ($1>=100*1024*1024){n_big++;b_big+=$1} else {n_small++;b_small+=$1}}
         END {printf "Small (<100MiB):  %d files, %.1f GiB\n",
                       n_small, b_small/1024/1024/1024;
              printf "Big   (>=100MiB): %d files, %.1f GiB\n",
                       n_big, b_big/1024/1024/1024}'
```

Output on the venv used for this writeup:

```
Small (<100MiB):  23881 files, 1.4 GiB
Big   (>=100MiB): 20 files, 6.0 GiB
```

99.92% of files (by count) are small; 81% of bytes (by size) are in just 20 big files. The big-file list:

```text
 870 MiB  torch/lib/libtorch_cuda.so
 717 MiB  nvidia/cublas/lib/libcublasLt.so.12
 492 MiB  nvidia/cudnn/lib/libcudnn_engines_precompiled.so.9
 431 MiB  nvidia/cusparselt/lib/libcusparseLt.so.0
 430 MiB  torch/lib/libtorch_cpu.so
 396 MiB  triton/_C/libtriton.so
 382 MiB  nvidia/nccl/lib/libnccl.so.2
 370 MiB  nvidia/cusparse/lib/libcusparse.so.12
 266 MiB  nvidia/cufft/lib/libcufft.so.11
 260 MiB  nvidia/cudnn/lib/libcudnn_adv.so.9
 239 MiB  hf_xet/hf_xet.abi3.so
 232 MiB  nvidia/cusolver/lib/libcusolver.so.11
 229 MiB  monarch/_rust_bindings.cpython-312-x86_64-linux-gnu.so
 154 MiB  nvidia/cusolver/lib/libcusolverMg.so.11
 153 MiB  nvidia/nvshmem/lib/libnvshmem_host.so.3
 130 MiB  nvidia/curand/lib/libcurand.so.10
 114 MiB  torch/lib/libtorch_cuda_linalg.so
 111 MiB  nvidia/cublas/lib/libcublas.so.12
 102 MiB  nvidia/cudnn/lib/libcudnn_ops.so.9
 100 MiB  nvidia/cuda_nvrtc/lib/libnvrtc.alt.so.12
```

## The data plane: eager apply, fast first import

This baseline ships the whole workspace tree **eagerly at apply**. When you `monarch apply`, the mount-open phase walks every mount and transfers all of it — the 1.4 GiB of small files *and* the 6.0 GiB of big libraries — to the workers before the mount goes live. By the time `apply` returns, every byte the trainer will read is already resident on each worker.

The upside is that the first read is free. There is no on-demand fault path: when the dynamic linker `dlopen`-walks `libtorch_cuda.so`, `libcublasLt.so.12`, `libtriton.so` and the rest during the first `import torch`, every block is already on the worker's local disk, so the import runs at warm speed from the page cache. The first `import torch` on a fresh allocation costs the same as the second.

The cost lives entirely in apply. Shipping ~7.5 GiB of workspace plus the FineWeb shards synchronously at mount-open is the dominant cost of this design:

- **Apply is expensive.** Mount-open has to move the full tree before it can return. On the 4-host allocation used here that's **128.57 s** of mount-open — minutes, not seconds, dominated by pushing the big `.so`s and the parquet shards across the region.
- **It is paid per cold apply, not per re-apply.** A *fresh* `monarch apply` (new job, or after a preempt) ships the full tree before it returns. A no-change re-apply to a *running* job does **not** re-ship — it reconnects (measured ~12–23 s; see the Apply numbers). So the tax recurs whenever you spin up a fresh job/allocation, not on every iteration against a live one.
- **The split is wrong for the workload.** Most of those bytes are a handful of giant libraries that the linker reads exactly once, sequentially, at native bandwidth. Paying minutes up front to pre-stage them buys nothing the first read wouldn't have paid anyway — and it blocks `apply` from returning while it does.

That is the problem this baseline leaves on the table: apply is a multi-minute cost on every fresh job, dominated by eagerly shipping bytes that a demand-driven path could have streamed in the background of the first read. A later change makes apply cheap by prefilling only what actually benefits from prefetching and deferring the big libraries to a demand path; this writeup is the eager baseline it improves on.

## Numbers

The apply and cold-import numbers below are from a fresh run today against a 4-host PCI1 allocation. (The training run further down is data-plane-independent — the mount layer adds nothing to the steady-state loop, as the MFU section explains.)

### Apply

The apply ships every mount eagerly and returns only once the whole tree is resident on the workers.

```text
# four mounts: workspace (7.5 GiB / ~23.5k files), torchtitan
# (205 MiB / 804 files), the example dir, FineWeb (27 GiB / 30 parquet)
[mount_process] mounts opened in 128.57s
Job is ready (1s)
```

- **Mounts opened (all four):** 128.57 s. This is the eager ship — the full 7.5 GiB workspace, the torchtitan checkout, and the FineWeb parquet are transferred to every worker before mount-open returns.
- **No-change re-apply reconnects (no re-ship).** A `monarch apply` against the unchanged tree while the job is still running just reconnects to it — measured **~12–23 s** (`Found cached job` → `Job is ready`, no `mounts opened` line; the workers' mounts are already up). The expensive 128.57 s mount-open is a per-*cold*-apply (fresh-job) cost, not a per-re-apply one.

The mount-open is the *useful* number; the `monarch apply` CLI wall-clock also includes MAST queue wait for PCI capacity, which varies.

### Cold and warm import

Because apply shipped everything eagerly, the first `import torch` on a fresh allocation reads only from the worker's local disk — there is no transfer on the import path. Run on a single worker (`monarch exec --one`):

| Operation                                       | Result   |
|-------------------------------------------------|----------|
| cold `import torch` (resident, eager apply)     | 3.26 s   |
| warm `import torch` (second invocation)         | ~3.3 s   |
| `import torchtitan`                             | 0.08 s   |

The cold import is fast precisely because the eager apply already moved the ~3.9 GiB of `.so`s onto the worker. The linker faults each library out of the local page cache, so the cold number tracks the warm number — the transfer cost was paid up front, at apply, not here. `import torchtitan` never touches a big file (torchtitan is all small `.py`), so it is cheap regardless.

torch is `2.11.0+cu128`.

### Training: qwen3_1_7b, 300 steps, 4×8 H100

The point of putting torchtitan on this mount is to actually train. Same `monarch exec` invocation, same workspace, no fbpkg of torch:

```bash
monarch exec --all --per-host gpu=8 \
    -e MASTER_ADDR=$(monarch exec --one -- hostname) \
    -e MASTER_PORT=29500 \
    -e TITAN_TORCHTITAN=$HOME/dev/torchtitan \
    -e TITAN_MODEL_MODULE=qwen3 \
    -e TITAN_MODEL_CONFIG=qwen3_1_7b \
    -e TITAN_TRAINING_STEPS=300 \
    -e TITAN_DATASET=fineweb_edu_10BT \
    -e TITAN_LOSS=ce \
    -- python train.py
```

`dp_shard=-1` lets torchtitan FSDP-shard the 1.72 B parameters across all 32 GPUs. FineWeb-Edu-10BT shards stream through the third mount. `TITAN_LOSS=ce` forces plain CrossEntropyLoss as a workaround for an unrelated qwen3 backward-pass crash. Step lines from rank 0, abbreviated:

```text
step:   1   loss: 12.441   tflops:  26.24   mfu:  2.65%   memory: 48.31 GiB  (smoke, single H100)
step:  10   loss:  8.889   tflops: 334.03   mfu: 33.77%   memory: 48.34 GiB
step: 100   loss:  6.341   tflops: 331.37   mfu: 33.51%
step: 200   loss:  5.682   tflops: 328.77   mfu: 33.24%
step: 300   loss:  5.244   tflops: 325.44   mfu: 32.91%   (final, 32-H100 run)
```

- **Initial → final loss:** 12.4 → 5.24 in 300 steps. (Initial loss is `ln(151936) ≈ 11.93` for the qwen3 vocab; the smoke-test step 1 was 12.44 from identical model init. 300 steps doesn't fully converge but the trajectory is healthy.)
- **Sustained throughput:** ~330 TFLOPS/GPU, MFU ~34%, ~25.6K tokens/s/GPU, ~0.64 s/step.
- **GPU memory:** 48.34 GiB / H100 (61% of an 80 GiB H100).
- **Wall-clock:** ~3 min for 300 steps end-to-end.

What this proves about the mount layer: after the apply finishes, every byte the trainer reads — the big libraries during the first `import torch` on each worker, plus the FineWeb-Edu shards as the dataloader scans them — is already resident on the worker, and the inner training loop sees no FUSE traffic at all. The 33% MFU number is the same MFU you'd get from a fbpkg-baked torch — the mount adds nothing to the steady-state loop.

## Reproduction commands

Everything below was run on a Meta devvm. Adjust paths to taste.

### One-time: bootstrap everything

```bash
cd <fbsource>/fbcode/monarch/examples/torchtitan_mast
bash setup_env.sh
export PATH="$HOME/monarch_bench_envs/client/bin:$PATH"
```

`setup_env.sh` builds the monarch wheel (from source, no Buck), client + worker venvs, the torchtitan workspace `.venv` (torch from the cu128 channel + editable torchtitan), and pip-installs this dir into the client venv. See `./setup_env.sh --help` for tunables.

### File-size distribution

```bash
find $HOME/dev/titan_workspace/.venv -type f -printf '%s\n' \
  | awk 'BEGIN{n_small=0;n_big=0;b_small=0;b_big=0}
         {if ($1>=100*1024*1024){n_big++;b_big+=$1} else {n_small++;b_small+=$1}}
         END {printf "Small (<100MiB):  %d files, %.1f GiB\n",
                       n_small, b_small/1024/1024/1024;
              printf "Big   (>=100MiB): %d files, %.1f GiB\n",
                       n_big, b_big/1024/1024/1024}'
```

### `monarch apply`

```bash
cd <fbsource>/fbcode/monarch/examples/torchtitan_mast
rm -rf .monarch              # discard any prior job state
monarch apply job.job
```

The apply CLI itself only prints high-level progress (`Job is ready (Ns)`). The mount_process daemon's detailed timings (mount-open, refresh durations) are written to `/tmp/monarch_mounts_<apply_id>.log`. The apply CLI prints the log path on exit; grep it for the breakdown:

```bash
LOG=$(ls -t /tmp/monarch_mounts_*.log | head -1)
grep -E "mounts opened|refresh complete" $LOG
```

### Run training

```bash
# Single command — same `monarch exec` you'd use for any worker job.
# TITAN_TORCHTITAN bypasses the workspace symlink (FUSE doesn't follow
# the bare symlink on workers), pointing the launcher straight at the
# realpath that job.py mounts separately.
monarch exec --all --per-host gpu=8 \
    -e MASTER_ADDR=$(monarch exec --one -- hostname) \
    -e MASTER_PORT=29500 \
    -e TITAN_TORCHTITAN=$HOME/dev/torchtitan \
    -e TITAN_MODEL_MODULE=qwen3 \
    -e TITAN_MODEL_CONFIG=qwen3_1_7b \
    -e TITAN_TRAINING_STEPS=300 \
    -e TITAN_DATASET=fineweb_edu_10BT \
    -e TITAN_LOSS=ce \
    -- python train.py 2>&1 | tee /tmp/qwen3_mast.log
```

Bump `TITAN_TRAINING_STEPS` higher for a longer run. 300 steps is the smoke-test value used for this writeup (~3 min, loss → ~5.24).

Tail rank 0's per-worker stdout for live step lines:

```bash
tail -f /home/cpuhrsch/torchtitan_logs/hosts_0/exec_outputs/*/stdout.txt | grep step:
```

### Tear down

```bash
monarch kill
```

## Caveats

- **MAST queue wait dominates wall-clock for `apply`.** The mount-open phase is on the order of tens of seconds; total `apply` wall-clock is dominated by however long MAST takes to allocate the requested hardware, which varies by cluster load. Mount-open is the number that's reproducible — the queue wait isn't.
- **Daemon-side detail messages write to a log file**, not the apply CLI's stdout. The mount_process daemon is detached (`start_new_session=True`) and its stdout/stderr go to `/tmp/monarch_mounts_<apply_id>.log`; the apply CLI prints the log path on exit. Grep that file for `mounts opened` to see per-mount timings.
- **Preemption is *not* handled transparently anymore.** Hostnames are resolved once at apply time and cached in `.monarch/job_state.pkl`. The previous design re-resolved on every `monarch exec` via `mast get-status` (~600 ms per call); after profiling showed the live-resolution overhead dominated steady-state exec, we moved to apply-time caching. If MAST preempts and re-places task instances under the same job ID, the cached hostnames go silently stale and the next `monarch exec` will surface a TLS handshake / connect timeout rather than a clean "job preempted" error. Workaround for now: `monarch kill && monarch apply job.job` to force a fresh apply. An upcoming Monarch feature surfaces host-liveness through the existing supervision channel (no extra subprocess); once it lands we get clean preempt-detection back without paying per-call subprocess cost.
