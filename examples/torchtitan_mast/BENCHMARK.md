# torchtitan on MAST, with the workspace served from your devvm

This example runs `torchtitan`'s `qwen3_1.7b` config on a 4-host MAST h100 allocation. No fbpkg packaging of torch or torchtitan; everything that isn't already in the worker fbpkg (torch, torchtitan, the FineWeb-Edu shards, an editable `monarch` checkout when you want it) is `remote_mount`-ed from your devvm at runtime.

It's a smoke test for an opinion: that you can run real training off of a local source tree without the half-hour packaging cycle in the middle, *and* without paying GB-class transfer latency on every `apply`. The qwen3_1.7b run at the end of this writeup is the end-to-end check — a 1.72 B-parameter dense model trains to a real loss on 32 H100s mounted off a devvm checkout, with no packaging step.

The numbers below are from a fresh `benchmark_table.sh` run (2026-06-30) on a 4×8 H100 MAST allocation, measuring this diff's **simple per-worker transport**: the client materialises each faulted block once and broadcasts it to every worker, deduplicated by a permanent delivered-block set so each block ships exactly once for the life of the mount. There is no leader gateway and no RDMA fan-out — the broadcast still crosses regions once per worker, which a follow-up RDMA optimization (a leader gateway plus intra-region RDMA fan-out) collapses to a single cross-region transfer. The `import torch` numbers are the 4-worker (`--all`) case — every worker faulting the same libraries at once.

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
Small (<100MiB):  22501 files, 1.3 GiB
Big   (>=100MiB): 19 files, 5.8 GiB
```

99.9% of files (by count) are small; ~82% of bytes (by size) are in just 19 big files. The big-file list (representative):

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

## Why this is annoying without a warm cache

If `monarch apply` shipped every byte synchronously, you'd wait minutes on every apply. So apply doesn't do that — it transfers a "warm" subset eagerly and streams the rest in a background thread that races your first read. The interesting question is: **what should be warm?**

The naive answers don't work:

- *Warm everything:* `apply` takes minutes per re-apply.
- *Warm nothing:* `apply` is fast, but the first `import torch` opens hundreds of small files (`__init__.py`, `_utils.py`, `version.py`, …) and each one stalls in the FUSE handler waiting for its chunk to arrive. Latency, not bandwidth, kills you here — each small file costs a daemon round-trip and reads ~200 bytes after it. A few hundred of those add up to more than the bulk transfer would have cost.
- *Warm by Python import graph:* good for `import torch`, surprises you constantly afterwards (torchtitan opens a YAML, `torch.compile` wants `triton/__init__.py`, the trainer hits `torch.distributed` primitives, …).

The pattern under all three: **small files are deadly to read cold; big files are not**. A `libtorch_cuda.so` read pays its latency once for the whole multi-hundred-MiB stream — the daemon round-trip is amortized over the whole bulk transfer. A `_utils.py` read pays the same round-trip but reads 200 bytes after it.

## The heuristic

Flip it: **prefill only the code; pull every library on demand.**

At `open()` the client packs the tree small-files-first (grouped by directory for read-locality) and prefills exactly the **code blocks**: `code_blocks(layout)` is every 64 MiB block backing at least one file under `BIG_FILE_THRESHOLD` (1 MiB) — the `.py`, the configs, and the thousands of tiny package files an `import` walks (one such block packs ~10 000 small files). Blocks that hold *only* big-file bytes — `libtorch_cuda.so`, the CUDA `.so`s, the parquet shards — are left out and pulled the instant a worker reads them.

This is more aggressive than the old "warm everything < 100 MiB" skip-set: it prefills only sub-1-MiB files and lets the on-demand path carry everything bigger, because that path is now fast enough (next section) that eagerly shipping a medium `.so` at apply buys nothing. The prefill is one in-order background queue the poll loop pumps; it overlaps the mount coming live, so `open()` returns without waiting on it.

## The data plane: simple per-worker transport

When a worker reads a not-yet-resident block, the Rust mount's FUSE handler faults it: it parks the read and fires a callback (briefly under the GIL, off the state lock) onto the client `MountHandler` actor's `enqueue` endpoint. `enqueue` materialises that one 64 MiB block from the source on the devvm and broadcasts it to every worker's `receive_block`, which copies the bytes into its slot in the mount's **positional block table** — a `Vec<Option<Bytes>>` with one slot per block, `None` until delivered — and wakes the parked read. The read then copies its requested slice straight out of the delivered block. There is **no leader, no RDMA fan-out, and no on-disk cache**: the client serves every worker directly via the broadcast. Delivery is deduplicated by a permanent set — once the client has broadcast a block it records it and never re-materialises or re-broadcasts it, so each block ships exactly once for the life of the mount even when all four workers fault it. What this base plane does *not* dedup is the cross-region hop: the broadcast still sends each block to all four workers across regions, so the cross-region byte count is 4x the unique block bytes. A follow-up RDMA optimization (a leader gateway plus an intra-region RDMA fan-out) collapses that to one cross-region transfer per block, and the gap shows up in the cold-import number below.

Delivery is one block per fault. A FUSE read spans at most two 64 MiB blocks, so a read faults a single block (the rare straddling read faults its second block on its next pass), and `enqueue`/`receive_block` each carry exactly one block — the signal/enqueue/deliver path stays loop-free. The `MountHandler` endpoints are synchronous, so the actor processes one delivery at a time and a cross-worker import storm for one block collapses against the permanent delivered set: the first fault broadcasts the block to every worker and records it, the rest see it delivered and skip. (An earlier version cleared this set once each delivery completed, so a re-fault after delivery re-materialised and re-broadcast the block — under an import storm that redelivered the hot library blocks several times over, which was the bulk of the cold import. The permanent set removes that.)

**Per-file freshness.** A 64 MiB block can back several files, so as the client materialises a block it re-stats every file the block touches against the recorded `(size, mtime_ns)`. A file whose source diverged under the fence — changed, vanished, or a short read — can't be reproduced, so `materialise_block` garbage-fills just that file's byte range and reports its vpath. The stale set rides with the block in the **same `receive_block` call** (the bytes and which-files-are-stale arrive atomically, with no separate signal to order); the mount marks each stale file so its reads return `EIO`, while co-located fresh files sharing the same block still serve. The mark is sticky until a `refresh` rebuilds the metadata — freshness itself is never stored, so a file that later goes stale is caught by the next delivery rather than trusted from a past one.

## Numbers

The numbers below are from a fresh `benchmark_table.sh` run (2026-06-30) on a 4×8 H100 MAST allocation, on this diff's simple per-worker transport. (Training is data-plane-independent once the data is resident — the mount adds nothing to the steady-state loop, as the MFU section explains.)

**Before → after.** Against the eager (warm) baseline this example replaces:

| Metric                                | Warm baseline (eager) | This diff (simple per-worker) | Win   |
|---------------------------------------|-----------------------|-------------------------------|-------|
| `apply` mount-open                    | ~117 s                | 14.7 s                        | ~8x   |
| first use (apply + first `import torch`) | ~137 s             | ~40 s                         | ~3.4x |
| no-change re-apply (running job)      | ~12–23 s (reconnect)  | 3.3 s (~0.5–1 s refresh)      | ~4–7x |

The warm baseline ships the whole tree eagerly at apply, so mount-open dominates each cold apply and the first import is cheap because everything is already resident. This diff inverts that: apply is cheap because it prefills only the code blocks and defers the big libraries, and the first import pays the deferred transfer once. Because the per-block delivery is deduplicated — each block ships exactly once even when all four workers fault it — the cold import is ~25 s, so the first-use win is large (apply + import ~40 s vs ~137 s eager). A follow-up RDMA optimization (a leader-gateway dedup + an intra-region RDMA fan-out) cuts it further by collapsing the cross-region broadcast. The no-change re-apply gap is the `monarch exec` connect/proc-spawn floor, not the data plane.

### Apply

The apply prefills the code blocks across all four mounts and returns; the big libraries stay on the devvm until read.

```text
# four mounts: workspace (~7 GiB / ~22.5k files), torchtitan
# (205 MiB / 804 files), the example dir, FineWeb (27 GiB / 30 parquet)
[job_sidecar] mounts opened in 14.66s
Job is ready (4s)
```

- **Mounts opened (all four):** 14.7 s — vs ~117 s under the eager warm baseline (~8x). The win is doing less at apply: this path prefills only the code blocks (a few hundred MiB of packed small files, plus the monarch bindings the worker bootstrap itself faults), not the full ~7 GiB the eager baseline ships at mount-open.
- **Held back for on-demand:** the ~3.9 GiB of torch + CUDA `.so`s and the full 27 GiB of FineWeb parquet — never sent unless a worker reads them.
- **No-change refresh** (re-apply or re-exec against an unchanged tree) short-circuits in the sidecar after a stat-only directory walk: **mounts ready in ~0.5–1 s** for all four mounts (re-apply end-to-end 3.3 s). The warm baseline does not re-ship on a no-change re-apply either — it reconnects to the running job in ~12–23 s.

The mount-open is the *useful* number; the `monarch apply` CLI wall-clock also includes MAST queue wait, which varies (~2 min on this run).

### Cold and warm import

The data plane exists for the first `import torch` on a fresh allocation — nothing resident, the dynamic linker `dlopen`-walking `libtorch_cuda.so`, `libcublasLt.so.12`, `libtriton.so` and the rest. With all four workers importing at once (`monarch exec --all`):

| Operation                                       | Result   |
|-------------------------------------------------|----------|
| cold `import torch` (4 workers, on-demand)      | ~28 s wall (per-worker ~25 s) |
| warm `import torch` (second invocation)         | 7.4 s (per-worker ~3.3 s) |
| `import torchtitan`                             | warm-speed (all small `.py`) |

The cold import moves **~3.9 GiB** of `.so`s into *each* worker. With the permanent delivered-block set each block is materialised once on the devvm and broadcast to all four workers once — no redelivery — so the four workers' imports share the materialise work and a 4-worker import costs about what a single worker's does (~25 s) rather than ~4x it. It is part import-paced (the linker faults libraries roughly one at a time, and the synchronous `MountHandler` delivers one block at a time) and part transfer-bound (each broadcast still sends the block to all four workers across regions). The remaining cost is that cross-region duplication — each unique block crosses the region boundary four times — which a follow-up RDMA optimization (a leader-gateway dedup + an intra-region RDMA fan-out) removes by sending each block across regions once and fanning it out intra-region. `import torchtitan` never touches a big file (torchtitan is all small `.py`), so it runs at warm speed even cold. Warm reads come from the worker's resident in-memory blocks. torch comes from the cu128 channel; delivery is byte-exact — `libtorch_cuda.so` imports, loads, and runs the training below.

### Refresh

| Refresh kind                                       | wall      |
|----------------------------------------------------|-----------|
| `monarch apply`, no changes (re-apply)             | **3.3 s** (mount refresh ~0.5–1 s) |
| code-edit probe — exec a new small file            | **4.85 s** |
| code-edit probe — same file grown (+500 lines)     | **4.88 s** |

The no-change re-apply comes back as `Job is ready (1s)` — the sidecar's incremental refresh walks file stats without I/O and short-circuits when every file matches by `(size, mtime_ns)`. The code-edit probe execs a brand-new small file, then a grown edit of it; each wall includes the mount refresh that picks the change up. A code edit re-delivers as a **one-block re-pull** — the block-aligned append means a small edit never spikes into a big re-transfer — so both stay ~4.9 s, and the grown file is served correctly (`edit_probe v2 ... 4999950000`). Neither path re-touches any big-file bytes.

The steady-state `monarch exec` wall on an applied job is ~1 s; that floor is the connect plus a fresh per-call worker-proc spawn (fork + monarch bootstrap + actor-mesh registration), not the mount — `state: ensure_open` walks the four mounts in well under a second. (Trimming the exec floor further is a separate change higher in the stack.)

### Training: qwen3_1.7B, 300 steps, 4×8 H100

The point of putting torchtitan on this mount is to actually train. Same `monarch exec` invocation, same workspace, no fbpkg of torch:

```bash
monarch exec --all --per-host gpu=8 \
    -e MASTER_ADDR=$(monarch exec --one -- hostname) \
    -e MASTER_PORT=29500 \
    -e TITAN_TORCHTITAN=$HOME/dev/torchtitan \
    -e TITAN_MODEL_MODULE=qwen3 \
    -e TITAN_MODEL_CONFIG=qwen3_1.7b \
    -e TITAN_TRAINING_STEPS=300 \
    -e TITAN_DATASET=fineweb_edu_10BT \
    -- python train.py
```

`dp_shard=-1` lets torchtitan FSDP-shard the 1.72 B parameters across all 32 GPUs. FineWeb-Edu-10BT shards stream through the third mount. Step lines from rank 0, abbreviated:

```text
step:   1   loss: 12.344   tps:  4,229   tflops:  47.68   mfu:  4.82%   memory: 49.08 GiB
step:  10   loss:  8.444   tps: 27,299   tflops: 307.80   mfu: 31.12%   <- FineWeb resident
step:  50   loss:  6.893   tps: 26,783   tflops: 301.98   mfu: 30.53%
step: 100   loss:  6.301   tps: 26,530   tflops: 299.13   mfu: 30.25%
step: 200   loss:  5.594   tps: 26,406   tflops: 297.73   mfu: 30.10%
step: 300   loss:  5.154   tps: 26,099   tflops: 294.27   mfu: 29.75%   (final)
```

- **Initial → final loss:** 12.34 → 5.15 in 300 steps. (Initial loss is `ln(151936) ≈ 11.93` for the qwen3 vocab; 300 steps doesn't fully converge but the trajectory is healthy.)
- **Two regimes, and the mount is visible in the first.** Step 1 runs at **~5% MFU** — the dataloader is **mount-bound**, waiting on the first FineWeb-Edu shards to stream in. By **~step 10** the shards are resident and throughput jumps to steady state: **~30% MFU, ~26.5K tokens/s/GPU, ~0.6 s/step, ~300 TFLOPS/GPU**. With the deduplicated per-block delivery that ramp is short — each shard block ships once and the four workers share it — and once data is resident the inner loop sees no FUSE traffic and the mount adds nothing; the steady ~30% MFU is what you'd get from an fbpkg-baked torch.
- **GPU memory:** 49.1 GiB / H100 (62% of an 80 GiB H100).

This is the end-to-end check: a 1.72 B-parameter model FSDP-trained on 32 H100s reading torch, torchtitan, and the FineWeb shards byte-perfect through the on-demand mount, no packaging step — the brief initial ramp is the FineWeb shards streaming in, and a follow-up RDMA optimization trims the remaining cross-region cost.

## The main idea, restated

There are two latency regimes on the cold read path:

1. **Many small files.** Cost dominated by per-file round-trips. Wall-clock floor is `N × RTT`. The warm cache is the only thing that helps.
2. **A few very large files.** Cost dominated by raw bandwidth. Wall-clock is `bytes / link_rate`. Cold reads cost the same as warm transfers — you'd have paid the bytes anyway.

The prefill split picks the regime where prefetching actually helps (1) and leaves the regime where it doesn't (2) to on-demand. Apply becomes cheap because we don't warm what we can't accelerate; first use becomes acceptable because we never had to deserialize thousands of small-file round-trips at runtime.

A small `.py` file you don't warm is a 1–2 ms stall the first time something imports it. A 3 GB `.so` file you do warm is a multi-minute apply tax every time. The heuristic optimizes the right loss function.

## Reproduction commands

Everything below was run on a Meta devvm. Adjust paths to taste.

**To reproduce the whole `benchmark_table.csv` in one shot, run `bash benchmark_table.sh`** — it rebuilds the env (`setup_env.sh --force`), applies, and runs every measured command, printing each value. The individual steps below break those commands out.

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

The apply CLI prints high-level progress (`Job is ready (Ns)`) along with the mount sidecar's per-mount timings (`[job_sidecar ...] mounts opened in Ns`, `refreshing mounts` / `refresh complete in Ns`) and on-demand transfer events inline. (Detaching the sidecar into its own session and redirecting its stdout/stderr to a log file beside its lock — so trailing background output doesn't spill into your shell — is a follow-up, split into a separate diff.)

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
    -e TITAN_MODEL_CONFIG=qwen3_1.7b \
    -e TITAN_TRAINING_STEPS=300 \
    -e TITAN_DATASET=fineweb_edu_10BT \
    -- python train.py 2>&1 | tee /tmp/qwen3_mast.log
```

Bump `TITAN_TRAINING_STEPS` higher for a longer run. 300 steps is the smoke-test value used for this writeup (loss → ~5.29; the first ~100 steps stream FineWeb in over the mount, then steady-state).

Tail rank 0's per-worker stdout for live step lines:

```bash
tail -f /home/cpuhrsch/torchtitan_logs/hosts_0/exec_outputs/*/stdout.txt | grep step:
```

### Refresh measurements

```bash
# no-change refresh
/usr/bin/time -f "WALL=%e" monarch apply job.job 2>&1 | tail -5

# touch a small .py and re-apply
touch $HOME/dev/titan_workspace/.venv/lib/python3.12/site-packages/torch/version.py
/usr/bin/time -f "WALL=%e" monarch apply job.job 2>&1 | tail -5
```

### Tear down

```bash
monarch kill
```

## Caveats

- **MAST queue wait dominates wall-clock for `apply`.** The mount-open phase is on the order of tens of seconds; total `apply` wall-clock is dominated by however long MAST takes to allocate the requested hardware, which varies by cluster load. Mount-open is the number that's reproducible — the queue wait isn't.
- **The mount sidecar's detail messages** (per-mount `mounts opened`, on-demand transfers) appear in the apply/exec output. Detaching the sidecar into its own session and redirecting its stdout/stderr to a log file beside its lock — so trailing background output doesn't spill into your shell after the foreground CLI exits — is a follow-up, split into a separate diff.
- **The first run after a fresh apply is the *only* cold one.** Each worker caches in memory every block it is sent, so once all four workers have a block, re-running the bench without re-applying measures warm timings everywhere.
- **Preemption is *not* handled transparently anymore.** Hostnames are resolved once at apply time and cached in `.monarch/job_state.pkl`. The previous design re-resolved on every `monarch exec` via `mast get-status` (~600 ms per call); after profiling showed the live-resolution overhead dominated steady-state exec, we moved to apply-time caching. If MAST preempts and re-places task instances under the same job ID, the cached hostnames go silently stale and the next `monarch exec` will surface a TLS handshake / connect timeout rather than a clean "job preempted" error. Workaround for now: `monarch kill && monarch apply job.job` to force a fresh apply. An upcoming Monarch feature surfaces host-liveness through the existing supervision channel (no extra subprocess); once it lands we get clean preempt-detection back without paying per-call subprocess cost.
