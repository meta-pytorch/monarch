# torchtitan on MAST via monarch (H100 x86)

Run torchtitan `llama3_debugmodel` across 4 h100 nodes (32 GPUs total) using
the `monarch apply` / `monarch exec` workflow. The trainer code (`torchtitan` +
`torch`) ships to workers via `remote_mount` of a uv-managed `.venv`, not via
the worker fbpkg.

H100 x86 only. monarch is built from source via `uv build` (no Buck). Jobs run
on MAST via the vendored `SimpleMastJob` in the `simple_mast_job/` package.

## What each file does

| File                      | Role                                                                                                                |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `job.py`                  | Defines `job` (a `SimpleMastJob` configured for 4 h100s with the workspace + example dir mounted via `remote_mount`).|
| `train.py`                | torchtitan launcher. Runs as plain `python train.py` locally (single GPU) or via `monarch exec` remotely (32 GPU distributed). No monarch imports. |
| `train_with_actors.py`    | **Alternative** client-side driver using the monarch actor framework — spawns a `TrainerActor` per GPU and uses the actor mailbox as the torch rdzv. See appendix. |
| `simple_mast_job/`        | Vendored `SimpleMastJob` (MAST-backed JobTrait) + `build_bootstrap.py`. `build_bootstrap` builds a slim monarch venv (reusing the wheel `setup_env.sh` cached in `/tmp/monarch_bootstrap_$USER/wheel`), fbpkgs it as `monarch_additional_packages`, and `launch_mast` schedules the h100 jobspec. |
| `setup_env.sh`            | One-shot bootstrap: builds the monarch wheel from source, creates the client venv, builds the torchtitan workspace `.venv`, caches the wheel for `build_bootstrap`, and pip-installs this dir editable. |
| `pyproject.toml`          | Declares this dir an installable project (`pip install -e .` puts `job`/`train`/`train_with_actors`/`simple_mast_job` on `sys.path`) and holds the default `workspace` path under `[tool.torchtitan_mast.paths]`. |

## Layout

```
~/dev/titan_workspace/         <-- this whole directory gets remote_mounted
├── .venv/                     <-- uv venv with torch installed
└── torchtitan -> ~/dev/torchtitan   <-- symlink (editable install)
```

Workers see the workspace at the same absolute path it has locally, and the
FUSE-served `.venv/bin/python3.12` is what `monarch exec` and the spawned
actor procs use. Torch lives in `.venv/lib/python3.12/site-packages/torch/` and
gets BFS-prefetched at mount time (via `python_exe=<path>` to
`MountHandler.open`).

## One-time setup

```bash
cd <fbsource>/fbcode/monarch/examples/torchtitan_mast
bash setup_env.sh
```

`setup_env.sh` builds, in one pass:

1. A monarch wheel from `<fbsource>/fbcode/monarch` (`uv build --wheel`, no
   Buck) at `/tmp/torchtitan_mast_monarch_wheel.<pid>/`, also copied into
   `/tmp/monarch_bootstrap_$USER/wheel/` where `build_bootstrap` reuses it.
2. A client venv at `~/monarch_bench_envs/client` with the monarch wheel +
   `fire` + a staged `libomp.so`.
3. A torchtitan workspace `.venv` at `~/dev/titan_workspace/.venv` with torch
   (`cu128` channel) + (non-editable) torchtitan + extras, plus a symlink to
   your local torchtitan checkout.
4. `pip install -e .` of this example dir into the client venv so `job` /
   `train` / `train_with_actors` / `simple_mast_job` are importable from any
   cwd.

The slim worker (bootstrap) fbpkg is not built here — `monarch apply` builds
it on demand via `simple_mast_job.build_bootstrap`, reusing the cached wheel.

Required prereqs (the script checks these):
- Python at `/usr/local/fbcode/platform010/bin/python3.12`
- `uv` on `$PATH`
- A torchtitan checkout at `~/dev/torchtitan` (override via `TITAN_TORCHTITAN=`)

After it finishes, add the client venv to your `PATH`:

```bash
export PATH="$HOME/monarch_bench_envs/client/bin:$PATH"
```

Or invoke `monarch` by full path: `~/monarch_bench_envs/client/bin/monarch`.

Re-run with `--force` to wipe the venvs and rebuild. Pass `MONARCH_CLIENT_EDITABLE=1`
to install monarch into the client venv editable from the source tree (the
bootstrap fbpkg always ships the wheel to MAST). Set `MONARCH_REBUILD=1` to make
`monarch apply` re-upload the bootstrap fbpkg even if cached (the wheel itself
is rebuilt by `setup_env.sh --force`).

## Run

The idea: write a vanilla training script (`train.py`) that runs locally with
`python train.py` against your activated torchtitan venv, then run the **same
script** remotely via `monarch exec`. monarch is only on the launch path; your
venv and your script don't import it.

```bash
# (Optional) local single-GPU smoke test with your torchtitan venv active.
source ~/dev/titan_workspace/.venv/bin/activate
python train.py     # rank 0 of world 1
deactivate

cd <fbsource>/fbcode/monarch/examples/torchtitan_mast

# 1. Allocate 4 h100s on MAST and mount the workspace + this example dir.
monarch apply job.job

# 2. Run 32-way distributed training. Two ``monarch exec`` calls:
#    first resolve rank 0's hostname for the torch.distributed TCPStore
#    rendezvous, then dispatch ``python train.py`` to every (host, gpu)
#    coordinate. The realpath of train.py matches the worker's mount
#    point regardless of whether you cd'd in via a symlink. job.py injects
#    TITAN_TORCHTITAN and TITAN_LOSS=ce into the worker env and MASTER_PORT
#    defaults to 29500, so MASTER_ADDR is the only -e you need here.
HOST0=$(monarch exec --one -- hostname | tail -1)
monarch exec --all --per-host gpu=8 -e MASTER_ADDR="$HOST0" -- python "$(realpath train.py)"

# 3. Tear down (or just `rm -rf .monarch` to forget the cached job).
monarch kill
```

### Why the bootstrap venv should stay slim

The bootstrap wheel is built with `USE_TENSOR_ENGINE=0` (no torch, no
`nvidia/`), so the worker fbpkg stays small (~530 MB vs the ~5 GB a fat one
would be). That matters: a fat worker bloats the upload, wastes host memory,
and creates a second torch installation that's easy to import by accident and
silently divergent from the workspace `.venv`'s torch — which is what the
workers actually run. To point MAST at a prebuilt fbpkg instead of building
one, set `MONARCH_MAST_WORKER_FBPKG_ID=<your-fbpkg-id>`.

The worker-side `python` resolves to the workspace `.venv/bin/python3.12`
(FUSE-mounted) because `job.py` prepends `<workspace>/.venv/bin` to the worker
`PATH` and passes that same binary as `python_exe` to `remote_mount` (so
`spawn_procs` uses it too). The workspace venv has monarch + torch +
torchtitan in its own site-packages, so `import monarch` / `import torch` /
`import torchtitan` Just Work — no PYTHONPATH bridging needed.

## Benchmark

[`benchmark_table.csv`](benchmark_table.csv) is the measured comparison: one
runnable command per row, the metric it produces, and the result on each data
plane — `baseline` (the eager warm build) vs `v3` (this example's on-demand
build). The commands are identical across planes; only the installed build
differs, so any row is paste-and-run. GitHub renders the file as a table.

- `mount_open` is the reproducible apply cost; `wall_incl_mast_alloc` also
  includes MAST queue/allocation, which varies run to run.
- v3's on-demand cost shows up in `perworker_import_cold` (it pulls the `.so`s
  on first import), not at the command wall — its faster `monarch exec` floor
  (`echo`/`hostname`) more than hides it.
- File-size and qwen3-training rows are data-plane-independent.

The command rows are one 4×h100 run (2026-06-19); `BENCHMARK.md` cites a
separate pinned run, so headline numbers differ by allocation noise.
`BENCHMARK.md` is the full writeup (data-plane mechanism, dedup table,
big-file list, exec-floor breakdown).

## Tunables

The workspace path default lives in `pyproject.toml` under
`[tool.torchtitan_mast.paths]`; the env vars below override per-run. The
`TITAN_*` knobs control job shape and are env-only.

| Variable                  | Default                                         | Description                                                                  |
| ------------------------- | ----------------------------------------------- | ---------------------------------------------------------------------------- |
| `TITAN_WORKSPACE`         | `paths.workspace` (`~/dev/titan_workspace`)     | Local dir holding `.venv/` and the torchtitan symlink. |
| `TITAN_TORCHTITAN`        | `~/dev/torchtitan`       | Your torchtitan checkout (used by `setup_env.sh`).                          |
| `TITAN_NUM_HOSTS`         | `4`                      | Number of MAST hosts to allocate.                                            |
| `TITAN_GPUS_PER_HOST`     | `8`                      | Procs/actors spawned per host.                                               |
| `TITAN_MODEL_CONFIG`      | `llama3_debugmodel`      | Any config registered in `torchtitan/models/.../config_registry.py`.         |
| `TITAN_TRAINING_STEPS`    | `20`                     | Training steps to run before stopping.                                       |
| `TITAN_FINEWEB_DIR`       | `~/dev/llm_data/fineweb_edu_10BT` | If present, mounted as an extra `remote_mount` and used when `TITAN_DATASET=fineweb_edu_10BT`. |
| `TITAN_LOG_DIR`           | `~/torchtitan_logs`      | Client-side gather mount target (per-worker `/tmp/torchtitan_logs` surfaced as `hosts_N/` subdirs). |

## Iteration loop

Because the workspace is `remote_mount`-ed, an edit in
`$TITAN_TORCHTITAN/torchtitan/...` is picked up on the next `monarch exec`
invocation — `_mounts.ensure_open` triggers a daemon-side `refresh()` that
re-syncs only the changed files. No need to kill MAST and re-`apply`. Same for
edits to `train.py` (the example dir is also remote-mounted).

## Appendix: actor-driven launch (alternative to step 3)

`train.py` is the "use monarch as a remote runner" approach — the script knows
nothing about monarch, just reads env vars. The other way to launch the same
training is to drive it through monarch's actor framework, where each rank is
a `TrainerActor` and the actor mailbox plays the role of torch's rendezvous
backend. That's what `train_with_actors.py` does:

```bash
monarch apply job.job
python train_with_actors.py    # client-side driver; runs as long as training runs
monarch kill
```

`train_with_actors.py` connects to the running MAST allocation, calls
`spawn_procs(per_host={"gpus": 8})` to lay out 32 procs, runs
`monarch.spmd.setup_torch_elastic_env_async(proc_mesh)` to populate
`MASTER_ADDR`/`MASTER_PORT`/`RANK`/`WORLD_SIZE`/`LOCAL_RANK` on each proc via
the actor mailbox, then spawns `TrainerActor` instances and calls
`.start_training.call()` on the whole mesh.

### When to pick which

| Aspect                          | Option (a) `train.py` via `monarch exec`           | Option (b) `train_with_actors.py` (actor framework)         |
| ------------------------------- | -------------------------------------------------- | ----------------------------------------------------------- |
| Mental model                    | "Run my existing script remotely."                 | "Build a distributed program on top of monarch primitives." |
| Coupling to monarch             | None inside the training script.                   | Trainer logic is wrapped in `@endpoint` methods.            |
| Multi-host rendezvous           | TCP via `MASTER_ADDR` (rank 0's hostname).         | Actor mailbox; no external port needed.                     |
| Surfacing per-rank Python errors| Whatever `torchrun`-style stderr you'd see.        | Each actor's exception comes back as `ActorError` with traceback. |
| Live introspection of a rank    | Not really; you only see stdout/stderr.            | `bench.slice(hosts=2).foo.call_one()` on a live actor.      |
| RDMA buffers / actor messaging  | Not available — plain process.                     | Available (`RDMABuffer`, `mesh_admin`, py-spy plumbing).    |
| Code in this dir                | `train.py` (~180 lines, no monarch).               | `train_with_actors.py` (~230 lines, monarch + torchtitan).  |
| Recommended first encounter     | Easiest path if you already have a torch script.   | Pick this once you want monarch features beyond remote-exec.|

Both depend on the same `job.py` apply step. You can keep both files checked
in and pick at run time; nothing forces a choice.
