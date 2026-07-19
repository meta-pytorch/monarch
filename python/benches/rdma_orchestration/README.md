# RDMA orchestration benchmark

Measures Monarch's RDMA control path -- buffer setup, reads, writes, and cold
start -- so you can tell whether a change (e.g. moving orchestration into Rust)
makes it faster or slower. It drives only the public Monarch surface; the
optimized data-plane transfer path is treated as frozen (see "What a pass means").

## Run one benchmark

```
buck run //monarch/python/benches/rdma_orchestration:bench -- \
  bench --backend tcp --out run.json
```

- `--backend tcp` runs on any host. `--backend ibverbs` needs RDMA hardware (RoCE
  or InfiniBand) and a `--ibverbs-target` (see "Pin the ibverbs device").
- one run writes one result file. `--smoke` does a quick tiny run for plumbing
  checks only -- never gate on smoke artifacts.

## Pin the ibverbs device

`--backend ibverbs` requires `--ibverbs-target`, one of `cpu:<numa>`,
`gpu:<ordinal>`, or `nic:<name>` (e.g. `--ibverbs-target nic:mlx5_0`). Without a
target Monarch picks a NIC per registration by hashing the buffer address, so on
a multi-NIC host two peers can land on different NICs -- an unreproducible data
plane, and a cross-NIC queue pair that cannot loopback (it fails with
`IBV_WC_RETRY_EXC_ERR`). Pinning one device makes every run measure the same path.
The target is recorded in each artifact's config; runs pinned to different devices
are not gate-comparable. (Any `[RdmaXcel] ... libcuda` lines are harmless: we
register host memory, which has no CUDA context.)

## Evaluate a refactor

The normal use: prove a change did not regress the RDMA orchestration.

### Prerequisites

- **same instrument, both sides.** use the final, unchanged benchmark for both
  revisions; do not edit `benchmark.py` or `collector.py` between the baseline and
  candidate runs. each artifact records a hash of both files and the comparator
  refuses a mismatch.
- **a matching checked-in policy.** its `backend`, `source_sha256`, and run shape
  must match the artifacts. use `thresholds/tcp.json` or `thresholds/ibverbs.json`
  as-is; do not hand-edit them.
- **one environment.** run baseline and candidate on the same hostname, CPU /
  platform, Python version, backend, config, and (for ibverbs) `--ibverbs-target`.
- **full gate runs** -- never `--smoke`.
- **at least five independent artifacts per side** (more is better).
- the **frozen-data-plane premise** -- that the change does not alter the transfer
  kernels themselves -- is established separately by code review, not by this tool.

Run the commands below from this package directory
(`fbcode/monarch/python/benches/rdma_orchestration`) so the `thresholds/*.json`
paths resolve; otherwise pass an absolute path to the policy. `before/` and
`after/` are scratch directories you pick.

### TCP

```
# baseline: at the pre-refactor revision
mkdir -p before after
for i in $(seq -w 1 5); do
  buck run //monarch/python/benches/rdma_orchestration:bench -- \
    bench --backend tcp --out before/run_$i.json
done

# candidate: at the refactored revision (benchmark files unchanged)
for i in $(seq -w 1 5); do
  buck run //monarch/python/benches/rdma_orchestration:bench -- \
    bench --backend tcp --out after/run_$i.json
done

# gate
buck run //monarch/python/benches/rdma_orchestration:bench -- \
  compare --baseline before/*.json --candidate after/*.json \
  --policy thresholds/tcp.json
```

### ibverbs (RoCE / InfiniBand)

Same procedure, with the hardware backend and a pinned device on every run. The
checked-in `thresholds/ibverbs.json` was calibrated with `--ibverbs-target
nic:mlx5_0`; use that target, on comparable hardware, for an authoritative verdict
against it.

```
for i in $(seq -w 1 5); do
  buck run //monarch/python/benches/rdma_orchestration:bench -- \
    bench --backend ibverbs --ibverbs-target nic:mlx5_0 --out before/run_$i.json
done
for i in $(seq -w 1 5); do
  buck run //monarch/python/benches/rdma_orchestration:bench -- \
    bench --backend ibverbs --ibverbs-target nic:mlx5_0 --out after/run_$i.json
done
buck run //monarch/python/benches/rdma_orchestration:bench -- \
  compare --baseline before/*.json --candidate after/*.json \
  --policy thresholds/ibverbs.json
```

### Reading the result (exit code)

- **0** -- pass: no regression detectable beyond the calibrated floors.
- **1** -- collection / harness failure (a run or the tool failed).
- **2** -- regression: an RDMA metric moved past its floor.
- **3** -- inconclusive: the ping sentinel moved, so the environment shifted and
  the comparison isn't trustworthy -- rerun on a quiet host.
- **4** -- refused: inputs or policy are incompatible (see the checklist).

Use the checked-in policy as-is. **Do not recalibrate after seeing the candidate
results** -- that tunes the gate to the answer you got. Recalibration is a
maintainer operation (see "Recalibrate"), needed only when the instrument, run
shape, or calibrated hardware context changes -- never per evaluation.

### What a pass means

A pass says: no regression detectable beyond the calibrated floors / MDEs for the
gated metrics -- 64-byte serial reads and writes, K=32 concurrent read/write
makespan, and cold init -- under the separately reviewed frozen-data-plane
premise. It does **not** prove bandwidth, behavior at other payloads or
concurrency levels, or an absolute "pure orchestration" time. Each metric's MDE
(the smallest regression it can catch) is printed at calibration time and bounds
what "no regression" covers.

### Refused (exit 4) checklist

`compare` refuses anything that isn't a valid differential. Check:
- source hash: baseline, candidate, and policy all agree (no edits to
  `benchmark.py` / `collector.py` between sides);
- backend matches the policy (tcp vs ibverbs);
- same hostname / platform / CPU / Python across all artifacts;
- same run shape (payload, K, counts);
- same `--ibverbs-target` on every ibverbs artifact;
- no `--smoke` artifacts;
- at least five artifacts per side.

## Recalibrate (maintainer)

Thresholds are per backend and derived from real runs on that hardware. Redo this
only when the instrument, run shape, or calibrated host/hardware changes -- not for
a normal evaluation.

```
# on an idle host; >= 10 runs, prefer 20. for ibverbs add --ibverbs-target.
mkdir -p /tmp/calib
for i in $(seq -w 1 20); do
  buck run //monarch/python/benches/rdma_orchestration:bench -- \
    bench --backend tcp --out /tmp/calib/run_$i.json
done
buck run //monarch/python/benches/rdma_orchestration:bench -- \
  calibrate --artifacts /tmp/calib/run_*.json \
  --out thresholds/tcp.json --version tcp-YYYY-MM-DD
```

Read the printed MDEs before trusting a policy: an MDE larger than the regression
you care about means the measurement is too noisy (use a quieter host or more
calibration runs), not that you should shrink the safety factor.

Checked-in policies: `thresholds/tcp.json` (calibrated over TCP) and
`thresholds/ibverbs.json` (calibrated on RDMA hardware pinned to `nic:mlx5_0`).
Each records the source hash it was calibrated against and only compares artifacts
from that exact instrument. Policies are per backend, not per device/host: the
comparator does not check the calibration target, so for an authoritative ibverbs
verdict run with `--ibverbs-target nic:mlx5_0` on comparable hardware, to match
how `thresholds/ibverbs.json` was calibrated.

## Details

The `ROB-*` invariants and the calibration math live in `benchmark.py` and
`collector.py`; the design rationale is in the pytokio-removal benchmark plan. You
should not need the plan to operate the benchmark.
