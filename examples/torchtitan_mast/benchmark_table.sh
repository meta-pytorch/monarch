#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Reproduce this commit's benchmark_table.csv on a Meta devvm with MAST access
# and torchtitan at $HOME/dev/torchtitan. Read numbers off the output: `time`
# prints each wall-clock; `apply` prints "Mounts ready in Ns"; the find block
# prints the four file-size rows; the code-edit probe prints two more walls.
# Per-worker import seconds and the train step/loss/mfu lines land in the
# per-rank logs, grepped at the end. The CSV is single-column (this commit's
# numbers); use `sl log benchmark_table.csv` for earlier commits' numbers.

set -u
cd "$(dirname "$0")" || exit 1
MON="${MONARCH:-$HOME/monarch_bench_envs/client/bin/monarch}"
VENV="$HOME/dev/titan_workspace/.venv"
LOGS="${TITAN_LOG_DIR:-$HOME/torchtitan_logs}"

bash setup_env.sh --force || exit 1

echo "### file sizes: files_lt100MiB files_ge100MiB GiB_lt100MiB GiB_ge100MiB"
find "$VENV" -type f -size -100M | wc -l
find "$VENV" -type f -size +100M | wc -l
find "$VENV" -type f -size -100M -printf '%s\n' | awk '{s+=$1} END{printf "%.1f\n",s/2^30}'
find "$VENV" -type f -size +100M -printf '%s\n' | awk '{s+=$1} END{printf "%.1f\n",s/2^30}'

echo "### apply (mount_open in output, wall from time)"
rm -rf .monarch && time "$MON" apply job.job

echo "### echo --all / hostname --one (wall)"
time "$MON" exec --all -- echo hello
time "$MON" exec --one -- hostname

echo "### cold import torch (wall)"
time "$MON" exec --all -- python -c 'import time; t=time.perf_counter(); import torch; print(time.perf_counter()-t)'
echo "### warm import torch (wall)"
time "$MON" exec --all -- python -c 'import time; t=time.perf_counter(); import torch; print(time.perf_counter()-t)'

echo "### re-apply, no change (wall + mount_open)"
time "$MON" apply job.job

echo "### code-edit probe: exec a NEW small file, then a GROWN edit of it. Each wall"
echo "### includes the mount refresh that picks the change up (monarch exec re-opens"
echo "### the mount), so a small code edit that triggered a big re-transfer / mount"
echo "### spike -- instead of a one-block re-pull -- would show up here."
PROBE="$(dirname "$VENV")/edit_probe.py"
printf 'import sys\nprint("edit_probe v1", tuple(sys.version_info[:2]))\n' > "$PROBE"
echo "# after create (wall):"
time "$MON" exec --one -- python "$PROBE"
{ printf 'import sys\n\n\ndef _added_on_edit() -> int:\n    return sum(range(100000))\n\n\nprint("edit_probe v2", tuple(sys.version_info[:2]), _added_on_edit())\n'; for i in $(seq 1 500); do printf '# padding line %d -- grows the file size on edit\n' "$i"; done; } > "$PROBE"
echo "# after edit, file grown (wall):"
time "$MON" exec --one -- python "$PROBE"

MASTER_ADDR=$("$MON" exec --one -- hostname | tail -1)
echo "### train 20 steps (wall)"
time timeout 900 "$MON" exec --all --per-host gpu=8 \
    -e MASTER_ADDR="$MASTER_ADDR" -e MASTER_PORT=29500 \
    -e TITAN_TORCHTITAN="$HOME/dev/torchtitan" \
    -- bash -c 'python train.py 2>&1'

echo "### train qwen3_1_7b 300 steps (loss + mfu)"
timeout 1800 "$MON" exec --all --per-host gpu=8 \
    -e MASTER_ADDR="$MASTER_ADDR" -e MASTER_PORT=29501 \
    -e TITAN_TORCHTITAN="$HOME/dev/torchtitan" \
    -e TITAN_MODEL_MODULE=qwen3 -e TITAN_MODEL_CONFIG=qwen3_1.7b \
    -e TITAN_TRAINING_STEPS=300 -e TITAN_DATASET=fineweb_edu_10BT \
    -- bash -c 'python train.py 2>&1'

echo "### per-worker import seconds + train step/loss/mfu (read before kill tears down the gather mount):"
grep -hE 'step:|^[0-9]+\.[0-9]+$' "$LOGS"/hosts_*/exec_outputs/*/stdout.txt 2>/dev/null | sed -E 's/\x1b\[[0-9;]*m//g' | tail -40

echo "### kill (wall)"
time "$MON" kill
