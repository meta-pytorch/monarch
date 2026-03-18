#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Helper for MAST job management with remotemount tests.
#
# Usage:
#   bash examples/mast_helper.sh run-test [args...]   # Run test_incremental.py
#   bash examples/mast_helper.sh run-bench [args...]   # Run bench_incremental.py
#   bash examples/mast_helper.sh kill                  # Kill cached MAST job
#   bash examples/mast_helper.sh kill-all              # Kill all your MAST jobs
#   bash examples/mast_helper.sh rebuild [target]      # Rebuild conda envs
#   bash examples/mast_helper.sh status                # Show cached job status

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"

ENVS="${MONARCH_CONDA_ENVS:-$HOME/monarch_conda_envs}"
CONDA_CLIENT="$ENVS/client/conda"
CONDA_WORKER="$ENVS/worker/conda"

_run_python() {
    CONDA_PREFIX="$CONDA_WORKER" \
    PATH="/usr/bin:/usr/local/bin:$CONDA_WORKER/bin:$PATH" \
    "$CONDA_CLIENT/bin/python3.12" "$@"
}

case "${1:-help}" in
    run-test)
        shift
        rm -rf .monarch/job_state.pkl 2>/dev/null
        _run_python examples/remotemount/test_incremental.py \
            --backend mast --host_type gb300 --num_hosts 2 "$@"
        ;;
    run-bench)
        shift
        _run_python examples/remotemount/bench_incremental.py "$@"
        ;;
    kill)
        if [ -f .monarch/job_state.pkl ]; then
            JOB=$("$CONDA_CLIENT/bin/python3.12" -c "
import pickle
with open('.monarch/job_state.pkl', 'rb') as f:
    job = pickle.load(f)
print(getattr(job, 'app_handle', getattr(job, '_app_handle', 'unknown')))
" 2>/dev/null || echo "unknown")
            if [ "$JOB" != "unknown" ]; then
                JOB_NAME=$(echo "$JOB" | grep -oP 'monarch-[a-f0-9]+' || echo "$JOB")
                echo "Killing $JOB_NAME..."
                mast kill "$JOB_NAME" --comment "cleanup" 2>/dev/null || echo "Kill failed or already dead"
            fi
            rm -f .monarch/job_state.pkl
            echo "Cache cleared."
        else
            echo "No cached job."
        fi
        ;;
    kill-all)
        echo "Listing your MAST jobs..."
        mast list-jobs --prefix monarch- --output json 2>/dev/null | \
            python3 -c "
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try:
        data = json.loads(line)
        for job in data if isinstance(data, list) else [data]:
            name = job.get('name', job.get('job_name', ''))
            if name: print(name)
    except: pass
" | while read -r job; do
                echo "Killing $job..."
                mast kill "$job" --comment "cleanup" 2>/dev/null || true
            done
        rm -f .monarch/job_state.pkl
        echo "Done."
        ;;
    rebuild)
        shift
        TARGET="${1:-gb300}"
        bash examples/remotemount/setup_conda_env.sh "$TARGET" "$ENVS"
        ;;
    status)
        if [ -f .monarch/job_state.pkl ]; then
            echo "Cached job exists at .monarch/job_state.pkl"
        else
            echo "No cached job."
        fi
        ;;
    help|*)
        echo "Usage: bash examples/mast_helper.sh <command> [args]"
        echo ""
        echo "Commands:"
        echo "  run-test [args]    Run test_incremental.py on MAST"
        echo "  run-bench [args]   Run bench_incremental.py on MAST"
        echo "  kill               Kill the cached MAST job"
        echo "  kill-all           Kill all your MAST jobs"
        echo "  rebuild [target]   Rebuild conda envs (default: gb300)"
        echo "  status             Show cached job status"
        ;;
esac
