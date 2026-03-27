#!/bin/bash
# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.
#
# Runs test_shutdown_host_mesh in a loop until failure.
# Usage: ./run_shutdown_loop.sh

set -euo pipefail

TARGET="fbcode//monarch/python/tests:test_host_mesh"
TEST="test_shutdown_host_mesh"

i=0
while true; do
    i=$((i + 1))
    t0=$(date +%s%N)
    if buck test @fbcode//mode/opt "$TARGET" -- "$TEST" 2>/dev/null; then
        t1=$(date +%s%N)
        elapsed=$(( (t1 - t0) / 1000000 ))
        echo "[${i}] OK (${elapsed}ms)"
    else
        t1=$(date +%s%N)
        elapsed=$(( (t1 - t0) / 1000000 ))
        echo "[${i}] FAIL (${elapsed}ms)"
        echo ""
        echo "Failed on iteration $i. Re-running with stderr..."
        buck test @fbcode//mode/opt "$TARGET" -- "$TEST"
        exit 1
    fi
done
