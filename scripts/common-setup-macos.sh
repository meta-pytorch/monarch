#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Common setup functions for Monarch macOS CI workflows

set -ex

install_macos_base_dependencies() {
    export HOMEBREW_NO_AUTO_UPDATE=1
    brew install protobuf
    python -m pip install --upgrade pip
}

install_macos_python_test_dependencies() {
    python -m pip install -r python/tests/requirements.txt
    python -m pip install --pre torch --extra-index-url https://download.pytorch.org/whl/nightly/cpu
}

install_macos_rust_test_dependencies() {
    cargo install cargo-nextest --locked
}

# Run Python test groups for Monarch on macOS.
# Usage: run_test_groups
#
# Tests are executed in 10 sequential groups with process cleanup
# between runs.
run_test_groups() {
    set +e
    local FAILED_GROUPS=()
    local TEST_EXIT_CODE=0
    for GROUP in $(seq 1 10); do
        echo "Running test group $GROUP of 10..."
        echo "Cleaning up Python processes before group $GROUP..."
        pkill -9 Python || true
        pkill -9 python || true
        pkill -9 pytest || true
        sleep 2
        LC_ALL=C pytest python/tests/ -s -v -m "not oss_skip" \
            --ignore-glob="**/meta/**" \
            --dist=no \
            --group="$GROUP" \
            --splits=10
        TEST_EXIT_CODE=$?
        if [[ $TEST_EXIT_CODE -eq 0 ]]; then
            echo "✓ Test group $GROUP completed successfully"
        else
            FAILED_GROUPS+=("$GROUP")
            echo "✗ Test group $GROUP failed with exit code $TEST_EXIT_CODE"
        fi
    done
    echo "Final cleanup of Python processes..."
    pkill -9 Python || true
    pkill -9 python || true
    pkill -9 pytest || true
    if [ ${#FAILED_GROUPS[@]} -eq 0 ]; then
        echo "✓ All test groups completed successfully!"
    else
        echo "✗ The following test groups failed: ${FAILED_GROUPS[*]}"
        echo "Failed groups count: ${#FAILED_GROUPS[@]}/10"
        return 1
    fi
    set -e
}
