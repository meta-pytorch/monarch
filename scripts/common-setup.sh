#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Common setup functions for Monarch CI workflows

set -ex

# Setup conda environment. Defaults to Python 3.10.
setup_conda_environment() {
    local python_version=${1:-3.10}
    echo "Setting up conda environment with Python ${python_version}..."
    conda create -n venv python="${python_version}" -y
    conda activate venv
    export LD_LIBRARY_PATH="${CONDA_PREFIX}/lib:$LD_LIBRARY_PATH"
    export PATH=/opt/rh/devtoolset-10/root/usr/bin/:$PATH
    python -m pip install --upgrade pip
}

# Install system-level dependencies
install_system_dependencies() {
    echo "Installing system dependencies..."
    dnf update -y
    # Protobuf compiler is required for the tracing-perfetto-sdk-schema crate.
    dnf install clang-devel libunwind libunwind-devel protobuf-compiler -y
}

# Install and configure Rust nightly toolchain
setup_rust_toolchain() {
    echo "Setting up Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "${HOME}"/.cargo/env
    rustup toolchain install nightly
    rustup default nightly
    # We use cargo nextest to run tests in individual processes for similarity
    # to buck test.
    # Replace "cargo test" commands with "cargo nextest run".
    cargo install cargo-nextest --locked
}

# Install Python test dependencies
install_python_test_dependencies() {
    echo "Installing test dependencies..."
    pip install -r python/tests/requirements.txt
    dnf install -y rsync # required for code sync tests
}

# Install wheel from artifact directory
install_wheel_from_artifact() {
    echo "Installing wheel from artifact..."
    pip install "${RUNNER_ARTIFACT_DIR}"/*.whl
}

# Setup and install dependencies for Tensor Engine
setup_tensor_engine() {
    echo "Installing Tensor Engine dependencies..."
    # Install the fmt library for C++ headers in pytorch.
    conda install -y -c conda-forge fmt
    dnf install -y libibverbs rdma-core libmlx5 libibverbs-devel rdma-core-devel
}

# Install PyTorch with C++ development headers (libtorch) for Rust compilation
setup_pytorch_with_headers() {
    local gpu_arch_version=${1:-"12.6"}
    local torch_spec=${2:-"--pre torch --index-url https://download.pytorch.org/whl/nightly/cu126"}

    echo "Setting up PyTorch with C++ headers (GPU arch: ${gpu_arch_version})..."

    # Extract CUDA version for libtorch URL (remove dots: "12.6" -> "126")
    local cuda_version_short=$(echo "${gpu_arch_version}" | tr -d '.')
    local libtorch_url="https://download.pytorch.org/libtorch/nightly/cu${cuda_version_short}/libtorch-cxx11-abi-shared-with-deps-latest.zip"

    echo "Downloading libtorch from: ${libtorch_url}"
    wget -q "${libtorch_url}"
    unzip -q "libtorch-cxx11-abi-shared-with-deps-latest.zip"

    # Set environment variables for libtorch
    export LIBTORCH_ROOT="$PWD/libtorch"
    export LD_LIBRARY_PATH="$LIBTORCH_ROOT/lib:${LD_LIBRARY_PATH:-}"
    export CMAKE_PREFIX_PATH="$LIBTORCH_ROOT:${CMAKE_PREFIX_PATH:-}"

    # Install PyTorch Python package using provided torch-spec
    echo "Installing PyTorch Python package with: ${torch_spec}"
    pip install ${torch_spec}

    # Verify installation
    echo "LibTorch C++ headers available at: $LIBTORCH_ROOT/include"
    if [[ -d "$LIBTORCH_ROOT/include/torch/csrc/api/include/torch" ]]; then
        echo "✓ PyTorch C++ API headers found"
    else
        echo "⚠ PyTorch C++ API headers not found at expected location"
    fi

    if [[ -d "$LIBTORCH_ROOT/include/c10/cuda" ]]; then
        echo "✓ C10 CUDA headers found"
    else
        echo "⚠ C10 CUDA headers not found"
    fi

    echo "LibTorch libraries available at: $LIBTORCH_ROOT/lib"
    ls -la "$LIBTORCH_ROOT/lib/lib"*.so | head -5 || echo "No .so files found"
}

# Common setup for build workflows (environment + system deps + rust)
setup_build_environment() {
    local python_version=${1:-3.10}
    setup_conda_environment "${python_version}"
    install_system_dependencies
    setup_rust_toolchain
}

# Detect and configure CUDA environment for linking
setup_cuda_environment() {
    echo "Setting up CUDA environment..."

    # Detect CUDA installation
    DETECTED_CUDA_HOME=""
    if command -v nvcc >/dev/null 2>&1; then
        DETECTED_CUDA_HOME=$(dirname $(dirname $(which nvcc)))
    elif [ -d "/usr/local/cuda" ]; then
        DETECTED_CUDA_HOME="/usr/local/cuda"
    fi

    # Set CUDA_LIB_DIR (resolve symlinks if needed)
    if [ -n "$DETECTED_CUDA_HOME" ] && [ -d "$DETECTED_CUDA_HOME/lib64" ]; then
        export CUDA_LIB_DIR=$(readlink -f "$DETECTED_CUDA_HOME/lib64")
    elif [ -n "$DETECTED_CUDA_HOME" ] && [ -d "$DETECTED_CUDA_HOME/lib" ]; then
        export CUDA_LIB_DIR=$(readlink -f "$DETECTED_CUDA_HOME/lib")
    else
        export CUDA_LIB_DIR="/usr/local/cuda/lib64"
    fi

    # Configure library paths to fix CUDA linking issues
    # Prioritize CUDA libraries over potentially incompatible system versions
    export LIBRARY_PATH="$CUDA_LIB_DIR:/lib64:/usr/lib64:${LIBRARY_PATH:-}"
    export LD_LIBRARY_PATH="$CUDA_LIB_DIR:/lib64:/usr/lib64:${LD_LIBRARY_PATH:-}"
    export RUSTFLAGS="-L native=$CUDA_LIB_DIR -L native=/lib64 -L native=/usr/lib64 ${RUSTFLAGS:-}"

    echo "✓ CUDA environment configured (CUDA_LIB_DIR: $CUDA_LIB_DIR)"
}

# Common setup for test workflows (environment only)
setup_test_environment() {
    local python_version=${1:-3.10}
    setup_conda_environment "${python_version}"
    install_python_test_dependencies
}

# Run Python test groups for Monarch.
# Usage: run_test_groups <enable_actor_error_test: 0|1>
#
# Arguments:
#   enable_actor_error_test:
#       0 → skip python/tests/test_actor_error.py
#       1 → include python/tests/test_actor_error.py
#
# Tests are executed in 10 sequential groups with process cleanup
# between runs.
run_test_groups() {
  set +e
  local test_results_dir="${RUNNER_TEST_RESULTS_DIR:-test-results}"
  local enable_actor_error_test="${2:-0}"
  # Validate argument enable_actor_error_test
  if [[ "$enable_actor_error_test" != "0" && "$enable_actor_error_test" != "1" ]]; then
    echo "Usage: run_test_groups <enable_actor_error_test: 0|1>"
    return 2
  fi
  # Make sure the runtime linker uses the conda env's libstdc++
  # (which was used to compile monarch) instead of the system's.
  # TODO: Revisit this to determine if this is the proper/most
  # sustainable/most robust solution.
  export CONDA_LIBSTDCPP="${CONDA_PREFIX}/lib/libstdc++.so.6"
  export LD_PRELOAD="${CONDA_LIBSTDCPP}${LD_PRELOAD:+:$LD_PRELOAD}"
  local FAILED_GROUPS=()
  for GROUP in $(seq 1 10); do
    echo "Running test group $GROUP of 10..."
    # Kill any existing Python processes to ensure clean state
    echo "Cleaning up Python processes before group $GROUP..."
    pkill -9 python || true
    pkill -9 pytest || true
    sleep 2
    # Isolate runs each test in a separate forked process which enhances reliability.
    if [[ "$enable_actor_error_test" == "1" ]]; then
        LC_ALL=C pytest --isolate python/tests/ -s -v -m "not oss_skip" \
            --ignore-glob="**/meta/**" \
            --dist=no \
            --group="$GROUP" \
            --junit-xml="$test_results_dir/test-results-$GROUP.xml" \
            --splits=10
    else
        LC_ALL=C pytest --isolate python/tests/ -s -v -m "not oss_skip" \
            --ignore-glob="**/meta/**" \
            --dist=no \
            --ignore=python/tests/test_actor_error.py \
            --group="$GROUP" \
            --junit-xml="$test_results_dir/test-results-$GROUP.xml" \
            --splits=10
    fi
    # Check result and record failures
    if [[ $? -eq 0 ]]; then
        echo "✓ Test group $GROUP completed successfully"
    else
        FAILED_GROUPS+=($GROUP)
        echo "✗ Test group $GROUP failed with exit code $?"
    fi
  done
  # Final cleanup after all groups
  echo "Final cleanup of Python processes..."
  pkill -9 python || true
  pkill -9 pytest || true
  # Check if any groups failed and exit with appropriate code
  if [ ${#FAILED_GROUPS[@]} -eq 0 ]; then
    echo "✓ All test groups completed successfully!"
  else
    echo "✗ The following test groups failed: ${FAILED_GROUPS[*]}"
    echo "Failed groups count: ${#FAILED_GROUPS[@]}/10"
    return 1
  fi
  set -e
}
