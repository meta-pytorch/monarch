#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Common setup functions for Monarch CI workflows

set -ex

# Ensure conda is available. Checks common locations first, then
# installs Miniconda if conda is not found anywhere.
initialize_conda() {
    if command -v conda &> /dev/null; then
        return
    fi
    # Some images have conda installed but not on PATH.
    if [ -f /opt/conda/etc/profile.d/conda.sh ]; then
        source /opt/conda/etc/profile.d/conda.sh
        return
    fi
    # Install Miniconda if conda is not present at all.
    echo "conda not found, installing Miniconda..."
    local arch
    arch=$(uname -m)
    local installer_url="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-${arch}.sh"
    curl -fsSL -o /tmp/miniconda.sh "$installer_url"
    bash /tmp/miniconda.sh -b -p /opt/conda
    rm /tmp/miniconda.sh
    source /opt/conda/etc/profile.d/conda.sh
}

# Setup conda environment. Defaults to Python 3.10.
setup_conda_environment() {
    local python_version=${1:-3.10}
    echo "Setting up conda environment with Python ${python_version}..."
    initialize_conda
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
    # ninja is needed by monarch_cpp_static_libs to build rdma-core from source
    # (cmake's Makefile generator cannot build by output file path, but Ninja can).
    # Try dnf first (package name varies by distro), fall back to pip if unavailable.
    if ! command -v ninja &> /dev/null; then
        dnf install -y ninja-build 2>/dev/null || dnf install -y ninja 2>/dev/null || pip install ninja || echo "Warning: ninja not available, rdma-core build may use make instead"
    fi
}

# Install Node.js and npm (needed to build the dashboard frontend).
install_node() {
    echo "Installing Node.js..."
    conda install -y -c conda-forge 'nodejs>=18'
    echo "Node $(node --version), npm $(npm --version)"
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
    ARCH=$(uname -m)
    if [ "$ARCH" = "aarch64" ]; then
        NEXTEST_URL="https://get.nexte.st/latest/linux-arm"
    else
        NEXTEST_URL="https://get.nexte.st/latest/linux"
    fi
    curl -LsSf "$NEXTEST_URL" | tar zxf - -C "${CARGO_HOME:-$HOME/.cargo}/bin"

    # Setup sccache for distributed Rust compilation caching via S3.
    setup_sccache

    # We amend the RUSTFLAGS here because they have already been altered by `setup_cuda_environment`
    # (and a few other places); RUSTFLAGS environment variable overrides the definition in
    # .cargo/config.toml.
    export RUSTFLAGS="--cfg tracing_unstable ${RUSTFLAGS:-}"
}

# Setup sccache for Rust compilation caching.
# Uses the pytorch ossci-compiler-cache S3 bucket if available.
setup_sccache() {
    echo "Setting up sccache..."
    pip install sccache

    export RUSTC_WRAPPER=sccache
    export SCCACHE_BUCKET=ossci-compiler-cache
    export SCCACHE_REGION=us-east-1
    export SCCACHE_S3_KEY_PREFIX=monarch

    echo "sccache configured: bucket=${SCCACHE_BUCKET}, prefix=${SCCACHE_S3_KEY_PREFIX}"
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
    # libnl3-devel is needed by rdma-core's cmake to enable mlx5/efa providers
    # and generate ENABLE_STATIC targets (lib/statics/*.a).
    dnf install -y libibverbs rdma-core libmlx5 libibverbs-devel rdma-core-devel libefa libnl3-devel
}

# Install PyTorch with C++ development headers (libtorch) for Rust compilation
setup_pytorch_with_headers() {
    local gpu_arch_version=${1:-"12.8"}
    local torch_spec=${2:-"--pre torch --index-url https://download.pytorch.org/whl/nightly/cu128"}

    echo "Setting up PyTorch with C++ headers (GPU arch: ${gpu_arch_version})..."

    # Extract CUDA version for libtorch URL (remove dots: "12.8" -> "128")
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

# Install CUDA toolkit for build-only (no GPU required).
# Used on ARM64 runners that lack a GPU but need CUDA for compilation.
setup_cuda_toolkit() {
    echo "Installing CUDA toolkit for build-only (no GPU required)..."
    ARCH=$(uname -m)
    if [ "$ARCH" = "aarch64" ]; then
        dnf config-manager --add-repo \
            https://developer.download.nvidia.com/compute/cuda/repos/rhel9/sbsa/cuda-rhel9.repo
    else
        dnf config-manager --add-repo \
            https://developer.download.nvidia.com/compute/cuda/repos/rhel9/x86_64/cuda-rhel9.repo
    fi
    dnf install -y cuda-toolkit-12-8
    export CUDA_HOME=/usr/local/cuda-12.8
    export PATH="$CUDA_HOME/bin:$PATH"
    export LD_LIBRARY_PATH="$CUDA_HOME/lib64:${LD_LIBRARY_PATH:-}"
}

# Common setup for build workflows (environment + system deps + rust)
setup_build_environment() {
    local python_version=${1:-3.10}
    setup_conda_environment "${python_version}"
    install_system_dependencies
    install_node
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
# Usage: run_test_groups
#
# Tests are executed in 10 sequential groups with process cleanup
# between runs.
run_test_groups() {
  set +e
  local test_results_dir="${RUNNER_TEST_RESULTS_DIR:-test-results}"
  # Make sure the runtime linker uses the conda env's libstdc++
  # (which was used to compile monarch) instead of the system's.
  # TODO: Revisit this to determine if this is the proper/most
  # sustainable/most robust solution.
  export CONDA_LIBSTDCPP="${CONDA_PREFIX}/lib/libstdc++.so.6"
  export LD_PRELOAD="${CONDA_LIBSTDCPP}${LD_PRELOAD:+:$LD_PRELOAD}"
  # Backtraces help with debugging remotely.
  export RUST_BACKTRACE=1
  local FAILED_GROUPS=()
  local TEST_EXIT_CODE=0
  local MAX_RETRIES=2
  for GROUP in $(seq 1 10); do
    local ATTEMPT=1
    local GROUP_PASSED=0
    while [[ $ATTEMPT -le $((MAX_RETRIES + 1)) ]]; do
      if [[ $ATTEMPT -gt 1 ]]; then
        echo "Retrying test group $GROUP (attempt $ATTEMPT/$((MAX_RETRIES + 1)))..."
      else
        echo "Running test group $GROUP of 10..."
      fi
      # Kill any existing Python processes to ensure clean state
      echo "Cleaning up Python processes before group $GROUP..."
      pkill -9 python || true
      pkill -9 pytest || true
      sleep 2
      LC_ALL=C pytest python/tests/ -s -v -m "not oss_skip" \
          --ignore-glob="**/meta/**" \
          --dist=no \
          --group="$GROUP" \
          --junit-xml="$test_results_dir/test-results-$GROUP.xml" \
          --splits=10 \
          --reruns=2
      TEST_EXIT_CODE=$?
      if [[ $TEST_EXIT_CODE -eq 0 ]]; then
        echo "✓ Test group $GROUP completed successfully"
        GROUP_PASSED=1
        break
      else
        echo "✗ Test group $GROUP failed with exit code $TEST_EXIT_CODE (attempt $ATTEMPT/$((MAX_RETRIES + 1)))"
      fi
      ATTEMPT=$((ATTEMPT + 1))
    done
    if [[ $GROUP_PASSED -eq 0 ]]; then
      FAILED_GROUPS+=("$GROUP")
      echo "✗ Test group $GROUP failed after $((MAX_RETRIES + 1)) attempts"
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
