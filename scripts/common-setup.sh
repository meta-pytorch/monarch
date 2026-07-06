#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Common setup functions for Monarch CI workflows

set -ex

# Retry a command with a fixed backoff. CI fetches from download.pytorch.org
# (the torch wheels and libtorch) intermittently fail the TLS handshake on the
# self-hosted runners' network path to the CDN (SSLV3_ALERT_HANDSHAKE_FAILURE);
# pip/wget exhaust their own retries inside a single bad window, so re-run the
# whole command a few times spaced far enough apart to outlast a transient one.
retry() {
    local -r -i max_attempts=5
    local -r -i delay_secs=30
    local -i attempt=1
    local -i status=0
    while (( attempt <= max_attempts )); do
        "$@" && return 0
        status=$?
        if (( attempt < max_attempts )); then
            echo "retry: attempt ${attempt}/${max_attempts} of '$*' failed (exit ${status}); retrying in ${delay_secs}s" >&2
            sleep "${delay_secs}"
        fi
        attempt=$(( attempt + 1 ))
    done
    echo "retry: '$*' failed after ${max_attempts} attempts (last exit ${status})" >&2
    return "${status}"
}

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
    # Explicitly install the pinned toolchain from rust-toolchain file.
    # Some Docker images have it registered with a corrupt/missing manifest,
    # which prevents rustup's auto-install from working.
    if [ -f rust-toolchain ]; then
        PINNED_CHANNEL=$(grep 'channel' rust-toolchain | sed 's/.*= *"\(.*\)"/\1/')
        if [ -n "$PINNED_CHANNEL" ]; then
            echo "Installing pinned toolchain: $PINNED_CHANNEL"
            rustup toolchain install "$PINNED_CHANNEL"
        fi
    fi
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
    # Skip sccache on ROCm — runners lack AWS credentials for the S3 cache backend
    if command -v rocminfo &>/dev/null; then
        echo "ROCm environment detected, skipping sccache setup"
        return
    fi

    echo "Setting up sccache..."
    if ! pip install sccache; then
        echo "Warning: failed to install sccache; continuing without Rust compiler cache"
        unset RUSTC_WRAPPER
        return
    fi

    export SCCACHE_BUCKET=ossci-compiler-cache
    export SCCACHE_REGION=us-east-1
    export SCCACHE_S3_KEY_PREFIX=monarch
    export SCCACHE_FALLBACK_DISABLE_FILE="${RUNNER_TEMP:-/tmp}/monarch-sccache-disabled"
    rm -f "${SCCACHE_FALLBACK_DISABLE_FILE}" 2>/dev/null || true

    # Validate sccache before handing it to cargo. sccache starts its server on
    # first use and verifies the remote S3 cache by reading .sccache_check; if
    # S3 returns a transient 5xx here, cargo would otherwise fail before the
    # build begins (for example while probing `rustc -vV`).
    local rustc_path
    rustc_path=$(rustup which rustc 2>/dev/null || command -v rustc || true)
    if [ -z "${rustc_path}" ]; then
        echo "Warning: could not locate rustc; continuing without Rust compiler cache"
        unset RUSTC_WRAPPER
        unset SCCACHE_BUCKET
        unset SCCACHE_REGION
        unset SCCACHE_S3_KEY_PREFIX
        unset SCCACHE_FALLBACK_DISABLE_FILE
        return
    fi
    local sccache_check_log
    sccache_check_log=$(mktemp -t monarch-sccache-check.XXXXXX)
    if ! sccache "${rustc_path}" -vV >"${sccache_check_log}" 2>&1; then
        echo "Warning: sccache failed its startup/cache check; continuing without Rust compiler cache"
        cat "${sccache_check_log}"
        rm -f "${sccache_check_log}"
        unset RUSTC_WRAPPER
        unset SCCACHE_BUCKET
        unset SCCACHE_REGION
        unset SCCACHE_S3_KEY_PREFIX
        unset SCCACHE_FALLBACK_DISABLE_FILE
        return
    fi
    rm -f "${sccache_check_log}"

    local common_setup_dir
    common_setup_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
    local sccache_wrapper="${common_setup_dir}/sccache-rustc-wrapper.sh"
    # The wrapper is committed executable (0755); ensure the bit survives odd
    # checkouts without aborting setup on a read-only mount.
    chmod +x "${sccache_wrapper}" 2>/dev/null || true
    export RUSTC_WRAPPER="${sccache_wrapper}"

    echo "sccache configured: bucket=${SCCACHE_BUCKET}, prefix=${SCCACHE_S3_KEY_PREFIX}, wrapper=${RUSTC_WRAPPER}"
}

# Install Python test dependencies
install_python_test_dependencies() {
    echo "Installing test dependencies..."
    pip install -r python/tests/requirements.txt
    dnf install -y rsync # required for code sync tests
}

# Install wheel from artifact directory.
# Moves the .whl out of RUNNER_ARTIFACT_DIR first so that linux_job_v2's
# upload-artifact step (which uploads everything in that directory) never
# re-uploads the wheel under the test-results artifact name.
install_wheel_from_artifact() {
    echo "Installing wheel from artifact..."
    local wheel_dir="${RUNNER_TEMP:-/tmp}/wheel"
    mkdir -p "$wheel_dir"
    mv -v "${RUNNER_ARTIFACT_DIR}"/*.whl "$wheel_dir/"
    pip install "$wheel_dir"/*.whl
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
# Usage: setup_pytorch_with_headers <gpu-arch-type> <gpu-arch-version> <torch-spec>
setup_pytorch_with_headers() {
    local gpu_arch_type=${1:-"cuda"}
    local gpu_arch_version=${2:-"13.2"}
    local torch_spec=${3:-"--pre torch --index-url https://download.pytorch.org/whl/nightly/cu132"}

    echo "Setting up PyTorch with C++ headers (${gpu_arch_type} ${gpu_arch_version})..."

    # Construct libtorch URL based on GPU type
    local libtorch_variant
    local libtorch_filename
    if [[ "${gpu_arch_type}" == "rocm" ]]; then
        # ROCm uses version with dots: "7.1" -> "rocm7.1"
        libtorch_variant="rocm${gpu_arch_version}"
    else
        # CUDA uses version without dots: "13.2" -> "cu132"
        libtorch_variant="cu$(echo "${gpu_arch_version}" | tr -d '.')"
    fi
    # PyTorch nightlies unified on cxx11 ABI and dropped the -cxx11-abi- infix;
    # cu130+ only publishes libtorch-shared-with-deps-latest.zip.
    libtorch_filename="libtorch-shared-with-deps-latest.zip"
    local libtorch_url="https://download.pytorch.org/libtorch/nightly/${libtorch_variant}/${libtorch_filename}"

    echo "Downloading libtorch from: ${libtorch_url}"
    retry wget -q "${libtorch_url}"
    unzip -q "${libtorch_filename}"

    # Set environment variables for libtorch
    export LIBTORCH_ROOT="$PWD/libtorch"
    export LD_LIBRARY_PATH="$LIBTORCH_ROOT/lib:${LD_LIBRARY_PATH:-}"
    export CMAKE_PREFIX_PATH="$LIBTORCH_ROOT:${CMAKE_PREFIX_PATH:-}"

    # Install PyTorch Python package using provided torch-spec
    echo "Installing PyTorch Python package with: ${torch_spec}"
    # torch_spec is a multi-word install spec (flags + index URL); it must word-split.
    # shellcheck disable=SC2086
    retry pip install ${torch_spec}

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

# Detect and configure ROCm environment for linking
setup_rocm_environment() {
    echo "Setting up ROCm environment..."

    ROCM_HOME="${ROCM_HOME:-/opt/rocm}"

    if [ -d "$ROCM_HOME" ]; then
        export PATH="$ROCM_HOME/bin:$PATH"
        export LD_LIBRARY_PATH="$ROCM_HOME/lib:$ROCM_HOME/lib64:${LD_LIBRARY_PATH:-}"
        export LIBRARY_PATH="$ROCM_HOME/lib:$ROCM_HOME/lib64:${LIBRARY_PATH:-}"
        echo "✓ ROCm environment configured (ROCM_HOME: $ROCM_HOME)"
    else
        echo "⚠ ROCm not found at $ROCM_HOME"
    fi
}

# Common setup for test workflows (environment only)
setup_test_environment() {
    local python_version=${1:-3.10}
    setup_conda_environment "${python_version}"
    install_python_test_dependencies
}

# Run Monarch Python tests with crash recovery.
# Usage: run_test_groups
#
# Tests run sequentially in a persistent worker subprocess. If the worker
# crashes mid-test, that test is recorded as failed and the worker restarts
# for the next test. All failures (including crashes) are reported at the end.
run_test_groups() {
  set +e
  local test_results_dir="${RUNNER_TEST_RESULTS_DIR:-test-results}"
  export CONDA_LIBSTDCPP="${CONDA_PREFIX}/lib/libstdc++.so.6"
  export LD_PRELOAD="${CONDA_LIBSTDCPP}${LD_PRELOAD:+:$LD_PRELOAD}"
  export RUST_BACKTRACE=1
  mkdir -p "$test_results_dir"
  LC_ALL=C pytest python/tests/ -s -v -m "not oss_skip" \
      --ignore-glob="**/meta/**" \
      --crash-recovery \
      --max-crashes=10 \
      --max-leaked-procs=16 \
      --restart-every=100 \
      --junit-xml="$test_results_dir/test-results.xml"
  local rc=$?
  set -e
  return $rc
}

# Copy cargo-nextest's JUnit XML to <dest>. Honors CARGO_TARGET_DIR
# when set, falling back to `target`.
# Logs and returns 0 when the file is missing (e.g. compile error before
# tests started); rely on upload-artifact's `if-no-files-found` to
# surface that case.
stage_nextest_junit() {
  local dest="${1:?usage: stage_nextest_junit <dest_dir>}"
  local src="${CARGO_TARGET_DIR:-target}/nextest/ci/junit.xml"
  mkdir -p "$dest"
  if [[ -f "$src" ]]; then
    cp -v "$src" "$dest/nextest-junit.xml"
  else
    echo "stage_nextest_junit: $src not found; nothing to stage" >&2
  fi
}

# Stage JUnit XML test results into RUNNER_ARTIFACT_DIR so linux_job_v2.yml
# uploads them as a workflow artifact (when the caller sets upload-artifact).
# Picks up pytest XMLs from RUNNER_TEST_RESULTS_DIR and the cargo-nextest
# junit.xml; safe to call from a workflow that only ran one of them.
stage_test_artifacts() {
  : "${RUNNER_ARTIFACT_DIR:?RUNNER_ARTIFACT_DIR must be set}"
  if [[ -d "${RUNNER_TEST_RESULTS_DIR:-test-results}" ]]; then
    cp -v "${RUNNER_TEST_RESULTS_DIR:-test-results}"/*.xml "${RUNNER_ARTIFACT_DIR}/" 2>/dev/null || true
  fi
  stage_nextest_junit "${RUNNER_ARTIFACT_DIR}"
}
