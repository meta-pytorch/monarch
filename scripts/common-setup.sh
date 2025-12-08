#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Common setup functions for Monarch CI workflows

set -ex


# Setup Python in manylinux container
setup_manylinux_python() {
    local python_version=${1:-3.10}
    echo "Setting up manylinux Python ${python_version}..."

    # Map Python version to manylinux cp version
    case "${python_version}" in
        3.10) PYTHON_DIR="/opt/python/cp310-cp310" ;;
        3.11) PYTHON_DIR="/opt/python/cp311-cp311" ;;
        3.12) PYTHON_DIR="/opt/python/cp312-cp312" ;;
        3.13) PYTHON_DIR="/opt/python/cp313-cp313" ;;
        *) echo "Unsupported Python version: ${python_version}"; return 1 ;;
    esac

    # Check if manylinux Python directory exists
    if [[ ! -d "${PYTHON_DIR}" ]]; then
        echo "WARNING: Manylinux Python directory ${PYTHON_DIR} not found"
        echo "Falling back to system Python..."

        # Use system Python instead
        if command -v python${python_version} >/dev/null 2>&1; then
            export PATH="$(dirname $(which python${python_version})):${PATH}"
            export PYTHON_BIN="python${python_version}"
        elif command -v python3 >/dev/null 2>&1; then
            export PYTHON_BIN="python3"
        else
            echo "ERROR: No Python found"
            return 1
        fi

        # Store Python lib dir for later use
        export PYTHON_LIB_DIR=$(${PYTHON_BIN} -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))")

        echo "Using system Python: $(${PYTHON_BIN} --version)"
        echo "Python library: ${PYTHON_LIB_DIR}"
        ${PYTHON_BIN} -m pip install --upgrade pip
        return
    fi

    export PATH="${PYTHON_DIR}/bin:${PATH}"
    export PYTHON_BIN="${PYTHON_DIR}/bin/python"
    export PYTHON_LIB_DIR="${PYTHON_DIR}/lib"

    # Tell PyO3 which Python to use (but don't modify LD_LIBRARY_PATH yet!)
    export PYO3_PYTHON="${PYTHON_DIR}/bin/python"
    export PYTHON_SYS_EXECUTABLE="${PYTHON_DIR}/bin/python"

    echo "Using Python from: ${PYTHON_DIR}"
    echo "Python library path: ${PYTHON_LIB_DIR}"
    python --version

    # Verify libpython exists
    if ls "${PYTHON_LIB_DIR}/libpython${python_version}"* 2>/dev/null; then
        echo "✓ Found libpython${python_version}"
    else
        echo "⚠ Warning: libpython${python_version} not found in ${PYTHON_LIB_DIR}"
        ls -la "${PYTHON_LIB_DIR}/" | grep python || true
    fi

    python -m pip install --upgrade pip
}

# Install system-level dependencies
install_system_dependencies() {
    echo "Installing system dependencies..."

    # Check if package manager is available (not in all manylinux containers)
    if command -v dnf >/dev/null 2>&1; then
        dnf update -y
        # Protobuf compiler is required for the tracing-perfetto-sdk-schema crate.
        dnf install clang-devel libunwind libunwind-devel protobuf-compiler -y
    elif command -v yum >/dev/null 2>&1; then
        yum update -y
        yum install clang-devel libunwind libunwind-devel protobuf-compiler -y
    else
        echo "Warning: No package manager (dnf/yum) available, skipping system dependencies"
    fi
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

    # We amend the RUSTFLAGS here because they have already been altered by `setup_cuda_environment`
    # (and a few other places); RUSTFLAGS environment variable overrides the definition in
    # .cargo/config.toml.
    export RUSTFLAGS="--cfg tracing_unstable ${RUSTFLAGS:-}"
}

# Install Python test dependencies
install_python_test_dependencies() {
    echo "Installing test dependencies..."
    pip install -r python/tests/requirements.txt

    # Install rsync if package manager is available
    if command -v dnf >/dev/null 2>&1; then
        dnf install -y rsync # required for code sync tests
    elif command -v yum >/dev/null 2>&1; then
        yum install -y rsync
    else
        echo "Warning: No package manager available, skipping rsync install"
    fi
}

# Install wheel from artifact directory
install_wheel_from_artifact() {
    echo "Installing wheel from artifact..."
    pip install "${RUNNER_ARTIFACT_DIR}"/*.whl
}

# Setup and install dependencies for Tensor Engine
setup_tensor_engine() {
    echo "Installing Tensor Engine dependencies..."

    # Install fmt library via pip (works in manylinux containers)
    echo "Installing fmt via pip..."
    pip install libfmt || echo "Warning: libfmt not available via pip, build may need system fmt"

    # Install RDMA libraries if package manager is available
    if command -v dnf >/dev/null 2>&1; then
        dnf install -y libibverbs rdma-core libmlx5 libibverbs-devel rdma-core-devel
    elif command -v yum >/dev/null 2>&1; then
        yum install -y libibverbs rdma-core libmlx5 libibverbs-devel rdma-core-devel
    else
        echo "Warning: No package manager available, skipping RDMA dependencies"
    fi
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

# Setup build environment - uses manylinux Python (CI standard)
# For local development, run inside the manylinux Docker container or use pyenv/system Python
setup_build_environment() {
    local python_version=${1:-3.10}

    # IMPORTANT: Install system dependencies FIRST, before we modify library paths
    # Otherwise dnf/yum will break when we set LD_LIBRARY_PATH
    install_system_dependencies
    setup_rust_toolchain

    # Set up Python - manylinux Python for wheel building
    setup_manylinux_python "${python_version}"

    # ALSO install conda for Rust binaries that need libpython.a (like process_allocator)
    # PyO3 needs libpython-static to build Rust binaries that embed Python
    # Manylinux Python doesn't provide this by design
    if [[ ! -n "${CONDA_PYTHON_LIB:-}" ]] || [[ ! -d "${CONDA_PYTHON_LIB:-}" ]]; then
        echo "Installing conda for libpython-static (needed by Rust binaries)..."
        install_conda "${python_version}"

        # Verify the export worked
        echo "After install_conda:"
        echo "  CONDA_PREFIX=${CONDA_PREFIX:-<not set>}"
        echo "  CONDA_PYTHON_LIB=${CONDA_PYTHON_LIB:-<not set>}"
    else
        echo "Conda already available at: ${CONDA_PYTHON_LIB}"
    fi
}

# Install minimal conda ONLY for libpython static library
# This is needed for Rust binaries that embed Python (like process_allocator)
# Manylinux Python doesn't provide libpython.a by design
# PyO3 requires libpython.a (static) to build Rust binaries that embed Python
install_conda() {
    local python_version=${1:-3.10}

    # Check if CONDA_PYTHON_LIB is already set and valid
    if [[ -n "${CONDA_PYTHON_LIB:-}" ]] && [[ -d "${CONDA_PYTHON_LIB}" ]]; then
        echo "Conda already configured at: ${CONDA_PYTHON_LIB}"
        return
    fi

    # Check if conda is pre-installed (e.g., in almalinux-builder image)
    if command -v conda >/dev/null 2>&1; then
        echo "Found existing conda installation"

        # Check if there's already a conda environment with Python
        if [[ -d "/opt/conda/lib" ]]; then
            # Use base conda environment (almalinux-builder case)
            export CONDA_PREFIX="/opt/conda"
            export CONDA_PYTHON_LIB="/opt/conda/lib"

            echo "✓ Using pre-installed conda at: ${CONDA_PYTHON_LIB}"
            ls -la "${CONDA_PYTHON_LIB}"/libpython*.so* 2>/dev/null || true
            return
        fi
    fi

    echo "Installing miniconda for libpython.so..."

    # Download and install miniconda
    MINICONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-$(uname -m).sh"
    wget -q "$MINICONDA_URL" -O /tmp/miniconda.sh
    bash /tmp/miniconda.sh -b -p /opt/conda
    rm /tmp/miniconda.sh

    # Create environment with matching Python version AND libpython-static
    # libpython-static provides libpython.a which PyO3 needs for Rust binaries that embed Python
    # Use full path to avoid needing to modify PATH
    /opt/conda/bin/conda create -n libpython python="${python_version}" "libpython-static=${python_version}" -y

    # Set paths directly - don't rely on conda activate in CI
    # IMPORTANT: Don't add conda to PATH - it will interfere with system compilers
    export CONDA_PREFIX="/opt/conda/envs/libpython"
    export CONDA_PYTHON_LIB="${CONDA_PREFIX}/lib"

    # Verify libpython exists (both .so and .a)
    echo "Checking for libpython files in ${CONDA_PYTHON_LIB}:"
    ls -la "${CONDA_PYTHON_LIB}"/libpython*.so* "${CONDA_PYTHON_LIB}"/libpython*.a 2>/dev/null || true

    if ls "${CONDA_PYTHON_LIB}"/libpython*.a >/dev/null 2>&1; then
        echo "✓ Conda installed with libpython-static at: ${CONDA_PYTHON_LIB}"
    else
        echo "WARNING: libpython.a not found in ${CONDA_PYTHON_LIB}"
    fi
}

# Helper function to run commands with Python/CUDA library paths set
# This avoids polluting the entire environment and breaking system tools
with_build_env() {
    # Build the library path based on what's been configured
    local lib_paths=()

    # Add Python lib if configured
    if [[ -n "${PYTHON_LIB_DIR:-}" ]]; then
        lib_paths+=("${PYTHON_LIB_DIR}")
    fi

    # Add conda lib if available (for libpython.so needed by Rust binaries)
    if [[ -n "${CONDA_PYTHON_LIB:-}" ]] && [[ -d "${CONDA_PYTHON_LIB}" ]]; then
        lib_paths+=("${CONDA_PYTHON_LIB}")
    fi

    # Add CUDA lib if configured
    if [[ -n "${CUDA_LIB_DIR:-}" ]]; then
        lib_paths+=("${CUDA_LIB_DIR}")
    fi

    # Add system paths based on architecture
    case "$(uname -m)" in
        x86_64)
            lib_paths+=("/lib64" "/usr/lib64" "/usr/lib/x86_64-linux-gnu")
            ;;
        aarch64|arm64)
            lib_paths+=("/usr/lib" "/lib" "/usr/lib/aarch64-linux-gnu")
            ;;
    esac

    # Build the path string
    local lib_path_str=$(IFS=:; echo "${lib_paths[*]}")

    # Build RUSTFLAGS preserving .cargo/config.toml settings
    local rustflags="--cfg tracing_unstable"
    for lib_path in "${lib_paths[@]}"; do
        rustflags="$rustflags -L native=$lib_path"
    done
    rustflags="$rustflags -C link-args=-Wl,-rpath,\$ORIGIN/../lib"

    # Run the command with library paths set
    # IMPORTANT: Don't use 'env' - it clears the environment and loses PYO3_PYTHON!
    echo "Running with build environment:"
    echo "  LIBRARY_PATH=${lib_path_str}"
    echo "  LD_LIBRARY_PATH=${lib_path_str}"
    echo "  PYO3_PYTHON=${PYO3_PYTHON:-<not set>}"
    echo "  CONDA_PYTHON_LIB=${CONDA_PYTHON_LIB:-<not set>}"

    # List libpython files in conda lib if available
    if [[ -n "${CONDA_PYTHON_LIB:-}" ]] && [[ -d "${CONDA_PYTHON_LIB}" ]]; then
        echo "  libpython files in conda:"
        ls -la "${CONDA_PYTHON_LIB}"/libpython* 2>/dev/null || echo "    (none found)"
    fi

    # Set the sysconfigdata file for PyO3 based on architecture
    # Conda provides multiple sysconfigdata files and PyO3 needs to know which to use
    # We set this here (not globally) to avoid breaking system Python
    local sysconfigdata_name=""
    case "$(uname -m)" in
        x86_64)
            sysconfigdata_name="_sysconfigdata_x86_64_conda_linux_gnu"
            ;;
        aarch64|arm64)
            sysconfigdata_name="_sysconfigdata_aarch64_conda_linux_gnu"
            ;;
    esac

    if [[ -n "${sysconfigdata_name}" ]]; then
        echo "  _PYTHON_SYSCONFIGDATA_NAME=${sysconfigdata_name}"
    fi

    LIBRARY_PATH="${lib_path_str}" \
    LD_LIBRARY_PATH="${lib_path_str}" \
    RUSTFLAGS="${rustflags}" \
    PYO3_CROSS_LIB_DIR="${CONDA_PYTHON_LIB:-}" \
    _PYTHON_SYSCONFIGDATA_NAME="${sysconfigdata_name}" \
    "$@"
}

# Detect and configure CUDA environment for linking
# This function only STORES the CUDA path, it doesn't modify environment
# Use with_build_env to actually set library paths when building
setup_cuda_environment() {
    echo "Setting up CUDA environment..."

    # Detect system architecture
    ARCH=$(uname -m)
    echo "Detected architecture: $ARCH"

    # Detect CUDA installation
    DETECTED_CUDA_HOME=""
    if command -v nvcc >/dev/null 2>&1; then
        DETECTED_CUDA_HOME=$(dirname $(dirname $(which nvcc)))
    elif [ -d "/usr/local/cuda" ]; then
        DETECTED_CUDA_HOME="/usr/local/cuda"
    fi

    # Set CUDA_LIB_DIR (resolve symlinks if needed)
    # Try architecture-specific paths first
    if [ -n "$DETECTED_CUDA_HOME" ]; then
        # For x86_64, try lib64 first, then lib
        # For aarch64, try lib first, then check targets/sbsa-linux or targets/aarch64-linux
        CUDA_LIB_CANDIDATES=()
        case "$ARCH" in
            x86_64)
                CUDA_LIB_CANDIDATES=("$DETECTED_CUDA_HOME/lib64" "$DETECTED_CUDA_HOME/lib" "$DETECTED_CUDA_HOME/targets/x86_64-linux/lib")
                ;;
            aarch64|arm64)
                CUDA_LIB_CANDIDATES=("$DETECTED_CUDA_HOME/lib" "$DETECTED_CUDA_HOME/targets/sbsa-linux/lib" "$DETECTED_CUDA_HOME/targets/aarch64-linux/lib" "$DETECTED_CUDA_HOME/lib64")
                ;;
            *)
                CUDA_LIB_CANDIDATES=("$DETECTED_CUDA_HOME/lib64" "$DETECTED_CUDA_HOME/lib")
                ;;
        esac

        for CUDA_LIB_CANDIDATE in "${CUDA_LIB_CANDIDATES[@]}"; do
            if [ -d "$CUDA_LIB_CANDIDATE" ]; then
                export CUDA_LIB_DIR=$(readlink -f "$CUDA_LIB_CANDIDATE")
                break
            fi
        done

        # Fallback if none found
        if [ -z "$CUDA_LIB_DIR" ]; then
            export CUDA_LIB_DIR="/usr/local/cuda/lib64"
            echo "Warning: Could not find CUDA lib directory, using fallback: $CUDA_LIB_DIR"
        fi
    else
        export CUDA_LIB_DIR="/usr/local/cuda/lib64"
        echo "Warning: CUDA installation not detected, using fallback: $CUDA_LIB_DIR"
    fi

    echo "✓ CUDA environment configured (CUDA_LIB_DIR: $CUDA_LIB_DIR)"
    echo "  Architecture: $ARCH"
    echo "  NOTE: Library paths will be set when using with_build_env command"
}

# Common setup for test workflows (environment only)
setup_test_environment() {
    local python_version=${1:-3.10}
    setup_manylinux_python "${python_version}"
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
  # Make sure the runtime linker uses a compatible libstdc++
  # (which was used to compile monarch) instead of the system's.
  # TODO: Revisit this to determine if this is the proper/most
  # sustainable/most robust solution.

  # Check manylinux Python first (CI), then conda (local dev)
  LIBSTDCPP_PATH=""
  if [[ -n "${PYTHON_DIR:-}" ]] && [[ -f "${PYTHON_DIR}/lib/libstdc++.so.6" ]]; then
    LIBSTDCPP_PATH="${PYTHON_DIR}/lib/libstdc++.so.6"
  elif [[ -n "${CONDA_PREFIX:-}" ]] && [[ -f "${CONDA_PREFIX}/lib/libstdc++.so.6" ]]; then
    LIBSTDCPP_PATH="${CONDA_PREFIX}/lib/libstdc++.so.6"
  fi

  if [[ -n "$LIBSTDCPP_PATH" ]]; then
    export LD_PRELOAD="${LIBSTDCPP_PATH}${LD_PRELOAD:+:$LD_PRELOAD}"
    echo "Using libstdc++ from: ${LIBSTDCPP_PATH}"
  else
    echo "No custom libstdc++ found, using system libstdc++"
  fi
  # Backtraces help with debugging remotely.
  export RUST_BACKTRACE=1
  local FAILED_GROUPS=()
  for GROUP in $(seq 1 10); do
    echo "Running test group $GROUP of 10..."
    # Kill any existing Python processes to ensure clean state
    echo "Cleaning up Python processes before group $GROUP..."
    pkill -9 python || true
    pkill -9 pytest || true
    sleep 2
    if [[ "$enable_actor_error_test" == "1" ]]; then
        LC_ALL=C pytest python/tests/ -s -v -m "not oss_skip" \
            --ignore-glob="**/meta/**" \
            --dist=no \
            --group="$GROUP" \
            --junit-xml="$test_results_dir/test-results-$GROUP.xml" \
            --splits=10
    else
        LC_ALL=C pytest python/tests/ -s -v -m "not oss_skip" \
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

# Retag wheels with manylinux platform tags
#
# When building wheels with `setup.py bdist_wheel`, the wheel is tagged with the
# generic platform tag (e.g., "linux_x86_64", "linux_aarch64"). However, PyPI
# requires proper manylinux tags (e.g., "manylinux2014_x86_64") to indicate
# compatibility with the manylinux standard. Simply renaming the .whl file is
# insufficient because the platform tag is also stored in the WHEEL metadata file
# inside the wheel archive.
#
# This function properly retags wheels by:
# 1. Unpacking the wheel archive
# 2. Modifying the "Tag:" field in the .dist-info/WHEEL metadata file
# 3. Repacking the wheel with the updated metadata and correct filename
#
# The `wheel pack` command automatically regenerates the RECORD file with updated
# hashes, ensuring wheel integrity. This is similar to how PyTorch does it in their
# manywheel build scripts (see pytorch/.ci/manywheel/build_common.sh).
#
# Usage: retag_wheel_platform <platform_tag> [wheel_dir]
#   platform_tag: Target platform (e.g., "manylinux2014_x86_64", "manylinux2014_aarch64")
#   wheel_dir: Directory containing wheels (defaults to "dist")
#
# Example:
#   retag_wheel_platform "manylinux2014_x86_64"
#   retag_wheel_platform "manylinux2014_aarch64" "build/wheels"
retag_wheel_platform() {
    local platform_tag="${1}"
    local wheel_dir="${2:-dist}"

    if [[ -z "$platform_tag" ]]; then
        echo "Error: platform_tag is required"
        echo "Usage: retag_wheel_platform <platform_tag> [wheel_dir]"
        return 1
    fi

    if [[ ! -d "$wheel_dir" ]]; then
        echo "Error: wheel directory '$wheel_dir' does not exist"
        return 1
    fi

    echo "Retagging wheels in '$wheel_dir' with platform tag: $platform_tag"

    # Install wheel tool if not present
    pip install -q wheel

    local wheel_count=0
    for whl in "$wheel_dir"/*.whl; do
        if [[ ! -f "$whl" ]]; then
            continue
        fi

        wheel_count=$((wheel_count + 1))
        echo "  Processing: $(basename "$whl")"

        # Unpack the wheel
        wheel unpack "$whl" -d "$wheel_dir"
        local whl_dir=$(find "$wheel_dir" -maxdepth 1 -type d -name "$(basename "$whl" .whl)" -print -quit)

        if [[ -n "$whl_dir" && -d "$whl_dir" ]]; then
            # Find and modify the WHEEL metadata file
            local wheel_file=$(find "$whl_dir" -name "WHEEL" -type f)

            if [[ -f "$wheel_file" ]]; then
                echo "    Updating WHEEL metadata: $wheel_file"

                # Replace platform tag based on target
                case "$platform_tag" in
                    manylinux*_x86_64)
                        sed -i 's/Tag:.*linux_x86_64/Tag: py3-none-'"$platform_tag"'/g' "$wheel_file"
                        ;;
                    manylinux*_aarch64)
                        sed -i 's/Tag:.*linux_aarch64/Tag: py3-none-'"$platform_tag"'/g' "$wheel_file"
                        ;;
                    *)
                        echo "    Warning: Unknown platform tag pattern '$platform_tag', attempting generic replacement"
                        sed -i 's/Tag: \(.*\)-linux_[^-]*/Tag: \1-'"$platform_tag"'/g' "$wheel_file"
                        ;;
                esac
            else
                echo "    Warning: WHEEL file not found in unpacked wheel"
            fi

            # Repack the wheel with new platform tag
            echo "    Repacking wheel..."
            wheel pack "$whl_dir" -d "$wheel_dir" >/dev/null

            # Clean up unpacked directory
            rm -rf "$whl_dir"
        fi

        # Remove original wheel
        rm "$whl"
        echo "    ✓ Retagged: $(basename "$whl")"
    done

    if [[ $wheel_count -eq 0 ]]; then
        echo "Warning: No wheels found in '$wheel_dir'"
        return 1
    fi

    echo "✓ Successfully retagged $wheel_count wheel(s)"
    echo "Final wheels:"
    ls -lh "$wheel_dir"/*.whl
}
