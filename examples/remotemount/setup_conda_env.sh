#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Set up conda environments for running remoterun on MAST hosts.
#
# Creates two conda environments:
#   $DEST/client/conda  - x86 client env (runs on your devserver)
#   $DEST/worker/conda  - worker env (deployed to MAST hosts)
#
# The four slowest operations (2 fbpkg fetches + 2 buck2 builds) run in
# parallel, then installs happen sequentially after they complete.
#
# Usage:
#   bash examples/setup_conda_env.sh <target> [DEST_DIR]
#
#   target:   "gb200"/"gb300" (aarch64) or "grandteton"/"h100" (x86)
#   DEST_DIR: where to create the envs (default: ~/monarch_conda_envs)
#
# After setup, run remoterun with:
#   CONDA_PREFIX=$DEST/worker/conda \
#   $DEST/client/conda/bin/python3.12 \
#     examples/remoterun.py <source_dir> <script> \
#     --backend mast --host_type <target>

set -euo pipefail

TARGET="${1:?Usage: $0 <target> [dest_dir]  (target: gb200, gb300, grandteton, or h100)}"
DEST="${2:-$HOME/monarch_conda_envs}"

case "$TARGET" in
    gb200|gb300)
        WORKER_ARCH="aarch64"
        WORKER_BASE_PKG="xlformers_gb200_conda:latest"
        WORKER_WHL_TARGET="fbcode//monarch/python/monarch:monarch_nightly_torch_gb200_py3.12.whl"
        WORKER_BUCK_ARGS="-c fbcode.arch=aarch64"
        ;;
    grandteton|h100)
        WORKER_ARCH="x86_64"
        WORKER_BASE_PKG="monarch_conda:latest_conveyor_build"
        WORKER_WHL_TARGET="fbcode//monarch/python/monarch:monarch.whl"
        WORKER_BUCK_ARGS=""
        ;;
    *)
        echo "Unknown target: $TARGET (expected: gb200, gb300, grandteton, h100)"
        exit 1
        ;;
esac

echo "=== Setting up $TARGET remoterun environments at $DEST ==="

# --- Launch all four slow operations in parallel ---
echo ""
echo "--- Fetching base envs + building wheels (parallel) ---"

mkdir -p "$DEST/client" "$DEST/worker"

# Temp files for capturing build outputs (stderr separate to avoid polluting paths).
CLIENT_WHL_OUT=$(mktemp)
CLIENT_WHL_ERR=$(mktemp)
WORKER_WHL_OUT=$(mktemp)
WORKER_WHL_ERR=$(mktemp)

# 1. Fetch client base env
fbpkg fetch monarch_conda:latest_conveyor_build -d "$DEST/client" &
CLIENT_FETCH_PID=$!

# 2. Fetch worker base env
fbpkg fetch "$WORKER_BASE_PKG" -d "$DEST/worker" &
WORKER_FETCH_PID=$!

# 3. Build x86 client wheel
buck2 build @fbcode//mode/opt \
    --show-full-simple-output \
    fbcode//monarch/python/monarch:monarch.whl \
    > "$CLIENT_WHL_OUT" 2>"$CLIENT_WHL_ERR" &
CLIENT_BUILD_PID=$!

# 4. Build worker wheel (may be cross-arch)
# shellcheck disable=SC2086
buck2 build @fbcode//mode/opt $WORKER_BUCK_ARGS \
    --show-full-simple-output \
    "$WORKER_WHL_TARGET" \
    > "$WORKER_WHL_OUT" 2>"$WORKER_WHL_ERR" &
WORKER_BUILD_PID=$!

echo "  Fetching client env (PID $CLIENT_FETCH_PID)"
echo "  Fetching worker env (PID $WORKER_FETCH_PID)"
echo "  Building x86 wheel (PID $CLIENT_BUILD_PID)"
echo "  Building $WORKER_ARCH wheel (PID $WORKER_BUILD_PID)"

# --- Wait for all background jobs ---
FAILED=0
for pid_var in CLIENT_FETCH_PID WORKER_FETCH_PID CLIENT_BUILD_PID WORKER_BUILD_PID; do
    pid=${!pid_var}
    if ! wait "$pid"; then
        echo "Error: $pid_var (PID $pid) failed" >&2
        FAILED=1
    fi
done
if [ "$FAILED" -ne 0 ]; then
    echo "Parallel step errors:" >&2
    cat "$CLIENT_WHL_ERR" "$WORKER_WHL_ERR" >&2
    rm -f "$CLIENT_WHL_OUT" "$CLIENT_WHL_ERR" "$WORKER_WHL_OUT" "$WORKER_WHL_ERR"
    exit 1
fi

X86_WHL=$(cat "$CLIENT_WHL_OUT")
WORKER_WHL=$(cat "$WORKER_WHL_OUT")
rm -f "$CLIENT_WHL_OUT" "$CLIENT_WHL_ERR" "$WORKER_WHL_OUT" "$WORKER_WHL_ERR"

echo ""
echo "  Client wheel: $X86_WHL"
echo "  Worker wheel: $WORKER_WHL"

# --- Install client wheel ---
echo ""
echo "--- Installing client env ---"
CLIENT_PIP="$DEST/client/conda/bin/python3.12 -m pip"
$CLIENT_PIP install fire

# Remove stale dist-info from previous installs — fbpkg fetch overwrites the
# conda dir but leaves pip metadata behind, causing "No such file" errors
# when pip tries to uninstall the old wheel.
rm -rf "$DEST/client/conda/lib/python3.12/site-packages/torchmonarch"*.dist-info
$CLIENT_PIP install --force-reinstall --no-deps "$X86_WHL"

# --- Install worker wheel ---
echo ""
echo "--- Installing worker env ---"

# Remove stale dist-info from previous installs (same fbpkg clobber issue).
rm -rf "$DEST/worker/conda/lib/python3.12/site-packages/torchmonarch"*.dist-info

if [ "$WORKER_ARCH" = "aarch64" ]; then
    # Cross-arch: can't pip install directly, use --target
    $CLIENT_PIP install \
        --platform linux_aarch64 \
        --target "$DEST/worker/conda/lib/python3.12/site-packages" \
        --no-deps --only-binary :all: --force-reinstall \
        "$WORKER_WHL"

    # Extract entrypoint + bootstrap scripts from the wheel.
    python3 -c "
import zipfile, os
with zipfile.ZipFile('$WORKER_WHL') as z:
    for name in z.namelist():
        if 'torchmonarch' in name and 'data/scripts/' in name:
            dest = '$DEST/worker/conda/bin/' + os.path.basename(name)
            with z.open(name) as src, open(dest, 'wb') as dst:
                dst.write(src.read())
            os.chmod(dest, 0o755)
            print(f'  Installed {dest}')
"

    # Install xxhash for aarch64 (needed by remotemount persistent cache).
    rm -rf /tmp/xxhash_wheels
    $CLIENT_PIP download xxhash \
        --platform manylinux2014_aarch64 \
        --python-version 3.12 \
        --only-binary :all: \
        -d /tmp/xxhash_wheels 2>/dev/null
    $CLIENT_PIP install \
        --platform manylinux2014_aarch64 \
        --target "$DEST/worker/conda/lib/python3.12/site-packages" \
        --no-deps --only-binary :all: --force-reinstall \
        /tmp/xxhash_wheels/xxhash-*.whl

    # Fix permissions (pip --target doesn't preserve world-readable).
    chmod -R a+rX "$DEST/worker/conda/lib/python3.12/site-packages/monarch/" 2>/dev/null || true
    chmod -R a+rX "$DEST/worker/conda/lib/python3.12/site-packages/torchmonarch/" 2>/dev/null || true

    # Copy libomp from fbcode aarch64 toolchain.
    cp /usr/local/fbcode/platform010-aarch64/lib/libomp.so "$DEST/worker/conda/lib/" 2>/dev/null || true
else
    # Same-arch: direct pip install.
    "$DEST/worker/conda/bin/python3.12" -m pip install --force-reinstall --no-deps "$WORKER_WHL"
fi

echo ""
echo "--- Fixing worker entrypoint ---"
cat > "$DEST/worker/conda/bin/entrypoint.sh" << 'ENTRY'
#!/bin/bash
set -eEx
export PYTHONDONTWRITEBYTECODE=1
export PATH="${CONDA_DIR}/bin:$PATH"

# Find libcuda. On MAST containers the real driver is bind-mounted at
# the platform010 path by Tupperware. We must NOT add this directory to
# LD_LIBRARY_PATH (its libstdc++ conflicts with conda), so instead we
# symlink just the CUDA/NVML libraries into conda's lib dir.
LIBCUDA_DIR=""
ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
    LIBCUDA_SEARCH="/usr/local/fbcode/platform010-aarch64/lib /usr/lib64 /usr/local/cuda/lib64"
else
    LIBCUDA_SEARCH="/usr/local/fbcode/platform010/lib /usr/lib64 /usr/local/cuda/lib64"
fi
for p in $LIBCUDA_SEARCH; do
    if [ -f "$p/libcuda.so.1" ] || [ -f "$p/libcuda.so" ]; then
        LIBCUDA_DIR="$p"
        break
    fi
done
CUDA_SHIM_DIR=""
if [ -n "$LIBCUDA_DIR" ]; then
    export TRITON_LIBCUDA_PATH="$LIBCUDA_DIR"
    CUDA_SHIM_DIR=$(mktemp -d /tmp/cuda_shim.XXXXXX)
    for lib in "$LIBCUDA_DIR"/libcuda.so* "$LIBCUDA_DIR"/libnvidia-ml.so*; do
        [ -f "$lib" ] && ln -sf "$lib" "$CUDA_SHIM_DIR/" 2>/dev/null || true
    done
fi

export LD_LIBRARY_PATH="${CONDA_DIR}/lib:${CONDA_DIR}/lib/python3.12/site-packages/torch/lib${CUDA_SHIM_DIR:+:$CUDA_SHIM_DIR}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}$TORCHX_RUN_PYTHONPATH"

if [ -f "${CONDA_DIR}/bin/activate" ]; then
    source "${CONDA_DIR}/bin/activate"
fi

if [ -n "$WORKSPACE_DIR" ] && [ -d "$WORKSPACE_DIR" ]; then
    cd "$WORKSPACE_DIR"
fi

exec "$@"
ENTRY
chmod +x "$DEST/worker/conda/bin/entrypoint.sh"

# Also overwrite the copy in site-packages/bin/ that conda-pack may prefer.
cp "$DEST/worker/conda/bin/entrypoint.sh" \
   "$DEST/worker/conda/lib/python3.12/site-packages/bin/entrypoint.sh" 2>/dev/null || true

echo ""
echo "=== Done ==="
echo "  Client env: $DEST/client/conda"
echo "  Worker env: $DEST/worker/conda"
echo ""
echo "Run remoterun:"
echo "  CONDA_PREFIX=$DEST/worker/conda \\"
echo "  $DEST/client/conda/bin/python3.12 \\"
echo "    examples/remotemount/remoterun.py <source_dir> <script> \\"
echo "    --backend mast --host_type $TARGET"
