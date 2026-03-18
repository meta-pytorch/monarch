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

# --- Client env (x86) ---
echo ""
echo "--- [1/4] Setting up x86 client env ---"
mkdir -p "$DEST/client"
fbpkg fetch monarch_conda:latest_conveyor_build -d "$DEST/client"

CLIENT_PIP="$DEST/client/conda/bin/python3.12 -m pip"
$CLIENT_PIP install fire
X86_WHL=$(buck2 build @fbcode//mode/opt \
    --show-full-simple-output \
    fbcode//monarch/python/monarch:monarch.whl)
$CLIENT_PIP install --force-reinstall --no-deps "$X86_WHL"

# --- Worker env ---
echo ""
echo "--- [2/4] Setting up $TARGET worker env ($WORKER_ARCH) ---"
mkdir -p "$DEST/worker"
fbpkg fetch "$WORKER_BASE_PKG" -d "$DEST/worker"

echo "--- [3/4] Building and installing $WORKER_ARCH monarch wheel ---"
# shellcheck disable=SC2086
WORKER_WHL=$(buck2 build @fbcode//mode/opt $WORKER_BUCK_ARGS \
    --show-full-simple-output \
    "$WORKER_WHL_TARGET")

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

echo "--- [4/4] Fixing worker entrypoint ---"
cat > "$DEST/worker/conda/bin/entrypoint.sh" << 'ENTRY'
#!/bin/bash
set -eEx
export PYTHONDONTWRITEBYTECODE=1
export PATH="${CONDA_DIR}/bin:$PATH"

LIBCUDA=""
for p in /usr/local/fbcode/platform010/lib /usr/local/fbcode/platform010-aarch64/lib; do
    if [ -f "$p/libcuda.so" ]; then
        LIBCUDA="$p/libcuda.so"
        break
    fi
done
if [ -n "$LIBCUDA" ]; then
    export LIBCUDA_DIR="${LIBCUDA%/*}"
    export TRITON_LIBCUDA_PATH="$LIBCUDA_DIR"
    export LD_PRELOAD="$LIBCUDA:$LIBCUDA_DIR/libnvidia-ml.so${PRELOAD_PATH:+:$PRELOAD_PATH}"
fi

export LD_LIBRARY_PATH="${CONDA_DIR}/lib:${CONDA_DIR}/lib/python3.12/site-packages/torch/lib${LIBCUDA_DIR:+:$LIBCUDA_DIR}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
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
