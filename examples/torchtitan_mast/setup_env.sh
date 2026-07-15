#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Build the monarch client venv, the torchtitan workspace .venv, and the
# monarch wheel, then pip-install this example dir editable into the client
# venv. The slim worker (bootstrap) fbpkg is built on demand by
# simple_mast_job.build_bootstrap, which reuses the wheel this script caches
# in /tmp/monarch_bootstrap_$USER/wheel.
# H100 x86, OSS tools only (uv + pip + maturin/setuptools-rust; no buck).
# --force wipes and rebuilds.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONARCH_SRC="$(cd "$HERE/../.." && pwd)"
PLATFORM="${PLATFORM:-/usr/local/fbcode/platform010}"
PYTHON="$PLATFORM/bin/python3.12"

ROOT="${ROOT:-$HOME/monarch_bench_envs}"
CLIENT="$ROOT/client"
WHEEL_DIR="${WHEEL_DIR:-/tmp/torchtitan_mast_monarch_wheel.$$}"
# build_bootstrap reuses a wheel from this cache dir (skips its own build).
BOOTSTRAP_WHEEL_DIR="/tmp/monarch_bootstrap_${USER}/wheel"
WORKSPACE="${TITAN_WORKSPACE:-$HOME/dev/titan_workspace}"
TORCHTITAN="${TITAN_TORCHTITAN:-$HOME/dev/torchtitan}"
TORCH_SPEC="${TITAN_TORCH_SPEC:-torch}"
TORCH_INDEX="${TITAN_TORCH_INDEX:-https://download.pytorch.org/whl/cu128}"
HF_ASSETS="${TITAN_HF_ASSETS:-Qwen/Qwen3-1.7B}"

[[ "${1:-}" =~ ^(-f|--force)$ ]] && { rm -rf "$CLIENT" "$WORKSPACE/.venv" "$WHEEL_DIR"; }
[ -x "$PYTHON" ]                 || { echo "ERROR: $PYTHON not found" >&2; exit 1; }
command -v uv >/dev/null         || { echo "ERROR: uv not on PATH" >&2; exit 1; }
[ -f "$MONARCH_SRC/setup.py" ]   || { echo "ERROR: no setup.py at $MONARCH_SRC" >&2; exit 1; }
[ -d "$TORCHTITAN" ]             || { echo "ERROR: TITAN_TORCHTITAN=$TORCHTITAN missing" >&2; exit 1; }
mkdir -p "$CLIENT" "$WHEEL_DIR" "$WORKSPACE" "$BOOTSTRAP_WHEEL_DIR"

# [1] Build the monarch wheel from $MONARCH_SRC (USE_TENSOR_ENGINE=0 -> slim,
# no torch/nccl; the `rdma` cargo feature is on by default in setup.py so
# monarch.rdma is still registered).
"$PYTHON" -m venv --clear "$CLIENT"
"$CLIENT/bin/python3.12" -m pip install --upgrade --disable-pip-version-check \
    libclang numpy "setuptools>=64" setuptools-rust wheel
( cd "$MONARCH_SRC" && \
    uv lock --check-exists && \
    USE_TENSOR_ENGINE=0 \
    LIBCLANG_PATH="$CLIENT/lib/python3.12/site-packages/clang/native" \
    BINDGEN_EXTRA_CLANG_ARGS="-I/usr/lib/gcc/x86_64-redhat-linux/11/include" \
    UV_FROZEN=1 uv build --wheel --no-build-isolation \
        --python "$CLIENT/bin/python3.12" --out-dir "$WHEEL_DIR" )
# Cache the freshly-built wheel where simple_mast_job.build_bootstrap looks
# first, so `monarch apply` reuses it instead of rebuilding the slim wheel.
rm -f "$BOOTSTRAP_WHEEL_DIR"/*.whl
cp -f "$WHEEL_DIR"/*.whl "$BOOTSTRAP_WHEEL_DIR/"

# [2] Client venv
"$CLIENT/bin/python3.12" -m pip install "$WHEEL_DIR"/*.whl fire
cp -f "$PLATFORM/lib/libomp.so" "$CLIENT/lib/"

# [3] Workspace: uv .venv with monarch + torch + (editable) torchtitan +
# extras, plus a symlink to your local torchtitan checkout. monarch is
# installed here too (from the same wheel as the client venv and the
# bootstrap fbpkg) so workers use this venv's python end-to-end -- no
# PYTHONPATH bridging between a slim worker python and the mounted workspace
# site-packages.
[ -e "$WORKSPACE/torchtitan" ] || ln -s "$TORCHTITAN" "$WORKSPACE/torchtitan"
[ -d "$WORKSPACE/.venv" ] || uv venv --python "$PYTHON" "$WORKSPACE/.venv"
VENV_PY="$WORKSPACE/.venv/bin/python3.12"
VIRTUAL_ENV="$WORKSPACE/.venv" uv pip install --upgrade --python "$VENV_PY" \
    --index "$TORCH_INDEX" "$TORCH_SPEC"
VIRTUAL_ENV="$WORKSPACE/.venv" uv pip install --python "$VENV_PY" \
    "$WHEEL_DIR"/*.whl -e "$WORKSPACE/torchtitan" psutil tqdm tomli
# HF tokenizer/config download (Xet CAS endpoint isn't reachable from Meta devvms).
IFS=',' read -r -a _hf <<< "$HF_ASSETS"
for _r in "${_hf[@]}"; do
    [ -f "$TORCHTITAN/assets/hf/${_r##*/}/tokenizer.json" ] && continue
    HF_HUB_DISABLE_XET="${HF_HUB_DISABLE_XET:-1}" VIRTUAL_ENV="$WORKSPACE/.venv" \
        "$VENV_PY" "$TORCHTITAN/scripts/download_hf_assets.py" \
        --repo_id "$_r" --assets tokenizer config --local_dir "$TORCHTITAN/assets/hf/"
done

# [4] Editable install of this dir into the client venv.
"$CLIENT/bin/python3.12" -m pip install --disable-pip-version-check -e "$HERE"

echo
echo "=== Done ==="
PATHW=0
for p in "$WHEEL_DIR" "$CLIENT" "$WORKSPACE" "$BOOTSTRAP_WHEEL_DIR"; do (( ${#p} > PATHW )) && PATHW=${#p}; done
for entry in "Monarch wheel:|$WHEEL_DIR" "Client venv:|$CLIENT" "Workspace:|$WORKSPACE" "Bootstrap wheel:|$BOOTSTRAP_WHEEL_DIR"; do
    printf '  %-16s %-*s  (%s)\n' "${entry%%|*}" "$PATHW" "${entry#*|}" \
        "$(du -sh "${entry#*|}" 2>/dev/null | cut -f1)"
done
command -v monarch >/dev/null && [[ "$(command -v monarch)" == "$CLIENT/bin/monarch" ]] || \
    echo "  PATH: export PATH=\"$CLIENT/bin:\$PATH\""
