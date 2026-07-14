#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set -euxo pipefail

source scripts/common-setup.sh

install_system_dependencies

python -m pip install --upgrade pip
python -m pip install -r build-requirements.txt

if [[ -n "${CONDA_ENV:-}" ]]; then
  conda install -y -p "${CONDA_ENV}" -c conda-forge 'nodejs>=18' fmt
else
  conda install -y -c conda-forge 'nodejs>=18' fmt
fi

# libnl3-devel is needed by rdma-core's cmake to enable mlx5/efa providers
# and generate ENABLE_STATIC targets.
dnf install -y libibverbs rdma-core libmlx5 libibverbs-devel rdma-core-devel libefa libnl3-devel

setup_rust_toolchain

if [[ "${CU_VERSION:-}" == rocm* ]]; then
  setup_rocm_environment
elif ! command -v nvcc &> /dev/null; then
  setup_cuda_toolkit
fi

cargo install --path monarch_hyperactor

{
  echo ""
  echo "# Monarch binary build environment"
  echo 'export PATH="${HOME}/.cargo/bin:${PATH}"'
  if [[ -n "${CARGO_HOME:-}" ]]; then
    printf "export CARGO_HOME=%q\n" "${CARGO_HOME}"
  fi
  for var in RUSTC_WRAPPER SCCACHE_BUCKET SCCACHE_REGION SCCACHE_S3_KEY_PREFIX SCCACHE_FALLBACK_DISABLE_FILE CUDA_HOME ROCM_HOME ROCM_PATH LD_LIBRARY_PATH LIBRARY_PATH; do
    if [[ -n "${!var:-}" ]]; then
      printf "export %s=%q\n" "${var}" "${!var}"
    fi
  done
} >> "${BUILD_ENV_FILE}"
