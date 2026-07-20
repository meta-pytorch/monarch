# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# This file is appended to test-infra's BUILD_ENV_FILE and sourced by later
# build steps.
export PATH="${HOME}/.cargo/bin:${PATH}"

if [[ "${CU_VERSION:-}" == rocm* ]]; then
  export MONARCH_GPU_PLATFORM=rocm
  export ROCM_HOME="${ROCM_HOME:-/opt/rocm}"
  export ROCM_PATH="${ROCM_PATH:-${ROCM_HOME}}"
  export PATH="${ROCM_HOME}/bin:${PATH}"
  export LD_LIBRARY_PATH="${ROCM_HOME}/lib:${ROCM_HOME}/lib64:${LD_LIBRARY_PATH:-}"
  export LIBRARY_PATH="${ROCM_HOME}/lib:${ROCM_HOME}/lib64:${LIBRARY_PATH:-}"
fi
