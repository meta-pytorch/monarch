# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

source "${GITHUB_WORKSPACE}/${REPOSITORY}/packaging/binary_build_env.sh"

: "${CU_VERSION:?CU_VERSION must be set by test-infra}"
export PIP_INSTALL_TORCH="pip install torch==2.13.0 --index-url https://download.pytorch.org/whl/${CU_VERSION}"
