#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set -euxo pipefail

base_version="${1:-}"
if [[ -z "${base_version}" ]]; then
  base_version="0.7.0.dev$(date +'%Y%m%d')"
fi

if [[ "${base_version}" == *+* || -z "${CU_VERSION:-}" || "${CU_VERSION}" == "cpu" ]]; then
  export MONARCH_VERSION="${base_version}"
else
  export MONARCH_VERSION="${base_version}+${CU_VERSION}"
fi

python setup.py bdist_wheel
ls -la dist/
