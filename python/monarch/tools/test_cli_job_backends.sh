#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set -euo pipefail

cli="${1:?Usage: test_cli_job_backends.sh <monarch-cli>}"

# Saved jobs are pickled with their concrete class module. Import each backend
# inside the packaged CLI to ensure every supported job can be loaded.
for module in \
    monarch._src.job.kubernetes \
    monarch._src.job.meta.mast \
    monarch._src.job.slurm \
    monarch._src.job.spmd
do
    PAR_MAIN_OVERRIDE="$module" "$cli"
done
