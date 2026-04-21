#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Run remoterun on MAST hosts using pre-built conda environments.
#
# Prerequisites:
#   Run setup_conda_env.sh first to create the conda environments.
#
# Usage:
#   bash examples/remotemount/remoterun.sh <source_dir> <script> [extra remoterun args...]
#
# Examples:
#   bash examples/remotemount/remoterun.sh /tmp/mydir /tmp/myscript.sh
#   bash examples/remotemount/remoterun.sh /tmp/mydir /tmp/myscript.sh --num_hosts 4
#   bash examples/remotemount/remoterun.sh /tmp/mydir stdin <<< '#!/bin/bash\nhostname'
#
# Environment variables:
#   MONARCH_HOST_TYPE   Host type to use (default: gb200). Options: gb200, gb300, grandteton
#   MONARCH_CONDA_ENVS  Path to conda environments (default: ~/monarch_conda_envs)

set -euo pipefail

ENVS="${MONARCH_CONDA_ENVS:-$HOME/monarch_conda_envs}"

if [ ! -d "$ENVS/client/conda" ] || [ ! -d "$ENVS/worker/conda" ]; then
    echo "Error: conda environments not found at $ENVS"
    echo "Run setup_conda_env.sh first:"
    echo "  bash examples/remotemount/setup_conda_env.sh <target> $ENVS"
    exit 1
fi

if [ $# -lt 2 ]; then
    echo "Usage: remoterun.sh <source_dir> <script> [extra remoterun args...]"
    exit 1
fi

SOURCE_DIR="$1"
SCRIPT="$2"
shift 2

HOST_TYPE="${MONARCH_HOST_TYPE:-gb200}"

CONDA_PREFIX="$ENVS/worker/conda" \
exec "$ENVS/client/conda/bin/python3.12" \
    "$(dirname "$0")/remoterun.py" \
    "$SOURCE_DIR" "$SCRIPT" \
    --backend mast \
    --host_type "$HOST_TYPE" \
    --locality_constraints "" \
    --verbose \
    "$@"
