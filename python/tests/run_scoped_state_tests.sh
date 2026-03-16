#!/bin/bash
# Run the tests that use the scoped_state helper from job_utils.py

set -e

TESTS=(
    "python/tests/_monarch/test_sync_workspace.py"
    "python/tests/simulator/test_actor_mock.py"
    "python/tests/test_cuda.py"
    "python/tests/test_distributed_telemetry.py"
)

# Resolve the directory of this script so it can be run from anywhere
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$REPO_ROOT"

uv run pytest "${TESTS[@]}" "$@"
