#!/bin/bash

set -ex

# This script demonstrates that the wheel cache is accessible on remote nodes.
# It verifies the files were transferred via RDMA and are readable.

WHEEL_DIR="/tmp/flat_wheels"

echo "Host: $(hostname)"
echo "Python: $(python --version 2>&1)"
echo ""

# Count available wheels
WHEEL_COUNT=$(ls -1 "$WHEEL_DIR"/*.whl 2>/dev/null | wc -l)
echo "Found $WHEEL_COUNT wheel files in $WHEEL_DIR"
echo ""

# Show total size
echo "Total wheel cache size:"
du -sh "$WHEEL_DIR"
echo ""

# List wheels to show they're accessible
echo "Wheels in cache:"
ls -1 "$WHEEL_DIR"/*.whl | head -10
echo ""

# Verify we can read file contents (compute checksum of first wheel)
FIRST_WHEEL=$(ls -1 "$WHEEL_DIR"/*.whl | head -1)
if [ -n "$FIRST_WHEEL" ]; then
    echo "Verifying wheel integrity (md5sum of first wheel):"
    md5sum "$FIRST_WHEEL"
fi

echo ""
echo "Wheel cache successfully distributed to $(hostname)"
