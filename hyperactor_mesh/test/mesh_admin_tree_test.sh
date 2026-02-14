#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Integration test for the mesh admin tree endpoint.
#
# Spins up the dining philosophers example, waits for the admin server
# to start, then verifies that GET /v1/tree and GET /v1/root return
# the expected structure.

set -euo pipefail

BIN="$1"

# Launch dining philosophers in background; capture output.
OUTFILE=$(mktemp)
"$BIN" > "$OUTFILE" 2>&1 &
BIN_PID=$!

cleanup() {
    kill "$BIN_PID" 2>/dev/null || true
    wait "$BIN_PID" 2>/dev/null || true
    rm -f "$OUTFILE"
}
trap cleanup EXIT

# Wait for the mesh admin server to print its address.
ADMIN_ADDR=""
for _i in $(seq 1 30); do
    if grep -q "Mesh admin server listening on" "$OUTFILE"; then
        ADMIN_ADDR=$(grep "Mesh admin server listening on" "$OUTFILE" | head -1 | sed 's/.*listening on //')
        break
    fi
    sleep 1
done

if [ -z "$ADMIN_ADDR" ]; then
    echo "FAIL: Mesh admin server did not start within 30 seconds"
    echo "--- output ---"
    cat "$OUTFILE"
    exit 1
fi

echo "Admin server at: $ADMIN_ADDR"

# --- Test /v1/root ---
echo "Testing GET /v1/root..."
if ! ROOT_RESP=$(curl -sf "$ADMIN_ADDR/v1/root"); then
    echo "FAIL: GET /v1/root failed"
    exit 1
fi

# Root should be JSON with identity "root" and at least one child.
if ! echo "$ROOT_RESP" | python3 -c "
import json, sys
d = json.load(sys.stdin)
assert d['identity'] == 'root', f'expected root identity, got {d[\"identity\"]}'
assert d['properties']['Root']['num_hosts'] >= 1, 'expected at least 1 host'
assert len(d['children']) >= 1, 'expected at least 1 child'
print('  root: OK (%d hosts, %d children)' % (d['properties']['Root']['num_hosts'], len(d['children'])))
"; then
    echo "FAIL: /v1/root response validation failed"
    echo "$ROOT_RESP"
    exit 1
fi

# --- Test /v1/tree ---
echo "Testing GET /v1/tree..."
if ! TREE_RESP=$(curl -sf "$ADMIN_ADDR/v1/tree"); then
    echo "FAIL: GET /v1/tree failed"
    exit 1
fi

# Tree should contain box-drawing characters and clickable URLs.
if ! echo "$TREE_RESP" | grep -q "├── \|└── "; then
    echo "FAIL: /v1/tree missing box-drawing connectors"
    echo "$TREE_RESP"
    exit 1
fi

if ! echo "$TREE_RESP" | grep -q " ->  http://"; then
    echo "FAIL: /v1/tree missing clickable URLs"
    echo "$TREE_RESP"
    exit 1
fi

# Should contain at least one philosopher proc.
if ! echo "$TREE_RESP" | grep -q "philosopher"; then
    echo "FAIL: /v1/tree missing philosopher procs"
    echo "$TREE_RESP"
    exit 1
fi

echo "  tree: OK"
echo ""
echo "--- tree output ---"
echo "$TREE_RESP"

echo "PASS: All mesh admin integration checks passed"
