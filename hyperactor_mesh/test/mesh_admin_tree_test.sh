#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Integration test for the mesh admin tree endpoint.
#
# Generates a self-signed CA + cert + key, launches the dining
# philosophers example with those certs (via HYPERACTOR_TLS_* env
# vars), then verifies /v1/tree and /v1/root via mTLS curl, and
# confirms that connections without a client cert are rejected.

set -euo pipefail

BIN="$1"

# --- Generate test PKI (self-signed CA + server/client cert) ---
CERTDIR=$(mktemp -d)

# CA key + cert
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout "$CERTDIR/ca.key" -out "$CERTDIR/ca.crt" \
    -days 1 -nodes -subj "/CN=test-ca" 2>/dev/null

# Server/client key + CSR + cert (signed by CA)
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout "$CERTDIR/server.key" -out "$CERTDIR/server.csr" \
    -nodes -subj "/CN=localhost" 2>/dev/null
openssl x509 -req -in "$CERTDIR/server.csr" \
    -CA "$CERTDIR/ca.crt" -CAkey "$CERTDIR/ca.key" -CAcreateserial \
    -out "$CERTDIR/server.crt" -days 1 2>/dev/null

# Combined PEM (cert + key) — matches Meta's server.pem format.
cat "$CERTDIR/server.crt" "$CERTDIR/ca.crt" "$CERTDIR/server.key" \
    > "$CERTDIR/combined.pem"

# Point hyperactor at our test certs.
export HYPERACTOR_TLS_CERT="$CERTDIR/combined.pem"
export HYPERACTOR_TLS_KEY="$CERTDIR/combined.pem"
export HYPERACTOR_TLS_CA="$CERTDIR/ca.crt"

# Launch dining philosophers in background; capture output.
OUTFILE=$(mktemp)
"$BIN" > "$OUTFILE" 2>&1 &
BIN_PID=$!

cleanup() {
    kill "$BIN_PID" 2>/dev/null || true
    wait "$BIN_PID" 2>/dev/null || true
    rm -f "$OUTFILE"
    rm -rf "$CERTDIR"
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

# Build curl flags. For HTTPS (mTLS), supply client cert and skip
# hostname verification (test cert CN=localhost may not match the
# advertised hostname).
CURL_FLAGS=(-sf)
if [[ "$ADMIN_ADDR" == https://* ]]; then
    CURL_FLAGS+=(--insecure --cacert "$CERTDIR/ca.crt" \
                 --cert "$CERTDIR/combined.pem" --key "$CERTDIR/combined.pem")
fi

# --- Test /v1/root ---
echo "Testing GET /v1/root..."
if ! ROOT_RESP=$(curl "${CURL_FLAGS[@]}" "$ADMIN_ADDR/v1/root"); then
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
if ! TREE_RESP=$(curl "${CURL_FLAGS[@]}" "$ADMIN_ADDR/v1/tree"); then
    echo "FAIL: GET /v1/tree failed"
    exit 1
fi

# Tree should contain box-drawing characters and clickable URLs.
if ! echo "$TREE_RESP" | grep -q "├── \|└── "; then
    echo "FAIL: /v1/tree missing box-drawing connectors"
    echo "$TREE_RESP"
    exit 1
fi

if ! echo "$TREE_RESP" | grep -qE " ->  https?://"; then
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

# --- Test mTLS rejection (no client cert) ---
if [[ "$ADMIN_ADDR" == https://* ]]; then
    echo "Testing mTLS rejection (no client cert)..."
    if curl -sf --insecure "$ADMIN_ADDR/v1/root" 2>/dev/null; then
        echo "FAIL: connection without client cert should be rejected"
        exit 1
    fi
    echo "  mTLS rejection: OK"
fi

echo "PASS: All mesh admin integration checks passed"
