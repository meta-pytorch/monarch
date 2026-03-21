#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Integration test for the config dump endpoint (CFG-1..CFG-5).
#
# Launches the dining philosophers example, discovers procs via JSON
# traversal, and verifies:
#   1. GET /v1/config/{worker_proc_ref} → 200, Ok envelope, sorted entries
#   2. GET /v1/config/{service_proc_ref} → 200, Ok envelope (HostAgent path)
#   3. GET /v1/config/{bogus_ref} → fast-fail error
#
# Exit codes:
#   0 — all tests passed
#   1 — at least one test failed

set -euo pipefail

echo "=== config_integration_test ==="

BIN="$1"

# --- Generate test PKI (self-signed CA + server/client cert) ---
CERTDIR=$(mktemp -d)

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout "$CERTDIR/ca.key" -out "$CERTDIR/ca.crt" \
    -days 1 -nodes -subj "/CN=test-ca" 2>/dev/null

HOSTNAME=$(hostname -f)
cat > "$CERTDIR/san.cnf" <<EOF
[req]
distinguished_name = req_dn
req_extensions = v3_req
[req_dn]
CN = localhost
[v3_req]
subjectAltName = DNS:localhost,DNS:$HOSTNAME,IP:127.0.0.1,IP:::1
EOF

openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout "$CERTDIR/server.key" -out "$CERTDIR/server.csr" \
    -nodes -subj "/CN=localhost" -config "$CERTDIR/san.cnf" 2>/dev/null
openssl x509 -req -in "$CERTDIR/server.csr" \
    -CA "$CERTDIR/ca.crt" -CAkey "$CERTDIR/ca.key" -CAcreateserial \
    -out "$CERTDIR/server.crt" -days 1 \
    -extfile "$CERTDIR/san.cnf" -extensions v3_req 2>/dev/null

cat "$CERTDIR/server.crt" "$CERTDIR/ca.crt" "$CERTDIR/server.key" \
    > "$CERTDIR/combined.pem"

export HYPERACTOR_TLS_CERT="$CERTDIR/combined.pem"
export HYPERACTOR_TLS_KEY="$CERTDIR/combined.pem"
export HYPERACTOR_TLS_CA="$CERTDIR/ca.crt"

# --- Launch dining philosophers ---
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
        ADMIN_ADDR=$(grep "Mesh admin server listening on" "$OUTFILE" \
            | head -1 | sed 's/.*listening on //')
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

# Build curl flags for mTLS.
CURL_FLAGS=(--insecure --cacert "$CERTDIR/ca.crt" \
            --cert "$CERTDIR/combined.pem" --key "$CERTDIR/combined.pem")

# --- Discover procs ---
echo "Discovering procs..."

SERVICE_PROC=""
WORKER_PROC=""
for _attempt in $(seq 1 15); do
    read -r SERVICE_PROC WORKER_PROC < <(python3 - "$ADMIN_ADDR" "$CERTDIR/combined.pem" <<'PYEOF'
import json, ssl, sys, urllib.parse, urllib.request

admin_url, certfile = sys.argv[1], sys.argv[2]

ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
ctx.load_cert_chain(certfile=certfile, keyfile=certfile)

def get(url):
    try:
        return json.loads(urllib.request.urlopen(url, context=ctx, timeout=10).read())
    except Exception as e:
        print(f"  [discovery] GET {url} failed: {e}", file=sys.stderr)
        return None

root = get(f"{admin_url}/v1/root")
if not root:
    print(" ")
    sys.exit(0)

service_proc = ""
worker_proc = ""

for host_ref in root.get("children", []):
    host = get(f"{admin_url}/v1/{urllib.parse.quote(host_ref, safe='')}")
    if not host:
        continue
    for proc_ref in host.get("children", []):
        proc = get(f"{admin_url}/v1/{urllib.parse.quote(proc_ref, safe='')}")
        if not proc:
            continue
        actor_children = proc.get("children", [])
        actor_names = []
        for actor_ref in actor_children:
            name = actor_ref.rsplit(",", 1)[-1] if "," in actor_ref else actor_ref
            base = name.split("[")[0]
            actor_names.append(base)
        if "host_agent" in actor_names:
            service_proc = proc_ref
        elif "proc_agent" in actor_names and not worker_proc:
            worker_proc = proc_ref

print(f"{service_proc} {worker_proc}")
PYEOF
    )
    if [ -n "$SERVICE_PROC" ] && [ -n "$WORKER_PROC" ]; then
        break
    fi
    echo "  attempt $_attempt: service=${SERVICE_PROC:-<none>} worker=${WORKER_PROC:-<none>}, retrying..."
    sleep 2
done

echo "Service proc: ${SERVICE_PROC:-<not found>}"
echo "Worker proc:  ${WORKER_PROC:-<not found>}"

FAILED=0

# --- Test 1: config on worker proc (ProcAgent path) ---
if [ -z "$WORKER_PROC" ]; then
    echo "FAIL: no worker proc found"
    FAILED=1
else
    echo ""
    echo "Test 1: config on worker proc..."
    ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$WORKER_PROC', safe=''))")
    HTTP_CODE=""
    BODY=""
    HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' \
        "${CURL_FLAGS[@]}" --max-time 15 \
        "$ADMIN_ADDR/v1/config/$ENCODED") || true
    BODY=$(curl -s "${CURL_FLAGS[@]}" --max-time 15 \
        "$ADMIN_ADDR/v1/config/$ENCODED" 2>/dev/null) || true

    if [ "$HTTP_CODE" = "200" ]; then
        # Validate: entries list, sorted by name, required fields present.
        if python3 -c "
import json, sys
d = json.loads('''$BODY''')
assert 'entries' in d, f'expected entries field, got keys: {list(d.keys())}'
entries = d['entries']
assert isinstance(entries, list), 'entries should be a list'
names = [e['name'] for e in entries]
assert names == sorted(names), f'entries not sorted: {names[:5]}'
# At least one CONFIG key should exist.
assert len(entries) > 0, 'expected at least one config entry'
# Verify each entry has required fields.
for e in entries:
    assert 'name' in e and 'value' in e and 'source' in e, f'missing fields in {e}'
print(f'  PASS: HTTP 200, {len(entries)} config entries, sorted')
" 2>&1; then
            :
        else
            echo "  FAIL: HTTP 200 but validation failed"
            echo "  Body: $BODY"
            FAILED=1
        fi
    else
        echo "  FAIL: expected HTTP 200, got $HTTP_CODE"
        echo "  Body: $BODY"
        FAILED=1
    fi
fi

# --- Test 2: config on service proc (HostAgent path) ---
if [ -z "$SERVICE_PROC" ]; then
    echo "FAIL: no service proc found"
    FAILED=1
else
    echo ""
    echo "Test 2: config on service proc..."
    ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$SERVICE_PROC', safe=''))")
    HTTP_CODE=""
    BODY=""
    HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' \
        "${CURL_FLAGS[@]}" --max-time 15 \
        "$ADMIN_ADDR/v1/config/$ENCODED") || true
    BODY=$(curl -s "${CURL_FLAGS[@]}" --max-time 15 \
        "$ADMIN_ADDR/v1/config/$ENCODED" 2>/dev/null) || true

    if [ "$HTTP_CODE" = "200" ]; then
        if python3 -c "
import json, sys
d = json.loads('''$BODY''')
assert 'entries' in d, f'expected entries field, got keys: {list(d.keys())}'
entries = d['entries']
assert isinstance(entries, list), 'entries should be a list'
assert len(entries) > 0, 'expected at least one config entry'
print(f'  PASS: HTTP 200, {len(entries)} config entries (HostAgent handled it)')
" 2>&1; then
            :
        else
            echo "  FAIL: HTTP 200 but validation failed"
            echo "  Body: $BODY"
            FAILED=1
        fi
    else
        echo "  FAIL: expected HTTP 200, got $HTTP_CODE"
        echo "  Body: $BODY"
        FAILED=1
    fi
fi

# --- Test 3: config on bogus proc (defensive probe fast-fail) ---
echo ""
echo "Test 3: config on bogus proc (probe fast-fail)..."
BOGUS_REF="unix:@nonexistent_bogus_socket_xyz,bogus-ffffffffffffffff"
ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$BOGUS_REF', safe=''))")
HTTP_CODE=""
BODY=""
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' \
    "${CURL_FLAGS[@]}" --max-time 5 \
    "$ADMIN_ADDR/v1/config/$ENCODED") || true
BODY=$(curl -s "${CURL_FLAGS[@]}" --max-time 5 \
    "$ADMIN_ADDR/v1/config/$ENCODED" 2>/dev/null) || true

if [ "$HTTP_CODE" != "200" ]; then
    if echo "$BODY" | grep -qE '"not_found"|"internal_error"'; then
        echo "  PASS: HTTP $HTTP_CODE, fast-fail with error code"
    else
        echo "  FAIL: HTTP $HTTP_CODE but unexpected body: $BODY"
        FAILED=1
    fi
else
    echo "  FAIL: expected non-200 for bogus proc, got HTTP 200"
    echo "  Body: $BODY"
    FAILED=1
fi

# --- Summary ---
echo ""
if [ "$FAILED" -eq 0 ]; then
    echo "=== RESULT: PASS ==="
else
    echo "=== RESULT: FAIL ==="
    exit 1
fi
