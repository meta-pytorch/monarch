#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Integration test for PS-12 (universal py-spy) and PS-13 (defensive probe).
#
# Launches pyspy_workload, discovers the service proc and a worker proc
# via structured JSON traversal, and verifies:
#   1. py-spy on a worker proc succeeds (ProcAgent handles PySpyDump)
#   2. py-spy on the service proc succeeds (HostAgent handles PySpyDump)
#   3. py-spy on a bogus proc fast-fails (PS-13 defensive probe)
#
# Exit codes:
#   0 — all tests passed
#   1 — at least one test failed

set -euo pipefail

echo "=== pyspy_preflight_test ==="

WORKLOAD_BIN="$1"

# --- Install py-spy ---
PYSPY_DIR=$(mktemp -d)
echo "Fetching fb-py-spy:prod..."
if fbpkg fetch fb-py-spy:prod -d "$PYSPY_DIR" 2>/dev/null; then
    export PYSPY_BIN="$PYSPY_DIR/py-spy"
    echo "py-spy installed: $PYSPY_BIN"
else
    echo "fbpkg fetch failed; falling back to system PATH"
    unset PYSPY_BIN
fi

# --- Generate test PKI ---
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
export HYPERACTOR_MESH_ADMIN_ADDR="[::]:0"

# --- Launch workload ---
OUTFILE=$(mktemp)
"$WORKLOAD_BIN" --mode cpu --work-ms 500 --concurrency 2 > "$OUTFILE" 2>&1 &
BIN_PID=$!

cleanup() {
    kill "$BIN_PID" 2>/dev/null || true
    wait "$BIN_PID" 2>/dev/null || true
    rm -f "$OUTFILE"
    rm -rf "$CERTDIR"
    rm -rf "$PYSPY_DIR"
}
trap cleanup EXIT

# Wait for admin server.
ADMIN_ADDR=""
for _i in $(seq 1 60); do
    if grep -q "Mesh admin server listening on" "$OUTFILE"; then
        ADMIN_ADDR=$(grep "Mesh admin server listening on" "$OUTFILE" \
            | head -1 | sed 's/.*listening on //')
        break
    fi
    sleep 1
done

if [ -z "$ADMIN_ADDR" ]; then
    echo "FAIL: workload did not start within 60 seconds"
    echo "--- output ---"
    cat "$OUTFILE"
    exit 1
fi

echo "Admin server at: $ADMIN_ADDR"

# Build curl flags for mTLS.
CURL_FLAGS=(--insecure --cacert "$CERTDIR/ca.crt" \
            --cert "$CERTDIR/combined.pem" --key "$CERTDIR/combined.pem")

# --- Discover procs ---
# Walk root -> host -> procs and classify by permanent actor children.
# Service proc has host_agent; worker procs have proc_agent.
# Retries because procs may still be registering after the admin server starts.
echo "Discovering procs..."

SERVICE_PROC=""
WORKER_PROC=""
for _attempt in $(seq 1 15); do
    read -r SERVICE_PROC WORKER_PROC < <(python3 - "$ADMIN_ADDR" "$CERTDIR/combined.pem" <<'PYEOF'
import json, ssl, sys, urllib.parse, urllib.request

admin_url, certfile = sys.argv[1], sys.argv[2]

# Match curl --insecure: skip hostname verification, still send client cert for mTLS.
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
            # Strip index suffix: "host_agent[0]" -> "host_agent"
            base = name.split("[")[0]
            actor_names.append(base)
        # Service proc has host_agent; worker procs have proc_agent.
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

# --- Test 1: py-spy on worker proc (PS-12: ProcAgent path) ---
if [ -z "$WORKER_PROC" ]; then
    echo "FAIL: no worker proc found"
    FAILED=1
else
    echo ""
    echo "Test 1: py-spy on worker proc..."
    ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$WORKER_PROC', safe=''))")
    HTTP_CODE=""
    BODY=""
    HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' \
        "${CURL_FLAGS[@]}" --max-time 15 \
        "$ADMIN_ADDR/v1/pyspy/$ENCODED") || true
    BODY=$(curl -s "${CURL_FLAGS[@]}" --max-time 15 \
        "$ADMIN_ADDR/v1/pyspy/$ENCODED" 2>/dev/null) || true

    if [ "$HTTP_CODE" = "200" ]; then
        if echo "$BODY" | grep -qE '"Ok"|"BinaryNotFound"|"Failed"'; then
            echo "  PASS: HTTP 200, valid PySpyResult"
        else
            echo "  FAIL: HTTP 200 but unexpected body: $BODY"
            FAILED=1
        fi
    else
        echo "  FAIL: expected HTTP 200, got $HTTP_CODE"
        echo "  Body: $BODY"
        FAILED=1
    fi
fi

# --- Test 2: py-spy on service proc (PS-12: HostAgent path) ---
if [ -z "$SERVICE_PROC" ]; then
    echo "FAIL: no service proc found"
    FAILED=1
else
    echo ""
    echo "Test 2: py-spy on service proc..."
    ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$SERVICE_PROC', safe=''))")
    HTTP_CODE=""
    BODY=""
    HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' \
        "${CURL_FLAGS[@]}" --max-time 15 \
        "$ADMIN_ADDR/v1/pyspy/$ENCODED") || true
    BODY=$(curl -s "${CURL_FLAGS[@]}" --max-time 15 \
        "$ADMIN_ADDR/v1/pyspy/$ENCODED" 2>/dev/null) || true

    if [ "$HTTP_CODE" = "200" ]; then
        if echo "$BODY" | grep -qE '"Ok"|"BinaryNotFound"|"Failed"'; then
            echo "  PASS: HTTP 200, valid PySpyResult (HostAgent handled it)"
        else
            echo "  FAIL: HTTP 200 but unexpected body: $BODY"
            FAILED=1
        fi
    else
        echo "  FAIL: expected HTTP 200, got $HTTP_CODE"
        echo "  Body: $BODY"
        FAILED=1
    fi
fi

# --- Test 3: py-spy on bogus proc (PS-13: defensive probe fast-fail) ---
# Construct a proc reference with a non-routable address. The probe should
# fail fast (within 3s) instead of waiting the full 13s bridge timeout.
echo ""
echo "Test 3: py-spy on bogus proc (probe fast-fail)..."
BOGUS_REF="unix:@nonexistent_bogus_socket_xyz,bogus-ffffffffffffffff"
ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$BOGUS_REF', safe=''))")
HTTP_CODE=""
BODY=""
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' \
    "${CURL_FLAGS[@]}" --max-time 5 \
    "$ADMIN_ADDR/v1/pyspy/$ENCODED") || true
BODY=$(curl -s "${CURL_FLAGS[@]}" --max-time 5 \
    "$ADMIN_ADDR/v1/pyspy/$ENCODED" 2>/dev/null) || true

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
