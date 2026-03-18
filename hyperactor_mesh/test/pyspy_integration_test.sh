#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Integration test for the GET /v1/pyspy/{proc_reference} endpoint.
#
# Runs three workload modes (cpu, block, mixed) sequentially, each
# with its own workload process. Reports per-mode results and
# aggregates at the end.
#
# Exit codes:
#   0 — all modes passed (or skipped due to py-spy unavailability)
#   1 — at least one mode failed

set -euo pipefail

echo "=== pyspy_integration_test ==="
echo "Starting at $(date)"

WORKLOAD_BIN="$1"
VERIFIER_BIN="$2"

echo "Workload binary: $WORKLOAD_BIN"
echo "Verifier binary: $VERIFIER_BIN"

# --- Install py-spy ---
PYSPY_DIR=$(mktemp -d)
echo "Fetching fb-py-spy:prod..."
if fbpkg fetch fb-py-spy:prod -d "$PYSPY_DIR" 2>/dev/null; then
    export PYSPY_BIN="$PYSPY_DIR/py-spy"
    echo "py-spy installed: $PYSPY_BIN"
else
    echo "fbpkg fetch failed; falling back to system PATH"
    unset PYSPY_BIN
    echo "py-spy: $(which py-spy 2>/dev/null || echo 'not found on PATH')"
fi

# --- Generate test PKI ---
echo "Generating test PKI..."
CERTDIR=$(mktemp -d)

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout "$CERTDIR/ca.key" -out "$CERTDIR/ca.crt" \
    -days 1 -nodes -subj "/CN=test-ca" 2>/dev/null

HOSTNAME=$(hostname -f)
echo "Hostname: $HOSTNAME"
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
echo "PKI generated in $CERTDIR"

export HYPERACTOR_TLS_CERT="$CERTDIR/combined.pem"
export HYPERACTOR_TLS_KEY="$CERTDIR/combined.pem"
export HYPERACTOR_TLS_CA="$CERTDIR/ca.crt"
export HYPERACTOR_MESH_ADMIN_ADDR="[::]:0"

# --- Helper: run one mode ---
OUTFILE=$(mktemp)
BIN_PID=""

cleanup_workload() {
    if [ -n "$BIN_PID" ]; then
        kill "$BIN_PID" 2>/dev/null || true
        wait "$BIN_PID" 2>/dev/null || true
        BIN_PID=""
    fi
}

cleanup_all() {
    cleanup_workload
    rm -f "$OUTFILE"
    rm -rf "$CERTDIR"
    rm -rf "$PYSPY_DIR"
}
trap cleanup_all EXIT

run_mode() {
    local MODE="$1"
    echo ""
    echo "--- mode: $MODE ---"

    # Start workload.
    cleanup_workload
    true > "$OUTFILE"
    echo "Launching workload (mode=$MODE, work_ms=500, concurrency=2)..."
    "$WORKLOAD_BIN" --mode "$MODE" --work-ms 500 --concurrency 2 > "$OUTFILE" 2>&1 &
    BIN_PID=$!
    echo "Workload PID: $BIN_PID"

    # Wait for admin server.
    local ADMIN_ADDR=""
    echo "Waiting for admin server..."
    for _i in $(seq 1 60); do
        if grep -q "Mesh admin server listening on" "$OUTFILE"; then
            ADMIN_ADDR=$(grep "Mesh admin server listening on" "$OUTFILE" \
                | head -1 | sed 's/.*listening on //')
            break
        fi
        sleep 1
    done

    if [ -z "$ADMIN_ADDR" ]; then
        echo "FAIL [$MODE]: workload did not start within 60 seconds"
        echo "--- workload output ---"
        cat "$OUTFILE"
        cleanup_workload
        return 1
    fi

    echo "Admin server at: $ADMIN_ADDR"

    # Wait for pyspy_worker procs to be discoverable via the same
    # root -> host -> proc -> actor traversal the verifier uses.
    # Requires >= 2 procs (matches --concurrency 2).
    echo "Waiting for worker procs to register..."
    local WORKERS_READY=0
    for _i in $(seq 1 60); do
        local NPROCS
        NPROCS=$(python3 - "$ADMIN_ADDR" "$CERTDIR/ca.crt" "$CERTDIR/combined.pem" <<'PYEOF' 2>/dev/null
import json, ssl, sys, urllib.parse, urllib.request

admin_url, cacert, certfile = sys.argv[1], sys.argv[2], sys.argv[3]
ctx = ssl.create_default_context(cafile=cacert)
ctx.load_cert_chain(certfile=certfile, keyfile=certfile)

def get(url):
    try:
        return json.loads(urllib.request.urlopen(url, context=ctx, timeout=5).read())
    except Exception:
        return None

root = get(f"{admin_url}/v1/root")
if not root:
    print(0); sys.exit(0)

count = 0
for host_ref in root.get("children", []):
    host = get(f"{admin_url}/v1/{urllib.parse.quote(host_ref, safe='')}")
    if not host:
        continue
    for proc_ref in host.get("children", []):
        proc = get(f"{admin_url}/v1/{urllib.parse.quote(proc_ref, safe='')}")
        if not proc:
            continue
        for actor_ref in proc.get("children", []):
            name = actor_ref.rsplit(",", 1)[-1] if "," in actor_ref else actor_ref
            if name.startswith("pyspy_worker"):
                count += 1
                break
print(count)
PYEOF
        )
        if [ "${NPROCS:-0}" -ge 2 ]; then
            echo "Worker procs ready (${NPROCS} pyspy_worker proc(s) discoverable)"
            WORKERS_READY=1
            break
        fi
        sleep 1
    done
    if [ "$WORKERS_READY" -eq 0 ]; then
        echo "FAIL [$MODE]: worker procs did not register within 60 seconds"
        echo "--- workload output (last 30 lines) ---"
        tail -30 "$OUTFILE"
        cleanup_workload
        return 1
    fi

    # Run verifier.
    echo "Running verifier (mode=$MODE, samples=5)..."
    local VERIFY_EXIT=0
    "$VERIFIER_BIN" \
        --admin-url "$ADMIN_ADDR" \
        --mode "$MODE" \
        --samples 5 \
        --cacert "$CERTDIR/ca.crt" \
        --cert "$CERTDIR/combined.pem" \
        --key "$CERTDIR/combined.pem" \
        || VERIFY_EXIT=$?
    echo "Verifier exit code: $VERIFY_EXIT"

    # Check workload survived.
    if ! kill -0 "$BIN_PID" 2>/dev/null; then
        echo "FAIL [$MODE]: workload died during verification"
        echo "--- workload output (last 30 lines) ---"
        tail -30 "$OUTFILE"
        cleanup_workload
        return 1
    fi
    echo "Workload still alive (good)"
    cleanup_workload

    if [ "$VERIFY_EXIT" -eq 0 ]; then
        echo "PASS [$MODE]"
        return 0
    elif [ "$VERIFY_EXIT" -eq 2 ]; then
        echo "SKIP [$MODE]: py-spy not available"
        # SKIP maps to test success (return 0) because py-spy
        # availability is environmental, not a code defect.
        # Environments without py-spy (or without ptrace
        # permissions) cannot validate stack capture, but the
        # endpoint, message routing, and error classification
        # are still exercised by the BinaryNotFound path.
        # In Sandcastle we expect py-spy to be available via
        # fbpkg and the test to take the PASS path; SKIP is
        # tolerated only for environmental gaps, not the target
        # steady state.
        return 0
    else
        echo "FAIL [$MODE]: verifier exit $VERIFY_EXIT"
        return 1
    fi
}

# --- Run all modes ---
MODES=(cpu block mixed)
FAILED=()

for MODE in "${MODES[@]}"; do
    if ! run_mode "$MODE"; then
        FAILED+=("$MODE")
    fi
done

# --- Summary ---
echo ""
echo "=== Summary ==="
for MODE in "${MODES[@]}"; do
    if printf '%s\n' "${FAILED[@]}" | grep -qx "$MODE" 2>/dev/null; then
        echo "  $MODE: FAIL"
    else
        echo "  $MODE: PASS"
    fi
done

if [ ${#FAILED[@]} -gt 0 ]; then
    echo "=== RESULT: FAIL (${FAILED[*]}) ==="
    exit 1
fi

echo "=== RESULT: PASS (all modes) ==="
echo "Finished at $(date)"
