#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Host-count scaling sweep for the torchtitan_mast data plane, on CPU-only MAST
# hosts. This is the scale-out companion to benchmark_table.sh: it drops the two
# GPU training rows (CPU hosts have no GPUs) and instead runs the host-count-
# dependent rows once per host count, so you can read how the numbers change as
# the allocation grows. Rows measured per host count:
#   apply_wall_incl_alloc  apply wall clock (includes MAST queue/alloc)
#   apply_mount_open       reproducible "mounts opened in Ns" from the sidecar
#   exec_all_echo_wall     `monarch exec --all -- echo hello`  (1 proc/host)
#   exec_one_hostname_wall `monarch exec --one -- hostname`
#   import_torch_cold_wall cold `import torch` on every worker (on-demand .so fan-out)
#   import_torch_warm_wall warm `import torch` (blocks already resident)
#   import_torch_*_perworker best-effort slowest-worker import secs (needs log forwarding)
#   checksum_wall          every worker streams+sha256s the mounted files (correctness read)
#   checksum_match         OK iff every host's bytes match the client source hash (else MISMATCH; TIMEOUT if the read was too slow to finish)
#   checksum_hosts_ok      hosts verified byte-identical (= N when checksum_match=OK; exec rc guarantees it)
#   checksum_nfiles        number of files verified per host
#   reapply_wall           no-change re-apply (mount refresh + exec/connect floor)
#   reapply_mount_open     refresh time, if the sidecar prints it
#   kill_wall              teardown
#
# Each host count is an independent `apply -> measure -> kill` cycle against a
# fresh MAST allocation of that many WHOLE T1 CPU hosts (one worker per host),
# selected via TITAN_HOST_TYPE=cpu (see simple_mast_job/build_bootstrap.py).
# `import torch` still exercises the mount's big-.so fan-out on CPU hosts even
# though CUDA never initializes.
#
# A host count of 0 is special: it runs the import-torch and checksum probes
# LOCALLY on the client (no MAST allocation, no remotemount, no network), against
# the workspace venv on local disk. This is the true performance floor -- the same
# work with zero delivery overhead -- emitted as hosts=0 rows so it sits beside the
# remote counts as a reference. Only import_torch_warm_wall (the compute floor;
# there is no local cold -- see run_local) and checksum_* are meaningful locally;
# apply/exec/mount_open/reapply/kill are remote-only and are omitted for hosts=0.
#
# Correctness at scale: the checksum step makes EVERY worker read every byte of
# the big mounted files and verify each against the client-side (source-of-truth)
# sha256. `monarch exec --all` returns nonzero iff any host's bytes diverged, so
# a green `checksum_match=OK` row means the on-demand mount delivered
# byte-identical content to all N hosts -- i.e. it scales accurately, not just
# fast. Any mismatch prints the offending file/host to stderr.
#
# Output: benchmark_table_scale.csv (long form: hosts,metric,value,unit), written
# incrementally so a partial/aborted sweep keeps its rows. On apply timeout or a
# job that never comes up, a `status,FAIL` row is recorded, the job is killed,
# and the sweep continues to the next size.
#
# By default this REBUILDS monarch from the CURRENT source first (setup_env.sh
# --force), like benchmark_table.sh, so the numbers reflect what you're editing
# rather than a stale cached wheel. Set SCALE_REBUILD=0 to reuse the existing
# build (for re-running / extending a sweep against one wheel). Either way the
# wheel's build time is printed at the start so a stale wheel is never silent.
#
# Tunables (env):
#   SCALE_HOSTS          host counts to sweep (default "1 2 4 8 16 32 64 128 256 512 1024");
#                        include 0 to also measure the local no-MAST/no-remotemount floor
#   SCALE_REBUILD        1=rebuild monarch from source before sweeping (default), 0=reuse existing wheel
#   SCALE_APPLY_TIMEOUT  per-apply timeout incl. MAST queue wait, seconds (default 1800)
#   SCALE_EXEC_TIMEOUT   per-exec/kill timeout, seconds (default 600)
#   SCALE_OUT            output CSV (default ./benchmark_table_scale.csv)
#   SCALE_APPEND         1=append to an existing CSV instead of overwriting (extend a sweep)
#   SCALE_CKSUM          1=run the per-cycle checksum correctness check (default), 0=skip
#   SCALE_CKSUM_MIN_MB   min file size (MiB) in the checksum set (default 100; the big .so's)
#   TITAN_CPU_CPUS       cores per CPU worker host (default 15)
#   TITAN_CPU_RAM_MB     RAM per CPU worker host, MB (default 54272)
#   TITAN_LOCALITY_REGIONS  region pin (default eag); set "none" to let MAST place
#                           workers in any region -- often needed past ~64 hosts.
#   MONARCH              monarch client binary (default ~/monarch_bench_envs/client/bin/monarch)

set -u
cd "$(dirname "$0")" || exit 1

MON="${MONARCH:-$HOME/monarch_bench_envs/client/bin/monarch}"
CLIENT_PY="$(dirname "$MON")/python3.12"
VENV="$HOME/dev/titan_workspace/.venv"
WORKSPACE="$(dirname "$VENV")"
LOGS="${TITAN_LOG_DIR:-$HOME/torchtitan_logs}"
OUT="${SCALE_OUT:-$PWD/benchmark_table_scale.csv}"
HOSTS_LIST="${SCALE_HOSTS:-1 2 4 8 16 32 64 128 256 512 1024}"
APPLY_TIMEOUT="${SCALE_APPLY_TIMEOUT:-1800}"
EXEC_TIMEOUT="${SCALE_EXEC_TIMEOUT:-600}"
APPEND="${SCALE_APPEND:-0}"
REBUILD="${SCALE_REBUILD:-1}"
CKSUM="${SCALE_CKSUM:-1}"
CKSUM_MIN_MB="${SCALE_CKSUM_MIN_MB:-100}"
# Whether any remote (MAST) host count is requested. A local-only sweep (just "0")
# needs neither the monarch client binary nor the bootstrap fbpkg.
HAS_REMOTE=0
for _n in $HOSTS_LIST; do [ "$_n" != "0" ] && HAS_REMOTE=1; done
# The verifier is written into the mounted workspace so every worker sees it at
# the same absolute path job.py mounts the workspace at; removed on exit.
VERIFY_PY="$WORKSPACE/_cksum_verify.py"
CKSUM_FILES=()
trap 'rm -f "$VERIFY_PY" 2>/dev/null' EXIT

# CPU-only whole-host workers; skip the ~25 GB FineWeb mount (training is
# skipped, so it is never read) by pointing its dir at a path that won't exist.
export TITAN_HOST_TYPE=cpu
export TITAN_FINEWEB_DIR="/tmp/nonexistent_fineweb_${USER:-x}_$$"

# External network (pypi wheel deps for the fbpkg build, torch/crates on a
# rebuild) needs the proxy; internal traffic (mast, fbpkg, worker metatls) must
# bypass it. Set only if the shell has no proxy configured (respect an already-
# set interactive env). Needed even when NOT rebuilding: a fresh wheel makes
# build_bootstrap re-create the fbpkg venv, which pip-installs the wheel's deps.
if [ -z "${https_proxy:-}" ]; then
    export https_proxy=http://fwdproxy:8080 http_proxy=http://fwdproxy:8080
    export no_proxy=".facebook.com,.internalfb.com,.thefacebook.com,.fbinfra.net,.internmc.facebook.com,localhost,127.0.0.1,::1${no_proxy:+,$no_proxy}"
fi

# Rebuild monarch from the current source (default) so numbers reflect what you
# edit -- the trap benchmark_table.sh avoids via `setup_env.sh --force`. cargo /
# CUDA / PROTOC are set only if missing, so this works from a non-interactive
# shell too.
if [ "$REBUILD" = "1" ]; then
    command -v cargo >/dev/null 2>&1 || export PATH="$HOME/.cargo/bin:$PATH"
    if [ -z "${CUDA_HOME:-}" ]; then
        for c in /usr/local/cuda-12.8 /usr/local/cuda; do
            [ -d "$c" ] && { export CUDA_HOME="$c"; export PATH="$CUDA_HOME/bin:$PATH"; break; }
        done
    fi
    [ -n "${PROTOC:-}" ] || export PROTOC=/usr/bin/protoc
    echo "### SCALE_REBUILD=1: rebuilding monarch from current source (setup_env.sh --force)"
    bash setup_env.sh --force || { echo "ERROR: setup_env.sh --force failed (env may be half-built)" >&2; exit 1; }
fi

if [ "$HAS_REMOTE" = "1" ] && [ ! -x "$MON" ]; then
    echo "ERROR: monarch client not found/executable at $MON -- run setup_env.sh (or SCALE_REBUILD=1), or set MONARCH=<path>" >&2; exit 1
fi
[ -x "$VENV/bin/python3.12" ] || { echo "ERROR: workspace venv missing at $VENV -- run: bash setup_env.sh (or SCALE_REBUILD=1)" >&2; exit 1; }

# Surface which wheel we're about to benchmark -- a stale wheel is never silent.
# setup_env.sh keeps exactly one wheel here, so a glob (no `ls`) is unambiguous.
_wheels=( "/tmp/monarch_bootstrap_${USER}/wheel"/*.whl )
_wheel="${_wheels[0]}"
if [ -e "$_wheel" ]; then
    echo "### monarch wheel: ${_wheel##*/} (built $(date -r "$_wheel" '+%Y-%m-%d %H:%M'))  SCALE_REBUILD=$REBUILD"
else
    echo "### monarch wheel: <none found>  SCALE_REBUILD=$REBUILD"
fi

# Fresh CSV with header, unless extending an existing sweep (SCALE_APPEND=1).
mkdir -p "$(dirname "$OUT")"
if [ "$APPEND" = "1" ] && [ -f "$OUT" ]; then
    echo "### appending to existing $OUT"
else
    printf 'hosts,metric,value,unit\n' > "$OUT"
fi

emit() {  # hosts metric value unit  -- append a CSV row and echo it live
    printf '%s,%s,%s,%s\n' "$1" "$2" "$3" "$4" | tee -a "$OUT"
}

# Run a command, writing combined output to $1 and its wall clock (2dp seconds)
# into the global REPLY_WALL. Returns the command's exit status.
timed() {  # logfile cmd...
    local log="$1"; shift
    local start end rc
    start="$(date +%s.%N)"
    "$@" >"$log" 2>&1
    rc=$?
    end="$(date +%s.%N)"
    REPLY_WALL="$(awk -v s="$start" -v e="$end" 'BEGIN{printf "%.2f", e-s}')"
    return "$rc"
}

# Extract the seconds value that follows a phrase (e.g. "mounts opened in 14.66s").
# -a: the monarch/sidecar output can contain non-text bytes that make grep bail.
parse_secs_after() {  # logfile phrase-regex
    sed -E 's/\x1b\[[0-9;]*m//g' "$1" \
        | grep -aoiE "$2[^0-9]*[0-9]+(\.[0-9]+)?s?" \
        | grep -aoE '[0-9]+(\.[0-9]+)?' | tail -1
}

# Best-effort slowest-worker import time. The probe tags its line "IMPORTSEC <f>"
# so we only pick up real per-worker import seconds (not some other float in the
# forwarded logs); "NA" if no tagged line was forwarded to the client.
max_float() {  # logfile
    local m
    m="$(grep -aoE 'IMPORTSEC [0-9]+\.[0-9]+' "$1" | grep -oE '[0-9]+\.[0-9]+' | sort -g | tail -1)"
    echo "${m:-NA}"
}

kill_job() {  # best-effort teardown; warn (do not abort) if it fails
    "$MON" kill >/dev/null 2>&1 || \
        echo "!!! WARNING: 'monarch kill' failed; if a job was scheduled it may be leaking -- check 'mast list-jobs -u $USER' / 'mast kill'." >&2
}

# Write a python verifier into the mounted workspace, with the client-side
# (source-of-truth) sha256 of each target file inlined. Each worker runs it to
# stream+hash every target file and compare; it prints CHECKSUM_OK/CHECKSUM_FAIL
# per file and a CHECKSUM_DONE summary, and exits nonzero if ANY file mismatches
# or is unreadable -- so `monarch exec --all` returns nonzero iff some host's
# bytes diverged from the source.
write_verify_script() {
    local golden dict
    golden="$(sha256sum "${CKSUM_FILES[@]}")" || return 1
    dict="$(awk '{ printf "    \"%s\": \"%s\",\n", $2, $1 }' <<<"$golden")"
    cat > "$VERIFY_PY" <<EOF
import hashlib, sys
WANT = {
$dict
}
def sha(path):
    x = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            x.update(chunk)
    return x.hexdigest()
bad = 0
for path, want in WANT.items():
    try:
        got = sha(path)
    except OSError as exc:
        print("CHECKSUM_FAIL", path, "read_error", exc)
        bad += 1
        continue
    if got == want:
        print("CHECKSUM_OK", path)
    else:
        print("CHECKSUM_FAIL", path, "got", got, "want", want)
        bad += 1
print("CHECKSUM_DONE", len(WANT) - bad, "of", len(WANT), "ok")
sys.exit(1 if bad else 0)
EOF
}

IMPORT_PROBE='import time; t=time.perf_counter(); import torch; print("IMPORTSEC %.4f" % (time.perf_counter()-t))'

run_one() {  # hosts
    local n="$1" alog elog clog wlog rc mo rmo
    echo "############################################################"
    echo "### hosts=$n   ($(date '+%Y-%m-%d %H:%M:%S'))"
    echo "############################################################"
    export TITAN_NUM_HOSTS="$n"
    rm -rf .monarch
    alog="$(mktemp)"; elog="$(mktemp)"; clog="$(mktemp)"; wlog="$(mktemp)"

    # --- apply (fresh): MAST queue/alloc + mount open ---
    timed "$alog" timeout "$APPLY_TIMEOUT" "$MON" apply job.job; rc=$?
    emit "$n" apply_wall_incl_alloc "$REPLY_WALL" s
    if [ "$rc" -ne 0 ]; then
        echo "!!! apply failed/timed out for hosts=$n (rc=$rc, wall=${REPLY_WALL}s). Tail:" >&2
        tail -25 "$alog" >&2
        emit "$n" status FAIL -
        kill_job
        rm -f "$alog" "$elog" "$clog" "$wlog"
        return
    fi
    mo="$(parse_secs_after "$alog" 'mounts opened in')"
    [ -z "$mo" ] && mo="$(parse_secs_after "$alog" 'ready \(')"
    emit "$n" apply_mount_open "${mo:-NA}" s

    # --- monarch exec floor ---
    timed "$elog" timeout "$EXEC_TIMEOUT" "$MON" exec --all -- echo hello
    emit "$n" exec_all_echo_wall "$REPLY_WALL" s
    timed "$elog" timeout "$EXEC_TIMEOUT" "$MON" exec --one -- hostname
    emit "$n" exec_one_hostname_wall "$REPLY_WALL" s

    # --- cold + warm import torch (on-demand .so fan-out to every worker) ---
    timed "$clog" timeout "$EXEC_TIMEOUT" "$MON" exec --all -- python -c "$IMPORT_PROBE"; rc=$?
    emit "$n" import_torch_cold_wall "$REPLY_WALL" s
    emit "$n" import_torch_cold_perworker "$(max_float "$clog")" s
    if [ "$rc" -ne 0 ]; then
        echo "!!! cold 'import torch' returned rc=$rc on hosts=$n (does torch import on these CPU hosts?). Tail:" >&2
        tail -15 "$clog" >&2
        emit "$n" import_torch_status FAIL -
    fi
    timed "$wlog" timeout "$EXEC_TIMEOUT" "$MON" exec --all -- python -c "$IMPORT_PROBE"
    emit "$n" import_torch_warm_wall "$REPLY_WALL" s
    emit "$n" import_torch_warm_perworker "$(max_float "$wlog")" s

    # --- checksum correctness: every worker streams+hashes the mounted files and
    #     verifies each against the client's source-of-truth sha256. exec rc (and
    #     any CHECKSUM_FAIL marker) is the all-hosts verdict; this reads every
    #     byte of the big files on every host, confirming the mount delivers
    #     byte-identical content at this scale, not just quickly. ---
    if [ "$CKSUM" = "1" ]; then
        timed "$clog" timeout "$EXEC_TIMEOUT" "$MON" exec --all -- python "$VERIFY_PY"; rc=$?
        emit "$n" checksum_wall "$REPLY_WALL" s
        if [ "$rc" -eq 0 ] && ! grep -aq CHECKSUM_FAIL "$clog"; then
            # exec rc=0 => every one of the n workers' verifier exited 0 => every
            # target file matched the client source hash on all n hosts.
            emit "$n" checksum_match OK -
            emit "$n" checksum_hosts_ok "$n" -
        elif [ "$rc" -eq 124 ] && ! grep -aq CHECKSUM_FAIL "$clog"; then
            # `timeout` exits 124: reading+hashing every byte on every host did not
            # finish within SCALE_EXEC_TIMEOUT. That is a speed ceiling at this
            # scale, NOT a byte mismatch -- raise SCALE_EXEC_TIMEOUT (or shrink the
            # set via SCALE_CKSUM_MIN_MB). Reported distinctly so it is not read as
            # corruption.
            emit "$n" checksum_match TIMEOUT -
            emit "$n" checksum_hosts_ok "?" -
            echo "!!! checksum TIMEOUT on hosts=$n (> ${EXEC_TIMEOUT}s): not a mismatch -- raise SCALE_EXEC_TIMEOUT or SCALE_CKSUM_MIN_MB." >&2
        else
            emit "$n" checksum_match MISMATCH -
            emit "$n" checksum_hosts_ok "?" -
            echo "!!! CHECKSUM MISMATCH on hosts=$n (rc=$rc): mount did NOT deliver byte-identical files." >&2
            echo "    per-host detail (worker stdout is gathered, not forwarded):" >&2
            echo "    grep -rl CHECKSUM_FAIL $LOGS/hosts_*/exec_outputs/" >&2
            grep -a CHECKSUM_FAIL "$clog" 2>/dev/null | head -20 >&2
        fi
        emit "$n" checksum_nfiles "${#CKSUM_FILES[@]}" -
    fi

    # --- no-change re-apply (mount refresh + exec/connect floor) ---
    timed "$alog" timeout "$EXEC_TIMEOUT" "$MON" apply job.job
    emit "$n" reapply_wall "$REPLY_WALL" s
    rmo="$(parse_secs_after "$alog" 'refresh complete in')"
    [ -z "$rmo" ] && rmo="$(parse_secs_after "$alog" 'ready \(')"
    [ -z "$rmo" ] && rmo="$(parse_secs_after "$alog" 'mounts opened in')"
    emit "$n" reapply_mount_open "${rmo:-NA}" s

    # --- teardown ---
    timed "$elog" timeout "$EXEC_TIMEOUT" "$MON" kill
    emit "$n" kill_wall "$REPLY_WALL" s
    emit "$n" status OK -
    rm -f "$alog" "$elog" "$clog" "$wlog"
}

# hosts=0: the local performance floor. Runs the import-torch and checksum probes
# on THIS client against the workspace venv on local disk -- no MAST, no
# remotemount, no network -- so the walls are the pure python-import and
# stream+sha256 compute with zero delivery overhead. Emitted as hosts=0 rows.
#
# There is deliberately no local "cold" import: nothing is faulted on demand here,
# so a local cold number would only be first-import overhead (pyc / one-time init),
# not the remote cold's delivery cost -- reporting it invites a bad comparison. We
# warm up once and emit only the steady-state WARM compute floor.
run_local() {
    local clog wlog rc PY="$VENV/bin/python3.12"
    echo "############################################################"
    echo "### hosts=0  (LOCAL floor: no MAST / no remotemount)   ($(date '+%Y-%m-%d %H:%M:%S'))"
    echo "############################################################"
    clog="$(mktemp)"; wlog="$(mktemp)"

    # warm import torch: a throwaway import primes pyc / page cache / torch's
    # one-time init, then the measured import is the steady-state compute floor
    # (python start + resident read + init), with no delivery leg to pay.
    "$PY" -c "$IMPORT_PROBE" >/dev/null 2>&1 || true
    timed "$wlog" "$PY" -c "$IMPORT_PROBE"; rc=$?
    emit 0 import_torch_warm_wall "$REPLY_WALL" s
    emit 0 import_torch_warm_perworker "$(max_float "$wlog")" s
    if [ "$rc" -ne 0 ]; then
        echo "!!! local 'import torch' returned rc=$rc. Tail:" >&2; tail -15 "$wlog" >&2
        emit 0 import_torch_status FAIL -
    fi

    # checksum floor: hash the same target files locally. They ARE the source of
    # truth, so this always matches; the wall is the pure stream+sha256 read cost
    # against local disk -- the floor the remote checksum_wall is measured against.
    if [ "$CKSUM" = "1" ]; then
        timed "$clog" "$PY" "$VERIFY_PY"; rc=$?
        emit 0 checksum_wall "$REPLY_WALL" s
        if [ "$rc" -eq 0 ] && ! grep -aq CHECKSUM_FAIL "$clog"; then
            emit 0 checksum_match OK -
            emit 0 checksum_hosts_ok 1 -
        else
            emit 0 checksum_match MISMATCH -
            emit 0 checksum_hosts_ok 0 -
            echo "!!! local checksum unexpectedly failed (rc=$rc) -- the client cannot hash its own source files?" >&2
            grep -a CHECKSUM_FAIL "$clog" 2>/dev/null | head >&2
        fi
        emit 0 checksum_nfiles "${#CKSUM_FILES[@]}" -
    fi
    emit 0 status OK -
    rm -f "$clog" "$wlog"
}

# Pretty pivot (metric rows x host columns) of the CSV, for quick reading.
pivot() {  # csv
    awk -F, '
        NR==1 { next }
        {
            val[$2 SUBSEP $1] = $3
            if (!($1 in seenh)) { seenh[$1]=1; hosts[++nh]=$1 }
            if (!($2 in seenm)) { seenm[$2]=1; metrics[++nm]=$2 }
        }
        END {
            for (i=1;i<=nh;i++) for (j=i+1;j<=nh;j++)
                if (hosts[j]+0 < hosts[i]+0) { t=hosts[i]; hosts[i]=hosts[j]; hosts[j]=t }
            printf "%-26s", "metric \\ hosts"
            for (i=1;i<=nh;i++) printf "%9s", hosts[i]
            printf "\n"
            for (k=1;k<=nm;k++) {
                printf "%-26s", metrics[k]
                for (i=1;i<=nh;i++) {
                    key = metrics[k] SUBSEP hosts[i]
                    printf "%9s", (key in val) ? val[key] : "-"
                }
                printf "\n"
            }
        }' "$1"
}

# Scaling report: rank each part (and derived sub-part) by how hard it scales
# with host count -- factor = wall at the largest host count / wall at the
# smallest. Answers "which part scales worst as hosts grow". Sub-parts are
# derived from walls we already collect (no extra runs), isolating the data
# plane from the exec machinery and per-worker compute:
#   cold DELIVERY  = cold_import - warm_import  (on-demand .so fan-out to N hosts)
#   import COMPUTE = warm_import - exec_echo     (python start + resident import)
#   exec MACHINERY = exec_echo                   (proc spawn + dispatch + gather)
#   prefill DELIVERY = mount_open                (code-block fan-out at open)
#   apply SETUP    = apply_wall - mount_open      (MAST alloc + attach/spawn/config-push)
scale_report() {  # csv
    awk -F, '
        NR==1 { next }
        $3 ~ /^[0-9.]+$/ && ($1+0) > 0 { v[$2 SUBSEP ($1+0)] = $3+0; if (!(($1+0) in sh)) { sh[$1+0]=1; H[++nh]=$1+0 } }
        function val(m,h,   k){ k=m SUBSEP h; return (k in v)?v[k]:"" }
        function cd(h,   a,b){ a=val("import_torch_cold_wall",h); b=val("import_torch_warm_wall",h); return (a==""||b=="")?"":a-b }
        function ic(h,   a,b){ a=val("import_torch_warm_wall",h); b=val("exec_all_echo_wall",h); return (a==""||b=="")?"":a-b }
        function as(h,   a,b){ a=val("apply_wall_incl_alloc",h); b=val("apply_mount_open",h); return (a==""||b=="")?"":a-b }
        function add(name,lo,hi){ np++; PN[np]=name; PLO[np]=lo; PHI[np]=hi; PF[np]=(lo!=""&&hi!=""&&lo>0)?hi/lo:-1 }
        END {
            if (nh < 2) { print "scale_report: need >= 2 host counts"; exit }
            for (i=1;i<=nh;i++) for (j=i+1;j<=nh;j++) if (H[j] < H[i]) { t=H[i]; H[i]=H[j]; H[j]=t }
            lo=H[1]; hi=H[nh]
            add("prefill DELIVERY (mount_open)",   val("apply_mount_open",lo),       val("apply_mount_open",hi))
            add("cold import TOTAL",               val("import_torch_cold_wall",lo), val("import_torch_cold_wall",hi))
            add("  - cold DELIVERY (cold-warm)",   cd(lo), cd(hi))
            add("  - import COMPUTE (warm-echo)",  ic(lo), ic(hi))
            add("checksum full-read",              val("checksum_wall",lo),          val("checksum_wall",hi))
            add("warm import TOTAL",               val("import_torch_warm_wall",lo), val("import_torch_warm_wall",hi))
            add("exec MACHINERY (echo --all)",     val("exec_all_echo_wall",lo),     val("exec_all_echo_wall",hi))
            add("apply SETUP (apply-mount_open)*", as(lo), as(hi))
            add("reapply",                         val("reapply_wall",lo),           val("reapply_wall",hi))
            add("kill",                            val("kill_wall",lo),              val("kill_wall",hi))
            for (i=1;i<=np;i++) for (j=i+1;j<=np;j++) if (PF[j] > PF[i]) {
                t=PF[i];PF[i]=PF[j];PF[j]=t; s=PN[i];PN[i]=PN[j];PN[j]=s;
                t=PLO[i];PLO[i]=PLO[j];PLO[j]=t; t=PHI[i];PHI[i]=PHI[j];PHI[j]=t }
            printf "\n=== worst-scaling parts (%d -> %d hosts; factor = wall@%d / wall@%d) ===\n", lo, hi, hi, lo
            printf "%-34s %10s %10s %8s\n", "part", "wall@"lo, "wall@"hi, "factor"
            for (i=1;i<=np;i++)
                printf "%-34s %10s %10s %8s\n", PN[i],
                    (PLO[i]==""?"-":sprintf("%.1f",PLO[i])), (PHI[i]==""?"-":sprintf("%.1f",PHI[i])),
                    (PF[i]<0?"n/a":sprintf("%.1fx",PF[i]))
            print "* apply SETUP includes MAST queue/alloc (varies run to run), not a pure monarch cost."
        }
    ' "$1"
}

echo "### CPU-host scaling sweep: hosts = $HOSTS_LIST"
echo "### monarch=$MON  out=$OUT  apply_timeout=${APPLY_TIMEOUT}s  exec_timeout=${EXEC_TIMEOUT}s"
echo "### CPU host shape: cpu=${TITAN_CPU_CPUS:-15} ram_mb=${TITAN_CPU_RAM_MB:-54272} region=${TITAN_LOCALITY_REGIONS:-eag}"

# --- checksum verifier setup (source-of-truth hashes, written once) ---
if [ "$CKSUM" = "1" ]; then
    while IFS= read -r f; do CKSUM_FILES+=("$f"); done \
        < <(find "$VENV" -type f -size +"${CKSUM_MIN_MB}"M 2>/dev/null | sort)
    small="$VENV/lib/python3.12/site-packages/torch/version.py"
    [ -f "$small" ] && CKSUM_FILES+=("$small")
    if [ "${#CKSUM_FILES[@]}" -gt 0 ] && write_verify_script; then
        echo "### checksum: each host verifies ${#CKSUM_FILES[@]} files (>= ${CKSUM_MIN_MB} MiB + version.py) against the client source -> $VERIFY_PY"
    else
        echo "### checksum: no target files or setup failed -- disabling"
        CKSUM=0
    fi
fi

# Pre-build/cache the slim bootstrap fbpkg once so the first apply's wall does
# not include the one-time venv build + fbpkg upload. A fresh wheel forces a
# rebuild here (pip installs the wheel's deps from pypi -> needs the proxy set
# above). Fatal on failure: every apply's build_bootstrap would fail the same way.
if [ "$HAS_REMOTE" = "1" ]; then
    echo "### pre-building bootstrap fbpkg (one-time)..."
    "$CLIENT_PY" -m simple_mast_job.build_bootstrap || {
        echo "ERROR: bootstrap fbpkg build failed (needs pypi via proxy for the wheel's deps)" >&2
        exit 1
    }
fi

for n in $HOSTS_LIST; do
    if [ "$n" = "0" ]; then run_local; else run_one "$n"; fi
done

echo
echo "=== scaling summary ($OUT) ==="
pivot "$OUT" 2>/dev/null || cat "$OUT"
scale_report "$OUT" 2>/dev/null || true
