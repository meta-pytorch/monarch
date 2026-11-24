#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Run the fault-tolerant client example once. With no injection the run must
# pass. With `check` or `hang` it must fail, and the keeper's log must show the
# failure that injection causes, not some other failure.
set -euo pipefail

binary="${1:?Usage: fault_tolerant_client_test.sh <binary> [check|hang]}"
injection="${2:-}"
case "$injection" in
  '' | check | hang) ;;
  *)
    echo "FAIL: unknown injection '$injection'; use check or hang" >&2
    exit 2
    ;;
esac

# Seconds the keeper may run. It allows each client 120 seconds, and `hang`
# waits that out.
keeper_limit=300
# Half-second polls that processes outliving the keeper get before they are
# killed.
settle_polls=60
# Tenth-second polls that a killed process gets to vanish.
reap_polls=100

# The keeper's clients and workers inherit its environment, so this marker
# finds every process of this run. The harness itself doesn't carry it.
run_marker="FTC_TEST_RUN_ID=$(</proc/sys/kernel/random/uuid)"

test_dir=''
own_pgid=''
keeper_pid=''
keeper_start=''
pending_status=''

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# Print the pid of every live process whose environment carries the marker.
# A zombie's environment reads as empty, so an exited process never matches.
run_pids() {
  local file pid
  printf '%s\0' /proc/[0-9]*/environ |
    xargs -0 grep -lzxF -- "$run_marker" 2>/dev/null |
    while IFS= read -r file; do
      pid=${file#/proc/}
      echo "${pid%/environ}"
    done || true
}

# Print a process's state, start time and process group from its stat line. A
# reused pid has a different start time.
process_stat() {
  local stat fields
  { read -r stat <"/proc/$1/stat"; } 2>/dev/null || return 1
  # The fields after the command name start at field 3, the state. The group
  # is field 5 and the start time field 22.
  read -r -a fields <<<"${stat##*) }"
  echo "${fields[0]} ${fields[19]} ${fields[2]}"
}

# Succeed once the process with this pid and start time no longer runs: it has
# exited, reaped or not, or its pid now names another process. A stopped
# process still runs.
process_gone() {
  local stat state start
  stat=$(process_stat "$1") || return 0
  read -r state start _ <<<"$stat"
  [[ "$state" == Z || "$start" != "$2" ]]
}

# Signal the process with this pid and start time, once, if it still runs: its
# process group, unless that is the harness's own, and otherwise the process
# alone.
signal_process() {
  local stat state start pgid
  stat=$(process_stat "$2") || return 0
  read -r state start pgid <<<"$stat"
  if [[ "$state" == Z || "$start" != "$3" ]]; then
    return 0
  fi
  if [[ "$pgid" -gt 1 && "$pgid" != "$own_pgid" ]]; then
    kill "-$1" -- "-$pgid" 2>/dev/null || true
  else
    kill "-$1" "$2" 2>/dev/null || true
  fi
}

# Give this run's processes up to a number of half-second polls to exit, then
# kill each survivor, checking first that its pid still carries the marker.
# Print the survivors' pids. Fail if any process of this run still runs after
# that.
stop_run_processes() {
  local pids pid stat start poll killed=''
  for ((poll = 0; poll < $1; poll++)); do
    pids=$(run_pids)
    if [[ -z "$pids" ]]; then
      return 0
    fi
    sleep 0.5
  done
  for pid in $(run_pids); do
    stat=$(process_stat "$pid") || continue
    read -r _ start _ <<<"$stat"
    grep -qzxF -- "$run_marker" "/proc/$pid/environ" 2>/dev/null || continue
    signal_process KILL "$pid" "$start"
    killed+=" $pid"
  done
  for ((poll = 0; poll < reap_polls; poll++)); do
    pids=$(run_pids)
    if [[ -z "$pids" ]]; then
      break
    fi
    sleep 0.1
  done
  echo "${killed# }"
  if [[ -n "$pids" ]]; then
    echo "still running after SIGKILL: ${pids//$'\n'/ }" >&2
    return 1
  fi
}

# Kill the keeper if it still runs, then reap it once it has exited. `timeout`
# leads the keeper's process group, which holds the keeper and its clients but
# not its workers.
stop_keeper() {
  local poll
  if [[ -z "$keeper_pid" ]]; then
    return 0
  fi
  if [[ -n "$keeper_start" ]]; then
    signal_process KILL "$keeper_pid" "$keeper_start"
    for ((poll = 0; poll < reap_polls; poll++)); do
      if process_gone "$keeper_pid" "$keeper_start"; then
        break
      fi
      sleep 0.1
    done
    if ! process_gone "$keeper_pid" "$keeper_start"; then
      echo "the keeper's timeout process $keeper_pid still runs after SIGKILL" >&2
      return 1
    fi
  fi
  wait "$keeper_pid" 2>/dev/null || true
  keeper_pid=''
}

cleanup() {
  local status="$1"
  local survivors clean=true
  # A second signal must not cut cleanup short and leave workers running.
  trap '' HUP INT QUIT PIPE TERM
  set +e
  stop_keeper || clean=false
  survivors=$(stop_run_processes 0) || clean=false
  if [[ -n "$survivors" ]]; then
    echo "killed processes left by this run: $survivors" >&2
  fi
  # A failed cleanup fails a run that would otherwise pass. Any other status,
  # including a signal's, is kept.
  if [[ "$clean" == false && "$status" -eq 0 ]]; then
    echo "FAIL: this run's processes could not all be stopped" >&2
    status=1
  fi
  if [[ -n "$test_dir" ]]; then
    if [[ "$status" -ne 0 && -f "$test_dir/keeper.log" ]]; then
      echo '--- keeper ---' >&2
      cat "$test_dir/keeper.log" >&2
    fi
    # Remove only this run's known file, not a directory tree.
    rm -f -- "$test_dir/keeper.log"
    rmdir -- "$test_dir"
  fi
  exit "$status"
}

# Each signal exits with 128+N, and EXIT alone performs cleanup.
exit_on_signals() {
  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 131' QUIT
  trap 'exit 141' PIPE
  trap 'exit 143' TERM
}

# While the keeper starts, a signal only records its exit status, so cleanup
# always knows the keeper's pid.
defer_signals() {
  trap 'pending_status=129' HUP
  trap 'pending_status=130' INT
  trap 'pending_status=131' QUIT
  trap 'pending_status=141' PIPE
  trap 'pending_status=143' TERM
}

# Install the traps before allocating the temporary directory or starting the
# keeper.
trap 'cleanup "$?"' EXIT
exit_on_signals

read -r _ _ own_pgid <<<"$(process_stat $$)"
test_dir=$(mktemp -d /tmp/monarch-fault-tolerant-client-test.XXXXXX)
keeper_log="$test_dir/keeper.log"

# `timeout` puts itself and the keeper in a new process group and signals that
# group at the limit, with KILL if TERM doesn't end it. Output goes to a file,
# so nothing left behind holds the test runner's pipes. Workers otherwise arm
# the parent-death signal and die with the keeper; turning it off lets the
# check below see what the keeper's own shutdown leaves.
defer_signals
env "$run_marker" FTC_INJECT="$injection" \
  HYPERACTOR_MESH_BOOTSTRAP_ENABLE_PDEATHSIG=false \
  timeout --kill-after=10s "${keeper_limit}s" "$binary" \
  </dev/null >"$keeper_log" 2>&1 &
keeper_pid=$!
if keeper_stat=$(process_stat "$keeper_pid"); then
  read -r _ keeper_start _ <<<"$keeper_stat"
fi
exit_on_signals
if [[ -n "$pending_status" ]]; then
  exit "$pending_status"
fi
launched_at=$SECONDS
keeper_status=0
wait "$keeper_pid" || keeper_status=$?
keeper_pid=''

# `timeout` exits 124 at the limit, or 137 if it then needed KILL. Another
# SIGKILL also gives 137, so that counts only once the limit has passed.
timed_out=false
if [[ "$keeper_status" -eq 124 ]] ||
  [[ "$keeper_status" -eq 137 && $((SECONDS - launched_at)) -ge "$keeper_limit" ]]; then
  timed_out=true
fi

# A keeper killed at the limit leaves its workers behind, so kill them without
# waiting and report the timeout. Otherwise the keeper stops its workers before
# it exits, so in a normal run anything still running is a failure.
settle=$settle_polls
if [[ "$timed_out" == true ]]; then
  settle=0
fi
if ! survivors=$(stop_run_processes "$settle"); then
  fail "processes of this run are still running after SIGKILL"
fi
if [[ "$timed_out" == true ]]; then
  fail "the keeper did not finish within ${keeper_limit}s${survivors:+; killed what it left: $survivors}"
fi
if [[ -n "$survivors" ]]; then
  if [[ -z "$injection" ]]; then
    fail "processes outlived the keeper: $survivors"
  fi
  echo "killed processes left by this run: $survivors" >&2
fi

expect_line() {
  grep -Eq -- "$1" "$keeper_log" || fail "missing '$1' in the keeper's log"
}

case "$injection" in
  '')
    [[ "$keeper_status" -eq 0 ]] || fail "keeper exited with $keeper_status, expected 0"
    expect_line '^\[client 1\] crashing during step 3$'
    expect_line '^\[client 2\] connected to trainer \(pid [0-9]+\) at step 3$'
    expect_line '^\[client\] supervision check: SupervisionError$'
    expect_line '^\[keeper\] result: PASS$'
    echo 'PASS: a replacement client resumed on the same workers; a killed generator raised SupervisionError'
    ;;
  check)
    [[ "$keeper_status" -eq 1 ]] || fail "keeper exited with $keeper_status, expected 1"
    expect_line '^\[client\] supervision check: returned$'
    expect_line '^\[keeper\] result: FAIL \(client 2 failed a check\)$'
    echo 'PASS: with the generator kill skipped, the supervision check failed the run'
    ;;
  hang)
    [[ "$keeper_status" -eq 1 ]] || fail "keeper exited with $keeper_status, expected 1"
    expect_line '^\[client 2\] FTC_INJECT=hang: sleeping past the client deadline$'
    expect_line '^\[keeper\] client 2 timed out; killed$'
    expect_line '^\[keeper\] result: FAIL \(client 2 timed out\)$'
    echo 'PASS: the keeper killed a hung client at its deadline and failed the run'
    ;;
esac
