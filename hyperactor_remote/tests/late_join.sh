#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Exercise late_join's trainer and clients as separate processes on one host.
set -euo pipefail

binary="${1:?Usage: late_join.sh <late-join-binary>}"
test_dir=''
trainer_pid=''

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# Wait for our child to exit before reaping it, so wait cannot hang. The
# trainer runs Tokio tasks but does not launch descendant OS processes.
wait_for_trainer_exit() {
  local attempt
  for ((attempt = 0; attempt < 50; attempt++)); do
    if ! kill -0 "$trainer_pid" 2>/dev/null; then
      wait "$trainer_pid" 2>/dev/null || true
      trainer_pid=''
      return 0
    fi
    sleep 0.1
  done
  return 1
}

stop_trainer() {
  if [[ -z "$trainer_pid" ]]; then
    return 0
  fi
  kill -TERM "$trainer_pid" 2>/dev/null || true
  if wait_for_trainer_exit; then
    return 0
  fi
  # A stopped or unresponsive trainer must not hold up the test's cleanup.
  kill -KILL "$trainer_pid" 2>/dev/null || true
  if ! wait_for_trainer_exit; then
    echo "FAIL: trainer $trainer_pid did not exit after SIGKILL" >&2
    return 1
  fi
}

cleanup() {
  local status="$1"
  set +e
  # A signal can arrive after launch but before trainer_pid=$!. This standalone
  # script has only one background job; recover it from Bash's job table.
  if [[ -z "$trainer_pid" ]]; then
    trainer_pid=$(jobs -p)
  fi
  stop_trainer
  if [[ -n "$test_dir" ]]; then
    if [[ "$status" -ne 0 ]]; then
      local log
      for log in trainer client_first client_second client_stale; do
        if [[ -f "$test_dir/$log.log" ]]; then
          echo "--- $log ---" >&2
          cat "$test_dir/$log.log" >&2
        fi
      done
    fi
    # Remove only this run's known files, not a directory tree.
    rm -f -- "$test_dir/token" "$test_dir/trainer.log" \
      "$test_dir/client_first.log" "$test_dir/client_second.log" \
      "$test_dir/client_stale.log"
    rmdir -- "$test_dir"
  fi
  exit "$status"
}

# Signal traps exit; EXIT alone performs cleanup. Install them before allocating
# the temporary directory or starting a child, including partial-startup failures.
trap 'cleanup "$?"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 131' QUIT
trap 'exit 141' PIPE
trap 'exit 143' TERM

test_dir=$(mktemp -d /tmp/monarch-late-join-test.XXXXXX)
token_file="$test_dir/token"
trainer_log="$test_dir/trainer.log"

# Redirect child descriptors to files, not the test runner's output pipes.
"$binary" trainer --token-file "$token_file" </dev/null >"$trainer_log" 2>&1 &
trainer_pid=$!

# Wait for actual training progress before starting the first client. The token
# file alone only proves that the actors were initialized, not that a step ran.
ready=false
for ((attempt = 0; attempt < 300; attempt++)); do
  if [[ -s "$token_file" ]] && grep -Eq '^trainer: step [0-9]+ loss ' "$trainer_log"; then
    ready=true
    break
  fi
  kill -0 "$trainer_pid" 2>/dev/null || fail 'trainer exited before becoming ready'
  sleep 0.1
done
[[ "$ready" == true ]] || fail 'trainer did not publish a token and report progress within 30s'

run_client() {
  local rate="$1"
  local log="$2"
  # GNU timeout supervises the client's process group; KILL is the fallback if
  # TERM is ignored. Do not use --foreground, which would exclude descendants.
  timeout --kill-after=2s 40s "$binary" client \
    --token-file "$token_file" --learning-rate "$rate" </dev/null >"$log" 2>&1
}

expect_line() {
  local pattern="$1"
  local log="$2"
  grep -Eq "$pattern" "$log" || fail "missing '$pattern' in $log"
}

# Each client is a fresh process. Both must read and update the same live state
# through the same token, rather than creating or resetting a trainer.
first_log="$test_dir/client_first.log"
run_client 0.05 "$first_log" || fail 'first client failed or timed out'
expect_line '^client: connected; probe is ' "$first_log"
expect_line '^client: step [0-9]+ loss [^ ]+ learning rate 0.01$' "$first_log"
expect_line '^client: learning rate changed from 0.01 to 0.05$' "$first_log"
expect_line '^client: step [0-9]+ loss [^ ]+ learning rate 0.05$' "$first_log"

second_log="$test_dir/client_second.log"
run_client 0.02 "$second_log" || fail 'second client failed or timed out'
expect_line '^client: connected; probe is ' "$second_log"
expect_line '^client: step [0-9]+ loss [^ ]+ learning rate 0.05$' "$second_log"
expect_line '^client: learning rate changed from 0.05 to 0.02$' "$second_log"
expect_line '^client: step [0-9]+ loss [^ ]+ learning rate 0.02$' "$second_log"
kill -0 "$trainer_pid" 2>/dev/null || fail 'trainer stopped when a client exited'

# Reap the trainer before trying its retained token, so a successful connection cannot
# race shutdown. The client's own failure must end it, not our outer timeout.
stop_trainer || fail 'could not stop the trainer'
set +e
run_client 0.03 "$test_dir/client_stale.log"
stale_status=$?
set -e
[[ "$stale_status" -eq 1 ]] || fail "stale-token client exited with $stale_status, expected 1"

echo 'PASS: late clients share live state; an old token fails after trainer exit'
