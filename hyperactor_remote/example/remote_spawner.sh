#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set -euo pipefail

target='fbcode//monarch/hyperactor_remote:hyperactor-remote-example-remote-spawner'
tmp="$(mktemp -d)"
proc_pid=''
driver_pid=''
mode="${1:-driver}"

case "${mode}" in
  driver | overflow) ;;
  *)
    echo "usage: $0 [driver|overflow]" >&2
    exit 2
    ;;
esac

cleanup() {
  if [[ -n "${driver_pid}" ]]; then
    kill "${driver_pid}" 2>/dev/null || true
    wait "${driver_pid}" 2>/dev/null || true
  fi
  if [[ -n "${proc_pid}" ]]; then
    kill "${proc_pid}" 2>/dev/null || true
    wait "${proc_pid}" 2>/dev/null || true
  fi
  rm -rf "${tmp}"
}
trap cleanup EXIT

wait_for_file() {
  local path="$1"
  for _ in $(seq 1 100); do
    if [[ -s "${path}" ]]; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for ${path}" >&2
  return 1
}

repo_root="$(buck root --kind project)"
bin="${repo_root}/$(buck build --show-output "${target}" | awk '{print $2}')"

main() {
  "${bin}" proc --token-file "${tmp}/proc-token" &
  proc_pid="$!"

  wait_for_file "${tmp}/proc-token"

  "${bin}" "${mode}" --token-file "${tmp}/proc-token" &
  driver_pid="$!"

  local driver_status=0
  wait "${driver_pid}" || driver_status="$?"
  driver_pid=''
  if [[ "${driver_status}" -ne 0 ]]; then
    echo "${mode} driver exited with status ${driver_status}"
  fi

  kill "${proc_pid}" 2>/dev/null || true
  wait "${proc_pid}" 2>/dev/null || true
  proc_pid=''

  return "${driver_status}"
}

main
