#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set -euo pipefail

bin_name='hyperactor_remote_example_remote_spawner'
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../.." && pwd)"
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

build_binary() {
  (
    cd "${repo_root}"
    cargo build -p hyperactor_remote --bin "${bin_name}"
  )
}

binary_path() {
  local target_dir="${CARGO_TARGET_DIR:-${repo_root}/target}"
  if [[ "${target_dir}" != /* ]]; then
    target_dir="${repo_root}/${target_dir}"
  fi
  printf '%s/debug/%s\n' "${target_dir}" "${bin_name}"
}

main() {
  build_binary
  local bin
  bin="$(binary_path)"

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
