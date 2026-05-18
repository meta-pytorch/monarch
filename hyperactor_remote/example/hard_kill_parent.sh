#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set -euo pipefail

target='fbcode//monarch/hyperactor_remote:hyperactor-remote-example-token-supervision'
tmp="$(mktemp -d)"
parent_pid=''
joiner_pid=''

cleanup() {
  if [[ -n "${joiner_pid}" ]]; then
    kill "${joiner_pid}" 2>/dev/null || true
  fi
  if [[ -n "${parent_pid}" ]]; then
    kill "${parent_pid}" 2>/dev/null || true
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

"${bin}" parent \
  --token-file "${tmp}/token" \
  --ready-file "${tmp}/parent-linked" \
  --event-file "${tmp}/parent-event" &
parent_pid="$!"

wait_for_file "${tmp}/token"

"${bin}" joiner \
  --token-file "${tmp}/token" \
  --ready-file "${tmp}/joiner-ready" \
  --stopped-file "${tmp}/child-stopped" \
  --exited-file "${tmp}/joiner-exited" &
joiner_pid="$!"

wait_for_file "${tmp}/joiner-ready"
kill -9 "${parent_pid}"
wait "${parent_pid}" 2>/dev/null || true
parent_pid=''

wait_for_file "${tmp}/child-stopped"
wait_for_file "${tmp}/joiner-exited"
echo "child stopped after parent death:"
sed -n '1,5p' "${tmp}/child-stopped"
