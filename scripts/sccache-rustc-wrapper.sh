#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# RUSTC_WRAPPER shim used by CI. It prefers sccache, but falls back to invoking
# rustc directly if sccache itself is unavailable (for example when the remote
# S3 cache returns a transient 5xx while the sccache server is starting). Rust
# compiler errors are still returned unchanged.

set -u

if [ "$#" -lt 1 ]; then
    echo "sccache-rustc-wrapper: expected rustc path as first argument" >&2
    exit 2
fi

# Once one invocation observes an sccache infrastructure failure, bypass
# sccache for the rest of the job. Cargo can invoke the wrapper many times in
# parallel; using a sentinel avoids paying the sccache startup failure cost for
# every rustc process.
disable_file="${SCCACHE_FALLBACK_DISABLE_FILE:-/tmp/monarch-sccache-disabled}"
if [ -f "$disable_file" ]; then
    exec "$@"
fi

if ! command -v sccache >/dev/null 2>&1; then
    echo "sccache-rustc-wrapper: sccache not found; falling back to rustc" >&2
    mkdir -p "$(dirname "$disable_file")" 2>/dev/null || true
    touch "$disable_file" 2>/dev/null || true
    exec "$@"
fi

stderr_file="$(mktemp -t monarch-sccache-stderr.XXXXXX)"

set +e
sccache "$@" 2>"$stderr_file"
status=$?
set -e

if [ "$status" -eq 0 ]; then
    rm -f "$stderr_file"
    exit 0
fi

# sccache failed. Rather than parse its error text to guess whether the fault
# was the cache or the compiler, just retry with plain rustc. A genuine
# compiler error reproduces (rustc fails the same way and we propagate it); an
# sccache infrastructure failure (S3 5xx, cache or connection error) does not,
# so rustc succeeds and the build proceeds without the cache.
set +e
"$@"
rustc_status=$?
set -e

if [ "$rustc_status" -eq 0 ]; then
    # rustc succeeded where sccache did not, so sccache itself is at fault.
    # Surface its error once and disable it for the rest of the job to avoid
    # repeating the failing startup on every subsequent invocation.
    echo "sccache-rustc-wrapper: sccache failed (exit ${status}) but rustc succeeded; disabling sccache for the rest of this job" >&2
    sed 's/^/sccache-rustc-wrapper: /' "$stderr_file" >&2
    mkdir -p "$(dirname "$disable_file")" 2>/dev/null || true
    touch "$disable_file" 2>/dev/null || true
fi

rm -f "$stderr_file"
exit "$rustc_status"
