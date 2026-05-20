# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Poll ci_signals_result until all oss_ci signals for a Phabricator diff version resolve.

Two bugs found during testing and fixed here:
  1. jf diff-properties returns the LATEST (post-landing) version FBID, not the
     version that CI ran on.  The Skycastle phabricator_version_number flag gives
     the correct version; we look up its FBID via phabricator_versions GraphQL.
  2. ci_signals_result silently returns 0 results when the query uses a GraphQL
     variable for the value field.  The FBID must be inlined into the query string.

Exits 0 if all OSS CI signals pass, 1 if any fail, 2 on timeout/infra error.

Usage (via Skycastle buck_run):
    buck run fbcode//monarch/ci:poll_github_ci -- --diff D12345678 --version-number 384114609
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from security.frameworks.python.exec.subprocess import TrustedSubprocessWithList


OSS_SIGNAL_PREFIX = "meta-pytorch/monarch: CI /"
MAX_WAIT_SECS = 5400  # 90 min — stay under the 2-hour Sandcastle wall-clock kill
INITIAL_BACKOFF_SECS = 30
MAX_BACKOFF_SECS = 300

_PASS_STATUSES = {"GOOD", "PASSED"}
_FAIL_STATUSES = {"FAILED", "ERROR"}
_WARN_STATUSES = {"WARNING", "WARNED"}
# Any of the above = terminal; anything else (PENDING*, NOT_FOUND) = keep waiting


def _jf(*args: str, timeout: int = 60) -> str:
    result = TrustedSubprocessWithList.run(
        executable="jf",
        cmd_args=list(args),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(f"jf {' '.join(args)} failed:\n{result.stderr.strip()}")
    return result.stdout


def _get_version_fbid(diff_num: int, version_num: int) -> str:
    """Look up the FBID for a specific phabricator version number.

    ci_signals_result requires the version FBID (not the diff FBID or version
    number).  We get it by querying phabricator_versions for the diff and
    matching on the version number.

    Note: the query uses an inline integer literal — variable substitution
    silently returns 0 results from ci_signals_result due to a JF GraphQL quirk.
    """
    query = (
        "{ phabricator_diff(number: %d) "
        "{ phabricator_versions { edges { node { id number } } } } }" % diff_num
    )
    raw = _jf("graphql", "--query", query)
    data = json.loads(raw)
    edges = (
        data.get("phabricator_diff", {})
        .get("phabricator_versions", {})
        .get("edges", [])
    )
    for edge in edges:
        node = edge["node"]
        if int(node["number"]) == version_num:
            return node["id"]
    raise RuntimeError(
        f"Version {version_num} not found in D{diff_num}'s phabricator_versions"
    )


def _get_oss_ci_signals(version_fbid: str) -> list[dict]:
    """Return all oss_ci signals for this diff version whose name starts with
    OSS_SIGNAL_PREFIX.

    The FBID must be inlined into the query string — GraphQL variables cause
    ci_signals_result to silently return 0 results (JF GraphQL bug).
    """
    query = (
        '{ ci_signals_result(query_key:{type:PHABRICATOR_VERSION_FBID,value:"%s"})'
        "{ signals(first:1000,filters:{}) { nodes { name status } } } }" % version_fbid
    )
    raw = _jf("graphql", "--query", query, timeout=90)
    data = json.loads(raw)
    nodes = data.get("ci_signals_result", {}).get("signals", {}).get("nodes", [])
    return [n for n in nodes if n["name"].startswith(OSS_SIGNAL_PREFIX)]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wait for monarch GitHub Actions CI signals to resolve."
    )
    parser.add_argument(
        "--diff", required=True, help="Phabricator diff, e.g. D12345678"
    )
    parser.add_argument(
        "--version-number",
        required=True,
        type=int,
        help="Phabricator version number for this CI run (phabricator_version_number flag)",
    )
    args = parser.parse_args()

    diff_str = args.diff if args.diff.startswith("D") else f"D{args.diff}"
    diff_num = int(diff_str.lstrip("D"))

    _jf("arcrc", "--reader-bot")

    print(f"Resolving version FBID for {diff_str} v{args.version_number}...")
    version_fbid = _get_version_fbid(diff_num, args.version_number)
    print(f"  FBID: {version_fbid}")

    backoff = INITIAL_BACKOFF_SECS
    start = time.monotonic()

    while True:
        elapsed = int(time.monotonic() - start)

        if elapsed > MAX_WAIT_SECS:
            print(
                f"TIMEOUT: OSS CI signals did not resolve within {MAX_WAIT_SECS // 60}m",
                file=sys.stderr,
            )
            sys.exit(2)

        try:
            signals = _get_oss_ci_signals(version_fbid)
        except Exception as e:
            print(f"[{elapsed}s] query error: {e}", file=sys.stderr)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECS)
            continue

        if not signals:
            print(f"[{elapsed}s] no OSS CI signals yet")
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECS)
            continue

        pending = [
            s
            for s in signals
            if s["status"] not in (_PASS_STATUSES | _FAIL_STATUSES | _WARN_STATUSES)
        ]
        failed = [s for s in signals if s["status"] in _FAIL_STATUSES]
        passed = [s for s in signals if s["status"] in _PASS_STATUSES | _WARN_STATUSES]

        print(
            f"[{elapsed}s] OSS CI: {len(passed)} passed, {len(failed)} failed, {len(pending)} pending"
        )
        for s in failed:
            print(f"  FAILED: {s['name']}")

        if not pending:
            if failed:
                print(
                    f"\nGitHub CI FAILED: {len(failed)} signal(s) failed",
                    file=sys.stderr,
                )
                sys.exit(1)
            print(f"\nGitHub CI passed: all {len(signals)} OSS CI signal(s) resolved.")
            sys.exit(0)

        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF_SECS)
