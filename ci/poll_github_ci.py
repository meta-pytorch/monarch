# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Poll GitHub Actions check runs until OSS CI for a Phabricator diff version resolves.

Auth uses the current user on devservers and the read-only diff reader bot in
Sandcastle. This avoids depending on jf being installed on Sandcastle workers.

Implementation notes:
  1. The version FBID must be looked up from phabricator_versions; jf diff-properties
     returns the latest (post-landing) FBID which may differ from the CI version.
  2. The Phabricator CI signal mirror can lag or omit OSS GitHub checks. The
     source of truth here is the linked GitHub PR's check runs.

Exits 0 if all OSS CI signals pass, 1 if any fail, 2 on timeout/infra error.

Usage (via Skycastle):
    buck run fbcode//monarch/ci:poll_github_ci -- --diff D12345678 --version-number 384114609
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Any

from libfb.py.environment import is_sandcastle
from libfb.py.interngraph.auth.interngraph_crypto_auth_token_util import (
    InternGraphCryptoAuthTokenUtil,
)
from libfb.py.interngraph.graphql.graphql_query import GraphQLClient


LOG: logging.Logger = logging.getLogger(__name__)

MAX_WAIT_SECS = 5400  # 90 min — stay under the 2-hour Sandcastle wall-clock kill
INITIAL_BACKOFF_SECS = 30
MAX_BACKOFF_SECS = 300

REQUIRED_CHECK_RUN_PREFIXES = (
    "Build CPU /",
    "Build Documentation /",
    "Build GPU /",
    "Test CPU Python /",
    "Test CPU Rust /",
    "Test GPU Python /",
    "Test GPU Rust /",
    "Type Check Python /",
)
IGNORED_CHECK_RUN_NAMES = {
    "Build CPU (macOS)",
    "Status Check",
    "Test CPU Python (macOS)",
    "Test CPU Rust (macOS)",
    "deploy",
}
IGNORED_CHECK_RUN_PREFIXES = ("Build Docker image /",)

_PASS_CONCLUSIONS = {"NEUTRAL", "SKIPPED", "SUCCESS"}
_FAIL_CONCLUSIONS = {"ACTION_REQUIRED", "CANCELLED", "FAILURE", "TIMED_OUT"}

# Matches PhabricatorAuthStrategyFactory.diff_reader_bot().
_DIFF_READER_BOT_FBID = 89002005288303
_DIFF_READER_BOT_SERVICE = "diff.reader.bot"


def _reader_bot_security_params() -> dict[str, str]:
    cats = InternGraphCryptoAuthTokenUtil.get_serialized_token_list_for_service(
        service_identity=_DIFF_READER_BOT_SERVICE,
        service_user_fbid=_DIFF_READER_BOT_FBID,
        app_id=GraphQLClient.INTERN_GRAPHQL_APP,
        token_timeout_seconds=7200,
    )
    return InternGraphCryptoAuthTokenUtil.get_auth_data(
        app_id=GraphQLClient.INTERN_GRAPHQL_APP,
        crypto_auth_tokens=cats,
    )


def _graphql(
    query: str,
    params: dict[str, Any] | None = None,
    timeout_seconds: int = 60,
) -> dict[str, Any]:
    if is_sandcastle():
        return GraphQLClient.intern_query(
            query,
            params or {},
            security_params=_reader_bot_security_params(),
            raise_exception=True,
            timeout_seconds=timeout_seconds,
        )

    try:
        return GraphQLClient.intern_query(
            query,
            params or {},
            raise_exception=True,
            timeout_seconds=timeout_seconds,
        )
    except RuntimeError:
        return GraphQLClient.intern_query(
            query,
            params or {},
            security_params=_reader_bot_security_params(),
            raise_exception=True,
            timeout_seconds=timeout_seconds,
        )


def _get_version_fbid(diff_num: int, version_num: int) -> str:
    """Look up the FBID for a specific phabricator version number.

    Uses the version list because diff-properties returns the latest version FBID,
    which may differ from the CI version.
    """
    query = (
        "{ phabricator_diff(number: %d) "
        "{ phabricator_versions { edges { node { id number } } } } }" % diff_num
    )
    data = _graphql(query)
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


def _get_github_prs(diff_num: int) -> list[dict[str, Any]]:
    query = (
        "query GetGitHubPRChecks($diffNumber: String!) { "
        "phabricator_diff(number: $diffNumber) { "
        "opensource_github_pull_requests(first: 10) { nodes { "
        "number github_url head_sha synced_at_timestamp "
        "phabricator_version_links(first: 50) { nodes { "
        "creation_time head_sha phabricator_version { id number } "
        "} } "
        "test_check_runs(include_internal: false) { "
        "name status conclusion app_name sha updated_at_timestamp details_url "
        "} "
        "} } "
        "} "
        "}"
    )
    data = _graphql(query, {"diffNumber": str(diff_num)}, timeout_seconds=90)
    return (
        data.get("phabricator_diff", {})
        .get("opensource_github_pull_requests", {})
        .get("nodes", [])
    )


def _version_links(pr: dict[str, Any]) -> list[dict[str, Any]]:
    return pr.get("phabricator_version_links", {}).get("nodes", [])


def _latest_version_link(
    prs: list[dict[str, Any]],
    version_fbid: str,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    matches = []
    for pr in prs:
        for link in _version_links(pr):
            version = link.get("phabricator_version")
            if version is not None and str(version.get("id")) == version_fbid:
                matches.append((pr, link))

    if not matches:
        return None

    return max(matches, key=lambda match: int(match[1]["creation_time"]))


def _matching_check_runs(
    pr: dict[str, Any],
    head_sha: str,
) -> list[dict[str, Any]]:
    return [
        check_run
        for check_run in pr.get("test_check_runs", [])
        if check_run.get("app_name") == "GitHub Actions"
        and check_run.get("sha") == head_sha
    ]


def _is_ignored_check_run(check_run: dict[str, Any]) -> bool:
    name = check_run["name"]
    return (
        name in IGNORED_CHECK_RUN_NAMES
        or " / set-matrix / " in name
        or any(name.startswith(prefix) for prefix in IGNORED_CHECK_RUN_PREFIXES)
    )


def _real_check_runs(check_runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        check_run for check_run in check_runs if not _is_ignored_check_run(check_run)
    ]


def _missing_required_check_runs(check_runs: list[dict[str, Any]]) -> list[str]:
    real_check_runs = _real_check_runs(check_runs)
    return [
        required_prefix
        for required_prefix in REQUIRED_CHECK_RUN_PREFIXES
        if not any(
            check_run["name"].startswith(required_prefix)
            for check_run in real_check_runs
        )
    ]


def _pending_check_runs(check_runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        check_run
        for check_run in check_runs
        if check_run.get("status") != "COMPLETED"
        or check_run.get("conclusion") not in (_PASS_CONCLUSIONS | _FAIL_CONCLUSIONS)
    ]


def _failed_check_runs(check_runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        check_run
        for check_run in check_runs
        if check_run.get("conclusion") in _FAIL_CONCLUSIONS
    ]


def _passed_check_runs(check_runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        check_run
        for check_run in check_runs
        if check_run.get("conclusion") in _PASS_CONCLUSIONS
    ]


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )

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

    LOG.info("resolving version FBID for %s v%s", diff_str, args.version_number)
    version_fbid = _get_version_fbid(diff_num, args.version_number)
    LOG.info("resolved version FBID: %s", version_fbid)

    backoff = INITIAL_BACKOFF_SECS
    start = time.monotonic()

    while True:
        elapsed = int(time.monotonic() - start)

        if elapsed > MAX_WAIT_SECS:
            LOG.error(
                "timeout: OSS CI signals did not resolve within %dm",
                MAX_WAIT_SECS // 60,
            )
            sys.exit(2)

        try:
            prs = _get_github_prs(diff_num)
        except Exception as e:
            LOG.warning("[%ss] query error: %s", elapsed, e)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECS)
            continue

        match = _latest_version_link(prs, version_fbid)
        if match is None:
            LOG.info(
                "[%ss] no linked GitHub PR for version FBID %s yet",
                elapsed,
                version_fbid,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECS)
            continue

        pr, link = match
        head_sha = link["head_sha"]
        check_runs = _matching_check_runs(pr, head_sha)
        if not check_runs:
            LOG.info(
                "[%ss] no GitHub Actions check runs yet for PR %s at %s",
                elapsed,
                pr["number"],
                head_sha,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECS)
            continue

        real_check_runs = _real_check_runs(check_runs)
        missing_required = _missing_required_check_runs(check_runs)
        pending = _pending_check_runs(real_check_runs)
        failed = _failed_check_runs(real_check_runs)
        passed = _passed_check_runs(real_check_runs)

        LOG.info(
            "[%ss] OSS CI for PR %s at %s: %s passed, %s failed, %s pending, %s required checks not visible yet",
            elapsed,
            pr["number"],
            head_sha,
            len(passed),
            len(failed),
            len(pending),
            len(missing_required),
        )
        for check_run in failed:
            LOG.error("failed check run: %s", check_run["name"])
        for check_run in pending[:20]:
            LOG.info("pending check run: %s", check_run["name"])
        for check_run in missing_required:
            LOG.info("waiting for required check run: %s", check_run)

        if not pending and not missing_required:
            if failed:
                LOG.error("GitHub CI failed: %s check run(s) failed", len(failed))
                sys.exit(1)
            LOG.info(
                "GitHub CI passed: all %s real check run(s) resolved",
                len(real_check_runs),
            )
            sys.exit(0)

        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF_SECS)
