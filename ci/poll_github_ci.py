# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Poll ci_signals_result until all oss_ci signals for a Phabricator diff version resolve.

Auth uses the current user on devservers and the read-only diff reader bot in
Sandcastle. This avoids depending on jf being installed on Sandcastle workers.

Two implementation notes:
  1. The version FBID must be looked up from phabricator_versions; jf diff-properties
     returns the latest (post-landing) FBID which may differ from the CI version.
  2. ci_signals_result silently returns 0 results when the FBID is a GraphQL variable;
     the FBID must be inlined into the query string.

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

OSS_SIGNAL_PREFIX = "meta-pytorch/monarch: "
REQUIRED_WORKFLOW_PREFIXES = (
    "meta-pytorch/monarch: CI /",
    "meta-pytorch/monarch: CI (macOS) /",
    "meta-pytorch/monarch: Docs /",
)
MAX_WAIT_SECS = 5400  # 90 min — stay under the 2-hour Sandcastle wall-clock kill
INITIAL_BACKOFF_SECS = 30
MAX_BACKOFF_SECS = 300

_PASS_STATUSES = {"GOOD", "PASSED", "SKIPPED"}
_FAIL_STATUSES = {"FAILED", "ERROR"}
_WARN_STATUSES = {"WARNING", "WARNED"}

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


def _get_oss_ci_signals(version_fbid: str) -> list[dict[str, Any]]:
    """Return all GitHub Actions signals for the Monarch OSS repository.

    The FBID must be inlined — GraphQL variables cause ci_signals_result to
    silently return 0 results (JF GraphQL bug).
    """
    version_fbid = str(int(version_fbid))
    query = (
        '{ ci_signals_result(query_key:{type:PHABRICATOR_VERSION_FBID,value:"%s"})'
        "{ signals(first:1000,filters:{}) { nodes { name status } } } }" % version_fbid
    )
    data = _graphql(query, timeout_seconds=90)
    nodes = data.get("ci_signals_result", {}).get("signals", {}).get("nodes", [])
    return [n for n in nodes if n["name"].startswith(OSS_SIGNAL_PREFIX)]


def _missing_required_workflows(signals: list[dict[str, Any]]) -> list[str]:
    return [
        prefix
        for prefix in REQUIRED_WORKFLOW_PREFIXES
        if not any(s["name"].startswith(prefix) for s in signals)
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
            signals = _get_oss_ci_signals(version_fbid)
        except Exception as e:
            LOG.warning("[%ss] query error: %s", elapsed, e)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECS)
            continue

        if not signals:
            LOG.info("[%ss] no OSS CI signals yet", elapsed)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECS)
            continue

        missing_workflows = _missing_required_workflows(signals)
        pending = [
            s
            for s in signals
            if s["status"] not in (_PASS_STATUSES | _FAIL_STATUSES | _WARN_STATUSES)
        ]
        failed = [s for s in signals if s["status"] in _FAIL_STATUSES]
        passed = [s for s in signals if s["status"] in _PASS_STATUSES | _WARN_STATUSES]

        LOG.info(
            "[%ss] OSS CI: %s passed, %s failed, %s pending, %s workflows not visible yet",
            elapsed,
            len(passed),
            len(failed),
            len(pending),
            len(missing_workflows),
        )
        for s in failed:
            LOG.error("failed signal: %s", s["name"])
        for workflow in missing_workflows:
            LOG.info("waiting for workflow: %s", workflow)

        if not pending and not missing_workflows:
            if failed:
                LOG.error("GitHub CI failed: %s signal(s) failed", len(failed))
                sys.exit(1)
            LOG.info("GitHub CI passed: all %s OSS CI signal(s) resolved", len(signals))
            sys.exit(0)

        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF_SECS)
