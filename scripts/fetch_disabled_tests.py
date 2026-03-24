#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Fetch tests disabled via GitHub issues and write them to disabled_tests.txt.

Any open issue on meta-pytorch/monarch whose title begins with "DISABLED "
is treated as a disabled test.  The remainder of the title is the test name
as it appears in CI (a pytest node ID or cargo nextest test path).

Run this script from the project root before running tests.

Outputs:
  disabled_tests.txt            -- one test name per line; read by conftest.py
  .config/nextest-disabled.toml -- nextest tool-config with a filter expression;
                                   pass to `cargo nextest run --tool-config-file`
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path


_REPO = "meta-pytorch/monarch"
_DISABLED_TESTS_FILE = Path("disabled_tests.txt")
_NEXTEST_FILTER_FILE = Path(".config/nextest-filter.txt")


def fetch_disabled_test_names() -> list[str]:
    url = f"https://api.github.com/repos/{_REPO}/issues?state=open&per_page=100"
    headers = {"Accept": "application/vnd.github+json"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            issues: list[dict[str, object]] = json.loads(resp.read().decode())
    except urllib.error.URLError as e:
        print(f"Warning: could not fetch GitHub issues: {e}", file=sys.stderr)
        return []

    return [
        issue["title"][len("DISABLED ") :].strip()  # type: ignore[index]
        for issue in issues
        if isinstance(issue.get("title"), str)
        and str(issue["title"]).startswith("DISABLED ")
        and "pull_request" not in issue
    ]


def write_disabled_tests(names: list[str]) -> None:
    _DISABLED_TESTS_FILE.write_text("\n".join(names) + ("\n" if names else ""))
    if names:
        print(f"Wrote {len(names)} disabled test(s) to {_DISABLED_TESTS_FILE}:")
        for name in names:
            print(f"  {name}")
    else:
        print(f"No disabled tests found; wrote empty {_DISABLED_TESTS_FILE}.")


def _nextest_predicate(name: str) -> str:
    """Build a nextest filter predicate for one test name.

    The nextest CI output format is "<binary> <test_path>", e.g.
    "hyperactor proc::tests::test_child_lifecycle".  When a name has
    exactly one space and the part before the space contains no "::",
    treat it as "<binary> <test_path>" and emit:
        binary(binary_name) and test(test_path)

    Otherwise emit a plain substring match:
        test(name)
    """
    parts = name.split(" ", 1)
    if len(parts) == 2 and "::" not in parts[0]:
        binary, test_path = parts
        return f"binary({binary}) and test({test_path})"
    return f"test({name})"


def write_nextest_filter(names: list[str]) -> None:
    """Write the nextest -E filter expression to .config/nextest-filter.txt.

    CI scripts read this file and pass the value to `cargo nextest run -E`.
    When there are no disabled tests the file contains "all()" so the -E
    flag can be unconditional.
    """
    if names:
        predicates = " | ".join(_nextest_predicate(n) for n in names)
        expr = f"not ({predicates})"
        print(f"Wrote {_NEXTEST_FILTER_FILE}: {expr}")
    else:
        expr = "all()"

    _NEXTEST_FILTER_FILE.write_text(expr + "\n")


def main() -> None:
    names = fetch_disabled_test_names()
    write_disabled_tests(names)
    write_nextest_filter(names)


if __name__ == "__main__":
    main()
