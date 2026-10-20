# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Validate manual release choices before starting wheel builds."""

import os
from pathlib import Path

from packaging.version import InvalidVersion, Version


def prepare_release(
    event_name: str, release_type: str, version: str
) -> tuple[str, str]:
    if event_name != "workflow_dispatch":
        return "nightly", ""

    modes = {
        "Nightly": "nightly",
        "Official prerelease": "prerelease",
        "Official release": "release",
    }
    if release_type not in modes:
        raise ValueError("Select Nightly, Official prerelease, or Official release.")
    mode = modes[release_type]
    version = version.strip()

    if mode == "nightly":
        if version:
            raise ValueError("Leave version empty when selecting Nightly.")
        return mode, ""

    if not version:
        raise ValueError("A version is required for an official release or prerelease.")
    try:
        parsed = Version(version)
    except InvalidVersion as error:
        raise ValueError("Version must be a valid Python package version.") from error
    if parsed.local is not None or parsed.epoch or parsed.is_devrelease:
        raise ValueError(
            "Official versions cannot use local, epoch, or dev components."
        )
    if (mode == "prerelease") != parsed.is_prerelease:
        raise ValueError("The version must match the selected release type.")
    return mode, str(parsed)


def main() -> None:
    try:
        mode, version = prepare_release(
            os.environ["GITHUB_EVENT_NAME"],
            os.environ.get("RELEASE_TYPE", "Nightly"),
            os.environ.get("RELEASE_VERSION", ""),
        )
    except ValueError as error:
        raise SystemExit(str(error)) from error
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write(f"release_type={mode}\nversion={version}\n")


if __name__ == "__main__":
    main()
