#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Run me as:
# curl https://raw.githubusercontent.com/meta-pytorch/monarch/refs/heads/main/scripts/install-nightly.py | python

import json
import subprocess
import sys
import urllib.request


def get_latest_version(package_name: str) -> str:
    """Get latest version from PyPI"""
    api_url = f"https://pypi.org/pypi/{package_name}/json"

    try:
        with urllib.request.urlopen(api_url) as response:
            data = json.loads(response.read().decode("utf-8"))
            return data["info"]["version"]
    except Exception as e:
        print(f"Failed to fetch version for {package_name}: {e}", file=sys.stderr)
        sys.exit(1)


def get_torch_release_version() -> str:
    """Get PyTorch version numbers"""
    version_url = (
        "https://raw.githubusercontent.com/pytorch/pytorch/refs/heads/main/version.txt"
    )
    try:
        with urllib.request.urlopen(version_url) as response:
            return response.read().decode("utf-8").split("a")[0]
    except Exception as e:
        print(f"Failed to fetch torch version: {e}", file=sys.stderr)
        sys.exit(1)


def convert_version_for_torch(version: str) -> str:
    """Convert version format for torch (YYYY.M.D or YYYY.MM.DD -> YYYYMMDD)"""
    # Split the version into components
    year, month, day = [int(x) for x in version.split(".")]

    return f"{year}{month:02}{day:02}"


def main() -> None:
    """Main function"""
    print("Starting torchmonarch-nightly installation script")

    # Get latest version
    torchmonarch_version = get_latest_version("torchmonarch-nightly")
    print(f"Latest torchmonarch-nightly version: {torchmonarch_version}")

    # Convert version for torch
    torch_release_version = get_torch_release_version()
    torch_date = convert_version_for_torch(torchmonarch_version)
    torch_version = f"{torch_release_version}.dev{torch_date}"

    print(f"Corresponding torch version: {torch_version}")

    # Construct the pip install command arguments
    pip_command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        f"torchmonarch-nightly=={torchmonarch_version}",
        f"torch=={torch_version}",
        "--pre",
        "--extra-index-url",
        "https://download.pytorch.org/whl/nightly/cu128",
    ]

    print(f"Executing command:\n\t{' '.join(pip_command)}\n\n")

    # Execute the command
    subprocess.check_call(pip_command)
    print("Installation completed successfully!")
    print("Installed packages:")
    print(f"  - torchmonarch-nightly=={torchmonarch_version}")
    print(f"  - torch=={torch_version}")


if __name__ == "__main__":
    main()
