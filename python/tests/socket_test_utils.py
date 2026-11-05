# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import tempfile
from collections.abc import Iterator

import pytest


@pytest.fixture
def unix_socket_path() -> Iterator[str]:
    """Yield a Unix socket path that fits macOS pathname limits."""
    with tempfile.TemporaryDirectory(prefix="monarch-", dir="/tmp") as directory:
        yield os.path.join(directory, "test.sock")
