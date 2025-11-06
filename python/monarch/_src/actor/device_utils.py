# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import re
from pathlib import Path


def _local_device_count() -> int:
    if "CUDA_VISIBLE_DEVICES" in os.environ:
        return len(os.environ["CUDA_VISIBLE_DEVICES"].split(","))
    dev_path = Path("/dev")
    pattern = re.compile(r"nvidia\d+$")
    nvidia_devices = [dev for dev in dev_path.iterdir() if pattern.match(dev.name)]
    return len(nvidia_devices)
