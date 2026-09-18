# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import re
import sys
from pathlib import Path


def _local_device_count() -> int:
    if "CUDA_VISIBLE_DEVICES" in os.environ:
        return len(os.environ["CUDA_VISIBLE_DEVICES"].split(","))
    if "ZE_AFFINITY_MASK" in os.environ:
        return len(os.environ["ZE_AFFINITY_MASK"].split(","))

    dev_path = Path("/dev")
    pattern = re.compile(r"nvidia\d+$")
    nvidia_devices = [dev for dev in dev_path.iterdir() if pattern.match(dev.name)]
    if nvidia_devices:
        return len(nvidia_devices)

    torch_mod = sys.modules.get("torch")
    accelerator = getattr(torch_mod, "accelerator", None)
    if accelerator is None:
        return 0
    return accelerator.device_count()
