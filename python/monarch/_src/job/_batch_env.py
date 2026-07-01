# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Batch-mode environment contract shared by the job schedulers and the
in-allocation runner.
"""

import os

# Set on the in-allocation batch client so its cached BatchJob reconnects to the
# surrounding allocation instead of submitting a new one.
MONARCH_BATCH_JOB_ENV: str = "MONARCH_BATCH_JOB"


def in_batch_job() -> bool:
    """True when running as the in-allocation batch client."""
    return os.environ.get(MONARCH_BATCH_JOB_ENV) == "1"
