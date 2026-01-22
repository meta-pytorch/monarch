# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

def hold_gil_for_test(delay_secs: float, hold_secs: float) -> None:
    """
    Test utility that holds the GIL for a specified duration.

    This spawns a background thread that waits for `delay_secs` before
    acquiring the Python GIL, then holds it for `hold_secs`.

    Args:
        delay_secs: Seconds to wait before acquiring the GIL
        hold_secs: Seconds to hold the GIL
    """
    ...
