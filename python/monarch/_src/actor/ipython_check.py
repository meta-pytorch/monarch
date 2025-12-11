# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Fast IPython detection by checking sys.modules instead of importing.
"""

import sys


def is_ipython() -> bool:
    """
    Check if code is running in an IPython/Jupyter environment.
    Avoids slow IPython import by checking sys.modules first.
    """
    if "IPython" not in sys.modules:
        return False

    # pyre-ignore[16]: get_ipython exists in IPython module
    get_ipython = sys.modules["IPython"].get_ipython
    return get_ipython() is not None
