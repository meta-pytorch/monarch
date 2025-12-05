# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Until https://github.com/pybind/pybind11/pull/5870) is upstreamed to all version of
pytorch we use with monarch, it is unsafe to import torch from a non-main thread.
This is because pybind11 will corrupt that threads internal state with a stale PyThreadState object.
This can cause deadlocks or segfaults. To workaround this, we recognize that the error
will only occur if the thread which imported torch later tries to use gil_scoped_acquire.
If we do the import on a fresh thread and then throw it away, we guarentee that we do not do this.
"""

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import torch


def _import():
    global torch
    import torch


_thread = threading.Thread(target=_import)
_thread.start()
_thread.join()


__all__ = ["torch"]
