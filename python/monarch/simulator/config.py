# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import contextlib

META_VAL = []


@contextlib.contextmanager
def set_meta(new_value):
    """
    Context manager that sets metadata for simulator tasks created within its scope.

    Args:
        new_value: The metadata value to associate with tasks created in this context.

    Example::

        with set_meta("training_phase"):
            # Tasks created here will have "training_phase" metadata
            ...
    """
    global META_VAL
    META_VAL.append(new_value)
    try:
        yield
    finally:
        META_VAL.pop()
