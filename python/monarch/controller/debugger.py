# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import sys

from monarch._src.actor.ipython_check import is_ipython


def write(msg: str) -> None:
    sys.stdout.write(msg)
    sys.stdout.flush()


def read(requested_size: int) -> bytes:
    if not is_ipython():
        b = bytearray(requested_size)
        bytes_read = sys.stdin.buffer.raw.readinto(b)
        return bytes(b[:bytes_read])

    # ipython doesn't have stdin directly connected
    # so we need to use input() instead.
    user_input = input() + "\n"
    input_bytes = user_input.encode("utf-8")
    num_bytes_to_write = len(input_bytes)
    if requested_size < num_bytes_to_write:
        raise RuntimeError(
            f"Debugger input line too long, max length is {requested_size}"
        )
    return input_bytes[:num_bytes_to_write]
