# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import runpy
import sys
from collections.abc import Callable
from typing import cast

__MONARCH_TENSOR_WORKER_ENV__ = True


def main() -> None:
    assert sys.argv[1] == "-m"
    main_module = sys.argv[2]

    # Remove the -m and the main module from the command line arguments before
    # forwarding
    sys.argv[1:] = sys.argv[3:]

    # The public run_module() uses a fresh namespace instead of preserving the
    # existing __main__ namespace required by Python's -m behavior.
    run_module_as_main = cast(
        Callable[[str, bool], object],
        vars(runpy)["_run_module_as_main"],
    )
    run_module_as_main(main_module, False)


if __name__ == "__main__":
    main()
