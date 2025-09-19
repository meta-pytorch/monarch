# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Simplified version of worker_main.py for testing the monarch_tensor_worker standalone.

We want a Python entrypoint here because we want to initialize the Monarch
Python extension on the main thread.
"""


def main() -> None:
    # torch is import to make sure all the dynamic types are registered
    import torch  # noqa

    # Force CUDA initialization early on. CUDA init is lazy, and Python CUDA
    # APIs are guarded to init CUDA if necessary. But our worker calls
    # raw libtorch APIs which are not similarly guarded. So just initialize here
    # to avoid issues with potentially using uninitialized CUDA state.
    torch.cuda.init()

    from monarch._rust_bindings.monarch_extension import (  # @manual=//monarch/monarch_extension:monarch_extension
        tensor_worker,
    )

    # pyre-ignore[16]
    tensor_worker.worker_main()


if __name__ == "__main__":
    # Do not add code here, it won't be run. Add them to the function called below.
    main()  # pragma: no cover
