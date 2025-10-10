# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Directly import to avoid import issues in Buck
from monarch._src.job.job import LocalJob
from monarch.actor import this_proc

if __name__ == "__main__":
    job = LocalJob()
    state = job.state()

    # Check which hosts are available in the state
    print(f"hosts {getattr(state, 'hosts', None) is not None}")

    # Wait until we know the hosts have finished initializing to avoid
    # fatal GIL release error.
    this_proc().initialized.get()

    print(
        f"batch_launched_hosts {getattr(state, 'batch_launched_hosts', None) is not None}"
    )
