# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import sys

# Propagate sys.path to PYTHONPATH so that worker subprocesses spawned by
# monarch (e.g. distributed_proc_mesh) see the same import paths as the
# pytest parent process. pytest's default "prepend" import mode modifies
# sys.path at the Python level, but child processes don't inherit that —
# they only see PYTHONPATH.
os.environ["PYTHONPATH"] = os.pathsep.join(sys.path)
