# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Monarch Actor API
"""

from monarch._src.actor.ipython_check import is_ipython
from monarch.config import configure

# Detect if we're running in IPython/Jupyter
_in_ipython: bool = is_ipython()

# Set notebook-friendly defaults for stdio piping when spawning procs.
# These config is read by:
# 1. Rust BootstrapProcManager::spawn() to decide whether to pipe
#    child stdio
# 2. Rust LoggingMeshClient::spawn() to decide whether to spawn
#   LogForwardActors
# Only apply these defaults overrides in notebook/IPython environments
# where stdout **needs** to be captured.
if _in_ipython:
    configure(enable_log_forwarding=True)
