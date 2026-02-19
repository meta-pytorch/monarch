# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Public interface for monarch torchrun launcher.

This module provides command-line entry points for both single-node worker
and multi-node client modes.

Use `python -m monarch.actor.torchrun` for single-node training (worker mode).
Use `python -m monarch.actor.torchrun.client` for multi-node orchestration (client mode).
"""

from monarch._src.actor.torchrun.worker import main

if __name__ == "__main__":
    main()
