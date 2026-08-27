# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Deadlines shared by the finalization model and the test that drives it.

Separate from both so the test can assert the nesting without importing the
model, which imports Monarch and registers an ``atexit`` handler at module
scope. Nothing here may import anything.

They must nest strictly: the parent's subprocess timeout is a backstop, and if
it is shorter than what the child can legitimately wait on then the child's own
topology error is replaced by a generic parent timeout.
"""

# Child (model).
LOOP_DISCOVERY_TIMEOUT_S = 20.0
LOOP_DISCOVERY_SETTLE_S = 1.0
LOOP_DISCOVERY_POLL_S = 0.01
MODEL_ARM_TIMEOUT_S = 5.0
THREAD_EXIT_POLL_S = 0.01

# CPython's default switch interval. Pinned because the unmitigated arm depends
# on it; see `actor_loop_finalization_model._exercise_finalization_boundary`.
FINALIZATION_SWITCH_INTERVAL_S = 0.005

# Parent (test).
SUBPROCESS_TIMEOUT_S = 90.0

# The child can wait on discovery, its settle window, and two arming waits.
CHILD_WORST_CASE_S = (
    LOOP_DISCOVERY_TIMEOUT_S + LOOP_DISCOVERY_SETTLE_S + MODEL_ARM_TIMEOUT_S * 2
)
