# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
SPMD job primitives for launching torchrun-style training over Monarch.

This module provides job abstractions for running SPMD (Single Program Multiple Data)
workloads. It parses torchrun arguments (including from torchx AppDef entrypoints)
and creates a Monarch mesh to run the training script, replicating torchrun behavior.

Key components:
- :func:`serve`: Launch an SPMD job from a torchx AppDef
- :class:`SPMDJob`: Job class wrapping torchx for SPMD workloads
"""

from monarch._src.job.spmd import serve, SPMDJob

__all__ = ["serve", "SPMDJob"]
