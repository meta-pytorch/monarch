# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
SPMD primitives for running torchrun-style distributed training over Monarch meshes.

This module provides actors and environment enablements to execute SPMD (Single Program
Multiple Data) scripts on Monarch actor meshes. It bridges PyTorch distributed training
with Monarch's actor-based distributed system by automatically configuring torch elastic
environment variables (RANK, LOCAL_RANK, WORLD_SIZE, etc.) across the mesh.

Key components:
- :class:`SPMDActor`: Actor that sets up PyTorch distributed environment and runs scripts
- :func:`setup_torch_elastic_env`: Configure torch elastic environment across a mesh
- :func:`setup_torch_elastic_env_async`: Async version of environment setup
"""

from monarch._src.spmd import setup_torch_elastic_env, setup_torch_elastic_env_async
from monarch._src.spmd.actor import SPMDActor


__all__ = ["SPMDActor", "setup_torch_elastic_env", "setup_torch_elastic_env_async"]
