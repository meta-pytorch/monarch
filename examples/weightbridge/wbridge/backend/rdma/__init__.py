# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""One-sided RDMA transport for the WeightBridge data plane (register + write only)."""

from wbridge.backend.rdma.base import RdmaEngine
from wbridge.backend.rdma.dual import DualMooncakeEngine
from wbridge.backend.rdma.local import LocalStagingEngine
from wbridge.backend.rdma.mooncake import MooncakeEngine

# MonarchEngine is imported lazily (router._init_engine): importing monarch pulls a large
# native extension and is only needed when protocol="monarch".
__all__ = ["RdmaEngine", "MooncakeEngine", "DualMooncakeEngine", "LocalStagingEngine"]
