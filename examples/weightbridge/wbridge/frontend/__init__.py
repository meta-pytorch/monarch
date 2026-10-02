# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Framework frontends (Megatron-Bridge, SGLang integration)."""

"""Framework frontends.

Only framework-agnostic classes are re-exported here. Framework-specific adapters live in their own
modules and must be imported directly, so importing this package does not transitively pull in heavy
framework dependencies on machines that don't have them installed.
"""

from wbridge.frontend.adapters import (
    AdapterContext,
    BaseAdapter,
    ReceiverAdapter,
    SenderAdapter,
)

__all__ = [
    "AdapterContext",
    "BaseAdapter",
    "ReceiverAdapter",
    "SenderAdapter",
]
