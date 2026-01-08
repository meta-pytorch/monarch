# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

# Before importing the C module, check that the pytorch version monarch was built
# against matches the runtime version
from monarch.common.tensor import check_torch_version

check_torch_version()

from ._gradient_generator import GradientGenerator  # noqa: E402

__all__ = ["GradientGenerator"]
