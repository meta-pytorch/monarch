# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import os

enabled = os.environ.get("MONARCH_V0_WORKAROUND_DO_NOT_USE", "0") != "1"
