# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Unit tests for XPU support in the actor runtime. No XPU hardware needed."""

import os
import sys
import unittest
from unittest.mock import patch

import torch


def _mock_accelerator(accel_type):
    # current_accelerator() returns a torch.device, not a str
    return patch(
        "torch.accelerator.current_accelerator", return_value=torch.device(accel_type)
    )


class TestXpuAcceleratorEnvVars(unittest.TestCase):
    def test_xpu_includes_xpu_vars(self):
        from monarch._src.actor.proc_mesh import _get_accelerator_env_vars

        with _mock_accelerator("xpu"):
            env_vars = _get_accelerator_env_vars()
            self.assertIn("ZE_AFFINITY_MASK", env_vars)
            self.assertIn("ZE_ENABLE_PCI_ID_DEVICE_ORDER", env_vars)
            self.assertIn("PYTORCH_XPU_ALLOC_CONF", env_vars)

    def test_xpu_snapshot_captures_ze_vars(self):
        from monarch._src.actor.proc_mesh import _accel_env_snapshot

        with (
            _mock_accelerator("xpu"),
            patch.dict(
                os.environ,
                {
                    "ZE_AFFINITY_MASK": "0,1",
                    "PYTORCH_XPU_ALLOC_CONF": "expandable_segments:True",
                },
            ),
        ):
            snap = _accel_env_snapshot()
            self.assertEqual(snap["ZE_AFFINITY_MASK"], "0,1")
            self.assertEqual(snap["PYTORCH_XPU_ALLOC_CONF"], "expandable_segments:True")

    def test_cuda_is_unchanged(self):
        from monarch._src.actor.proc_mesh import (
            _COMMON_CUDA_ENV_VARS,
            _get_accelerator_env_vars,
        )

        with _mock_accelerator("cuda"):
            self.assertEqual(_get_accelerator_env_vars(), _COMMON_CUDA_ENV_VARS)

    def test_xpu_dispatches_to_torch_xpu(self):
        from monarch._src.actor.proc_mesh import _torch_accelerator_already_initialized

        with (
            _mock_accelerator("xpu"),
            patch.dict(sys.modules, {"torch.xpu": None, "torch.cuda": None}),
        ):
            self.assertFalse(_torch_accelerator_already_initialized())

        with (
            _mock_accelerator("xpu"),
            patch("torch.xpu.is_initialized", return_value=True),
            patch("torch.cuda.is_initialized", return_value=False),
        ):
            self.assertTrue(_torch_accelerator_already_initialized())

    def test_no_torch_returns_cuda_defaults(self):
        from monarch._src.actor.proc_mesh import (
            _COMMON_CUDA_ENV_VARS,
            _get_accelerator_env_vars,
        )

        with patch.dict(sys.modules, {"torch": None}):
            self.assertEqual(_get_accelerator_env_vars(), _COMMON_CUDA_ENV_VARS)
