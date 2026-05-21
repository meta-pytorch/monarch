# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import unittest

import pytest
import torch
from monarch.common import messages
from monarch.simulator.profiling import RuntimeEstimator, RuntimeProfiler, TimingType


# pyre-ignore-all-errors[6]
# pyre-ignore-all-errors[16]
class TestRuntimeEstimator(unittest.TestCase):
    def test_user_manual_setting(self):
        runtime = RuntimeEstimator()

        input_tensor = torch.rand(10, 10)
        # pyrefly: ignore [missing-attribute]
        input_tensor.ref = 1
        # pyrefly: ignore [missing-attribute]
        input_tensor._fake = None
        output_tensor = torch.rand(10, 10)
        # pyrefly: ignore [missing-attribute]
        output_tensor.ref = 2
        # pyrefly: ignore [missing-attribute]
        output_tensor._fake = None

        send_tensor = messages.SendTensor(
            # pyrefly: ignore [bad-argument-type]
            result=output_tensor,
            # pyrefly: ignore [bad-argument-type]
            from_ranks=[1],
            # pyrefly: ignore [bad-argument-type]
            to_ranks=[2],
            # pyrefly: ignore [bad-argument-type]
            tensor=input_tensor,
            # pyrefly: ignore [bad-argument-type]
            factory=None,
            # pyrefly: ignore [bad-argument-type]
            from_stream=None,
            # pyrefly: ignore [bad-argument-type]
            to_stream=None,
        )
        reduce = messages.Reduce(
            # pyrefly: ignore [bad-argument-type]
            result=output_tensor,
            # pyrefly: ignore [bad-argument-type]
            local_tensor=input_tensor,
            # pyrefly: ignore [bad-argument-type]
            factory=None,
            # pyrefly: ignore [bad-argument-type]
            source_mesh=None,
            # pyrefly: ignore [bad-argument-type]
            stream=None,
            # pyrefly: ignore [bad-argument-type]
            dims=None,
            # pyrefly: ignore [bad-argument-type]
            reduction=None,
            scatter=False,
            inplace=False,
            out=None,
        )
        call_function = messages.CallFunction(
            ident=1,
            result=None,
            # pyrefly: ignore [bad-argument-type]
            mutates=None,
            # pyrefly: ignore [bad-argument-type]
            function=None,
            # pyrefly: ignore [bad-argument-type]
            args=None,
            # pyrefly: ignore [bad-argument-type]
            kwargs=None,
            # pyrefly: ignore [bad-argument-type]
            stream=None,
            # pyrefly: ignore [bad-argument-type]
            device_mesh=None,
            # pyrefly: ignore [bad-argument-type]
            remote_process_groups=None,
        )

        self.assertEqual(runtime.get_runtime(send_tensor), 100_000)
        self.assertEqual(runtime.get_runtime(reduce), 100_000)
        self.assertEqual(runtime.get_runtime(call_function), 10_000)
        self.assertEqual(runtime.get_runtime("kernel_launch"), 500)
        self.assertEqual(runtime.get_runtime("wait_event"), 500)

        runtime.set_custom_timing(
            {
                TimingType.SEND_TENSOR: 1_000,
                TimingType.REDUCE: 2_000,
                TimingType.CALL_FUNCTION: 3_000,
                TimingType.KERNEL_LAUNCH: 4_000,
                TimingType.WAIT_EVENT: 5_000,
            }
        )
        self.assertEqual(runtime.get_runtime(send_tensor), 1_000)
        self.assertEqual(runtime.get_runtime(reduce), 2_000)
        self.assertEqual(runtime.get_runtime(call_function), 3_000)
        self.assertEqual(runtime.get_runtime("kernel_launch"), 4_000)
        self.assertEqual(runtime.get_runtime("wait_event"), 5_000)

        runtime.set_custom_timing(
            # pyrefly: ignore [bad-argument-type]
            {
                TimingType.SEND_TENSOR: lambda msg: 4_000,
                TimingType.REDUCE: lambda msg: 5_000,
                TimingType.CALL_FUNCTION: lambda msg: 6_000,
                TimingType.KERNEL_LAUNCH: lambda: 8_000,
                TimingType.WAIT_EVENT: lambda: 9_000,
            }
        )
        self.assertEqual(runtime.get_runtime(send_tensor), 4_000)
        self.assertEqual(runtime.get_runtime(reduce), 5_000)
        self.assertEqual(runtime.get_runtime(call_function), 6_000)
        self.assertEqual(runtime.get_runtime("kernel_launch"), 8_000)
        self.assertEqual(runtime.get_runtime("wait_event"), 9_000)

    @pytest.mark.oss_skip
    def test_runtime_profiler(self) -> None:
        m1 = torch.rand(1000, 2000).cuda()
        m2 = torch.rand(2000, 4000).cuda()
        # pyrefly: ignore [missing-attribute]
        m1.ref = 1
        # pyrefly: ignore [missing-attribute]
        m2.ref = 2
        msg = messages.CallFunction(
            ident=1,
            result=None,
            # pyrefly: ignore [bad-argument-type]
            mutates=None,
            # pyrefly: ignore [bad-argument-type]
            function=torch.ops.aten.mm.default,
            args=(m1, m2),
            # pyrefly: ignore [bad-argument-type]
            kwargs=None,
            # pyrefly: ignore [bad-argument-type]
            stream=None,
            # pyrefly: ignore [bad-argument-type]
            device_mesh=None,
            # pyrefly: ignore [bad-argument-type]
            remote_process_groups=None,
        )
        profiler = RuntimeProfiler()

        ret = profiler.profile_cmd(msg, ranks=[0])[0]
        # pyrefly: ignore [unsupported-operation]
        self.assertEqual(ret[0].factory.size, (1000, 4000))
        # Should be at least 0.1 ms
        # pyrefly: ignore [unsupported-operation]
        self.assertTrue(ret[1] > 100)
        # Should be at most 100 ms
        # pyrefly: ignore [unsupported-operation]
        self.assertTrue(ret[1] < 100_000)

        # Change the cached profiling result to verify if cached mechanism works
        key = next(iter(profiler.cached.keys()))
        profiler.cached[key][0] = (profiler.cached[key][0][0], 987_654_321)

        ret = profiler.profile_cmd(msg, ranks=[0])[0]
        # pyrefly: ignore [unsupported-operation]
        self.assertEqual(ret[0].factory.size, (1000, 4000))
        # pyrefly: ignore [unsupported-operation]
        self.assertEqual(ret[1], 987_654_321)


if __name__ == "__main__":
    unittest.main()
