# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import json
import os
import tempfile
import unittest

from monarch.simulator.communication_model import (
    estimate_collective_time_us,
    estimate_send_time_us,
    NetworkConfig,
    tensor_size_bytes,
)
from monarch.simulator.ir import IRGraph


class TestTensorSizeBytes(unittest.TestCase):
    def test_scalar(self):
        self.assertEqual(tensor_size_bytes([1], "float32"), 4)

    def test_2d_float32(self):
        self.assertEqual(tensor_size_bytes([3, 3], "float32"), 36)

    def test_large_tensor(self):
        self.assertEqual(tensor_size_bytes([1024, 1024], "float32"), 1024 * 1024 * 4)

    def test_bfloat16(self):
        self.assertEqual(tensor_size_bytes([10, 10], "bfloat16"), 200)

    def test_int8(self):
        self.assertEqual(tensor_size_bytes([256], "int8"), 256)

    def test_unknown_dtype_defaults_to_4(self):
        self.assertEqual(tensor_size_bytes([10], "custom_type"), 40)

    def test_empty_shape(self):
        # Product of empty shape is 1 (vacuous product).
        self.assertEqual(tensor_size_bytes([], "float32"), 4)


class TestEstimateCollectiveTimeUs(unittest.TestCase):
    def test_single_device_returns_zero(self):
        self.assertEqual(
            estimate_collective_time_us("all_reduce", [1024], "float32", 1), 0
        )

    def test_empty_shape_returns_zero(self):
        self.assertEqual(estimate_collective_time_us("all_reduce", [], "float32", 4), 0)

    def test_larger_tensor_takes_longer(self):
        small = estimate_collective_time_us("all_reduce", [64, 64], "float32", 8)
        large = estimate_collective_time_us("all_reduce", [1024, 1024], "float32", 8)
        self.assertGreater(large, small)

    def test_more_devices_changes_estimate(self):
        few = estimate_collective_time_us("all_reduce", [1024, 1024], "float32", 2)
        many = estimate_collective_time_us("all_reduce", [1024, 1024], "float32", 8)
        self.assertNotEqual(few, many)

    def test_reduce_scatter(self):
        t = estimate_collective_time_us("reduce_scatter", [1024, 1024], "float32", 4)
        self.assertGreater(t, 0)

    def test_all_gather(self):
        t = estimate_collective_time_us("all_gather", [1024, 1024], "float32", 4)
        self.assertGreater(t, 0)

    def test_all_to_all(self):
        t = estimate_collective_time_us("all_to_all", [1024, 1024], "float32", 4)
        self.assertGreater(t, 0)

    def test_unknown_type_falls_back_to_allreduce(self):
        t = estimate_collective_time_us("unknown_op", [1024, 1024], "float32", 4)
        self.assertGreater(t, 0)

    def test_inter_node_slower_than_intra_node(self):
        config = NetworkConfig()
        intra = estimate_collective_time_us(
            "all_reduce", [1024, 1024], "float32", 4, config
        )
        # 16 devices spans multiple nodes (gpus_per_node=8), uses slower IB BW.
        inter = estimate_collective_time_us(
            "all_reduce", [1024, 1024], "float32", 16, config
        )
        self.assertGreater(inter, intra)

    def test_custom_config(self):
        slow = NetworkConfig(intra_node_bandwidth_gbs=100.0)
        fast = NetworkConfig(intra_node_bandwidth_gbs=900.0)
        t_slow = estimate_collective_time_us(
            "all_reduce", [1024, 1024], "float32", 4, slow
        )
        t_fast = estimate_collective_time_us(
            "all_reduce", [1024, 1024], "float32", 4, fast
        )
        self.assertGreater(t_slow, t_fast)

    def test_contention_factor_affects_all_to_all(self):
        low_contention = NetworkConfig(contention_factor=0.25)
        high_contention = NetworkConfig(contention_factor=0.75)
        t_low = estimate_collective_time_us(
            "all_to_all", [1024, 1024], "float32", 4, low_contention
        )
        t_high = estimate_collective_time_us(
            "all_to_all", [1024, 1024], "float32", 4, high_contention
        )
        self.assertGreater(t_low, t_high)


class TestEstimateSendTimeUs(unittest.TestCase):
    def test_empty_shape_returns_zero(self):
        self.assertEqual(estimate_send_time_us([], "float32"), 0)

    def test_positive_result(self):
        t = estimate_send_time_us([512, 512], "float32")
        self.assertGreater(t, 0)

    def test_larger_tensor_takes_longer(self):
        small = estimate_send_time_us([64, 64], "float32")
        large = estimate_send_time_us([1024, 1024], "float32")
        self.assertGreater(large, small)


class TestExportCommandTypesIntegration(unittest.TestCase):
    """Verify that export_command_types with communication modeling produces
    non-uniform timing for Reduce and SendTensor commands."""

    def _build_simple_graph(self) -> IRGraph:
        """Build a minimal IRGraph with Reduce and SendTensor commands."""
        import torch

        graph = IRGraph()

        # Add a small reduce_scatter command.
        graph.insert_node(
            worker_rank=0,
            stream_name="stream0",
            command_id=0,
            command_name="Reduce: reduce_scatter",
            devices=[0, 1, 2, 3],
            control_dependencies=[],
            traceback=[],
        )
        # Track tensors so shapes appear in timing keys.
        graph.update_tensor(
            temp_id=100,
            ref=200,
            dtype=torch.float32,
            dims=(3, 3),
            worker_rank=0,
            stream_name="stream0",
            command_id=0,
            tensor_size=36,
            mesh_ref=1,
        )

        # Add a large all_reduce command.
        graph.insert_node(
            worker_rank=0,
            stream_name="stream0",
            command_id=1,
            command_name="Reduce: all_reduce",
            devices=[0, 1, 2, 3, 4, 5, 6, 7],
            control_dependencies=[0],
            traceback=[],
        )
        graph.update_tensor(
            temp_id=101,
            ref=201,
            dtype=torch.float32,
            dims=(1024, 1024),
            worker_rank=0,
            stream_name="stream0",
            command_id=1,
            tensor_size=1024 * 1024 * 4,
            mesh_ref=1,
        )

        # Add a SendTensor command.
        graph.insert_node(
            worker_rank=0,
            stream_name="stream0",
            command_id=2,
            command_name="SendTensor: 7",
            devices=[0, 1],
            control_dependencies=[1],
            traceback=[],
        )
        graph.update_tensor(
            temp_id=102,
            ref=202,
            dtype=torch.float32,
            dims=(512, 512),
            worker_rank=0,
            stream_name="stream0",
            command_id=2,
            tensor_size=512 * 512 * 4,
            mesh_ref=1,
        )

        return graph

    def test_non_uniform_timing(self):
        graph = self._build_simple_graph()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            graph.export_command_types(
                tmp_path,
                include_timing=True,
                network_config=NetworkConfig(),
            )
            with open(tmp_path) as f:
                output = json.load(f)

            timing = output["timing"]
            # Collect timing values for Reduce keys.
            reduce_timings = {k: v for k, v in timing.items() if k.startswith("Reduce")}
            # At least two distinct Reduce entries with different timing.
            self.assertGreaterEqual(len(reduce_timings), 2)
            self.assertGreater(
                len(set(reduce_timings.values())),
                1,
                "Reduce timings should vary by tensor size and device count",
            )

            # SendTensor should have a positive timing.
            send_timings = {
                k: v for k, v in timing.items() if k.startswith("SendTensor")
            }
            for v in send_timings.values():
                self.assertGreater(v, 0)
        finally:
            os.unlink(tmp_path)

    def test_network_config_parameter_accepted(self):
        """export_command_types should accept the network_config kwarg."""
        graph = self._build_simple_graph()
        config = NetworkConfig(intra_node_bandwidth_gbs=100.0)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            graph.export_command_types(
                tmp_path, include_timing=True, network_config=config
            )
            with open(tmp_path) as f:
                output = json.load(f)
            self.assertIn("timing", output)
        finally:
            os.unlink(tmp_path)


if __name__ == "__main__":
    unittest.main()
