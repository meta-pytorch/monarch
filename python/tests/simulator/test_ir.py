# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import json
import tempfile
import unittest

from monarch.simulator.ir import (
    Command,
    IRGraph,
    TensorAccessEvent,
    TensorCreationEvent,
)


class TestExportCommandTypes(unittest.TestCase):
    def _make_ir(self, commands, data_events=None):
        ir = IRGraph()
        ir.control_dag = commands
        ir.data_dag = data_events or []
        return ir

    def _export_and_read(self, ir):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = f.name
        ir.export_command_types(path)
        with open(path) as f:
            return json.load(f)

    def test_reduce_empty_shapes_no_consecutive_colons(self):
        """Reduce timing key should not have consecutive colons when shapes are empty."""
        ir = self._make_ir(
            [
                Command(
                    worker_rank=0,
                    stream_name="main",
                    command_id=1,
                    command_name="Reduce: reduce_scatter: 42",
                    devices=[0, 1, 2, 3],
                    control_dependencies=[],
                    traceback=[],
                ),
            ]
        )
        data = self._export_and_read(ir)
        timing_key = data["command_types"][0]["timing_key"]
        self.assertNotIn(
            "::", timing_key, f"Consecutive colons in timing key: {timing_key}"
        )
        self.assertEqual(timing_key, "Reduce:reduce_scatter:unknown:4")

    def test_reduce_with_shapes(self):
        """Reduce timing key should include shapes when present."""
        ir = self._make_ir(
            [
                Command(
                    worker_rank=0,
                    stream_name="main",
                    command_id=1,
                    command_name="Reduce: reduce_scatter: 42",
                    devices=[0, 1, 2, 3],
                    control_dependencies=[],
                    traceback=[],
                ),
            ],
            [
                TensorCreationEvent(
                    command_id=1,
                    DTensorRef=100,
                    storage_id=200,
                    dtype=None,
                    dims=(3, 3),
                    devices=[0],
                    mesh_ref=None,
                    stream_name="main",
                ),
                TensorAccessEvent(
                    command_id=1,
                    DTensorRef=99,
                    storage_id=201,
                    dtype=None,
                    dims=(3, 3),
                    devices=[0],
                    mesh_ref=None,
                    stream_name="main",
                ),
            ],
        )
        data = self._export_and_read(ir)
        timing_key = data["command_types"][0]["timing_key"]
        self.assertNotIn("::", timing_key)
        self.assertIn("(3x3)", timing_key)

    def test_call_function_empty_shapes(self):
        """CallFunction timing key should omit shapes when empty."""
        ir = self._make_ir(
            [
                Command(
                    worker_rank=0,
                    stream_name="main",
                    command_id=1,
                    command_name="CallFunction: aten.mm",
                    devices=[0],
                    control_dependencies=[],
                    traceback=[],
                ),
            ]
        )
        data = self._export_and_read(ir)
        timing_key = data["command_types"][0]["timing_key"]
        self.assertNotIn("::", timing_key)
        self.assertEqual(timing_key, "CallFunction:aten.mm:unknown")

    def test_send_tensor_empty_shapes(self):
        """SendTensor timing key should omit shapes when empty."""
        ir = self._make_ir(
            [
                Command(
                    worker_rank=0,
                    stream_name="main",
                    command_id=1,
                    command_name="SendTensor: 7",
                    devices=[0, 1],
                    control_dependencies=[],
                    traceback=[],
                ),
            ]
        )
        data = self._export_and_read(ir)
        timing_key = data["command_types"][0]["timing_key"]
        self.assertNotIn("::", timing_key)
        self.assertEqual(timing_key, "SendTensor:unknown")

    def test_borrow_timing_key(self):
        """Borrow commands should use just the command type as timing key."""
        ir = self._make_ir(
            [
                Command(
                    worker_rank=0,
                    stream_name="main",
                    command_id=1,
                    command_name="BorrowCreate: 5",
                    devices=[0],
                    control_dependencies=[],
                    traceback=[],
                ),
            ]
        )
        data = self._export_and_read(ir)
        timing_key = data["command_types"][0]["timing_key"]
        self.assertEqual(timing_key, "BorrowCreate")

    def test_command_count(self):
        """Duplicate timing keys should be counted correctly."""
        cmds = [
            Command(
                worker_rank=i,
                stream_name="main",
                command_id=i,
                command_name="CallFunction: aten.mm",
                devices=[i],
                control_dependencies=[],
                traceback=[],
            )
            for i in range(4)
        ]
        ir = self._make_ir(cmds)
        data = self._export_and_read(ir)
        self.assertEqual(len(data["command_types"]), 1)
        self.assertEqual(data["command_types"][0]["count"], 4)
