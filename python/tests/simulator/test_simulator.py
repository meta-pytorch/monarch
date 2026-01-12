# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import functools
import json
import os
import tempfile
from typing import cast, Dict, List, Tuple

import monarch
import numpy as np
import pytest
import torch
import torch.distributed as dist
from monarch import fetch_shard, NDSlice, Stream, Tensor
from monarch.simulator.simulator import Simulator, SimulatorTraceMode
from monarch.simulator.utils import file_path_with_iter


def with_tempfile(suffix=".json", unlink=True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            temp_fd, temp_path = tempfile.mkstemp(suffix=suffix)
            os.close(temp_fd)
            try:
                return func(self, *args, trace_path=temp_path, **kwargs)
            finally:
                # unlink should only be False when debugging.
                if unlink:
                    os.unlink(temp_path)
                else:
                    import logging

                    logging.warning(temp_path)

        return wrapper

    return decorator


@monarch.remote(propagate=lambda x, group: x.add_(1))
def simple_all_reduce_local(x, group=None):
    dist.all_reduce(x, op=dist.ReduceOp.SUM, group=group)
    return x


# pyre-ignore-all-errors[6]
# pyre-ignore-all-errors[16]
# Set global timeout--sandcastle's timeout is 600s. A test that sandcastle times
# out is not counted as a failure, so we set a more restrictive timeout to
# ensure we see a hard failure in CI.
@pytest.mark.timeout(120)
class TestSimulator:
    def _get_simulation_result(
        self, pid: int, trace_path
    ) -> Tuple[Dict[str, List[str] | List[Tuple[float, float]]], List[int]]:
        with open(file_path_with_iter(trace_path, 0), "r") as f:
            traces = json.load(f)["traceEvents"]
            simulator_commands: Dict[str, List[str] | List[Tuple[float, float]]] = {}
            tid_to_name = {}
            memory = []
            for trace in traces:
                if trace["pid"] != pid:
                    continue
                if trace["name"] == "process_name":
                    continue

                if trace["name"] == "thread_name":
                    tid = trace["tid"]
                    name = trace["args"]["name"]
                    tid_to_name[tid] = name
                    simulator_commands[name] = []
                    simulator_commands[f"{name} timestamp"] = []
                elif trace["cat"] == "compute":
                    tid = trace["tid"]
                    name = tid_to_name[tid]
                    simulator_commands[name].append(trace["name"])
                    cast(
                        List[Tuple[float, float]],
                        simulator_commands[f"{name} timestamp"],
                    ).append((float(trace["ts"]), float(trace["ts"] + trace["dur"])))
                elif trace["cat"] == "memory":
                    memory.append(trace["args"]["allocated"])

        return simulator_commands, memory

    @pytest.mark.parametrize("group_workers", [False, True])
    @with_tempfile()
    def test_borrow(self, group_workers, trace_path=None):
        mesh = monarch.Simulator(
            hosts=1,
            gpus=2,
            trace_path=trace_path,
            group_workers=group_workers,
            trace_mode=SimulatorTraceMode.EVERYTHING,
        ).mesh
        other_stream = Stream("other")

        with mesh.activate(), torch.device("cuda"):
            ac1 = torch.randn(100, 100)
            ac2 = torch.mm(ac1, ac1)
            ac3 = torch.nn.init.uniform_(ac2)
            borrow_ac3, borrow = other_stream.borrow(ac3, mutable=True)
            with other_stream.activate():
                borrow_ac3.add_(borrow_ac3)
            borrow.drop()
        mesh.exit()

        commands, _ = self._get_simulation_result(0, trace_path)
        assert commands["Controller"] == [
            "aten.randn",
            "aten.mm",
            "aten.uniform_",
            "DeleteRefs",  # Delete ac2
            "BorrowCreate",  # borrow()
            "BorrowFirstUse",  # borrow_ac3 of add_()
            "aten.add_.Tensor",  # add_()
            "DeleteRefs",  # delete the result of add_()
            "BorrowLastUse",  # drop() will cal _drop_ref()
            "DeleteRefs",  # delete borrow_ac3
            "BorrowDrop",  # drop()
            "RequestStatus",  # drop()
            "Exit",  # isn't this obvious :)
        ]

        _, memory = self._get_simulation_result(1, trace_path)
        assert memory == [
            0.04,  # randn()
            0.08,  # mm()
        ]

    @pytest.mark.parametrize("group_workers", [False, True])
    @with_tempfile()
    def test_to_mesh(self, group_workers, trace_path=None) -> None:
        mesh = monarch.Simulator(
            hosts=2, gpus=2, trace_path=trace_path, group_workers=group_workers
        ).mesh
        pp_meshes = [mesh(host=0), mesh(host=1)]

        with pp_meshes[0].activate(), torch.device("cuda"):
            x = cast(Tensor, torch.randn(100, 100))
            y = x.to_mesh(pp_meshes[0])  # noqa
            z = x.to_mesh(pp_meshes[1])  # noqa
        mesh.exit()

        commands, memory = self._get_simulation_result(1, trace_path)
        assert commands["main"] == [
            "aten.randn",
            "SendTensor",
            "SendTensor",
        ]

        # note in simulator definition of simulator's SendTensor
        # mentions that memory might not be accurately modelled
        # when destination/src is the same. When to_mesh
        # received aliasing fixes, this seemed to throw off
        # the simulators memory calculations here.
        # We need to address the memory copy behavior of to_mesh
        # first, and then align the simulator with the fix in
        # copy behavior.

        # assert memory == [
        #     0.04,  # randn()
        # ]
        # commands, memory = self._get_simulation_result(3, trace_path)
        # assert memory == [
        #     0.04,  # SendTensor
        # ]

    @pytest.mark.parametrize("group_workers", [False, True])
    @with_tempfile()
    def test_reduce_with_stream_trace_only(
        self, group_workers, trace_path=None
    ) -> None:
        mesh = monarch.Simulator(
            hosts=1,
            gpus=2,
            trace_path=trace_path,
            trace_mode=SimulatorTraceMode.STREAM_ONLY,
            group_workers=group_workers,
        ).mesh
        reducer_stream = Stream("reducer_stream")

        with mesh.activate(), torch.device("cuda"):
            x = cast(Tensor, torch.randn(100, 100))
            y = cast(Tensor, torch.randn(100, 100))
            z = cast(Tensor, torch.randn(100, 100))
            flatten = torch.cat((x.view((10000,)), y.view((10000,))))
            flatten_borrow, borrow = reducer_stream.borrow(cast(Tensor, flatten))
            with reducer_stream.activate():
                flatten_borrow.reduce_("gpu", reduction="avg")
            y = y @ z
            borrow.drop()
            x = cast(Tensor, torch.randn(100, 100))
            new_x, new_y = flatten.split((10000, 10000))
            del flatten
            # Need another command to trigger the controller to send the delete
            # command.
            no_use_1 = cast(Tensor, torch.randn(100, 100))  # noqa
            del new_x
            del new_y
            no_use_2 = cast(Tensor, torch.randn(100, 100))  # noqa

        mesh.exit()

        commands, memory = self._get_simulation_result(1, trace_path)

        assert memory == [
            0.04,  # x
            0.08,  # y
            0.12,  # z
            0.20,  # torch.cat
            0.24,  # mm
            0.20,  # del the original y
            0.24,  # new x
            0.20,  # del the original x
            0.24,  # no_use1
            0.16,  # del new_x, del_y => flatten removed
            0.20,  # no_use_2
        ]
        self.maxDiff = 10000
        assert commands["main"] == [
            "aten.randn",  # x
            "aten.randn",  # y
            "aten.randn",  # z
            "aten.cat",  # cat
            "aten.mm",  # mm
            "waiting for reducer_stream",  # drop
            "aten.randn",  # second x
            "aten.split_with_sizes",  # split
            "aten.randn",  # no_use_1
            "aten.randn",  # no_use_2
        ]

        assert commands["main timestamp"] == [
            (0.0, 10.0),
            (10.0, 20.0),
            (20.0, 30.0),
            (30.0, 40.0),
            (40.0, 50.0),
            # Reduce is set to 100ms which is partially overlapped with mm
            # which is set to 10ms.
            (50.0, 140.0),
            (140.0, 150.0),
            (150.0, 160.0),
            (160.0, 170.0),
            (170.0, 180.0),
        ]

        assert commands["reducer_stream"] == [
            "waiting for main",  # borrow first use
            "reduce_scatter",  # reduce_
        ]
        assert commands["reducer_stream timestamp"] == [
            (0.0, 40.0),
            (40.0, 140.0),  # reduce_
        ]

        if not group_workers:
            assert commands["Device 0"] == []
        else:
            assert commands["Device 0 [0-1]"] == []
        commands, _ = self._get_simulation_result(0, trace_path)
        assert commands["Controller"] == []

    def test_ndslice_to_worker_group(self) -> None:
        simulator = Simulator(world_size=1024, group_workers=True)

        # [0, 1024]
        ranks = [NDSlice(offset=0, sizes=[1024], strides=[1])]
        groups = list(simulator._ndslice_to_worker_group(ranks))
        assert len(groups) == 1

        # [0, 512), [512, 1024)
        ranks = [NDSlice(offset=0, sizes=[512], strides=[1])]
        groups = list(simulator._ndslice_to_worker_group(ranks))
        assert len(groups) == 1
        assert len(simulator._worker_groups) == 2
        np.testing.assert_array_equal(
            simulator._worker_groups[0].workers, np.arange(512)
        )
        np.testing.assert_array_equal(
            simulator._worker_groups[1].workers, np.arange(512, 1024)
        )

        # [0, 512), ([512, 640), [768, 1024)), [640, 768)
        ranks = [NDSlice(offset=640, sizes=[128], strides=[1])]
        groups = list(simulator._ndslice_to_worker_group(ranks))
        assert len(groups) == 1
        assert len(simulator._worker_groups) == 3
        np.testing.assert_array_equal(
            simulator._worker_groups[0].workers, np.arange(512)
        )
        np.testing.assert_array_equal(
            simulator._worker_groups[1].workers, np.arange(640, 768)
        )
        np.testing.assert_array_equal(
            simulator._worker_groups[2].workers,
            np.concatenate((np.arange(512, 640), np.arange(768, 1024))),
        )

        # [0, 256), [256, 512), [512, 600), ([600, 640), [768, 1024)), [640, 768)
        ranks = [NDSlice(offset=256, sizes=[344], strides=[1])]
        groups = list(simulator._ndslice_to_worker_group(ranks))
        assert len(groups) == 2
        assert len(simulator._worker_groups) == 5
        np.testing.assert_array_equal(
            simulator._worker_groups[0].workers, np.arange(256, 512)
        )
        np.testing.assert_array_equal(
            simulator._worker_groups[1].workers, np.arange(0, 256)
        )
        np.testing.assert_array_equal(
            simulator._worker_groups[2].workers, np.arange(640, 768)
        )
        np.testing.assert_array_equal(
            simulator._worker_groups[3].workers, np.arange(512, 600)
        )
        np.testing.assert_array_equal(
            simulator._worker_groups[4].workers,
            np.concatenate((np.arange(600, 640), np.arange(768, 1024))),
        )

    @with_tempfile(unlink=False)
    def test_cached_remote_function(self, trace_path=None) -> None:
        mesh = monarch.Simulator(
            hosts=1,
            gpus=2,
            trace_path=trace_path,
            trace_mode=SimulatorTraceMode.STREAM_ONLY,
        ).mesh
        with mesh.activate():
            pg = mesh.process_group(("gpu",))
            myrank = mesh.rank("host") * 8 + mesh.rank("gpu")
            x = torch.ones((3, 4), device="cuda") * myrank
            reduce = simple_all_reduce_local(x, group=pg)
            assert reduce is not None
            local_reduce = fetch_shard(reduce)
            _ = local_reduce.result()
        mesh.exit()

    @with_tempfile(unlink=False)
    def test_chunk_cat(self, trace_path=None) -> None:
        mesh = monarch.Simulator(
            hosts=1,
            gpus=2,
            trace_path=trace_path,
            trace_mode=SimulatorTraceMode.STREAM_ONLY,
        ).mesh

        with mesh.activate():
            x = torch.ones((4, 4), device="cuda")
            y = torch.ones((4, 4), device="cuda")
            out = torch.zeros((2, 8), device="cuda")
            input_tensors = [x, y]
            torch._chunk_cat(
                input_tensors,
                dim=0,
                num_chunks=2,
                out=out,
            )
            torch._chunk_cat(
                input_tensors,
                dim=0,
                num_chunks=2,
                out=out,
            )
        mesh.exit()

    @with_tempfile(unlink=False)
    def test_view(self, trace_path=None) -> None:
        mesh = monarch.Simulator(
            hosts=1,
            gpus=2,
            trace_path=trace_path,
            trace_mode=SimulatorTraceMode.STREAM_ONLY,
        ).mesh

        with mesh.activate():
            x = torch.ones((4, 4), device="cuda")
            x = x.flatten()
        mesh.exit()
        commands, memory = self._get_simulation_result(1, trace_path)
        # Only one should be capture as view is a CPU op.
        assert commands["main"] == ["aten.ones"]

    @with_tempfile(unlink=False)
    def test_send_tensor(self, trace_path=None) -> None:
        mesh = monarch.Simulator(
            hosts=1,
            gpus=2,
            trace_path=trace_path,
            trace_mode=SimulatorTraceMode.STREAM_ONLY,
        ).mesh

        with mesh(gpu=0).activate():
            x = torch.ones((4, 4), device="cuda")
        _ = x.to_mesh(mesh(gpu=1))
        mesh.exit()
        commands, memory = self._get_simulation_result(1, trace_path)
        assert commands["main"], ["aten.ones", "SendTensor"]
        commands, memory = self._get_simulation_result(3, trace_path)
        assert commands["main"], ["RecvTensor"]
