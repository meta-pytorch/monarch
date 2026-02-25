# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import Union

from monarch._src.actor.shape import NDSlice
from monarch.common.client import Client as _Client
from monarch.common.device_mesh import DeviceMesh
from monarch.simulator.ir import IRGraph
from monarch.simulator.simulator import (
    SimulatorBackendMode,
    SimulatorController as _SimulatorController,
    SimulatorInterface,
    SimulatorTraceMode,
)


def Simulator(
    hosts: int,
    gpus: int,
    *,
    simulate_mode: Union["str", SimulatorBackendMode] = SimulatorBackendMode.SIMULATE,
    trace_mode: Union["str", SimulatorTraceMode] = SimulatorTraceMode.STREAM_ONLY,
    upload_trace: bool = False,
    trace_path: str = "trace.json",
    command_history_path: str = "command_history.pkl",
    group_workers: bool = False,
    build_ir: bool = False,
) -> "SimulatorInterface":
    if isinstance(simulate_mode, str):
        simulate_mode = getattr(SimulatorBackendMode, simulate_mode.upper())
    if isinstance(trace_mode, str):
        trace_mode = getattr(SimulatorTraceMode, trace_mode.upper())

    ir = IRGraph() if build_ir else None
    ctrl = _SimulatorController(
        hosts * gpus,
        gpu_per_host=gpus,
        simulate_mode=simulate_mode,
        trace_mode=trace_mode,
        upload_trace=upload_trace,
        trace_path=trace_path,
        command_history_path=command_history_path,
        group_workers=group_workers,
        ir=ir,
    )
    client = _Client(ctrl, ctrl.world_size, ctrl.gpu_per_host)
    dm = DeviceMesh(
        client,
        NDSlice(offset=0, sizes=[hosts, gpus], strides=[gpus, 1]),
        ("host", "gpu"),
    )

    dm.exit = lambda: client.shutdown()
    return SimulatorInterface(dm, ctrl, ir)
