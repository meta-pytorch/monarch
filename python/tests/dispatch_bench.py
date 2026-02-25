# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import logging
import sys

import torch
import torch.utils.benchmark as benchmark

# this function helps get a local device mesh for testing
from monarch._testing import mock_mesh
from monarch.builtins.log import set_logging_level_remote
from monarch.common._coalescing import coalescing
from monarch.common.remote import remote
from monarch.fetch import fetch_shard
from monarch.python_local_mesh import python_local_mesh
from monarch_supervisor.logging import initialize_logging
from tests.dispatch_bench_helper import run_loop, run_loop_local

NITER = 10000
DEFAULT_TENSOR_SIZE = (100, 100)

initialize_logging("dispatch_bench")


# user-defined remote functions
log = remote("monarch.worker._testing_function.log", propagate="inspect")


def local_run():
    run_loop_local(NITER, DEFAULT_TENSOR_SIZE)


def dispatch_to_worker(device_mesh, n_iter, tensor_size):
    with device_mesh.activate():
        result = run_loop_local(n_iter, tensor_size)
        local_result = fetch_shard(result, {"host": 0, "gpu": 0})
    local_result = local_result.result()


def dispatch_to_worker_remote_function(device_mesh, n_iter, tensor_size):
    with device_mesh.activate():
        result = run_loop(n_iter, tensor_size)
        local_result = fetch_shard(result, {"host": 0, "gpu": 0})
    local_result = local_result.result()


def dispatch_to_worker_coalescing(device_mesh, n_iter, tensor_size):
    with device_mesh.activate():
        with coalescing():
            result = run_loop_local(n_iter, tensor_size)
        local_result = fetch_shard(result, {"host": 0, "gpu": 0})
    local_result = local_result.result()


def main():
    mocked = False
    torch.set_default_device("cuda")
    if mocked:
        device_mesh = mock_mesh(hosts=1, gpus=1)
    else:
        device_mesh = python_local_mesh(hosts=1, gpus=1)

    with device_mesh.activate():
        torch.set_default_device("cuda")
        set_logging_level_remote(logging.WARNING)

    # bench 1: local compute only
    t0 = benchmark.Timer(
        stmt="run_loop_local(niter, tensor_size)",
        setup="from __main__ import run_loop_local",
        globals={"niter": NITER, "tensor_size": DEFAULT_TENSOR_SIZE},
    )
    local_only_results = t0.blocked_autorange(min_run_time=10)
    print(local_only_results)

    t1 = benchmark.Timer(
        stmt="dispatch_to_worker(device_mesh, niter, tensor_size)",
        setup="from __main__ import dispatch_to_worker",
        globals={
            "device_mesh": device_mesh,
            "niter": NITER,
            "tensor_size": DEFAULT_TENSOR_SIZE,
        },
    )
    dispatch_to_worker_results = t1.blocked_autorange(min_run_time=10)
    print(dispatch_to_worker_results)

    t2 = benchmark.Timer(
        stmt="dispatch_to_worker_remote_function(device_mesh, niter, tensor_size)",
        setup="from __main__ import dispatch_to_worker_remote_function",
        globals={
            "device_mesh": device_mesh,
            "niter": NITER,
            "tensor_size": DEFAULT_TENSOR_SIZE,
        },
    )
    dispatch_to_worker_remote_function_results = t2.blocked_autorange(min_run_time=10)
    print(dispatch_to_worker_remote_function_results)

    t3 = benchmark.Timer(
        stmt="dispatch_to_worker_coalescing(device_mesh, niter, tensor_size)",
        setup="from __main__ import dispatch_to_worker_coalescing",
        globals={
            "device_mesh": device_mesh,
            "niter": NITER,
            "tensor_size": DEFAULT_TENSOR_SIZE,
        },
    )
    dispatch_to_worker_coalescing_results = t3.blocked_autorange(min_run_time=10)
    print(dispatch_to_worker_coalescing_results)

    device_mesh.exit()

    return 0


if __name__ == "__main__":
    sys.exit(main())
