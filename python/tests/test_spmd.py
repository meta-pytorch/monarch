# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-safe

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from monarch._src.job.spmd import _parse_torchrun, SPMDJob
from monarch.actor import Actor, current_rank, current_size, endpoint, this_host
from monarch.spmd import (
    setup_torch_elastic_env,
    setup_torch_elastic_env_async,
    SPMDActor,
)


class EnvCapture(Actor):
    """Actor to capture environment variables after setup."""

    @endpoint
    async def get_env_vars(self) -> dict[str, str]:
        """Capture torch elastic environment variables."""
        env_keys = [
            "MASTER_ADDR",
            "MASTER_PORT",
            "RANK",
            "LOCAL_RANK",
            "LOCAL_WORLD_SIZE",
            "GROUP_RANK",
            "GROUP_WORLD_SIZE",
            "ROLE_RANK",
            "ROLE_WORLD_SIZE",
            "ROLE_NAME",
            "WORLD_SIZE",
        ]
        return {key: os.environ.get(key, "") for key in env_keys}

    @endpoint
    async def get_rank_info(self) -> dict[str, int]:
        """Get rank information from current_rank() and current_size()."""
        point = current_rank()
        sizes = current_size()
        return {
            "rank": point.rank,
            "local_rank": point["gpus"],
            "nproc_per_node": sizes["gpus"],
            "world_size": sizes["hosts"] * sizes["gpus"],
        }


def test_spmd_actor_main_with_script() -> None:
    """Test that SPMDActor can execute a PyTorch DDP training script."""
    proc_mesh = this_host().spawn_procs(
        name="test_spmd_script", per_host={"hosts": 1, "gpus": 2}
    )
    spmd_actors = proc_mesh.spawn("spmd", SPMDActor)

    # Create a DDP training script using CPU (gloo backend)
    with tempfile.TemporaryDirectory() as tmpdir:
        test_script_path = Path(tmpdir) / "ddp_train.py"
        test_script_path.write_text(
            """
import os

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.optim as optim
from torch.nn.parallel import DistributedDataParallel as DDP


def main():
    dist.init_process_group("gloo")
    rank = dist.get_rank()

    model = nn.Linear(10, 1)
    ddp_model = DDP(model)

    optimizer = optim.SGD(ddp_model.parameters(), lr=0.01)

    for step in range(5):
        inputs = torch.randn(4, 10)
        outputs = ddp_model(inputs)
        loss = outputs.sum()
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

    dist.destroy_process_group()


if __name__ == "__main__":
    main()
"""
        )

        # Execute DDP script on all ranks
        results = spmd_actors.main.call(
            "localhost", 29500, [str(test_script_path)]
        ).get()

        # Verify all ranks completed successfully
        for _point, result in results.items():
            assert result is True


def test_setup_torch_elastic_env() -> None:
    """Test the setup_torch_elastic_env helper function."""
    proc_mesh = this_host().spawn_procs(
        name="test_elastic_env", per_host={"hosts": 2, "gpus": 4}
    )

    # Setup with automatic master selection
    setup_torch_elastic_env(proc_mesh)

    # Verify environment was set correctly
    env_capture = proc_mesh.spawn("env_capture", EnvCapture)
    env_vars_mesh = env_capture.get_env_vars.call().get()

    # All ranks should have MASTER_ADDR and MASTER_PORT set
    for point, env in env_vars_mesh.items():
        rank = point["hosts"] * 4 + point["gpus"]
        assert env["MASTER_ADDR"] != ""
        assert env["MASTER_PORT"] != ""
        assert env["RANK"] == str(rank)
        assert env["WORLD_SIZE"] == "8"


async def test_setup_torch_elastic_env_async() -> None:
    """Test the async setup_torch_elastic_env_async helper function."""
    proc_mesh = this_host().spawn_procs(
        name="test_elastic_env_async", per_host={"hosts": 2, "gpus": 4}
    )

    # Setup with automatic master selection
    await setup_torch_elastic_env_async(proc_mesh)

    # Verify environment was set correctly
    env_capture = proc_mesh.spawn("env_capture", EnvCapture)
    env_vars_mesh = await env_capture.get_env_vars.call()

    # All ranks should have MASTER_ADDR and MASTER_PORT set
    for point, env in env_vars_mesh.items():
        rank = point["hosts"] * 4 + point["gpus"]
        assert env["MASTER_ADDR"] != ""
        assert env["MASTER_PORT"] != ""
        assert env["RANK"] == str(rank)
        assert env["WORLD_SIZE"] == "8"


def test_spmd_actor_rank_calculations() -> None:
    """Test that rank calculations match expected values for different mesh sizes."""
    GPUS_PER_HOST = 4
    HOSTS = 2
    world_size = HOSTS * GPUS_PER_HOST
    proc_mesh = this_host().spawn_procs(
        name="test_rank_calc_1x2",
        per_host={"hosts": HOSTS, "gpus": GPUS_PER_HOST},
    )
    spmd_actors = proc_mesh.spawn("spmd", SPMDActor)
    spmd_actors.setup_env.call("localhost", 29500).get()

    env_capture = proc_mesh.spawn("env_capture", EnvCapture)
    env_vars_mesh = env_capture.get_env_vars.call().get()

    # Verify each rank
    for point, env in env_vars_mesh.items():
        expected_local_rank = point["gpus"]
        rank = point["hosts"] * GPUS_PER_HOST + point["gpus"]
        expected_group_rank = rank // GPUS_PER_HOST
        expected_group_world_size = (world_size + GPUS_PER_HOST - 1) // GPUS_PER_HOST

        assert env["LOCAL_RANK"] == str(expected_local_rank)
        assert env["LOCAL_WORLD_SIZE"] == str(GPUS_PER_HOST)
        assert env["GROUP_RANK"] == str(expected_group_rank)
        assert env["GROUP_WORLD_SIZE"] == str(expected_group_world_size)
        assert env["WORLD_SIZE"] == str(world_size)


def test_parse_torchrun() -> None:
    """Test _parse_torchrun extracts script args and nproc_per_node correctly."""
    original_roles = [
        {
            "entrypoint": "workspace/entrypoint.sh",
            "args": [
                "torchrun",
                "--nnodes=2",
                "--nproc-per-node=8",
                "-m",
                "train",
                "--lr",
                "0.001",
            ],
        }
    ]
    script_args, nproc_per_node = _parse_torchrun(original_roles)
    assert script_args == ["-m", "train", "--lr", "0.001"]
    assert nproc_per_node == 8


def test_parse_torchrun_entrypoint() -> None:
    """Test _parse_torchrun when entrypoint is torchrun (not in args)."""
    original_roles = [
        {
            "entrypoint": "torchrun",
            "args": [
                "--nnodes=2",
                "--nproc-per-node=8",
                "-m",
                "train",
                "--lr",
                "0.001",
            ],
        }
    ]
    script_args, nproc_per_node = _parse_torchrun(original_roles)
    assert script_args == ["-m", "train", "--lr", "0.001"]
    assert nproc_per_node == 8


def test_run_spmd() -> None:
    """Test run_spmd parses args and spawns actors correctly."""
    job = SPMDJob(
        handle="test_handle",
        scheduler="mast_conda",
        original_roles=[
            {
                "entrypoint": "workspace/entrypoint.sh",
                "args": ["torchrun", "--nproc-per-node=4", "-m", "train"],
            }
        ],
    )

    mock_workers = MagicMock()
    mock_procs = MagicMock()
    mock_am = MagicMock()

    mock_workers.spawn_procs.return_value = mock_procs
    mock_procs.spawn.return_value = mock_am
    mock_procs._labels = ["hosts", "gpus"]

    mock_slice = MagicMock()
    mock_am.slice.return_value = mock_slice
    mock_slice.get_host_port.call_one.return_value.get.return_value = (
        "localhost",
        29500,
    )
    mock_am.main.call.return_value.get.return_value = None

    with patch.object(job, "_state") as mock_state:
        mock_state.return_value = MagicMock(workers=mock_workers)
        job.run_spmd()

    mock_workers.spawn_procs.assert_called_once_with(per_host={"gpus": 4})
    mock_procs.spawn.assert_called_once_with("_SPMDActor", SPMDActor)
    mock_am.main.call.assert_called_once_with("localhost", 29500, ["-m", "train"])
