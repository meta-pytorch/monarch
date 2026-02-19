# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import json
from typing import Any
from unittest import mock

from monarch._src.actor.allocator import LocalAllocator
from monarch.actor import Actor, current_rank, endpoint, HostMesh, ValueMesh
from monarch.session import connect, MonarchSession
from monarch.tools.components import hyperactor
from monarch.tools.config import Config
from monarch.tools.config.workspace import Workspace


def mock_slurm_subprocess_run(*args: Any, **kwargs: Any) -> mock.MagicMock:
    """Mock subprocess.run for SLURM commands."""
    cmd = args[0] if args else []
    result = mock.MagicMock()
    result.returncode = 0
    result.stderr = b""

    if isinstance(cmd, list) and len(cmd) > 0:
        command = cmd[0]
        if command == "sbatch":
            result.stdout = b"Submitted batch job 1234567\n"
        elif command == "sinfo":
            # Mock sinfo output for partition memory info
            result.stdout = b"PARTITION,MEMORY\ndefault,65536\ngpu_h,131072\n"
        else:
            result.stdout = b""
    else:
        result.stdout = b""

    return result


def mock_slurm_subprocess_check_output(*args: Any, **kwargs: Any) -> bytes:
    """Mock subprocess.check_output for SLURM commands."""
    slurm_json = {
        "jobs": [
            {
                "job_id": "1234567",
                "name": "test-0",
                "command": "process_allocator",
                "current_working_directory": "/tmp/workspace",
                "job_state": ["RUNNING"],
                "job_resources": {
                    "allocated_nodes": [
                        {
                            "nodename": "node001",
                            "cpus_used": 32,
                            "memory_allocated": 65536,
                            "state": "RUNNING",
                        },
                    ],
                },
            }
        ]
    }

    return json.dumps(slurm_json).encode("utf-8")


def mock_remote_allocator(*args: Any, **kwargs: Any) -> LocalAllocator:
    """Mock RemoteAllocator to return LocalAllocator instead."""
    return LocalAllocator()


class MockActor(Actor):
    @endpoint
    def test(self) -> int:
        return current_rank()["procs"]


@mock.patch("subprocess.run", side_effect=mock_slurm_subprocess_run)
@mock.patch("subprocess.check_output", side_effect=mock_slurm_subprocess_check_output)
@mock.patch("monarch.session.RemoteAllocator", side_effect=mock_remote_allocator)
def test_connect(
    mock_remote_allocator: Any, mock_check_output: Any, mock_run: Any
) -> None:
    # This mostly just tests the APIs with a mock SLURM call and local allocator
    # We choose a mocked SLURM vs something like local for simplicity.
    # Local would attempt to spin up process_allocator which is not included
    # on all Monarch builds.
    num_hosts = 1
    appdef = hyperactor.host_mesh(
        image="test",
        meshes=[f"mesh_0:{num_hosts}:NULL"],
    )
    config = Config(scheduler="slurm", appdef=appdef, workspace=Workspace(env=None))
    ms = connect(name="test", config=config)
    assert isinstance(ms, MonarchSession)
    host_mesh = ms.host_mesh(num_hosts=num_hosts)
    assert isinstance(host_mesh, HostMesh)

    procs = host_mesh.spawn_procs(per_host={"procs": 8})
    actors = procs.spawn("test", MockActor)
    results = actors.test.call().get()
    assert isinstance(results, ValueMesh)
    extent = results.extent
    assert extent["procs"] == 8
