# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from monarch.actor import Actor, current_rank, endpoint
from monarch.actor import this_host
from monarch.actor import HostMesh
from monarch.job import SlurmJob
import logging

from remotemount import remotemount
import os
import subprocess
import tempfile
import fire
import sys
from pathlib import Path


class BashActor(Actor):

    @endpoint
    def run(self, script: str):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=True) as f:
            f.write(script)
            f.flush()
            result = subprocess.run(['bash', f.name], capture_output=True, text=True)
        return {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}

def main(source_dir: str,
         script: str,
         mount_point: str | None = None,
         run_local=False,
         verbose=False):
    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%H:%M:%S'
        )

    mount_point = source_dir if mount_point is None else mount_point
    mount_point = Path(mount_point).resolve()
    source_dir = Path(source_dir).resolve()

    if run_local and mount_point == source_dir:
        raise ValueError(f"If running locally mount_point and source_dir need to be different paths. Instead got source_dir {source_dir} and mount_point {mount_point}.")

    # # Spawn a process for each GPU
    if run_local:
        host_mesh = this_host()
        procs = host_mesh.spawn_procs()
    else:
        # Create a slurm job with 2 hosts
        slurm_job = SlurmJob({"mesh1": 2},
                             slurm_args=["--account=atom", "--qos=h100_lowest"],
                             exclusive=False,
                             gpus_per_node=1,
                             cpus_per_task=24,
                             time_limit="1:00:00",
                             log_dir="/storage/home/cpuhrsch/dev/clusterfs/slurm_logs",
                             mem="100G")
        host_meshes = slurm_job.state()
        host_mesh = host_meshes.mesh1
        procs = host_mesh.spawn_procs()


    if script == 'stdin':
        script = sys.stdin.read()
    else:
        with open(Path(script).resolve()) as f:
            script = f.read()

    with remotemount(host_mesh, str(source_dir), str(mount_point)):
        bash_actors = procs.spawn("BashActor", BashActor)
        results = bash_actors.run.call(script).get()
        # Print stdout for each host in order
        print("\n".join([f"== host{i} stdout ==\n{r[1]['stdout']}" for i, r in enumerate(results)]))
        print("\n".join([f"== host{i} stderr ==\n{r[1]['stderr']}" for i, r in enumerate(results)]))

if __name__ == "__main__":
    fire.Fire(main)
