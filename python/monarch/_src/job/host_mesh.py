# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import subprocess
import sys
import tempfile

from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.actor.host_mesh import HostMesh
from monarch._src.job.service_identity import new_service_proc_id, service_proc_addr


def hosts_from_config(name: str) -> HostMesh:
    """
    Get the host mesh 'name' from the monarch configuration for the project.

    This config can be modified so that the same code can create meshes from scheduler sources,
    and different sizes etc.
    """
    num_hosts = 2
    tmpdir = tempfile.mkdtemp(prefix="monarch_hosts_from_config_")
    workers = []
    for i in range(num_hosts):
        addr = f"ipc://{tmpdir}/{name}_{i}"
        service_proc_id = new_service_proc_id()
        proc_addr = service_proc_addr(addr, service_proc_id)
        env = {**os.environ}
        cmd = [
            sys.executable,
            "-c",
            "from monarch.actor import run_worker_loop_forever; "
            f"run_worker_loop_forever(address={proc_addr!r}, "
            'ca="trust_all_connections")',
        ]
        subprocess.Popen(cmd, env=env, start_new_session=True)
        workers.append(proc_addr)

    return attach_to_workers(
        name=name,
        ca="trust_all_connections",
        # pyrefly: ignore [bad-argument-type]
        workers=workers,
    )
