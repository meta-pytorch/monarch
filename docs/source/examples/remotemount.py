# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Remote Filesystem Mounting with remotemount
============================================

This example demonstrates ``monarch.remotemount``, which mounts a local
directory as a read-only FUSE filesystem on every host in a Monarch mesh.
Files are packed into a contiguous buffer, transferred via Monarch messaging
(optionally over RDMA), and served from RAM on each remote host.

Use cases:

- Distribute local Python environments to remote workers
- Ship pip wheel caches for offline installs
- Broadcast model checkpoints without NFS or object storage
"""

import subprocess
import tempfile

# %%
# Basic usage
# -----------
# ``remotemount`` is a context manager. While it is open the source directory
# appears at the mount point on every host in the mesh.

import cloudpickle
from monarch.actor import Actor, endpoint, this_host
from monarch.remotemount import remotemount

# %%
# We define a simple actor that runs a bash script on each worker.


class BashActor(Actor):
    @endpoint
    def run(self, script: str):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=True) as f:
            f.write(script)
            f.flush()
            result = subprocess.run(["bash", f.name], capture_output=True, text=True)
        return {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }


# Register BashActor for pickle-by-value so it is serialized to remote workers.
import docs.source.examples.remotemount as _this_module  # noqa: E402

cloudpickle.register_pickle_by_value(_this_module)

# %%
# Mount a local directory on remote workers
# ------------------------------------------
# Create a local process mesh (in practice this would be a multi-node mesh
# from ``SlurmJob`` or ``MASTJob``), mount a source directory, and run a
# command that reads from the mount.

host_mesh = this_host()
procs = host_mesh.spawn_procs(per_host={"gpus": 2})

SOURCE_DIR = "/tmp/remotemount_example_src"
MOUNT_POINT = "/tmp/remotemount_example_mnt"

# Create a small source directory for the example.
subprocess.run(["mkdir", "-p", SOURCE_DIR], check=True)
with open(f"{SOURCE_DIR}/hello.txt", "w") as f:
    f.write("hello from remotemount!\n")

# %%
# The ``remotemount`` context manager packs, transfers, and FUSE-mounts the
# directory. Inside the ``with`` block, every worker sees the files at
# ``MOUNT_POINT``.

with remotemount(host_mesh, SOURCE_DIR, MOUNT_POINT):
    bash_actors = procs.spawn("BashActor", BashActor)
    results = bash_actors.run.call(f"cat {MOUNT_POINT}/hello.txt").get()
    for i, r in enumerate(results):
        print(f"rank {i}: {r[1]['stdout'].strip()}")

# %%
# Chunk size
# ----------
# Transfers use RDMA by default (ibverbs, EFA, or TCP fallback).
# You can control the chunk size (default 8 GiB):
#
# .. code-block:: python
#
#     with remotemount(host_mesh, source_dir, mount_point,
#                      chunk_size=1024 * 1024 * 1024):  # 1 GiB chunks
#         ...

# %%
# Distributing a Python environment
# ----------------------------------
# A powerful pattern is mounting an entire virtualenv or conda environment
# on remote workers. This lets you iterate without rebuilding packages or
# waiting for NFS.
#
# .. code-block:: python
#
#     # On your devbox, create and populate the env:
#     #   python -m venv /scratch/myenv && pip install torch transformers
#     #
#     # Then in your Monarch script:
#     with remotemount(host_mesh, "/scratch/myenv"):
#         bash_actors = procs.spawn("BashActor", BashActor)
#         bash_actors.run.call("""
#             source /scratch/myenv/bin/activate
#             python -c "import torch; print(torch.randn(4).cuda().mean())"
#         """).get()

print("Example completed successfully!")
