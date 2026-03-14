#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Test incremental updates for remotemount.

Exercises:
  1. First run — full transfer
  2. Re-run with no changes — skip transfer (instant)
  3. Re-run after modifying a file — re-transfer
  4. Re-run with no changes again — skip transfer

Requires MAST allocation; run manually:
  buck run fbcode//monarch/python/tests:test_remotemount_incremental -- \
    [--backend mast] [--host_type gb300] [--num_hosts 2]
"""

import logging
import os
import shutil
import tempfile
import time

import fire
from monarch.actor import Actor, endpoint, this_host
from monarch.config import configure
from monarch.remotemount import remotemount


class TestActor(Actor):
    @endpoint
    def check_file(self, path):
        """Read a file from the mount and return its contents."""
        try:
            with open(path) as f:
                return f.read()
        except Exception as e:
            return f"ERROR: {e}"

    @endpoint
    def check_file_bytes(self, path, n=256):
        """Read first N bytes of a file and return hex + repr."""
        try:
            with open(path, "rb") as f:
                data = f.read(n)
            return {
                "size": os.path.getsize(path),
                "first_bytes": data[:64].hex(),
                "repr": repr(data[:64]),
                "nonzero": sum(1 for b in data if b != 0),
            }
        except Exception as e:
            return f"ERROR: {e}"

    @endpoint
    def list_dir(self, path):
        """List files in a directory."""
        try:
            return sorted(os.listdir(path))
        except Exception as e:
            return f"ERROR: {e}"


def main(
    backend="mast",
    num_hosts=2,
    gpus_per_host=1,
    host_type="gb300",
    locality_constraints="",
    verbose=True,
):
    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%H:%M:%S",
        )

    configure(
        enable_log_forwarding=True,
        tail_log_lines=100,
        host_spawn_ready_timeout="120s",
        mesh_proc_spawn_max_idle="120s",
        message_delivery_timeout="600s",
        rdma_max_chunk_size_mb=256,
    )

    # Create test directory.
    test_dir = tempfile.mkdtemp(prefix="remotemount_test_")
    os.makedirs(os.path.join(test_dir, "src"), exist_ok=True)

    for i in range(5):
        with open(os.path.join(test_dir, "src", f"mod_{i}.py"), "w") as f:
            f.write(f"# Module {i}\ndef func(): return {i}\n")

    with open(os.path.join(test_dir, "config.json"), "w") as f:
        f.write('{"lr": 0.001, "epochs": 10}\n')

    # Add a ~50MB data file so transfer is measurable.
    with open(os.path.join(test_dir, "data.bin"), "wb") as f:
        f.write(os.urandom(50 * 1024 * 1024))

    print(f"\nTest directory: {test_dir}")
    print(f"Files: {sum(len(f) for _, _, f in os.walk(test_dir))}")
    total = sum(
        os.path.getsize(os.path.join(r, f))
        for r, _, fs in os.walk(test_dir)
        for f in fs
    )
    print(f"Size: {total / (1024 * 1024):.1f} MB\n")

    # Set up job.
    if backend == "mast":
        from monarch.actor import enable_transport
        from monarch.job.meta import MASTJob

        enable_transport("metatls-hostname")
        lc = None
        if locality_constraints and locality_constraints != "":
            lc = locality_constraints.split(";")
        job = MASTJob(
            hpcIdentity="hyper_monarch",
            hpcJobOncall="monarch",
            rmAttribution="msl_infra_pytorch_dev",
            hpcClusterUuid="MastGenAICluster",
            useStrictName=True,
            localityConstraints=lc,
            env={
                "PYTHONDONTWRITEBYTECODE": "1",
                "MAST_PRECHECK_SKIP_TIME_CONSUMING_CHECKS": "1",
            },
        )
        job.add_mesh("workers", num_hosts, host_type=host_type)
        host_meshes = job.state()
        host_mesh = host_meshes.workers
    else:
        host_mesh = this_host()

    mount_point = test_dir  # Mount at same path on workers.

    # Create a single MountHandler that persists across tests.
    # This way FUSEActors keep their content hash between open/close.
    handler = remotemount(host_mesh, test_dir, mount_point, backend=backend)

    procs = host_mesh.spawn_procs(per_host={"gpus": gpus_per_host})
    test_actors = procs.spawn("TestActor", TestActor)

    def verify(label):
        print(f"\n  Verifying {label}:")
        # Check config.json content.
        results = test_actors.check_file.call(
            os.path.join(mount_point, "config.json")
        ).get()
        for point, content in results:
            print(f"    rank{point.rank} config.json: {content.strip()!r}")

        # Check config.json bytes for diagnostic.
        results = test_actors.check_file_bytes.call(
            os.path.join(mount_point, "config.json")
        ).get()
        for point, info in results:
            print(f"    rank{point.rank} config.json bytes: {info}")

        # Check a .py file too.
        results = test_actors.check_file.call(
            os.path.join(mount_point, "src", "mod_0.py")
        ).get()
        for point, content in results:
            print(f"    rank{point.rank} mod_0.py: {content.strip()!r}")

        # List src directory.
        results = test_actors.list_dir.call(os.path.join(mount_point, "src")).get()
        for point, files in results:
            print(f"    rank{point.rank} src/: {files}")

    # Test 1: First run.
    print(f"\n{'=' * 60}")
    print("  Test 1: First run (full transfer)")
    print(f"{'=' * 60}")
    t0 = time.time()
    handler.open()
    print(f"  open() time: {time.time() - t0:.1f}s")
    verify("after first mount")

    # Test 2: Close and re-open with no changes.
    handler.close()
    print(f"\n{'=' * 60}")
    print("  Test 2: Re-open, no changes (should skip)")
    print(f"{'=' * 60}")
    t0 = time.time()
    handler.open()
    print(f"  open() time: {time.time() - t0:.1f}s")
    verify("after skip")

    # Test 3: Modify config and re-open.
    handler.close()
    print("\n>>> Modifying config.json...")
    with open(os.path.join(test_dir, "config.json"), "w") as f:
        f.write('{"lr": 0.01, "epochs": 20}\n')
    print(f"\n{'=' * 60}")
    print("  Test 3: Re-open after modification (should re-transfer)")
    print(f"{'=' * 60}")
    t0 = time.time()
    handler.open()
    print(f"  open() time: {time.time() - t0:.1f}s")
    verify("after re-transfer")

    # Test 4: No changes again.
    handler.close()
    print(f"\n{'=' * 60}")
    print("  Test 4: Re-open, no changes (should skip)")
    print(f"{'=' * 60}")
    t0 = time.time()
    handler.open()
    print(f"  open() time: {time.time() - t0:.1f}s")
    verify("after second skip")

    handler.close()

    # Cleanup.
    if backend == "mast":
        job.kill()
        print("MAST job killed.")
    shutil.rmtree(test_dir, ignore_errors=True)
    print("Done.")


# Register for pickle-by-value.
import sys as _sys  # noqa: E402

import cloudpickle  # noqa: E402

cloudpickle.register_pickle_by_value(_sys.modules[__name__])

if __name__ == "__main__":
    fire.Fire(main)
