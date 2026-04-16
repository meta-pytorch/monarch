# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Test remote_mount with KubernetesJob in out-of-cluster mode.

Provisions a mesh, mounts a local directory via remote_mount, and
verifies files are accessible on workers through the duplex channel.

Usage (from the monarch repo root):
    # With pre-provisioned manifests (hello_mesh.yaml already applied):
    uv run --no-build-isolation examples/kubernetes/hello_kubernetes_job/test_remotemount_k8s.py

    # With auto-provisioning:
    uv run --no-build-isolation examples/kubernetes/hello_kubernetes_job/test_remotemount_k8s.py --provision --image ghcr.io/meta-pytorch/monarch:latest
"""

from __future__ import annotations

import argparse
import hashlib
import os
import socket
import sys
import tempfile

from monarch.actor import Actor, endpoint
from monarch.job.kubernetes import ImageSpec, KubeConfig, KubernetesJob


class FileCheckActor(Actor):
    @endpoint
    def check_file(self, path: str) -> dict:
        """Read a file and return its contents and hash."""
        try:
            with open(path, "rb") as f:
                data = f.read()
            return {
                "hostname": socket.gethostname(),
                "exists": True,
                "size": len(data),
                "md5": hashlib.md5(data).hexdigest(),
            }
        except Exception as e:
            return {
                "hostname": socket.gethostname(),
                "exists": False,
                "error": str(e),
            }

    @endpoint
    def list_dir(self, path: str) -> dict:
        """List contents of a directory."""
        try:
            entries = sorted(os.listdir(path))
            return {
                "hostname": socket.gethostname(),
                "entries": entries,
            }
        except Exception as e:
            return {
                "hostname": socket.gethostname(),
                "error": str(e),
            }


def main() -> None:
    parser = argparse.ArgumentParser(description="Test remote_mount with KubernetesJob")
    parser.add_argument(
        "--provision",
        action="store_true",
        help="Provision MonarchMesh CRDs (otherwise expects pre-existing pods)",
    )
    parser.add_argument(
        "--image",
        type=str,
        default="ghcr.io/meta-pytorch/monarch:latest",
        help="Container image for provisioned pods",
    )
    parser.add_argument(
        "--namespace",
        type=str,
        default="monarch-tests",
        help="Kubernetes namespace",
    )
    parser.add_argument(
        "--num-replicas",
        type=int,
        default=2,
        help="Number of replicas per mesh",
    )
    args = parser.parse_args()

    # Create a temporary directory with test files.
    test_dir = tempfile.mkdtemp(prefix="remotemount_k8s_test_")
    test_file = os.path.join(test_dir, "hello.txt")
    with open(test_file, "w") as f:
        f.write("hello from remote_mount over duplex channel!\n")

    # Add a small data file to exercise block transfer.
    data_file = os.path.join(test_dir, "data.bin")
    with open(data_file, "wb") as f:
        f.write(os.urandom(1024))

    local_md5 = hashlib.md5(open(data_file, "rb").read()).hexdigest()
    print(f"Test directory: {test_dir}")
    print(f"  hello.txt: {os.path.getsize(test_file)} bytes")
    print(f"  data.bin: {os.path.getsize(data_file)} bytes (md5={local_md5})")

    # Set up KubernetesJob in out-of-cluster mode.
    job = KubernetesJob(
        namespace=args.namespace,
        kubeconfig=KubeConfig.from_path("~/.kube/config"),
    )

    if args.provision:
        job.add_mesh(
            "workers",
            args.num_replicas,
            image_spec=ImageSpec(args.image),
        )
    else:
        job.add_mesh("mesh1", args.num_replicas)

    # Register remote_mount — the override forces num_parallel_streams=1
    # so data flows through the duplex channel.
    mesh_name = "mesh1" if not args.provision else "workers"
    job.remote_mount(test_dir, python_exe=None, meshes=[mesh_name])

    print(f"\nConnecting to cluster (out-of-cluster mode)...")
    state = job.state(cached_path=None)

    mesh = getattr(state, mesh_name)
    procs = mesh.spawn_procs()
    actors = procs.spawn("FileCheckActor", FileCheckActor)

    # Verify files are accessible on all workers.
    print("\nChecking file access on workers...")

    list_results = actors.list_dir.call(test_dir).get()
    for point, result in list_results:
        hostname = result.get("hostname", "unknown")
        if "error" in result:
            print(f"  rank {point.rank} ({hostname}): ERROR: {result['error']}")
        else:
            print(f"  rank {point.rank} ({hostname}): {result['entries']}")

    check_results = actors.check_file.call(data_file).get()
    all_ok = True
    for point, result in check_results:
        hostname = result.get("hostname", "unknown")
        if not result.get("exists"):
            print(f"  rank {point.rank} ({hostname}): FAIL - {result.get('error')}")
            all_ok = False
        elif result["md5"] != local_md5:
            print(
                f"  rank {point.rank} ({hostname}): FAIL - "
                f"md5 mismatch: {result['md5']} != {local_md5}"
            )
            all_ok = False
        else:
            print(
                f"  rank {point.rank} ({hostname}): OK - "
                f"{result['size']} bytes, md5={result['md5']}"
            )

    procs.stop().get()

    if args.provision:
        job.kill()

    if all_ok:
        print("\nSUCCESS: All workers can access mounted files with correct content.")
    else:
        print("\nFAILURE: Some workers could not access files correctly.")
        sys.exit(1)


if __name__ == "__main__":
    main()
