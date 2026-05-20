# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import mmap
import os
import socket
import sys
import time
from typing import Optional

import fire
import torch
import xxhash
from monarch.actor import Actor, endpoint, shutdown_context
from monarch.rdma import RDMABuffer


def _checksum(buf: bytes) -> str:
    """Compute an xxhash-based checksum hex digest."""
    return xxhash.xxh64(buf).hexdigest()


class PingPongActor(Actor):
    """Actor that participates in RDMA pingpong."""

    def __init__(self, size_bytes: int, buffer_type: str = "cpu_tensor"):
        self.hostname = socket.gethostname()
        self.size_bytes = size_bytes
        self.buffer_type = buffer_type

        if buffer_type in ("cpu_tensor", "cuda_tensor"):
            n = size_bytes // 4
            device = "cuda" if buffer_type == "cuda_tensor" else "cpu"
            self.data = torch.rand(n, dtype=torch.float32, device=device)
            self.recv_buf = torch.zeros(n, dtype=torch.float32, device=device)
        elif buffer_type == "bytearray":
            self.data = bytearray(os.urandom(size_bytes))
            self.recv_buf = bytearray(size_bytes)
        elif buffer_type == "memoryview":
            self._data_mmap = mmap.mmap(-1, size_bytes)
            self._data_mmap.write(os.urandom(size_bytes))
            self._data_mmap.seek(0)
            self.data = self._data_mmap
            self._recv_mmap = mmap.mmap(-1, size_bytes)
            self.recv_buf = self._recv_mmap
        else:
            raise ValueError(f"Unknown buffer_type: {buffer_type!r}")

    @endpoint
    async def init_rdma(self):
        """Pre-initialize the RDMA manager (avoids block_on deadlock)."""
        from monarch._src.actor.future import Future
        from monarch._src.rdma.rdma import _ensure_init_rdma_manager

        await Future(coro=_ensure_init_rdma_manager())

    @endpoint
    async def get_buffer(self) -> RDMABuffer:
        if self.buffer_type in ("cpu_tensor", "cuda_tensor"):
            return RDMABuffer(self.data.view(torch.uint8).flatten())
        else:
            return RDMABuffer(memoryview(self.data))

    @endpoint
    async def read_from(self, peer_buf: RDMABuffer) -> float:
        """Read peer's data into recv_buf, return elapsed seconds."""
        if self.buffer_type in ("cpu_tensor", "cuda_tensor"):
            self.recv_buf.zero_()
            local = self.recv_buf.view(torch.uint8).flatten()
        elif self.buffer_type == "bytearray":
            for i in range(len(self.recv_buf)):
                self.recv_buf[i] = 0
            local = memoryview(self.recv_buf)
        else:
            self._recv_mmap.seek(0)
            self._recv_mmap.write(b"\x00" * self.size_bytes)
            self._recv_mmap.seek(0)
            local = memoryview(self.recv_buf)

        t0 = time.perf_counter()
        await peer_buf.read_into(local, timeout=60)
        return time.perf_counter() - t0

    @endpoint
    async def checksum(self, which: str = "data") -> str:
        buf = self.data if which == "data" else self.recv_buf
        if self.buffer_type in ("cpu_tensor", "cuda_tensor"):
            # .cpu() is a no-op on CPU tensors, required for cuda_tensor
            # since numpy doesn't accept CUDA storage.
            raw = buf.cpu().numpy().tobytes()
        else:
            raw = bytes(buf)
        return _checksum(raw)


def main(
    data_size_mb: int = 100,
    num_iterations: int = 5,
    backend: str = "slurm",
    partition: Optional[str] = None,
    hpc_identity: str = "hyper_monarch",
    hpc_job_oncall: str = "monarch",
    hpc_cluster_uuid: str = "MastGenAICluster",
    rm_attribution: str = "msl_infra_pytorch_dev",
    k8s_namespace: str = "monarch-tests",
    k8s_image: str = "ghcr.io/meta-pytorch/monarch:latest",
    buffer_type: str = "cpu_tensor",
):
    """RDMA Pingpong: transfer data between two nodes via RDMABuffer."""
    sys.stdout.reconfigure(line_buffering=True)
    size = data_size_mb * 1024 * 1024

    if buffer_type not in ("cpu_tensor", "cuda_tensor", "bytearray", "memoryview"):
        raise ValueError(
            f"Unknown buffer_type: {buffer_type!r}; "
            "choose from 'cpu_tensor', 'cuda_tensor', 'bytearray', 'memoryview'"
        )

    if backend == "mast":
        from monarch.actor import enable_transport
        from monarch.config import ChannelTransport
        from monarch.job.meta import MASTJob

        enable_transport("metatls")
        job = MASTJob(
            hpcIdentity=hpc_identity,
            hpcJobOncall=hpc_job_oncall,
            hpcClusterUuid=hpc_cluster_uuid,
            rmAttribution=rm_attribution,
            useStrictName=True,
            localityConstraints=["region", "gtn"],
            default_transport=ChannelTransport.MetaTlsWithIpV6,
            use_case="example",
        )
        job.add_mesh("workers", 2)
    elif backend == "k8s":
        from kubernetes.client import (
            V1Affinity,
            V1Container,
            V1EnvVar,
            V1LabelSelector,
            V1LabelSelectorRequirement,
            V1PodAffinityTerm,
            V1PodAntiAffinity,
            V1PodSpec,
            V1PodTemplateSpec,
            V1ResourceRequirements,
        )
        from monarch._src.job.kubernetes import _WORKER_BOOTSTRAP_SCRIPT
        from monarch.job.kubernetes import KubernetesJob

        # Hard anti-affinity on the operator's per-mesh label forces the two
        # replicas onto distinct nodes, so RDMA pingpong actually exercises
        # the cross-host fabric instead of a same-HCA loopback path.
        #
        # NOTE: requesting "rdma/ib": 1 only guarantees each pod gets an IB
        # device; it does NOT guarantee the two pods can reach each other
        # over IB. On clusters with multiple isolated IB fabrics, add a
        # required pod_affinity term keyed on your provider's fabric label
        # so the two replicas land on the same fabric. Without it, the example will fail.
        worker_resources = {"nvidia.com/gpu": "1", "rdma/ib": "1"}
        pod_template = V1PodTemplateSpec(
            spec=V1PodSpec(
                affinity=V1Affinity(
                    pod_anti_affinity=V1PodAntiAffinity(
                        required_during_scheduling_ignored_during_execution=[
                            V1PodAffinityTerm(
                                topology_key="kubernetes.io/hostname",
                                label_selector=V1LabelSelector(
                                    match_expressions=[
                                        V1LabelSelectorRequirement(
                                            key="monarch.pytorch.org/mesh-name",
                                            operator="In",
                                            values=["workers"],
                                        ),
                                    ],
                                ),
                            ),
                        ],
                    ),
                ),
                containers=[
                    V1Container(
                        name="worker",
                        image=k8s_image,
                        command=["python", "-u", "-c", _WORKER_BOOTSTRAP_SCRIPT],
                        env=[V1EnvVar(name="MONARCH_PORT", value="26600")],
                        resources=V1ResourceRequirements(
                            requests=worker_resources,
                            limits=worker_resources,
                        ),
                    ),
                ],
            ),
        )
        job = KubernetesJob(namespace=k8s_namespace)
        job.add_mesh("workers", num_replicas=2, pod_template=pod_template)
    else:
        from monarch.job import SlurmJob

        job = SlurmJob(
            meshes={"workers": 2},
            gpus_per_node=1,
            partition=partition,
            exclusive=False,
            log_dir=os.path.expanduser("~/monarch_slurm_logs"),
        )

    workers = job.state().workers
    procs = workers.spawn_procs()
    a0 = procs.spawn("a0", PingPongActor, size, buffer_type).slice(hosts=0)
    a1 = procs.spawn("a1", PingPongActor, size, buffer_type).slice(hosts=1)

    a0.init_rdma.call_one().get()
    a1.init_rdma.call_one().get()
    buf0 = a0.get_buffer.call_one().get()
    buf1 = a1.get_buffer.call_one().get()
    cksum0 = a0.checksum.call_one("data").get()
    cksum1 = a1.checksum.call_one("data").get()

    print(
        f"RDMA Pingpong: {data_size_mb} MB x {num_iterations} iters (buffer_type={buffer_type})"
    )
    for i in range(num_iterations):
        # Ping: a0 reads from a1
        dt = a0.read_from.call_one(buf1).get()
        got = a0.checksum.call_one("recv").get()
        ok = got == cksum1
        print(
            f"  [{i + 1}] ping {dt:.3f}s {size / dt / 1e9:.2f} GB/s {'PASS' if ok else 'FAIL'}"
        )

        # Pong: a1 reads from a0
        dt = a1.read_from.call_one(buf0).get()
        got = a1.checksum.call_one("recv").get()
        ok = got == cksum0
        print(
            f"  [{i + 1}] pong {dt:.3f}s {size / dt / 1e9:.2f} GB/s {'PASS' if ok else 'FAIL'}"
        )

    workers.shutdown().get()
    shutdown_context().get()
    print("Done!")


if __name__ == "__main__":
    fire.Fire(main)
