#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Torchrun implementation using Monarch as a process manager.

This module implements torch.distributed.run (torchrun) functionality for
distributed training, using Monarch actors to manage worker processes instead
of traditional subprocess spawning.

Workers are launched as Monarch actors using the host mesh API, with automatic
setup of PyTorch distributed environment variables (RANK, WORLD_SIZE, etc.).

WARNING: Certificate authority validation is not yet implemented. All connections
use 'trust_all_connections' which is insecure and suitable for development only.
"""

import os
import socket
import subprocess
import sys
import uuid
import warnings
from argparse import ArgumentParser, Namespace, REMAINDER
from datetime import timedelta
from typing import Tuple

from monarch._src.spmd.actor import SPMDActor
from monarch.actor import (  # pyre-ignore[21]
    attach_to_workers,
    enable_transport,
    HostMesh,
    this_host,
)

from torch.distributed import TCPStore
from torch.distributed.elastic.rendezvous.utils import (
    _matches_machine_hostname,
    parse_rendezvous_endpoint,
)


def _parse_args(args=None) -> Namespace:
    parser = ArgumentParser(
        description="Torchrun implementation using Monarch as a process manager."
    )

    #
    # Worker/node size related arguments.
    #

    parser.add_argument(
        "--nnodes",
        type=int,
        default=1,
        help="Number of nodes (exact count, no ranges supported).",
    )
    parser.add_argument(
        "--nproc-per-node",
        "--nproc_per_node",
        type=str,
        default="1",
        help="Number of workers per node; supported values: [auto, cpu, gpu, xpu, int].",
    )

    #
    # Rendezvous related arguments (TCPStore)
    #

    parser.add_argument(
        "--rdzv-backend",
        "--rdzv_backend",
        type=str,
        default="static",
        help="Rendezvous backend (for compatibility with torchrun). "
        "Only 'static' and 'c10d' are supported; both use TCPStore.",
    )
    parser.add_argument(
        "--rdzv-endpoint",
        "--rdzv_endpoint",
        type=str,
        default="",
        help="Rendezvous backend endpoint; usually in form <host>:<port>.",
    )
    parser.add_argument(
        "--rdzv-id",
        "--rdzv_id",
        type=str,
        default="none",
        help="User-defined group id for the rendezvous.",
    )
    parser.add_argument(
        "--rdzv-conf",
        "--rdzv_conf",
        type=str,
        default="",
        help="Additional rendezvous configuration (<key1>=<value1>,<key2>=<value2>,...).",
    )
    parser.add_argument(
        "--standalone",
        action="store_true",
        help="Start a local standalone rendezvous using TCPStore on a free port. "
        "Useful when launching single-node, multi-worker job.",
    )

    #
    # User-code launch related arguments.
    #

    parser.add_argument(
        "--max-restarts",
        "--max_restarts",
        type=int,
        default=0,
        help="Maximum number of worker group restarts before failing.",
    )
    parser.add_argument(
        "--monitor-interval",
        "--monitor_interval",
        type=float,
        default=0.1,
        help="Interval, in seconds, to monitor the state of workers.",
    )
    parser.add_argument(
        "--start-method",
        "--start_method",
        type=str,
        default="spawn",
        choices=["spawn", "fork", "forkserver"],
        help="Multiprocessing start method to use when creating workers.",
    )
    parser.add_argument(
        "--role",
        type=str,
        default="default",
        help="User-defined role for the workers.",
    )
    parser.add_argument(
        "-m",
        "--module",
        action="store_true",
        help="Change each process to interpret the launch script as a Python module, executing "
        "with the same behavior as 'python -m'.",
    )
    parser.add_argument(
        "--no-python",
        "--no_python",
        action="store_true",
        help="Skip prepending the training script with 'python' - just execute it directly.",
    )
    parser.add_argument(
        "--log-dir",
        "--log_dir",
        type=str,
        default=None,
        help="Base directory to use for log files.",
    )
    parser.add_argument(
        "-r",
        "--redirects",
        type=str,
        default="0",
        help="Redirect std streams into a log file in the log directory.",
    )
    parser.add_argument(
        "-t",
        "--tee",
        type=str,
        default="0",
        help="Tee std streams into a log file and also to console.",
    )

    #
    # Backwards compatible parameters with caffe2.distributed.launch.
    #

    parser.add_argument(
        "--node-rank",
        "--node_rank",
        type=int,
        default=0,
        help="Rank of the node for multi-node distributed training.",
    )
    parser.add_argument(
        "--master-addr",
        "--master_addr",
        default="127.0.0.1",
        type=str,
        help="Address of the master node (rank 0). Default: 127.0.0.1",
    )
    parser.add_argument(
        "--master-port",
        "--master_port",
        default=29500,
        type=int,
        help="Port on the master node (rank 0) for communication. Default: 29500",
    )
    parser.add_argument(
        "--local-addr",
        "--local_addr",
        default=None,
        type=str,
        help="Address of the local node.",
    )
    parser.add_argument(
        "--monarch-port",
        "--monarch_port",
        default=22222,
        type=int,
        help="Port for Monarch worker communication. Default: 22222",
    )

    #
    # Positional arguments.
    #

    parser.add_argument(
        "training_script",
        type=str,
        help="Full path to the training program/script to be launched in parallel.",
    )

    # Rest from the training program.
    parser.add_argument("training_script_args", nargs=REMAINDER)

    return parser.parse_args(args)


def _determine_local_world_size(nproc_per_node: str) -> int:
    try:
        return int(nproc_per_node)
    except ValueError:
        if nproc_per_node == "cpu":
            return os.cpu_count() or 1
        elif nproc_per_node == "gpu":
            try:
                import torch

                if torch.cuda.is_available():  # pyre-ignore[16]
                    return torch.cuda.device_count()  # pyre-ignore[16]
            except ImportError:
                pass
            raise ValueError("GPU not available or torch not installed.")
        elif nproc_per_node == "auto":
            try:
                import torch

                if torch.cuda.is_available():  # pyre-ignore[16]
                    return torch.cuda.device_count()  # pyre-ignore[16]
            except ImportError:
                pass
            return os.cpu_count() or 1
        else:
            raise ValueError(f"Unsupported nproc_per_node value: {nproc_per_node}")


def _get_rdzv_endpoint(args: Namespace) -> str:
    if not args.rdzv_endpoint:
        return f"{args.master_addr}:{args.master_port}"
    return args.rdzv_endpoint


def _create_tcpstore(
    rdzv_endpoint: str, world_size: int, timeout: int = 600
) -> Tuple[TCPStore, bool]:  # pyre-ignore[11]
    """
    Create a TCPStore for rendezvous, determining if this host should be the server.

    Returns:
        Tuple of (TCPStore instance, is_master bool)
    """
    # pyre-ignore[16]
    host, port = parse_rendezvous_endpoint(rdzv_endpoint, default_port=29400)
    is_master = _matches_machine_hostname(host)  # pyre-ignore[16]

    for is_server in [is_master, False]:
        try:
            store = TCPStore(  # pyre-ignore[16]
                host_name=host,
                port=port,
                world_size=world_size,
                is_master=is_server,
                timeout=timedelta(seconds=timeout),
                multi_tenant=True,  # Allows rendezvous + training stores to coexist
            )

            if is_server:
                print(f"Process {os.getpid()} hosts the TCP store on {host}:{port}")

            return store, is_server
        except (ValueError, RuntimeError, TimeoutError) as exc:
            if not is_server or is_master is False:
                raise RuntimeError(
                    f"Failed to connect to TCPStore at {host}:{port}. "
                    f"See inner exception for details."
                ) from exc

    raise RuntimeError("Failed to create TCPStore")


def _start_worker_loop_background(monarch_port: int) -> None:
    warnings.warn(
        "Certificate authority validation not yet implemented - using insecure connections",
        UserWarning,
        stacklevel=2,
    )
    hostname = socket.gethostname()
    address = f"tcp://{hostname}:{monarch_port}"

    subprocess.Popen(
        [
            sys.executable,
            "-c",
            f"from monarch.actor import run_worker_loop_forever; "
            f"run_worker_loop_forever(address='{address}', ca='trust_all_connections')",
        ],
    )


def _setup_multinode_master(
    store: TCPStore,
    nnodes: int,
    monarch_port: int,
) -> HostMesh:
    warnings.warn(
        "Certificate authority validation not yet implemented - using insecure connections",
        UserWarning,
        stacklevel=2,
    )
    print(f"\nWaiting for all {nnodes} nodes to join...")
    hostnames = []
    for rank in range(nnodes):
        hostname = store.get(f"node_{rank}_hostname").decode("utf-8")
        hostnames.append(hostname)

    workers = [f"tcp://{hostname}:{monarch_port}" for hostname in hostnames]
    print(f"Worker addresses: {workers}")
    return attach_to_workers(ca="trust_all_connections", workers=workers)  # type: ignore[arg-type]


def _register_node_in_store(store: TCPStore, node_rank: int) -> None:
    hostname = socket.gethostname()
    store.set(f"node_{node_rank}_hostname", hostname)


def _run_client_script_multinode(
    args: Namespace,
    store: TCPStore,
    nnodes: int,
    nproc_per_node: int,
) -> None:
    enable_transport("tcp")

    hm = _setup_multinode_master(store, nnodes, args.monarch_port)
    _run_client_script(hm, args, nproc_per_node)


def _run_client_script(hm: HostMesh, args: Namespace, nproc_per_node: int) -> None:
    pm = hm.spawn_procs({"gpus": nproc_per_node})
    trainers = pm.spawn(
        "spmd_actor",
        SPMDActor,
        master_addr=args.master_addr,
        master_port=args.master_port,
    )

    if args.module:
        script_args = ["-m", args.training_script] + args.training_script_args
    else:
        script_args = [args.training_script] + args.training_script_args

    trainers.main.call(script_args).get()
    pm.stop().get()


def main(args=None) -> None:
    args = _parse_args(args)

    if args.standalone:
        args.rdzv_backend = "static"
        args.rdzv_endpoint = "localhost:0"
        args.rdzv_id = str(uuid.uuid4())
        print(
            "\n**************************************\n"
            "Rendezvous info:\n"
            f"  --rdzv-backend={args.rdzv_backend}\n"
            f"  --rdzv-endpoint={args.rdzv_endpoint}\n"
            f"  --rdzv-id={args.rdzv_id}\n"
            "**************************************\n"
        )

    if args.rdzv_backend not in ("static", "c10d"):
        warnings.warn(
            f"Rendezvous backend '{args.rdzv_backend}' is not supported. "
            f"This implementation only supports static TCPStore-based rendezvous. "
            f"Proceeding with static TCPStore backend.",
            UserWarning,
            stacklevel=2,
        )

    if args.no_python and args.module:
        raise ValueError(
            "Don't use both the '--no-python' flag and the '--module' flag at the same time."
        )

    if args.no_python:
        raise ValueError(
            "The '--no-python' flag is not supported. "
            "This implementation runs scripts using runpy in the same interpreter."
        )

    if args.nnodes < 1:
        raise ValueError(f"nnodes must be >= 1, got {args.nnodes}")

    nnodes = args.nnodes
    nproc_per_node = _determine_local_world_size(args.nproc_per_node)
    rdzv_endpoint = _get_rdzv_endpoint(args)

    print("Launching with configuration:")
    print(f"  - nnodes: {nnodes}")
    print(f"  - nproc_per_node: {nproc_per_node}")
    print(f"  - rdzv_backend: {args.rdzv_backend} (using static TCPStore)")
    print(f"  - rdzv_endpoint: {rdzv_endpoint}")
    print(f"  - training_script: {args.training_script}")
    print(f"  - training_script_args: {args.training_script_args}")

    if nnodes == 1:
        _run_client_script(this_host(), args, nproc_per_node)
        return

    world_size = nnodes * nproc_per_node
    _start_worker_loop_background(args.monarch_port)

    store, is_master = _create_tcpstore(rdzv_endpoint, nnodes)
    host, port = parse_rendezvous_endpoint(rdzv_endpoint, default_port=29400)

    print("\nMulti-node training:")
    print(f"  - This host is {'MASTER' if is_master else 'WORKER'}")
    print(f"  - Rendezvous server: {host}:{port}")
    print(f"  - World size: {world_size} ({nnodes} nodes × {nproc_per_node} procs)")

    _register_node_in_store(store, args.node_rank)

    if is_master:
        _run_client_script_multinode(args, store, nnodes, nproc_per_node)


if __name__ == "__main__":
    main()
