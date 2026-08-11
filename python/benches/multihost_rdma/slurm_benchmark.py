#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
SLURM wrapper around the multi-host RDMA benchmark.

The benchmark itself lives in ``benchmark_common`` and knows nothing about
SLURM. This file only builds the ``SlurmJob`` it runs on and adds the SLURM
flags.

Two subcommands:

- ``run``: this process allocates the job and drives the actors, exiting
  non-zero if a direction fails.
- ``run-batch``: submit the benchmark to run inside its own allocation and
  return immediately. Submit from a filesystem shared with the head node — the
  job state is cached at the CWD-relative ``.monarch/job_state.pkl``, which the
  in-allocation client reads back.

Shared flags precede the subcommand:
``slurm_benchmark.py --partition p --cross-host run --teardown-policy on_failure``.
"""

from __future__ import annotations

import argparse

from benchmark_common import add_benchmark_args, BenchConfig, config_from_args, run
from monarch.job import SlurmJob


MESH_NAME = "workers"


def make_slurm_job(cfg: BenchConfig, args: argparse.Namespace) -> SlurmJob:
    """Build the SLURM job the benchmark runs on."""
    return SlurmJob(
        meshes={MESH_NAME: cfg.job_hosts},
        job_name="monarch_rdma_bench",
        partition=args.partition,
        gpus_per_node=args.gpus_per_node,
        cpus_per_task=args.cpus_per_task,
        mem=args.mem,
        qos=args.qos,
        exclusive=args.exclusive,
        slurm_args=tuple(args.slurm_arg),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Multi-host RDMA Performance Benchmark (SLURM)"
    )
    add_benchmark_args(parser)
    parser.add_argument(
        "--partition",
        default=None,
        help="SLURM partition to submit to (default: the cluster's default)",
    )
    parser.add_argument(
        "--gpus-per-node",
        type=int,
        default=1,
        help="GPUs to request per node (default: 1)",
    )
    parser.add_argument(
        "--cpus-per-task",
        type=int,
        default=None,
        help="CPUs per task (default: the cluster's default)",
    )
    parser.add_argument(
        "--mem",
        default=None,
        help="Memory per node, e.g. '64G' (default: the cluster's default)",
    )
    parser.add_argument(
        "--qos",
        default=None,
        help="SLURM quality-of-service to request (default: the cluster's default)",
    )
    parser.add_argument(
        "--exclusive",
        action="store_true",
        default=False,
        help=(
            "Request exclusive node access. Off by default; note that sharing a "
            "node while --partition and --gpus-per-node are set makes SlurmJob "
            "call share_node(), which raises unless the clusterscope package is "
            "installed."
        ),
    )
    parser.add_argument(
        "--slurm-arg",
        action="append",
        default=[],
        metavar="ARG",
        help=(
            "Extra sbatch directive, repeatable, e.g. --slurm-arg=--time=01:00:00 "
            "--slurm-arg=--account=my_account. Each value starting with '-' "
            "becomes a raw #SBATCH line."
        ),
    )

    args = parser.parse_args()
    cfg = config_from_args(args)

    banner = [
        f"Mode: {'local sender -> SLURM receivers' if cfg.local_sender else 'all SLURM'}",
        f"Partition: {args.partition or 'default'}",
        f"GPUs per node: {args.gpus_per_node}",
    ]

    exit_code = run(
        cfg,
        make_job=lambda c: make_slurm_job(c, args),
        mesh_name=MESH_NAME,
        banner=banner,
    )

    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
