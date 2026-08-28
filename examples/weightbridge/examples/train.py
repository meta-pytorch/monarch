# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Minimal WeightBridge example: Ray node pinning + weight transfer.

Uses a single-layer Qwen2-style HF checkpoint built on each worker via ``build_checkpoint``.
Trainer Workers hold TP shards
of HF names in ``wksd``; Rollout Workers hold merged weights (``qkv_proj``, ``gate_up_proj``, ...).
:class:`~wbridge.frontend.adapters.SenderAdapter` / :class:`~wbridge.frontend.adapters.ReceiverAdapter`

Weight transfer uses the selected WeightBridge data-plane backend. Mooncake provides the TCP toy transport
and EFA RDMA; Monarch provides a separate libibverbs RDMA backend. With ``--protocol auto``,
``--network-provider`` selects the Mooncake transport. ``wksd`` tensors stay on GPU.

Usage::

    ray start --head          # node A (GPUs for trainer + rollout workers)
    ray start --address=...   # node B
    python examples/train.py
"""

from __future__ import annotations

import argparse
import logging
import os
import tempfile
from functools import partial
from pathlib import Path

import torch
from qwen_tiny import build_qwen_tiny_hf_checkpoint, DEFAULT_QWEN_TINY_CONFIG
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from utils import apply_network_env, get_ray_nodes
from worker_bodies import EngineArgs
from workers import RayOrchestrator

logger = logging.getLogger("example")

DTYPE = torch.float32


def _fmt(stat: dict) -> str:
    nvl = stat.get("nvlink_active_links")
    link = "nvlink=?" if nvl is None else f"nvlink_links={nvl}"
    return (
        f"  {stat['role']:8s} rank {stat['rank']:2d} gpu {stat.get('pci_bus_id') or '?':12s} {link:16s} "
        f"wire: ipc={stat['wire_ipc_bytes']:>12,d} B  rdma={stat['wire_rdma_bytes']:>12,d} B   "
        f"agh: ipc={stat['agh_ipc_bytes']:>10,d} B  rdma={stat['agh_rdma_bytes']:>10,d} B   "
        f"ipc_peers={stat['ipc_peers']} rdma_peers={stat['rdma_peers']}"
    )


def check_transports(
    stats: list[dict], colocate: bool, expect: str = "auto", transport: str = "Mooncake"
) -> None:
    """Assert the data plane used the transport the placement implies, and print the evidence.

    This is the point of ``--colocate``: prove that co-located trainer/rollout traffic really does bypass
    the network RDMA backend. The byte counters are exact — ``wire_rdma_bytes`` is incremented at the
    ``engine.write_async`` call site and ``wire_ipc_bytes`` at the CUDA-IPC ``copy_`` — so
    ``wire_rdma_bytes == 0`` with ``wire_ipc_bytes > 0`` means every weight byte moved GPU-to-GPU without
    entering the transfer engine. (Flags are excluded from both counters: they are 8 bytes each and use
    the selected control path.) The NVLink column is what the CUDA-IPC copies ran over; it is
    reported rather than asserted because NVML reports NVSwitch endpoints on HGX-class nodes.

    *transport* only names the engine in the output — the counters are engine-agnostic, so this reads
    "Monarch" under ``--protocol monarch`` instead of claiming bytes went through Mooncake.

    *expect* is normally ``"auto"`` (co-located ⇒ IPC, split ⇒ RDMA). Force it to ``"rdma"`` to check
    the A/B control: same co-located placement, ``WBRIDGE_SAME_NODE_IPC=0``, so the identical run must
    fall back through the transfer engine.
    """
    want_ipc = colocate if expect == "auto" else (expect == "ipc")
    print(
        f"\ntransport breakdown (bulk weight bytes, flags excluded) — expecting "
        f"{'CUDA-IPC' if want_ipc else transport}:"
    )
    for s in sorted(stats, key=lambda s: (s["role"], s["rank"])):
        print(_fmt(s))

    bad = []
    for s in stats:
        tag = f"{s['role']} rank {s['rank']}"
        if want_ipc:
            if s["wire_rdma_bytes"]:
                bad.append(
                    f"{tag}: {s['wire_rdma_bytes']} trainer<->rollout bytes went through {transport} "
                    f"(expected 0 — co-located peers should use the CUDA-IPC bypass)"
                )
            if not s["wire_ipc_bytes"]:
                bad.append(f"{tag}: no CUDA-IPC bytes recorded (bypass never engaged)")
            if s["agh_rdma_bytes"]:
                bad.append(
                    f"{tag}: {s['agh_rdma_bytes']} dedup-exchange bytes went through {transport}"
                )
        elif s["wire_ipc_bytes"]:
            # Negative control: nothing may take the IPC path when it is not expected — either the
            # engines are on different nodes, or the bypass was disabled.
            bad.append(
                f"{tag}: {s['wire_ipc_bytes']} bytes took the CUDA-IPC path unexpectedly"
            )

    total_ipc = sum(s["wire_ipc_bytes"] + s["agh_ipc_bytes"] for s in stats)
    total_rdma = sum(s["wire_rdma_bytes"] + s["agh_rdma_bytes"] for s in stats)
    if not want_ipc and not total_rdma:
        # Per-rank byte counts depend on the shard overlap, so only the aggregate is meaningful here.
        bad.append(f"recorded no {transport} bytes at all")
    if bad:
        raise AssertionError("transport check failed:\n  " + "\n  ".join(bad))

    if want_ipc:
        no_nvml = [s for s in stats if not s.get("nvlink_active_links")]
        if no_nvml:
            print(
                "  NOTE: NVML reported no active NVLink on "
                f"{len(no_nvml)}/{len(stats)} GPU(s) — the IPC copies ran over PCIe P2P, not NVLink "
                "(or pynvml is unavailable in this container)."
            )
        print(
            f"\nOK: co-located run moved {total_ipc:,d} B over CUDA-IPC and {total_rdma:,d} B "
            f"through {transport} — the transfer engine carried no weight bytes."
        )
    else:
        where = "co-located (bypass disabled)" if colocate else "2-node"
        print(
            f"\nOK: {where} run moved {total_rdma:,d} B through {transport}, {total_ipc:,d} B over CUDA-IPC."
        )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d - %(message)s",
    )

    parser = argparse.ArgumentParser(description="WeightBridge train/rollout example")
    parser.add_argument("--rollout-ip", default=os.environ.get("WB_ROLLOUT_IP"))
    parser.add_argument("--trainer-ip", default=os.environ.get("WB_TRAINER_IP"))
    parser.add_argument(
        "--colocate",
        action="store_true",
        default=os.environ.get("WB_COLOCATE", "") == "1",
        help="Pin the trainer and rollout engines to the SAME Ray node (needs 4 free GPUs there) and "
        "assert that the weight bytes bypass the RDMA backend for a direct CUDA-IPC copy over NVLink. "
        "Without it the two engines land on different nodes and the same run asserts the opposite.",
    )
    parser.add_argument(
        "--expect-transport",
        choices=("auto", "ipc", "rdma"),
        default=os.environ.get("WB_EXPECT_TRANSPORT", "auto"),
        help="Which transport the bulk weight bytes must use. 'auto' (default) derives it from the "
        "placement. Force 'rdma' for the A/B control: co-located placement with "
        "WBRIDGE_SAME_NODE_IPC=0, where the same run must fall back through the RDMA backend.",
    )
    parser.add_argument(
        "--network-provider",
        choices=("tcp", "efa"),
        default=os.environ.get("WB_NETWORK_PROVIDER", "tcp"),
        help="Process-group network provider. Use efa on AWS EFA clusters; tcp only sets socket IFNAME.",
    )
    parser.add_argument(
        "--orchestrator",
        choices=("monarch", "ray"),
        default=os.environ.get("WB_ORCHESTRATOR", "ray"),
        help="Which actor framework runs the workers. Ray is the portable default; the Monarch wbridge "
        "transport requires Monarch actors (RDMABuffer/RDMAAction only work inside one).",
    )
    parser.add_argument(
        "--monarch-workers",
        default=os.environ.get("WB_MONARCH_WORKERS", ""),
        help="Comma-separated tcp://ip:port Monarch worker-loop addresses (started by the launcher). "
        "Required for --orchestrator monarch; also supplies node placement, so Ray is not needed.",
    )
    parser.add_argument(
        "--protocol",
        choices=("auto", "tcp", "efa", "monarch"),
        default=os.environ.get("WB_PROTOCOL", "auto"),
        help="wbridge RDMA backend. 'auto' follows --network-provider (Mooncake tcp/efa); 'monarch' "
        "selects MonarchEngine, which needs --orchestrator monarch.",
    )
    parser.add_argument(
        "--network-interface", default=os.environ.get("WB_NETWORK_INTERFACE", "")
    )
    parser.add_argument(
        "--rollout-port",
        type=int,
        default=int(os.environ.get("WB_ROLLOUT_PORT", "15000")),
    )
    parser.add_argument(
        "--trainer-pg-port",
        type=int,
        default=int(os.environ.get("WB_TRAINER_PG_PORT", "60010")),
    )
    parser.add_argument(
        "--num-rollout-workers",
        type=int,
        default=int(os.environ.get("WB_NUM_ROLLOUT_WORKERS", "2")),
    )
    parser.add_argument(
        "--num-trainer-workers",
        type=int,
        default=int(os.environ.get("WB_NUM_TRAINER_WORKERS", "2")),
    )
    parser.add_argument(
        "--load-spec-dir",
        default=os.environ.get(
            "WB_LOAD_SPEC_DIR",
            str(Path(tempfile.gettempdir()) / "wbridge_example_qwen_loadspec_v1"),
        ),
    )
    cli = parser.parse_args()
    apply_network_env(cli.network_provider, cli.network_interface)

    protocol = cli.protocol
    if protocol == "auto":
        protocol = "efa" if cli.network_provider == "efa" else "tcp"
    if protocol == "monarch" and cli.orchestrator != "monarch":
        parser.error(
            "--protocol monarch requires --orchestrator monarch "
            "(RDMABuffer/RDMAAction only work inside a Monarch actor)"
        )

    # Node placement. Ray discovers it from the cluster; Monarch takes it from the worker-loop addresses
    # the launcher already knows, so a Monarch run needs no Ray cluster at all.
    worker_addrs: list[str] = [
        w.strip() for w in cli.monarch_workers.split(",") if w.strip()
    ]
    if cli.orchestrator == "monarch":
        if not worker_addrs:
            parser.error(
                "--orchestrator monarch requires --monarch-workers tcp://ip:port,..."
            )
        ips = [a.rsplit(":", 1)[0].removeprefix("tcp://") for a in worker_addrs]
        if cli.colocate:
            host = cli.rollout_ip or cli.trainer_ip or ips[0]
            rollout_ip = trainer_ip = host
        else:
            if len(ips) < 2:
                parser.error(
                    "a 2-node run needs >=2 --monarch-workers (or pass --colocate)"
                )
            trainer_ip = cli.trainer_ip or ips[0]
            rollout_ip = cli.rollout_ip or ips[1]
        rollout_sched = trainer_sched = None
    else:
        rollout_ip, trainer_ip, rollout_node_id, trainer_node_id = get_ray_nodes(
            cli.rollout_ip, cli.trainer_ip, colocate=cli.colocate
        )
        rollout_sched = NodeAffinitySchedulingStrategy(
            node_id=rollout_node_id, soft=False
        )
        trainer_sched = NodeAffinitySchedulingStrategy(
            node_id=trainer_node_id, soft=False
        )

    logger.info(
        "orchestrator %s, protocol %s, rollout node %s, trainer node %s, network provider %s, "
        "interface %s, colocate %s",
        cli.orchestrator,
        protocol,
        rollout_ip,
        trainer_ip,
        cli.network_provider,
        cli.network_interface,
        cli.colocate,
    )
    # Bump dirname if layout / LoadSpec format changes (stale JSON under old dirs still hurts until deleted).

    cfg = DEFAULT_QWEN_TINY_CONFIG
    build_checkpoint = partial(
        build_qwen_tiny_hf_checkpoint, cfg, dtype=DTYPE, seed=42, device="cpu"
    )

    engine_args = EngineArgs(
        rollout_host=rollout_ip,
        rollout_port=cli.rollout_port,
        rollout_scheduling_strategy=rollout_sched,
        num_rollout_workers=cli.num_rollout_workers,
        trainer_host=trainer_ip,
        trainer_pg_port=cli.trainer_pg_port,
        trainer_scheduling_strategy=trainer_sched,
        num_trainer_workers=cli.num_trainer_workers,
        model_config=cfg,
        build_checkpoint=build_checkpoint,
        dtype=DTYPE,
        network_provider=cli.network_provider,
        network_interface=cli.network_interface,
        protocol=protocol,
    )

    if cli.orchestrator == "monarch":
        from workers_monarch import MonarchOrchestrator

        orch = MonarchOrchestrator(engine_args, worker_addrs)
    else:
        orch = RayOrchestrator(engine_args)

    orch.start()
    orch.run_transfer()
    logger.info("Weights received")
    logger.info(orch.verify_all())

    check_transports(
        orch.transport_stats(),
        colocate=cli.colocate,
        expect=cli.expect_transport,
        transport="Monarch" if protocol == "monarch" else "Mooncake",
    )

    orch.shutdown()


if __name__ == "__main__":
    main()
