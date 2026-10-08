# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Shared WeightBridge frontend adapters.

:class:`BaseAdapter` runs specgen (infer + verify) in :meth:`__init__` and stores the
resulting :class:`~wbridge.utils.data.LoadSpec`.  Framework-specific subclasses only build an
:class:`AdapterContext` and forward it.

:class:`SenderAdapter` wraps a :class:`~wbridge.backend.sender.WeightSender`; :class:`ReceiverAdapter`
wraps a :class:`~wbridge.backend.receiver.WeightReceiver` and applies each round's partial buffer into
``wksd`` through the receiver's ``load_weights`` callback when the scheduler calls
:meth:`ReceiverAdapter.poll_requests`.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

import torch
from wbridge.backend.receiver import WeightReceiver
from wbridge.backend.sender import SenderArgs, WeightSender
from wbridge.utils.data import (
    LoadSpec,
    logical_tensor_cap_bytes,
    ShardSpec,
    split_large_load_spec_sources,
)
from wbridge.utils.specgen import (
    HFWeightFetcher,
    infer_load_spec,
    LoadWeightsFn,
    verify_load_spec,
    WksdFactory,
)

logger = logging.getLogger(__name__)


@dataclass
class AdapterContext:
    """Framework-specific Metadata Plane inputs every adapter needs to build / verify a LoadSpec.

    Attributes:
        hf_weights: Dict mapping HF tensor names to zero-arg callables that return the
            tensor.  Each factory may be called multiple times (for probing, restore, verify).
        hf_shapes: Dict mapping HF tensor names to their shapes.
        wksd_factory: Zero-arg callable returning a fresh GPU worker state dict snapshot.
            Called once per probe chunk so that specgen reads current parameter pointers
            (needed when ``load_weights`` replaces parameters, e.g. SGLang MoE expert fusion).
        load_weights: Framework callable that accepts an :data:`~wbridge.utils.specgen.HFWeightFetcher`
            and writes into ``wksd``.  Used as a probe by
            :func:`~wbridge.utils.specgen.infer_load_spec`.
        rank: Adapter rank.
    """

    hf_weights: HFWeightFetcher
    hf_shapes: dict[str, tuple[int, ...]]
    wksd_factory: WksdFactory
    load_weights: LoadWeightsFn
    rank: int


def _dtype_spec_from_load_spec(
    load_spec: LoadSpec, wksd: dict[str, torch.Tensor]
) -> dict[str, torch.dtype]:
    """Per-HF-name dtypes for :class:`~wbridge.backend.receiver.WeightReceiver`.

    For a source name mapped to multiple destinations, pick the widest dtype so a single buffer
    can safely hold every destination's view.
    """
    return {
        hf_name: max(
            (wksd[wk_name].dtype for wk_name in entry), key=lambda d: d.itemsize
        )
        for hf_name, entry in load_spec.entries.items()
    }


def _dump_loadspec_if_requested(
    role: str, rank: int, engine_id, adapter: "BaseAdapter", topo: dict
) -> None:
    """Diagnostic: when ``WBRIDGE_DUMP_LOADSPEC=<dir>`` is set, pickle this rank's inferred metadata
    (LoadSpec + dtype/src-shard specs + worker tensor shapes/dtypes + topology) so a frameworkless
    harness can rebuild and replay the exact transfer without Megatron/SGLang/specgen. Never raises
    (diagnostics must not break a real run)."""
    import os

    d = os.environ.get("WBRIDGE_DUMP_LOADSPEC")
    if not d:
        return
    try:
        import pickle

        os.makedirs(d, exist_ok=True)
        rec = {
            "role": role,
            "rank": rank,
            "engine_id": engine_id,
            "load_spec": adapter.load_spec.entries,
            "dtype_spec": adapter.dtype_spec,
            "src_shard_spec": adapter.src_shard_spec.entries,
            "wksd_meta": {
                n: (tuple(t.shape), t.dtype, tuple(t.stride()))
                for n, t in adapter.wksd.items()
            },
            **topo,
        }
        path = os.path.join(d, f"loadspec_{role}_r{rank}_pid{os.getpid()}.pkl")
        with open(path, "wb") as f:
            pickle.dump(rec, f)
        logger.info(
            "[wbridge] WBRIDGE_DUMP_LOADSPEC: role=%s rank=%s engine=%s -> %s (%d wksd tensors)",
            role,
            rank,
            engine_id,
            path,
            len(rec["wksd_meta"]),
        )
    except Exception as e:  # noqa: BLE001 — diagnostics must never break a run
        logger.warning("[wbridge] WBRIDGE_DUMP_LOADSPEC dump failed: %s", e)


class BaseAdapter:
    """Shared :class:`~wbridge.utils.data.LoadSpec` lifecycle.

    Runs specgen from :meth:`__init__`: infer a LoadSpec with
    :func:`~wbridge.utils.specgen.infer_load_spec` and verify it.
    The resulting :class:`~wbridge.utils.data.LoadSpec`, per-HF-name dtypes (``dtype_spec``), and
    HF wire layout (``src_shard_spec``) are stored on ``self``.
    """

    def __init__(self, ctx: AdapterContext) -> None:
        self.ctx = ctx

        self.load_spec: LoadSpec
        self.dtype_spec: dict[str, torch.dtype]
        self.src_shard_spec: ShardSpec

        self.load_spec = infer_load_spec(
            ctx.hf_weights,
            ctx.hf_shapes,
            ctx.wksd_factory,
            ctx.load_weights,
        )
        verify_load_spec(ctx.hf_weights, ctx.wksd_factory, self.load_spec)

        self.dtype_spec = _dtype_spec_from_load_spec(self.load_spec, ctx.wksd_factory())
        logical_cap = logical_tensor_cap_bytes()
        self.load_spec, self.dtype_spec, split_report = split_large_load_spec_sources(
            self.load_spec,
            self.dtype_spec,
            logical_cap,
        )
        if split_report:
            logger.info(
                "[wbridge] logical tensor split: %d physical sources -> %d local logical pieces, "
                "cap=%.1f MiB",
                len(split_report),
                sum(len(item["logical_names"]) for item in split_report),
                logical_cap / 1024**2,
            )
        self.src_shard_spec = self.load_spec.src_shard_spec

        # Snapshot wksd AFTER specgen (which may call lw and replace model
        # parameters).  This is the live reference used for ongoing
        # copy_fromto_params during weight transfer.
        self.wksd = ctx.wksd_factory()


class SenderAdapter(BaseAdapter):
    """Trainer Worker adapter: owns a :class:`~wbridge.backend.sender.WeightSender`.

    The :class:`~wbridge.backend.sender.WeightSender` is constructed in :meth:`__init__` from the
    transport args. Call :meth:`connect` once to join the sender process group, then
    :meth:`send_weights` per weight update.
    """

    def __init__(self, ctx: AdapterContext, args: SenderArgs) -> None:
        super().__init__(ctx)
        self.sender = WeightSender(
            args,
            ctx.rank,
            self.src_shard_spec,
            self.load_spec,
            self.wksd,
        )
        _dump_loadspec_if_requested(
            "sender",
            ctx.rank,
            None,
            self,
            {"world_size": args.world_size, "sender_staging": args.sender_staging},
        )

    def connect(self) -> None:
        self.sender.connect()

    def send_weights(self) -> "torch.cuda.Event | None":
        """Pack (+ CPU offload under sender-staging) and hand the RDMA to the sender's Stage-2 thread.

        Returns the CUDA event marking pack+offload completion (model weights safe to overwrite). The
        caller waits it (standard) or defers the wait until just before it overwrites the weights.
        """
        return self.sender.send()

    def wait_send_complete(self) -> None:
        """Block until the last :meth:`send_weights` has been delivered+consumed by all receivers.
        The caller uses this in debugging mode (equality check) to guarantee delivery before reading."""
        self.sender.wait_send_complete()

    def flush_profile_outputs(self) -> None:
        """Release the latest epoch's profiling output after the caller records trainer ``block_end``."""
        self.sender.flush_profile_outputs()


class ReceiverAdapter(BaseAdapter):
    """Rollout Worker adapter: owns a :class:`~wbridge.backend.receiver.WeightReceiver`.

    The receiver is created eagerly in :meth:`__init__` so it can handshake with the controller
    before any transfer begins.
    """

    def __init__(
        self,
        ctx: AdapterContext,
        controller_ipc_name: str,
        num_workers: int,
        receiver_staging: bool = False,
        control_hub_name: str | None = None,
    ) -> None:
        super().__init__(ctx)
        self.controller_ipc_name = controller_ipc_name
        self.receiver = WeightReceiver(
            controller_ipc_name,
            ctx.rank,
            self.src_shard_spec,
            self.dtype_spec,
            self.load_spec,
            self.wksd,
            num_workers=num_workers,
            control_hub_name=control_hub_name,
            receiver_staging=receiver_staging,
        )
        _dump_loadspec_if_requested(
            "receiver",
            ctx.rank,
            controller_ipc_name,
            self,
            {"num_workers": num_workers, "receiver_staging": receiver_staging},
        )

    def poll_requests(
        self, before_receive: Callable[[int], None] | None = None
    ) -> bool:
        """Poll and service one Rollout Worker control request on the scheduler thread.

        ``before_receive(epoch)`` runs only for a real weight update, after the readiness decision and
        before model weights are mutated. Returns ``True`` only when an update was received and loaded;
        empty polls, staging kicks, and connection setup return ``False``.
        """
        return self.receiver.poll_requests(before_receive=before_receive)

    def flush_profile_outputs(self) -> None:
        """Release the latest epoch's profiling output after the caller records rollout ``block_end``."""
        self.receiver.flush_profile_outputs()
