# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Read per-rank LoadSpecs captured by ``WBRIDGE_DUMP_LOADSPEC`` and rebuild the objects needed to
replay the exact transfer frameworklessly (no Megatron/SGLang/specgen).

Each pkl (written by ``wbridge.frontend.adapters._dump_loadspec_if_requested``) holds one rank's
``load_spec``/``dtype_spec``/``src_shard_spec`` entries + ``wksd_meta`` (name -> shape, dtype) +
topology. ``rebuild()`` reconstructs the specs and allocates a **synthetic** worker state dict of the
recorded shapes/dtypes — values are irrelevant to WTT (specgen + the equality check are skipped;
correctness stays with the real run).
"""

from __future__ import annotations

import glob
import hashlib
import os
import pickle
import zlib

import torch
from wbridge.utils.data import (
    LoadSpec,
    logical_tensor_cap_bytes,
    ShardSpec,
    split_large_load_spec_sources,
)


def load_records(spec_dir: str) -> list[dict]:
    """Load every ``loadspec_*.pkl`` in *spec_dir* (one per captured rank)."""
    recs = []
    for fp in sorted(glob.glob(os.path.join(spec_dir, "loadspec_*.pkl"))):
        with open(fp, "rb") as f:
            r = pickle.load(f)
        r = translate_record_logical_tensors(r)
        r["_path"] = fp
        recs.append(r)
    if not recs:
        raise FileNotFoundError(f"no loadspec_*.pkl under {spec_dir}")
    return recs


def translate_record_logical_tensors(rec: dict) -> dict:
    """Apply the configured logical-source cap to captured metadata without allocating tensors.

    This keeps placement logic, offline memory inspection, and replay actors on the exact same translated
    LoadSpec. New captures are already translated and pass through idempotently.
    """
    load_spec = LoadSpec(rec["load_spec"])
    dtype_spec = dict(rec["dtype_spec"])
    load_spec, dtype_spec, split_report = split_large_load_spec_sources(
        load_spec,
        dtype_spec,
        logical_tensor_cap_bytes(),
    )
    if not split_report:
        return rec
    translated = dict(rec)
    translated["load_spec"] = load_spec.entries
    translated["dtype_spec"] = dtype_spec
    translated["src_shard_spec"] = load_spec.src_shard_spec.entries
    translated["logical_tensor_splits"] = split_report
    return translated


def group_records(
    recs: list[dict],
) -> tuple[dict[int, dict], dict[object, dict[int, dict]]]:
    """Return (senders, engines): senders={rank: rec}; engines={engine_id: {rank: rec}}.

    De-duplicates by (role, engine_id, rank) — a real run may launch a rank's process more than once
    (Ray retries); the last write for a (role, engine, rank) wins.
    """
    senders: dict[int, dict] = {}
    engines: dict[object, dict[int, dict]] = {}
    for r in sorted(recs, key=lambda x: x["_path"]):
        if r["role"] == "sender":
            senders[r["rank"]] = r
        else:
            engines.setdefault(r["engine_id"], {})[r["rank"]] = r
    return senders, engines


def _is_contiguous(shape, stride) -> bool:
    exp = 1
    for s, st in zip(reversed(shape), reversed(stride)):
        if s != 1 and st != exp:
            return False
        exp *= s
    return True


def _alloc(shape, dtype, stride, device):
    """Synthetic worker tensor of the recorded shape/dtype — matching the recorded strides (so a real
    non-contiguous SGLang/Megatron param reproduces the same copy path), else contiguous."""
    shape = tuple(shape)
    if not stride or _is_contiguous(shape, tuple(stride)):
        return torch.empty(shape, dtype=dtype, device=device)
    need = 1 + sum((s - 1) * st for s, st in zip(shape, stride)) if shape else 1
    base = torch.empty(int(need), dtype=dtype, device=device)
    return torch.as_strided(base, shape, tuple(stride))


def _fill_deterministic(t: torch.Tensor, salt: str, name: str, rank: int) -> None:
    """Fill *t* with values fixed by ``(salt, name, rank)`` — same content on every run, on any transport.

    This is what turns the perf replay into a correctness check: the sender's shards become known,
    reproducible bytes, so a receiver's post-consume hash is a function of the transport delivering them
    correctly and of nothing else. ``torch.empty`` would make the hash depend on whatever was in HBM.

    The generator is per-tensor and seeded from the *name*, not drawn from a single stream, so the content
    does not depend on dict iteration order or on which ranks happen to exist.
    """
    seed = zlib.crc32(f"{salt}|{name}|{rank}".encode()) & 0x7FFFFFFF
    g = torch.Generator(device=t.device).manual_seed(seed)
    if t.dtype.is_floating_point:
        # normal_ works elementwise, so it fills a non-contiguous as_strided view correctly too.
        t.normal_(mean=0.0, std=1.0, generator=g)
    else:
        t.random_(0, 127, generator=g)


def rebuild(
    rec: dict, device: str = "cuda", seed_salt: str = ""
) -> tuple[ShardSpec, dict, LoadSpec, dict]:
    """Rebuild (src_shard_spec, dtype_spec, load_spec, synthetic wksd) from a captured record.

    wksd_meta entries are (shape, dtype) [legacy] or (shape, dtype, stride) [current].

    *seed_salt* non-empty makes the wksd contents deterministic instead of uninitialized (see
    :func:`_fill_deterministic`); pass different salts for the sender and receiver roles so a region the
    transport never wrote is still visible as "not the sender's bytes".
    """
    load_spec = LoadSpec(rec["load_spec"])
    dtype_spec = dict(rec["dtype_spec"])
    load_spec, dtype_spec, split_report = split_large_load_spec_sources(
        load_spec,
        dtype_spec,
        logical_tensor_cap_bytes(),
    )
    # New captures already store logical names; old captures are translated here. Recompute from LoadSpec
    # whenever translation occurred so the synthetic replay sees the same virtual checkpoint layout as a
    # live adapter. Otherwise retain the recorded source spec byte-for-byte for legacy compatibility.
    src_shard_spec = (
        load_spec.src_shard_spec if split_report else ShardSpec(rec["src_shard_spec"])
    )
    wksd = {}
    for name, meta in rec["wksd_meta"].items():
        shape, dtype = meta[0], meta[1]
        stride = meta[2] if len(meta) > 2 else None
        t = _alloc(shape, dtype, stride, device)
        if seed_salt:
            _fill_deterministic(t, seed_salt, name, int(rec["rank"]))
        wksd[name] = t
    return src_shard_spec, dtype_spec, load_spec, wksd


def wksd_digest(wksd: dict) -> str:
    """Order-independent-of-dict, content-dependent digest of a worker state dict.

    Used to compare a receiver's post-consume weights across two transports. Names are sorted and folded
    in alongside the bytes, so a transport that delivered the right bytes to the wrong tensor still
    changes the digest. Hashing on the CPU (rather than a GPU reduction) keeps it exact and trivially
    comparable across processes; it runs once after the timed loop, never inside it.
    """
    h = hashlib.blake2b(digest_size=16)
    for name in sorted(wksd):
        t = wksd[name]
        h.update(name.encode())
        h.update(f"{tuple(t.shape)}|{t.dtype}".encode())
        h.update(t.detach().contiguous().view(torch.uint8).cpu().numpy().tobytes())
    return h.hexdigest()


def summary(recs: list[dict]) -> str:
    senders, engines = group_records(recs)
    lines = [
        f"senders={len(senders)} (ranks {sorted(senders)})",
        f"engines={len(engines)}",
    ]
    for eid, ranks in engines.items():
        r0 = next(iter(ranks.values()))
        lines.append(
            f"  engine {eid}: ranks {sorted(ranks)} num_workers={r0.get('num_workers')} "
            f"({len(r0['wksd_meta'])} wksd tensors/rank)"
        )
    return "\n".join(lines)
