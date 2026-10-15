# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Compare fuse vs 2-stage (canonical) packing of the RECEIVER's own slice (recv_spec[0]) for decomposed
tensors. The assemble fills own canonically (setitem_pairs) but the consume reads it via fuse; if fuse and
canonical disagree on the own/full_spec layout, that mix corrupts. Natural should agree; consolidated may not.
"""

import os

import torch
from analyze_consolidate import build_all_specs
from loadspec_replay import group_records, load_records, rebuild
from wbridge.backend.router import WeightRouter
from wbridge.utils.data import batched_copy

recs = load_records(os.environ["SPECS"])
all_specs, ws, dts = build_all_specs(recs)
senders, engines = group_records(recs)
eid0 = next(iter(engines))
rec0 = engines[eid0][sorted(engines[eid0])[0]]
_src, dtype_spec, load_spec, wksd = rebuild(rec0, device="cuda")
for name, t in wksd.items():
    flat = t.reshape(-1)
    flat.copy_(
        torch.arange(flat.numel(), device=t.device, dtype=torch.float32).to(t.dtype)
    )

TENSORS = [
    "model.layers.0.self_attn.q_norm.weight",
    "model.layers.0.mlp.gate.weight",
    "model.layers.0.input_layernorm.weight",
    "model.layers.0.self_attn.q_proj.weight",
]


def cmp(cons):
    if cons:
        os.environ["WBRIDGE_DEDUP_PAIR_BYTES"] = str(20 * 1024 * 1024)
    else:
        os.environ["WBRIDGE_DEDUP_PAIR_BYTES"] = "0"
    r = WeightRouter(rank=0, sender_ws=ws, all_specs=all_specs, dtype_spec=dict(dts))
    print(
        f"\n===== consolidate={cons} : fuse SAVE vs canonical SAVE of own (recv_spec[0]) ====="
    )
    for t in TENSORS:
        if t not in r.recv_specs[0].entries:
            continue
        spec = r.recv_specs[0].subset({t})
        nb = spec.nbytes(dtype_spec)
        wire_fuse = torch.zeros(nb, dtype=torch.uint8, device="cuda")
        batched_copy(
            load_spec.fuse_copy_pairs(
                spec, wire_fuse, wksd, dtype_spec, src_to_dst=False
            )
        )
        # canonical 2-stage: model -> logical buf -> wire
        buf = spec.make_named_buffer(dtype_spec, "cuda")
        batched_copy(load_spec.copy_fromto_pairs(spec, buf, wksd, src_to_dst=False))
        wire_2s = torch.zeros(nb, dtype=torch.uint8, device="cuda")
        _n, pp = spec(buf).pack_into_pairs(spec, wire_2s)
        batched_copy(pp)
        torch.cuda.synchronize()
        eq = torch.equal(wire_fuse, wire_2s)
        ndiff = int((wire_fuse != wire_2s).sum())
        print(
            f"  {t.split('.', 2)[-1]:<28} nb={nb:<8} fuse==canonical: {eq}  bytes_diff={ndiff}"
        )


cmp(0)
cmp(1)
