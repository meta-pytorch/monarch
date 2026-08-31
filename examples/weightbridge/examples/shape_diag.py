# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Print natural-dedup vs consolidated recv_specs shard shapes for decomposed tensors, to see what
'correct shape' the fuse kernel expects. The natural shapes map cleanly; the consolidated ones don't."""

import os

import torch  # noqa: F401
from analyze_consolidate import build_all_specs
from loadspec_replay import load_records
from wbridge.backend.router import WeightRouter

recs = load_records(os.environ["SPECS"])
all_specs, ws, dts = build_all_specs(recs)
TENSORS = [
    "model.layers.0.self_attn.q_norm.weight",
    "model.layers.0.mlp.gate.weight",
    "model.layers.0.input_layernorm.weight",
    "model.layers.0.self_attn.q_proj.weight",
]


def show(cons):
    if cons:
        os.environ["WBRIDGE_DEDUP_PAIR_BYTES"] = str(20 * 1024 * 1024)
    else:
        os.environ["WBRIDGE_DEDUP_PAIR_BYTES"] = "0"
    r = WeightRouter(rank=0, sender_ws=ws, all_specs=all_specs, dtype_spec=dict(dts))
    print(f"\n===== consolidate={cons} =====")
    for t in TENSORS:
        orig = (
            all_specs[ws][t] if t in all_specs[ws].entries else None
        )  # receiver 0 original full shard
        rs0 = r.recv_specs[0][t] if t in r.recv_specs[0].entries else None
        cls = r.recv_tensor_classes().get(t)
        print(f"{t.split('.', 2)[-1]:<28} class0={cls}")
        print(f"    orig recv0 : {[list(s) for s in orig] if orig else None}")
        print(f"    dedup recv0: {[list(s) for s in rs0] if rs0 else None}")


show(0)
show(1)

# Load receiver rank 0's load_spec mappings for the decomposed tensors — reveals why the fuse handles the
# narrow natural slice but mis-maps the wide consolidated one (multiple mappings / transpose per piece).
from loadspec_replay import group_records, rebuild  # noqa: E402

_senders, engines = group_records(recs)
eid0 = next(iter(engines))
rec0 = engines[eid0][
    sorted(engines[eid0])[0]
]  # engine0 local rank 0 = global receiver 0
_src, _dt, load_spec, _wksd = rebuild(rec0, device="cuda")
print(
    "\n===== receiver0 load_spec.entries (mappings: HF name -> {model: [(s_shard, d_shard),...]}) ====="
)
for t in TENSORS:
    ent = load_spec.entries.get(t)
    if ent is None:
        print(f"{t.split('.', 2)[-1]:<28} (not in load_spec)")
        continue
    for dname, mappings in ent.items():
        print(
            f"{t.split('.', 2)[-1]:<28} -> {dname.split('.', 2)[-1]}: {len(mappings)} mapping(s)"
        )
        for s_shard, d_shard in mappings[:6]:
            print(
                f"        s={[list(x) for x in s_shard]}  d={[list(x) for x in d_shard]}"
            )
