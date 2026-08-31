# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""test_transpose_copy.py — demonstrate the transpose gap in the transfer path, then (after fix) verify.

Uses the toy builders (qwen_tiny) with the now-transposed rollout down_proj. Runs specgen to get the
trainer/rollout LoadSpecs, then simulates the stage-1 transfer round-trip:
    trainer wksd --(save)--> HF comm buffer --(load)--> fresh rollout wksd
and checks the round-tripped rollout params equal the reference rollout layout. With the current
copy_fromto_params (ignores the w sign) the transposed down_proj must FAIL; after the Triton fix it
must PASS.

Run in-container:  python tests/test_transpose_copy.py
"""

import os
import sys

import torch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "examples"))

from qwen_tiny import (  # noqa: E402
    build_qwen_tiny_hf_checkpoint,
    build_rollout_wksd,
    build_trainer_wksd,
    DEFAULT_QWEN_TINY_CONFIG,
    make_rollout_load_weights,
    make_trainer_load_weights,
)
from utils import make_hf_weights  # noqa: E402
from wbridge.utils.specgen import infer_load_spec  # noqa: E402


def _side(build_wksd, make_lw, cfg, hf_weights, hf_shapes, device, dtype):
    wksd = build_wksd(cfg, device=device, dtype=dtype, tp_rank=0, tp_size=1)
    lw_raw = make_lw(wksd, cfg, device=device, dtype=dtype, tp_rank=0, tp_size=1)

    def load_weights(fetcher):
        lw_raw((name, fn()) for name, fn in fetcher.items())

    load_spec = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, load_weights)
    dtype_spec = {
        hf: max((wksd[wk].dtype for wk in entry), key=lambda d: d.itemsize)
        for hf, entry in load_spec.entries.items()
    }
    return wksd, load_spec, dtype_spec, load_weights


def main(device="cpu"):
    cfg = DEFAULT_QWEN_TINY_CONFIG
    dtype = torch.float32
    hf_cpu = build_qwen_tiny_hf_checkpoint(cfg, dtype=dtype, seed=1, device=device)
    hf_weights, hf_shapes = make_hf_weights(hf_cpu)

    tr_wksd, tr_spec, tr_dt, tr_lw = _side(
        build_trainer_wksd,
        make_trainer_load_weights,
        cfg,
        hf_weights,
        hf_shapes,
        device,
        dtype,
    )
    ro_wksd, ro_spec, ro_dt, ro_lw = _side(
        build_rollout_wksd,
        make_rollout_load_weights,
        cfg,
        hf_weights,
        hf_shapes,
        device,
        dtype,
    )

    # reference correct layouts: load HF into each side directly
    tr_lw(hf_weights)
    ro_lw(hf_weights)

    # report whether the rollout LoadSpec carries a transpose (negative w) for down_proj
    neg = {}
    for sname, shards in ro_spec.src_shard_spec:
        if any(w < 0 for shard in shards for (_, _, w) in shard):
            neg[sname] = True
    print(f"rollout LoadSpec transposed HF names: {sorted(neg)}")

    # ---- stage-1 round-trip: trainer wksd -> HF comm -> fresh rollout wksd ----
    tr_src = tr_spec.src_shard_spec
    ro_src = ro_spec.src_shard_spec
    comm = tr_src.make_named_buffer(tr_dt, device)  # per-HF-name 1D buffers (HF layout)
    tr_spec.copy_fromto_params(
        tr_src, comm, tr_wksd, src_to_dst=False
    )  # comm <- trainer worker
    ro_new = build_rollout_wksd(cfg, device=device, dtype=dtype, tp_rank=0, tp_size=1)
    for t in ro_new.values():
        t.zero_()
    ro_spec.copy_fromto_params(
        ro_src, comm, ro_new, src_to_dst=True
    )  # rollout worker <- comm

    ok = True
    for name in ro_wksd:
        a, b = ro_new[name], ro_wksd[name]
        match = a.shape == b.shape and torch.allclose(a, b)
        if not match:
            ok = False
            print(
                f"  MISMATCH {name}: new{tuple(a.shape)} vs ref{tuple(b.shape)} allclose={a.shape == b.shape and torch.allclose(a, b)}"
            )
        else:
            print(f"  ok {name} {tuple(a.shape)}")
    print("ROUND-TRIP:", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    dev = sys.argv[1] if len(sys.argv) > 1 else "cpu"
    try:
        ok = main(dev)
    except Exception as e:
        import traceback

        traceback.print_exc()
        print("ROUND-TRIP: FAIL (exception)")
        ok = False
    sys.exit(0 if ok else 1)
