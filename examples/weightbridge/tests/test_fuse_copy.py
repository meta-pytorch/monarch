# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""test_fuse_copy.py — verify the fused model↔wire copy is byte-identical to the 2-stage path.

The fused path (`LoadSpec.fuse_copy_pairs`, data.py) collapses stage-1 (`copy_fromto_params`,
model↔logical, transpose) + stage-2 (`pack_into`/`__setitem__`, logical↔wire overlap reshard) into a
single model↔wire copy with no intermediate logical buffer. This test builds the REAL toy LoadSpecs via
specgen (so it exercises the transposed rollout down_proj + QKV/gate_up fusion), then asserts, for both
full and partial overlaps, on both trainer and rollout sides:

  * SAVE (model→wire): fused wire bytes == 2-stage `copy_fromto`+`pack_into` wire bytes.
  * LOAD (wire→model): fused model == 2-stage `setitem`+`copy_fromto` model.

Requires CUDA (specgen + the Triton batched copy). Run: ``python tests/test_fuse_copy.py``.
"""

import os
import sys

import pytest
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
from wbridge.utils.data import (  # noqa: E402
    batched_copy,
    FuseUnsupported,
    LoadSpec,
    ShardSpec,
    split_large_load_spec_sources,
)
from wbridge.utils.specgen import infer_load_spec  # noqa: E402


def _side(
    build_wksd,
    make_lw,
    cfg,
    hf_weights,
    hf_shapes,
    device,
    dtype,
    *,
    tp_rank=0,
    tp_size=1,
):
    wksd = build_wksd(
        cfg,
        device=device,
        dtype=dtype,
        tp_rank=tp_rank,
        tp_size=tp_size,
    )
    lw_raw = make_lw(
        wksd,
        cfg,
        device=device,
        dtype=dtype,
        tp_rank=tp_rank,
        tp_size=tp_size,
    )

    def load_weights(fetcher):
        lw_raw((name, fn()) for name, fn in fetcher.items())

    load_spec = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, load_weights)
    dtype_spec = {
        hf: max((wksd[wk].dtype for wk in entry), key=lambda d: d.itemsize)
        for hf, entry in load_spec.entries.items()
    }
    load_weights(hf_weights)  # fill wksd with real (reference) values
    return wksd, load_spec, dtype_spec


def _subregion(spec: ShardSpec) -> ShardSpec:
    """A strict sub-region of *spec*: halve the first splittable axis of each name's first shard."""
    out = {}
    for name, shards in spec:
        sh = list(shards[0])
        for d, (l, r, w) in enumerate(sh):
            if r - l >= 2:
                sh[d] = (l, l + (r - l) // 2, w)
                break
        out[name] = [sh]
    return ShardSpec(out)


def _check(tag, load_spec, wksd, dtype_spec, ospec, device):
    full = load_spec.src_shard_spec

    # ---- SAVE: model -> wire ----
    logical = full.make_named_buffer(dtype_spec, device)
    load_spec.copy_fromto_params(
        full, logical, wksd, src_to_dst=False
    )  # model -> logical
    wire_ref = ospec.make_byte_chunk(dtype_spec, device)
    full(logical).pack_into(ospec, wire_ref)  # logical -> wire (2-stage)

    wire_fused = ospec.make_byte_chunk(dtype_spec, device)
    batched_copy(
        load_spec.fuse_copy_pairs(ospec, wire_fused, wksd, dtype_spec, src_to_dst=False)
    )
    torch.cuda.synchronize()
    assert torch.equal(wire_ref, wire_fused), (
        f"[{tag}] SAVE wire mismatch ({(wire_ref != wire_fused).sum()} bytes)"
    )

    # ---- LOAD: wire -> model (zero everything so non-overlap regions match trivially) ----
    logical2 = full.make_named_buffer(dtype_spec, device)
    for t in logical2.values():
        t.zero_()
    full(logical2)[{0: ospec}] = {0: wire_ref}  # wire -> logical
    model_ref = {k: torch.zeros_like(v) for k, v in wksd.items()}
    load_spec.copy_fromto_params(
        full, logical2, model_ref, src_to_dst=True
    )  # logical -> model (2-stage)

    model_fused = {k: torch.zeros_like(v) for k, v in wksd.items()}
    batched_copy(
        load_spec.fuse_copy_pairs(
            ospec, wire_ref, model_fused, dtype_spec, src_to_dst=True
        )
    )
    torch.cuda.synchronize()
    for name in model_ref:
        assert torch.equal(model_ref[name], model_fused[name]), (
            f"[{tag}] LOAD model mismatch for {name}"
        )
    print(f"  ok [{tag}]  names={len(ospec.entries)}  wire={wire_ref.numel()}B")


def main(device="cuda"):
    assert device == "cuda", "fused copy needs CUDA (specgen + Triton)"
    cfg = DEFAULT_QWEN_TINY_CONFIG
    dtype = torch.float32
    hf_cpu = build_qwen_tiny_hf_checkpoint(cfg, dtype=dtype, seed=1, device=device)
    hf_weights, hf_shapes = make_hf_weights(hf_cpu)

    for tp_rank, tp_size in ((0, 1), (0, 4), (3, 4)):
        sides = {
            "trainer": _side(
                build_trainer_wksd,
                make_trainer_load_weights,
                cfg,
                hf_weights,
                hf_shapes,
                device,
                dtype,
                tp_rank=tp_rank,
                tp_size=tp_size,
            ),
            "rollout": _side(
                build_rollout_wksd,
                make_rollout_load_weights,
                cfg,
                hf_weights,
                hf_shapes,
                device,
                dtype,
                tp_rank=tp_rank,
                tp_size=tp_size,
            ),
        }
        for who, (wksd, load_spec, dtype_spec) in sides.items():
            full = load_spec.src_shard_spec
            tag = f"{who}/tp{tp_size}-rank{tp_rank}"
            _check(f"{tag}/full", load_spec, wksd, dtype_spec, full, device)
            partial = ShardSpec.compute_overlap(full, _subregion(full))
            _check(f"{tag}/partial", load_spec, wksd, dtype_spec, partial, device)
            if tp_size > 1:
                split_spec, split_dtypes, _report = split_large_load_spec_sources(
                    load_spec,
                    dtype_spec,
                    max_bytes=256,
                )
                split_full = split_spec.src_shard_spec
                _check(
                    f"{tag}/logical/full",
                    split_spec,
                    wksd,
                    split_dtypes,
                    split_full,
                    device,
                )
                split_partial = ShardSpec.compute_overlap(
                    split_full,
                    _subregion(split_full),
                )
                _check(
                    f"{tag}/logical/partial",
                    split_spec,
                    wksd,
                    split_dtypes,
                    split_partial,
                    device,
                )
    print("ALL FUSE COPY TESTS PASSED")
    return True


def test_fuse_copy():
    if not torch.cuda.is_available():
        import pytest

        pytest.skip("no CUDA")
    assert main("cuda")


def test_fuse_copy_rejects_copying_parameter_reshape():
    """A cached plan must never retain a raw pointer into a temporary reshape allocation."""
    if not torch.cuda.is_available():
        pytest.skip("fused copy requires CUDA")
    source = [(0, 2, 2), (0, 3, 3)]
    destination = [(0, 2, 2), (0, 3, 3)]
    load_spec = LoadSpec({"weight": {"model.weight": [(source, destination)]}})
    # Shape differs from destination's logical shape and flattening this transpose requires a copy.
    model = {"model.weight": torch.arange(6, device="cuda").reshape(2, 3).t()}
    wire = torch.empty(
        6 * model["model.weight"].element_size(), dtype=torch.uint8, device="cuda"
    )
    with pytest.raises(FuseUnsupported, match="reshape.*allocates"):
        load_spec.fuse_copy_pairs(
            load_spec.src_shard_spec,
            wire,
            model,
            {"weight": model["model.weight"].dtype},
            src_to_dst=False,
        )


@pytest.mark.parametrize("sender_start,peer_start", [(0, 384), (3072, 3456)])
def test_fused_save_matches_two_stage_for_deduplicated_partial_source(
    sender_start, peer_start
):
    """Reproduce GLM's nested sender-de-dup and receiver-overlap slicing.

    The sender owns a strict 768-element piece of a 6144-element replicated norm, and one rollout peer
    receives a 48-element sub-piece.  The self-check reference must populate that ordinary (non-logical)
    partial source before comparing it with the fused model-to-wire copy.
    """
    if not torch.cuda.is_available():
        pytest.skip("fused copy requires CUDA")
    source = [(0, 6144, 6144)]
    load_spec = LoadSpec({"weight": {"model.weight": [(source, source)]}})
    sender_spec = ShardSpec(
        {
            "weight": [[(sender_start, sender_start + 768, 6144)]],
        }
    )
    peer_spec = ShardSpec(
        {
            "weight": [[(peer_start, peer_start + 48, 6144)]],
        }
    )
    dtype_spec = {"weight": torch.bfloat16}
    model = {
        "model.weight": torch.arange(6144, device="cuda", dtype=torch.bfloat16),
    }

    logical = sender_spec.make_named_buffer(dtype_spec, "cuda")
    load_spec.copy_fromto_params(sender_spec, logical, model, src_to_dst=False)
    reference = peer_spec.make_byte_chunk(dtype_spec, "cuda")
    sender_spec(logical).pack_into(peer_spec, reference)

    fused = peer_spec.make_byte_chunk(dtype_spec, "cuda")
    batched_copy(
        load_spec.fuse_copy_pairs(
            peer_spec,
            fused,
            model,
            dtype_spec,
            src_to_dst=False,
        )
    )
    torch.cuda.synchronize()
    assert torch.equal(reference, fused)


if __name__ == "__main__":
    dev = sys.argv[1] if len(sys.argv) > 1 else "cuda"
    try:
        ok = main(dev)
    except Exception:
        import traceback

        traceback.print_exc()
        ok = False
    sys.exit(0 if ok else 1)
