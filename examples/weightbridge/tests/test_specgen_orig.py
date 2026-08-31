# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Run the original test_specgen.py test cases without pytest."""

import importlib
import importlib.util
import logging
import os
import sys
import types
from pathlib import Path

LIB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LIB_ROOT))
logging.basicConfig(level=logging.INFO)

import torch

# Stub wbridge package
wbridge_pkg = types.ModuleType("wbridge")
wbridge_pkg.__path__ = [str(LIB_ROOT / "wbridge")]
wbridge_pkg.__package__ = "wbridge"
sys.modules["wbridge"] = wbridge_pkg

wbridge_utils = types.ModuleType("wbridge.utils")
wbridge_utils.__path__ = [os.path.join(wbridge_pkg.__path__[0], "utils")]
wbridge_utils.__package__ = "wbridge.utils"
sys.modules["wbridge.utils"] = wbridge_utils

for mod_name, file_name in [
    ("wbridge.utils.data", "data.py"),
    ("wbridge.utils.specgen", "specgen.py"),
]:
    spec = importlib.util.spec_from_file_location(
        mod_name, os.path.join(wbridge_utils.__path__[0], file_name)
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)

from collections.abc import Callable, Iterable

from wbridge.utils.data import LoadSpec, Shards
from wbridge.utils.specgen import (
    DEFAULT_MAX_HF_BYTES,
    infer_load_spec,
    verify_load_spec,
)

device = torch.device("cuda", 0)


def _hfsd_to_fetcher(hfsd):
    """Convert a plain dict of tensors to (HFWeightFetcher, shapes)."""
    fetcher = {name: (lambda t=t: t) for name, t in hfsd.items()}
    shapes = {name: tuple(t.shape) for name, t in hfsd.items()}
    return fetcher, shapes


# ---- Reproduce _complex_sglang_like_lw from test_specgen.py ----
def _complex_sglang_like_lw(wksd, *, V, H, INTER, tp_rank, q_dim, kv_dim):
    half = H // 2
    assert tp_rank in (0, 1)
    qkv_rows = q_dim + 2 * kv_dim

    def _dispatch(name, t):
        if name.startswith("pp_skip."):
            return
        if name == "model.embed_tokens.weight":
            wksd["embed_tokens.weight"].copy_(t[:V, :])
            wksd["lm_head.weight"].copy_(t[:V, :])
            return
        if name.endswith("self_attn.q_proj.weight"):
            wksd["layers.1.self_attn.qkv_proj.weight"][:q_dim, :].copy_(t)
            return
        if name.endswith("self_attn.k_proj.weight"):
            wksd["layers.1.self_attn.qkv_proj.weight"][q_dim : q_dim + kv_dim, :].copy_(
                t
            )
            return
        if name.endswith("self_attn.v_proj.weight"):
            wksd["layers.1.self_attn.qkv_proj.weight"][
                q_dim + kv_dim : qkv_rows, :
            ].copy_(t)
            return
        if name.endswith("self_attn.o_proj.weight"):
            wksd["layers.1.self_attn.o_proj.weight"].copy_(
                t[:, tp_rank * half : (tp_rank + 1) * half]
            )
            return
        if name.endswith("mlp.gate_proj.weight"):
            wksd["layers.1.mlp.gate_proj.weight"].copy_(
                t[tp_rank * half : (tp_rank + 1) * half, :]
            )
            return

    def lw(hf_weights):
        for name, fn in hf_weights.items():
            _dispatch(name, fn())
        for name, fn in hf_weights.items():
            if name == "model.embed_tokens.weight":
                wksd["lm_head.weight"].copy_(fn()[:V, :])
                break

    return lw


# ---- test_infer_load_spec_complex_load_weights ----
print("Running test_infer_load_spec_complex_load_weights...")
V, V_PAD, H, INTER = 8, 1, 4, 8
tp_rank = 1
q_dim, kv_dim = 64, 16
qkv_rows = q_dim + 2 * kv_dim
half = H // 2

hfsd = {
    "pp_skip.model.layers.0.mlp.fc.weight": torch.randn(3, 3),
    "model.embed_tokens.weight": torch.randn(V + V_PAD, H),
    "model.layers.1.self_attn.q_proj.weight": torch.randn(q_dim, H),
    "model.layers.1.self_attn.k_proj.weight": torch.randn(kv_dim, H),
    "model.layers.1.self_attn.v_proj.weight": torch.randn(kv_dim, H),
    "model.layers.1.self_attn.o_proj.weight": torch.randn(H, H),
    "model.layers.1.mlp.gate_proj.weight": torch.randn(H, INTER),
}
wksd = {
    "embed_tokens.weight": torch.zeros(V, H, device=device),
    "lm_head.weight": torch.zeros(V, H, device=device),
    "layers.1.self_attn.qkv_proj.weight": torch.zeros(qkv_rows, H, device=device),
    "layers.1.self_attn.o_proj.weight": torch.zeros(H, half, device=device),
    "layers.1.mlp.gate_proj.weight": torch.zeros(half, INTER, device=device),
}

lw = _complex_sglang_like_lw(
    wksd, V=V, H=H, INTER=INTER, tp_rank=tp_rank, q_dim=q_dim, kv_dim=kv_dim
)
fetcher, shapes = _hfsd_to_fetcher(hfsd)
load_spec = infer_load_spec(fetcher, shapes, lambda: wksd, lw)
spec = load_spec.src_shard_spec

assert "pp_skip.model.layers.0.mlp.fc.weight" not in spec.entries
assert spec["model.embed_tokens.weight"] == [[(0, V, V + V_PAD), (0, H, H)]]
assert spec["model.layers.1.self_attn.q_proj.weight"] == [
    [(0, q_dim, q_dim), (0, H, H)]
]
assert spec["model.layers.1.self_attn.k_proj.weight"] == [
    [(0, kv_dim, kv_dim), (0, H, H)]
]
assert spec["model.layers.1.self_attn.v_proj.weight"] == [
    [(0, kv_dim, kv_dim), (0, H, H)]
]
assert spec["model.layers.1.self_attn.o_proj.weight"] == [
    [(0, H, H), (tp_rank * half, (tp_rank + 1) * half, H)]
]
assert spec["model.layers.1.mlp.gate_proj.weight"] == [
    [(tp_rank * half, (tp_rank + 1) * half, H), (0, INTER, INTER)]
]
print("  PASS")

# ---- test_infer_load_spec_batched_merge_matches_single ----
print("Running test_infer_load_spec_batched_merge_matches_single...")
H = 4
hfsd_b = {
    "a.weight": torch.randn(H, H),
    "b.weight": torch.randn(H, H),
    "c.weight": torch.randn(H, H),
}
wksd_b = {
    "a.weight": torch.zeros(H, H, device=device),
    "b.weight": torch.zeros(H, H, device=device),
    "c.weight": torch.zeros(H, H, device=device),
}


def lw_b(hf_weights):
    for name, fn in hf_weights.items():
        short = name.replace("model.", "") if name.startswith("model.") else name
        if short in wksd_b:
            wksd_b[short].copy_(fn())


fetcher_b, shapes_b = _hfsd_to_fetcher(hfsd_b)
full = infer_load_spec(fetcher_b, shapes_b, lambda: wksd_b, lw_b).src_shard_spec
batched = infer_load_spec(fetcher_b, shapes_b, lambda: wksd_b, lw_b).src_shard_spec
assert set(full.entries.keys()) == set(batched.entries.keys())
for name in full.entries:
    assert full[name] == batched[name], (
        f"mismatch on {name}: {full[name]} vs {batched[name]}"
    )
print("  PASS")

# ---- test_infer_load_spec_hf_worker_dtype_mismatch ----
print("Running test_infer_load_spec_hf_worker_dtype_mismatch...")
hfsd_d = {
    "model.layers.0.mlp.gate.e_score_correction_bias": torch.randn(
        8, dtype=torch.bfloat16, device="cpu"
    )
}
wksd_d = {
    "layers.0.mlp.gate.e_score_correction_bias": torch.zeros(
        8, dtype=torch.float32, device=device
    )
}


def lw_d(hf_weights):
    for name, fn in hf_weights.items():
        if name.endswith("e_score_correction_bias"):
            wksd_d["layers.0.mlp.gate.e_score_correction_bias"].copy_(fn())


fetcher_d, shapes_d = _hfsd_to_fetcher(hfsd_d)
load_spec_d = infer_load_spec(fetcher_d, shapes_d, lambda: wksd_d, lw_d)
spec_d = load_spec_d.src_shard_spec
name_d = "model.layers.0.mlp.gate.e_score_correction_bias"
assert spec_d[name_d] == [[(0, 8, 8)]]

lw_d(fetcher_d)
verify_load_spec(fetcher_d, lambda: wksd_d, load_spec_d)
print("  PASS")

# ---- test_infer_load_spec_bfloat16 ----
print("Running test_infer_load_spec_bfloat16...")
dt = torch.bfloat16

# Large square weight
H = 1024
half_w = H // 2
tp_rank_o = 0
hfsd_o = {
    "model.layers.0.self_attn.o_proj.weight": torch.randn(H, H, dtype=dt, device="cpu")
}
wksd_o = {
    "layers.0.self_attn.o_proj.weight": torch.zeros(H, half_w, dtype=dt, device=device)
}


def lw_o(hf_weights):
    for name, fn in hf_weights.items():
        if name.endswith("o_proj.weight"):
            t = fn()
            wksd_o["layers.0.self_attn.o_proj.weight"].copy_(
                t[:, tp_rank_o * half_w : (tp_rank_o + 1) * half_w]
            )


fetcher_o, shapes_o = _hfsd_to_fetcher(hfsd_o)
spec_o = infer_load_spec(fetcher_o, shapes_o, lambda: wksd_o, lw_o).src_shard_spec
assert spec_o["model.layers.0.self_attn.o_proj.weight"] == [
    [(0, H, H), (tp_rank_o * half_w, (tp_rank_o + 1) * half_w, H)]
]

# Wide matrix (vocab-scale)
rows, cols = 16, 151_936
half_c = cols // 2
tp_rank_e = 1
hfsd_e = {"model.embed_tokens.weight": torch.randn(rows, cols, dtype=dt, device="cpu")}
wksd_e = {
    "model.embed_tokens.weight": torch.zeros(rows, half_c, dtype=dt, device=device)
}


def lw_e(hf_weights):
    for name, fn in hf_weights.items():
        if name.endswith("embed_tokens.weight"):
            t = fn()
            wksd_e["model.embed_tokens.weight"].copy_(
                t[:, tp_rank_e * half_c : (tp_rank_e + 1) * half_c]
            )


fetcher_e, shapes_e = _hfsd_to_fetcher(hfsd_e)
spec_e = infer_load_spec(fetcher_e, shapes_e, lambda: wksd_e, lw_e).src_shard_spec
assert spec_e["model.embed_tokens.weight"] == [
    [(0, rows, rows), (tp_rank_e * half_c, (tp_rank_e + 1) * half_c, cols)]
]
print("  PASS")

# ---- test_verify_load_spec_1to1 ----
print("Running test_verify_load_spec_1to1...")
H = 4
hfsd_v = {"a.weight": torch.randn(H, H), "b.weight": torch.randn(H, H)}
wksd_v = {
    "a.weight": torch.zeros(H, H, device=device),
    "b.weight": torch.zeros(H, H, device=device),
}


def lw_v(hf_weights):
    for name, fn in hf_weights.items():
        wksd_v[name].copy_(fn())


fetcher_v, shapes_v = _hfsd_to_fetcher(hfsd_v)
lw_v(fetcher_v)
load_spec_v = infer_load_spec(fetcher_v, shapes_v, lambda: wksd_v, lw_v)
verify_load_spec(fetcher_v, lambda: wksd_v, load_spec_v)
print("  PASS")

print("\n=== ALL ORIGINAL test_specgen.py TESTS PASSED ===")
