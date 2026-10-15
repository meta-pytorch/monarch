# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Quick GPU test of the new specgen algorithm."""

import importlib
import logging
import os
import sys
import types
from pathlib import Path

LIB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LIB_ROOT))
logging.basicConfig(level=logging.INFO)

import torch

print(
    f"torch {torch.__version__}, CUDA: {torch.cuda.is_available()}, devices: {torch.cuda.device_count()}"
)

# Stub wbridge package to avoid importing backend (which needs fastapi/pyzmq)
wbridge_pkg = types.ModuleType("wbridge")
wbridge_pkg.__path__ = [str(LIB_ROOT / "wbridge")]
wbridge_pkg.__package__ = "wbridge"
sys.modules["wbridge"] = wbridge_pkg

wbridge_utils = types.ModuleType("wbridge.utils")
wbridge_utils.__path__ = [os.path.join(wbridge_pkg.__path__[0], "utils")]
wbridge_utils.__package__ = "wbridge.utils"
sys.modules["wbridge.utils"] = wbridge_utils

# Now import the actual modules
import importlib.util

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

from wbridge.utils.data import LoadSpec
from wbridge.utils.specgen import infer_load_spec, verify_load_spec

device = torch.device("cuda", 0)


def _hfsd_to_fetcher(hfsd):
    """Convert a plain dict of tensors to (HFWeightFetcher, shapes)."""
    fetcher = {name: (lambda t=t: t) for name, t in hfsd.items()}
    shapes = {name: tuple(t.shape) for name, t in hfsd.items()}
    return fetcher, shapes


# ---- Test 1: trivial 1:1 copy ----
H = 4
hfsd = {"a.weight": torch.randn(H, H), "b.weight": torch.randn(H, H)}
wksd = {
    "a.weight": torch.zeros(H, H, device=device),
    "b.weight": torch.zeros(H, H, device=device),
}


def lw1(hf_weights):
    for name, fn in hf_weights.items():
        if name in wksd:
            wksd[name].copy_(fn())


fetcher, shapes = _hfsd_to_fetcher(hfsd)
spec = infer_load_spec(fetcher, shapes, lambda: wksd, lw1)
ss = spec.src_shard_spec
assert ss["a.weight"] == [[(0, H, H), (0, H, H)]]
assert ss["b.weight"] == [[(0, H, H), (0, H, H)]]
print("PASS test_1to1_copy")

# ---- Test 2: TP column-parallel ----
H = 8
half = H // 2
tp_rank = 1
hfsd2 = {"gate.weight": torch.randn(H, H)}
wksd2 = {"gate.weight": torch.zeros(half, H, device=device)}


def lw2(hf_weights):
    for name, fn in hf_weights.items():
        if name == "gate.weight":
            t = fn()
            wksd2["gate.weight"].copy_(t[tp_rank * half : (tp_rank + 1) * half, :])


fetcher2, shapes2 = _hfsd_to_fetcher(hfsd2)
spec2 = infer_load_spec(fetcher2, shapes2, lambda: wksd2, lw2)
ss2 = spec2.src_shard_spec
expected = [[(tp_rank * half, (tp_rank + 1) * half, H), (0, H, H)]]
assert ss2["gate.weight"] == expected, f"got {ss2['gate.weight']}"
print("PASS test_tp_column_parallel")

# ---- Test 3: TP row-parallel ----
H = 8
half = H // 2
tp_rank = 0
hfsd3 = {"o.weight": torch.randn(H, H)}
wksd3 = {"o.weight": torch.zeros(H, half, device=device)}


def lw3(hf_weights):
    for name, fn in hf_weights.items():
        if name == "o.weight":
            t = fn()
            wksd3["o.weight"].copy_(t[:, tp_rank * half : (tp_rank + 1) * half])


fetcher3, shapes3 = _hfsd_to_fetcher(hfsd3)
spec3 = infer_load_spec(fetcher3, shapes3, lambda: wksd3, lw3)
ss3 = spec3.src_shard_spec
expected3 = [[(0, H, H), (tp_rank * half, (tp_rank + 1) * half, H)]]
assert ss3["o.weight"] == expected3, f"got {ss3['o.weight']}"
print("PASS test_tp_row_parallel")

# ---- Test 4: QKV merge (3 HF -> 1 worker) ----
H = 4
q_dim, kv_dim = 8, 2
hfsd4 = {
    "q.weight": torch.randn(q_dim, H),
    "k.weight": torch.randn(kv_dim, H),
    "v.weight": torch.randn(kv_dim, H),
}
wksd4 = {"qkv.weight": torch.zeros(q_dim + 2 * kv_dim, H, device=device)}


def lw4(hf_weights):
    for name, fn in hf_weights.items():
        t = fn()
        if name == "q.weight":
            wksd4["qkv.weight"][:q_dim, :].copy_(t)
        elif name == "k.weight":
            wksd4["qkv.weight"][q_dim : q_dim + kv_dim, :].copy_(t)
        elif name == "v.weight":
            wksd4["qkv.weight"][q_dim + kv_dim :, :].copy_(t)


fetcher4, shapes4 = _hfsd_to_fetcher(hfsd4)
spec4 = infer_load_spec(fetcher4, shapes4, lambda: wksd4, lw4)
ss4 = spec4.src_shard_spec
assert ss4["q.weight"] == [[(0, q_dim, q_dim), (0, H, H)]]
assert ss4["k.weight"] == [[(0, kv_dim, kv_dim), (0, H, H)]]
assert ss4["v.weight"] == [[(0, kv_dim, kv_dim), (0, H, H)]]
print("PASS test_qkv_merge")

# ---- Test 5: PP skip ----
H = 4
hfsd5 = {
    "pp_skip.layer0.weight": torch.randn(H, H),
    "layer1.weight": torch.randn(H, H),
}
wksd5 = {"layer1.weight": torch.zeros(H, H, device=device)}


def lw5(hf_weights):
    for name, fn in hf_weights.items():
        if name == "layer1.weight":
            wksd5["layer1.weight"].copy_(fn())


fetcher5, shapes5 = _hfsd_to_fetcher(hfsd5)
spec5 = infer_load_spec(fetcher5, shapes5, lambda: wksd5, lw5)
assert "pp_skip.layer0.weight" not in spec5.entries
assert "layer1.weight" in spec5.entries
print("PASS test_pp_skip")

# ---- Test 6: Padded vocab ----
V, V_PAD, H = 8, 2, 4
hfsd6 = {"embed.weight": torch.randn(V + V_PAD, H)}
wksd6 = {"embed.weight": torch.zeros(V, H, device=device)}


def lw6(hf_weights):
    for name, fn in hf_weights.items():
        if name == "embed.weight":
            t = fn()
            wksd6["embed.weight"].copy_(t[:V, :])


fetcher6, shapes6 = _hfsd_to_fetcher(hfsd6)
spec6 = infer_load_spec(fetcher6, shapes6, lambda: wksd6, lw6)
ss6 = spec6.src_shard_spec
expected6 = [[(0, V, V + V_PAD), (0, H, H)]]
assert ss6["embed.weight"] == expected6, f"got {ss6['embed.weight']}"
print("PASS test_padded_vocab")

# ---- Test 7: Tied embeddings (double-pass lw) ----
V, H = 8, 4
hfsd7 = {"model.embed_tokens.weight": torch.randn(V, H)}
wksd7 = {
    "embed_tokens.weight": torch.zeros(V, H, device=device),
    "lm_head.weight": torch.zeros(V, H, device=device),
}


def lw7(hf_weights):
    for name, fn in hf_weights.items():
        if name == "model.embed_tokens.weight":
            t = fn()
            wksd7["embed_tokens.weight"].copy_(t)
    for name, fn in hf_weights.items():
        if name == "model.embed_tokens.weight":
            t = fn()
            wksd7["lm_head.weight"].copy_(t)


fetcher7, shapes7 = _hfsd_to_fetcher(hfsd7)
spec7 = infer_load_spec(fetcher7, shapes7, lambda: wksd7, lw7)
ss7 = spec7.src_shard_spec
expected7 = [[(0, V, V), (0, H, H)]]
assert ss7["model.embed_tokens.weight"] == expected7, (
    f"got {ss7['model.embed_tokens.weight']}"
)
print("PASS test_tied_embeddings")

# ---- Test 8: 1D tensor partial copy ----
W = 16
hfsd8 = {"bias": torch.randn(W)}
wksd8 = {"bias": torch.zeros(8, device=device)}


def lw8(hf_weights):
    for name, fn in hf_weights.items():
        if name == "bias":
            t = fn()
            wksd8["bias"].copy_(t[4:12])


fetcher8, shapes8 = _hfsd_to_fetcher(hfsd8)
spec8 = infer_load_spec(fetcher8, shapes8, lambda: wksd8, lw8)
ss8 = spec8.src_shard_spec
expected8 = [[(4, 12, W)]]
assert ss8["bias"] == expected8, f"got {ss8['bias']}"
print("PASS test_1d_tensor")

# ---- Test 9: Complex SGLang-like (from test file) ----
V, V_PAD, H, INTER = 8, 1, 4, 8
tp_rank = 1
q_dim, kv_dim = 64, 16
qkv_rows = q_dim + 2 * kv_dim
half = H // 2

hfsd9 = {
    "pp_skip.model.layers.0.mlp.fc.weight": torch.randn(3, 3),
    "model.embed_tokens.weight": torch.randn(V + V_PAD, H),
    "model.layers.1.self_attn.q_proj.weight": torch.randn(q_dim, H),
    "model.layers.1.self_attn.k_proj.weight": torch.randn(kv_dim, H),
    "model.layers.1.self_attn.v_proj.weight": torch.randn(kv_dim, H),
    "model.layers.1.self_attn.o_proj.weight": torch.randn(H, H),
    "model.layers.1.mlp.gate_proj.weight": torch.randn(H, INTER),
}
wksd9 = {
    "embed_tokens.weight": torch.zeros(V, H, device=device),
    "lm_head.weight": torch.zeros(V, H, device=device),
    "layers.1.self_attn.qkv_proj.weight": torch.zeros(qkv_rows, H, device=device),
    "layers.1.self_attn.o_proj.weight": torch.zeros(H, half, device=device),
    "layers.1.mlp.gate_proj.weight": torch.zeros(half, INTER, device=device),
}


def _dispatch9(name, t):
    if name.startswith("pp_skip."):
        return
    if name == "model.embed_tokens.weight":
        wksd9["embed_tokens.weight"].copy_(t[:V, :])
        wksd9["lm_head.weight"].copy_(t[:V, :])
        return
    if name.endswith("self_attn.q_proj.weight"):
        wksd9["layers.1.self_attn.qkv_proj.weight"][:q_dim, :].copy_(t)
        return
    if name.endswith("self_attn.k_proj.weight"):
        wksd9["layers.1.self_attn.qkv_proj.weight"][q_dim : q_dim + kv_dim, :].copy_(t)
        return
    if name.endswith("self_attn.v_proj.weight"):
        wksd9["layers.1.self_attn.qkv_proj.weight"][q_dim + kv_dim : qkv_rows, :].copy_(
            t
        )
        return
    if name.endswith("self_attn.o_proj.weight"):
        wksd9["layers.1.self_attn.o_proj.weight"].copy_(
            t[:, tp_rank * half : (tp_rank + 1) * half]
        )
        return
    if name.endswith("mlp.gate_proj.weight"):
        wksd9["layers.1.mlp.gate_proj.weight"].copy_(
            t[tp_rank * half : (tp_rank + 1) * half, :]
        )
        return


def lw9(hf_weights):
    for name, fn in hf_weights.items():
        _dispatch9(name, fn())
    for name, fn in hf_weights.items():
        if name == "model.embed_tokens.weight":
            wksd9["lm_head.weight"].copy_(fn()[:V, :])
            break


fetcher9, shapes9 = _hfsd_to_fetcher(hfsd9)
spec9 = infer_load_spec(fetcher9, shapes9, lambda: wksd9, lw9)
ss9 = spec9.src_shard_spec

assert "pp_skip.model.layers.0.mlp.fc.weight" not in ss9.entries
assert ss9["model.embed_tokens.weight"] == [[(0, V, V + V_PAD), (0, H, H)]]
assert ss9["model.layers.1.self_attn.q_proj.weight"] == [[(0, q_dim, q_dim), (0, H, H)]]
assert ss9["model.layers.1.self_attn.k_proj.weight"] == [
    [(0, kv_dim, kv_dim), (0, H, H)]
]
assert ss9["model.layers.1.self_attn.v_proj.weight"] == [
    [(0, kv_dim, kv_dim), (0, H, H)]
]
assert ss9["model.layers.1.self_attn.o_proj.weight"] == [
    [(0, H, H), (tp_rank * half, (tp_rank + 1) * half, H)]
]
assert ss9["model.layers.1.mlp.gate_proj.weight"] == [
    [(tp_rank * half, (tp_rank + 1) * half, H), (0, INTER, INTER)]
]
print("PASS test_complex_sglang_like")

# ---- Test 10: verify_load_spec works with new algorithm ----
H = 4
hfsd10 = {"a.weight": torch.randn(H, H), "b.weight": torch.randn(H, H)}
wksd10 = {
    "a.weight": torch.zeros(H, H, device=device),
    "b.weight": torch.zeros(H, H, device=device),
}


def lw10(hf_weights):
    for name, fn in hf_weights.items():
        if name in wksd10:
            wksd10[name].copy_(fn())


fetcher10, shapes10 = _hfsd_to_fetcher(hfsd10)
lw10(fetcher10)
spec10 = infer_load_spec(fetcher10, shapes10, lambda: wksd10, lw10)
verify_load_spec(fetcher10, lambda: wksd10, spec10)
print("PASS test_verify_load_spec")

# ---- Test 11: dtype mismatch (bf16 HF -> fp32 wk) ----
hfsd11 = {
    "model.layers.0.mlp.gate.e_score_correction_bias": torch.randn(
        8, dtype=torch.bfloat16
    ),
}
wksd11 = {
    "layers.0.mlp.gate.e_score_correction_bias": torch.zeros(
        8, dtype=torch.float32, device=device
    ),
}


def lw11(hf_weights):
    for name, fn in hf_weights.items():
        if name.endswith("e_score_correction_bias"):
            wksd11["layers.0.mlp.gate.e_score_correction_bias"].copy_(fn())


fetcher11, shapes11 = _hfsd_to_fetcher(hfsd11)
spec11 = infer_load_spec(fetcher11, shapes11, lambda: wksd11, lw11)
ss11 = spec11.src_shard_spec
name11 = "model.layers.0.mlp.gate.e_score_correction_bias"
assert ss11[name11] == [[(0, 8, 8)]], f"got {ss11[name11]}"

lw11(fetcher11)
verify_load_spec(fetcher11, lambda: wksd11, spec11)
print("PASS test_dtype_mismatch_bf16_to_fp32")

# ---- Test 12: bfloat16 production-like sizes ----
dt = torch.bfloat16
H = 1024
half_w = H // 2
tp_rank_o = 0

hfsd12 = {"model.layers.0.self_attn.o_proj.weight": torch.randn(H, H, dtype=dt)}
wksd12 = {
    "layers.0.self_attn.o_proj.weight": torch.zeros(H, half_w, dtype=dt, device=device)
}


def lw12(hf_weights):
    for name, fn in hf_weights.items():
        if name.endswith("o_proj.weight"):
            t = fn()
            wksd12["layers.0.self_attn.o_proj.weight"].copy_(
                t[:, tp_rank_o * half_w : (tp_rank_o + 1) * half_w]
            )


fetcher12, shapes12 = _hfsd_to_fetcher(hfsd12)
spec12 = infer_load_spec(fetcher12, shapes12, lambda: wksd12, lw12)
ss12 = spec12.src_shard_spec
assert ss12["model.layers.0.self_attn.o_proj.weight"] == [
    [(0, H, H), (tp_rank_o * half_w, (tp_rank_o + 1) * half_w, H)]
]
print("PASS test_bfloat16_production_size")

print("\n=== ALL 12 TESTS PASSED ===")
