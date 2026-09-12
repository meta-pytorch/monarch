# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Tests for :func:`wbridge.utils.specgen.infer_load_spec` with a composite ``lw``."""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable

import pytest
import torch
import wbridge.utils.specgen as specgen_module
from wbridge.utils.data import LoadSpec, Shards
from wbridge.utils.specgen import (
    _diff2_inplace,
    _plan_logical_sources,
    _plan_worker_slices,
    _prefix_sum_2d,
    _select_probe_build_device,
    DEFAULT_MAX_HF_BYTES,
    HFWeightFetcher,
    infer_load_spec,
    SPECGEN_PROBE_DEVICE_ENV,
    verify_load_spec,
)


@pytest.fixture(scope="module")
def device() -> torch.device:
    if not torch.cuda.is_available():
        pytest.skip("infer_load_spec requires CUDA worker tensors")
    return torch.device("cuda", torch.cuda.current_device())


def _hfsd_to_fetcher(
    hfsd: dict[str, torch.Tensor],
) -> tuple[HFWeightFetcher, dict[str, tuple[int, ...]]]:
    """Convert a test HF state dict to (fetcher, shapes)."""
    return (
        {name: (lambda t=t: t) for name, t in hfsd.items()},
        {name: tuple(t.shape) for name, t in hfsd.items()},
    )


def test_plan_logical_sources_caps_encoded_i64_not_checkpoint_dtype() -> None:
    shape = (17, 11)
    row_i64_bytes = shape[1] * 8

    unsplit, unsplit_shapes = _plan_logical_sources({"weight": shape}, math.inf)
    assert [(piece.start, piece.end) for piece in unsplit["weight"]] == [(0, 17)]
    assert unsplit_shapes == {"weight": shape}

    pieces, logical_shapes = _plan_logical_sources(
        {"weight": shape},
        5 * row_i64_bytes,
    )
    assert [(piece.start, piece.end) for piece in pieces["weight"]] == [
        (0, 5),
        (5, 10),
        (10, 15),
        (15, 17),
    ]
    assert all(
        (piece.end - piece.start) * row_i64_bytes <= 5 * row_i64_bytes
        for piece in pieces["weight"]
    )
    assert set(logical_shapes) == {piece.logical_name for piece in pieces["weight"]}


def test_plan_worker_slices_flattens_fused_leading_dimensions() -> None:
    shape = (12, 4096, 7168)
    cap = 64 * 1024**2
    rows_per_slice = cap // (shape[-1] * 8)
    slices = _plan_worker_slices(shape, cap)

    assert slices[0] == (0, rows_per_slice, (rows_per_slice, shape[-1]))
    assert slices[-1][1] == shape[0] * shape[1]
    assert all(math.prod(slice_shape) * 8 <= cap for _, _, slice_shape in slices)
    assert sum(end - start for start, end, _ in slices) == shape[0] * shape[1]


def test_diff2_strided_diff1_round_trip() -> None:
    original = torch.arange(35, dtype=torch.int64).reshape(5, 7) ** 2
    expected = original.clone()
    expected[:, 2:] = original[:, 2:] - 2 * original[:, 1:-1] + original[:, :-2]
    expected[:, 1] = original[:, 1] - 2 * original[:, 0]
    column_diff = expected.clone()
    expected[2:, :] = (
        column_diff[2:, :] - 2 * column_diff[1:-1, :] + column_diff[:-2, :]
    )
    expected[1, :] = column_diff[1, :] - 2 * column_diff[0, :]

    actual = _diff2_inplace(original.clone())
    assert torch.equal(actual, expected)
    assert torch.equal(_prefix_sum_2d(actual.clone()), original)


def test_infer_load_spec_i64_caps_are_canonical(device: torch.device) -> None:
    """INF and small caps produce the same physical LoadSpec after merging.

    Transpose makes source dim-0 cuts land on destination dim 1, while the
    small cap independently cuts the worker on destination dim 0.  Thus this
    also exercises two-dimensional coalescing of the resulting shard grid.
    """
    # Small caps create enough logical ids that the encoded index spans two
    # fp16 chunks, exercising carry propagation as well as canonicalisation.
    rows, cols = 65, 17
    hfsd = {
        "model.weight": torch.randn(rows, cols, dtype=torch.float16),
    }
    wksd = {
        "weight": torch.zeros(cols, rows, dtype=torch.float16, device=device),
    }

    def lw(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            if name == "model.weight":
                wksd["weight"].copy_(fn().T)

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    row_i64_bytes = cols * 8
    specs = [
        infer_load_spec(
            hf_weights,
            hf_shapes,
            lambda: wksd,
            lw,
            i64_cap_bytes=cap,
        )
        for cap in (math.inf, 5 * row_i64_bytes, 4 * row_i64_bytes)
    ]

    assert specs[1].entries == specs[0].entries
    assert specs[2].entries == specs[0].entries
    assert set(specs[2].entries) == {"model.weight"}
    assert all(".__wbridge_dim0_" not in name for name in specs[2].entries)
    lw(hf_weights)
    verify_load_spec(hf_weights, lambda: wksd, specs[2])


def test_infer_load_spec_i64_caps_fused_3d_worker(device: torch.device) -> None:
    """Flattened-row slicing preserves separately loaded transposed experts."""
    experts, source_rows, source_cols = 4, 7, 5
    hfsd = {
        f"model.experts.{index}.weight": torch.randn(
            source_rows,
            source_cols,
            dtype=torch.float16,
        )
        for index in range(experts)
    }
    wksd = {
        "experts.weight": torch.zeros(
            experts,
            source_cols,
            source_rows,
            dtype=torch.float16,
            device=device,
        ),
    }

    def lw(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            if name.startswith("model.experts."):
                index = int(name.split(".")[2])
                wksd["experts.weight"][index].copy_(fn().T)

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    dim0_row_i64_bytes = source_rows * source_cols * 8
    flattened_row_i64_bytes = source_rows * 8
    specs = [
        infer_load_spec(
            hf_weights,
            hf_shapes,
            lambda: wksd,
            lw,
            i64_cap_bytes=cap,
        )
        for cap in (
            math.inf,
            2 * dim0_row_i64_bytes,
            dim0_row_i64_bytes,
            flattened_row_i64_bytes,
        )
    ]
    assert specs[1].entries == specs[0].entries
    assert specs[2].entries == specs[0].entries
    assert specs[3].entries == specs[0].entries
    lw(hf_weights)
    verify_load_spec(hf_weights, lambda: wksd, specs[3])


def test_infer_load_spec_cuda_row_staged_cpu_placeholders(
    device: torch.device,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Oversized CPU placeholders are populated only by CUDA row tiles."""
    monkeypatch.setenv(SPECGEN_PROBE_DEVICE_ENV, "cpu")
    tile_bytes = 64
    monkeypatch.setattr(specgen_module, "_SPECGEN_CUDA_ROW_TILE_BYTES", tile_bytes)
    hfsd = {
        "model.weight": torch.randn(11, 7, dtype=torch.float32),
    }
    wksd = {
        "weight": torch.zeros(7, 11, dtype=torch.float32, device=device),
    }
    probe_devices: list[str] = []
    build_devices: list[str] = []
    build_intervals: list[tuple[int, int]] = []
    original_build_chunk = specgen_module._build_chunk_gpu

    def recording_build_chunk(*args, **kwargs) -> torch.Tensor:
        build_devices.append(args[6].type)
        build_intervals.append((kwargs["dim0_start"], kwargs["dim0_end"]))
        return original_build_chunk(*args, **kwargs)

    monkeypatch.setattr(specgen_module, "_build_chunk_gpu", recording_build_chunk)

    def lw(hf_weights: HFWeightFetcher) -> None:
        source = hf_weights["model.weight"]()
        probe_devices.append(source.device.type)
        wksd["weight"].copy_(source.T)

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    spec = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, lw)

    assert probe_devices
    assert set(probe_devices) == {"cpu"}
    assert build_devices and set(build_devices) == {"cuda"}
    assert len(build_intervals) > 1
    assert all(
        (end - start) * 7 * torch.float32.itemsize <= tile_bytes
        for start, end in build_intervals
    )
    lw(hf_weights)
    verify_load_spec(hf_weights, lambda: wksd, spec)


def test_cpu_fallback_keeps_bounded_probe_on_cuda(
    device: torch.device,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A CPU fallback mode does not move bounded probes off CUDA."""
    del device  # CUDA availability is the relevant fixture precondition.
    shape = (3, 4)
    pieces, _logical_shapes = _plan_logical_sources({"weight": shape}, math.inf)
    monkeypatch.setattr(specgen_module, "_SPECGEN_CUDA_ROW_TILE_BYTES", 1024)

    probe = specgen_module._build_physical_chunk(
        pieces["weight"],
        {"weight": 0},
        0,
        0,
        32,
        torch.float32,
        torch.device("cpu"),
    )

    assert probe.is_cuda
    assert probe.shape == shape


def test_auto_probe_device_budgets_two_live_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto placement covers loaders that retain one source during fetch."""
    gib = 1024**3
    monkeypatch.delenv(SPECGEN_PROBE_DEVICE_ENV, raising=False)
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "empty_cache", lambda: None)
    monkeypatch.setattr(
        torch.cuda,
        "mem_get_info",
        lambda: (int(5.8 * gib), 80 * gib),
    )

    # This reproduces Kimi's expanded FP32 expert source: one 4.38-GiB
    # tensor appears to fit in 5.8 GiB, but two loader-live sources do not.
    shape = (1, int(4.38 * gib) // torch.float32.itemsize)
    pieces, _logical_shapes = _plan_logical_sources({"weight": shape}, math.inf)
    assert (
        _select_probe_build_device(
            ["weight"],
            pieces,
            torch.float32,
        ).type
        == "cpu"
    )

    small_shape = (1, 128 * 1024**2 // torch.float32.itemsize)
    small_pieces, _ = _plan_logical_sources(
        {"weight": small_shape},
        math.inf,
    )
    assert (
        _select_probe_build_device(
            ["weight"],
            small_pieces,
            torch.float32,
        ).type
        == "cuda"
    )


def _complex_sglang_like_lw(
    wksd: dict[str, torch.Tensor],
    *,
    V: int,
    H: int,
    INTER: int,
    tp_rank: int,
    q_dim: int,
    kv_dim: int,
) -> Callable[[HFWeightFetcher], None]:
    """Mimics several SGLang / vLLM-style load paths (PP skip, padding, QKV merge, TP, transpose, tie).

    * **PP:** names prefixed with ``pp_skip.`` are ignored (weights not on this pipeline stage).
    * **Padded vocab:** ``embed_tokens`` / ``lm_head`` HF tensors are wider than the runtime vocab;
      only ``[:V, :]`` is copied (extra rows mirror padded ``lm_head`` in large checkpoints).
    * **QKV merge:** three HF matrices are written into non-overlapping row blocks of ``qkv_proj``
      (like ``stacked_params_mapping`` merging ``q_proj`` / ``k_proj`` / ``v_proj`` into ``qkv_proj``).
    * **TP column-parallel:** ``gate_proj`` shards along dim 0 (output features).
    * **TP row-parallel:** ``o_proj`` shards along dim 1 (input features split across ranks).
    * **Tied embeddings:** after the first full pass, iterate weights again and copy
      ``model.embed_tokens.weight`` into ``lm_head.weight`` (``qwen2``-style second scan).
    """

    half = H // 2
    assert tp_rank in (0, 1)
    qkv_rows = q_dim + 2 * kv_dim

    def _dispatch(name: str, t: torch.Tensor) -> None:
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

    def lw(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            _dispatch(name, fn())
        # Second pass for tied embeddings
        for name, fn in hf_weights.items():
            if name == "model.embed_tokens.weight":
                wksd["lm_head.weight"].copy_(fn()[:V, :])
                break

    return lw


def test_infer_load_spec_complex_load_weights(device: torch.device) -> None:
    # V, V_PAD, H, INTER = 256, 32, 64, 128
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
    for t in hfsd.values():
        assert t.device.type == "cpu"

    wksd = {
        "embed_tokens.weight": torch.zeros(V, H, device=device),
        "lm_head.weight": torch.zeros(V, H, device=device),
        "layers.1.self_attn.qkv_proj.weight": torch.zeros(qkv_rows, H, device=device),
        "layers.1.self_attn.o_proj.weight": torch.zeros(H, half, device=device),
        "layers.1.mlp.gate_proj.weight": torch.zeros(half, INTER, device=device),
    }

    lw = _complex_sglang_like_lw(
        wksd,
        V=V,
        H=H,
        INTER=INTER,
        tp_rank=tp_rank,
        q_dim=q_dim,
        kv_dim=kv_dim,
    )

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    load_spec = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, lw)
    spec = load_spec.src_shard_spec

    assert "pp_skip.model.layers.0.mlp.fc.weight" not in spec.entries

    def _shard(name: str) -> Shards:
        return spec[name]

    assert _shard("model.embed_tokens.weight") == [[(0, V, V + V_PAD), (0, H, H)]]
    assert _shard("model.layers.1.self_attn.q_proj.weight") == [
        [(0, q_dim, q_dim), (0, H, H)]
    ]
    assert _shard("model.layers.1.self_attn.k_proj.weight") == [
        [(0, kv_dim, kv_dim), (0, H, H)]
    ]
    assert _shard("model.layers.1.self_attn.v_proj.weight") == [
        [(0, kv_dim, kv_dim), (0, H, H)]
    ]
    assert _shard("model.layers.1.self_attn.o_proj.weight") == [
        [(0, H, H), (tp_rank * half, (tp_rank + 1) * half, H)]
    ]
    assert _shard("model.layers.1.mlp.gate_proj.weight") == [
        [(tp_rank * half, (tp_rank + 1) * half, H), (0, INTER, INTER)]
    ]


def test_infer_load_spec_batched_merge_matches_single(device: torch.device) -> None:
    """Small max_hf_bytes forces multiple batches; merged spec matches one-shot."""
    V, H = 8, 4
    hfsd = {
        "a.weight": torch.randn(H, H),
        "b.weight": torch.randn(H, H),
        "c.weight": torch.randn(H, H),
    }
    wksd = {
        "a.weight": torch.zeros(H, H, device=device),
        "b.weight": torch.zeros(H, H, device=device),
        "c.weight": torch.zeros(H, H, device=device),
    }

    def lw(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            short = name.replace("model.", "") if name.startswith("model.") else name
            if short in wksd:
                wksd[short].copy_(fn())

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    full = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, lw).src_shard_spec

    assert set(full.entries.keys()) == {"a.weight", "b.weight", "c.weight"}


@pytest.mark.skipif(
    not torch.cuda.is_available() or not torch.cuda.is_bf16_supported(),
    reason="needs CUDA with bfloat16",
)
def test_infer_load_spec_hf_worker_dtype_mismatch(device: torch.device) -> None:
    """HF tensor bf16, worker fp32 (like SGLang e_score_correction_bias); infer + verify."""
    hfsd = {
        "model.layers.0.mlp.gate.e_score_correction_bias": torch.randn(
            8, dtype=torch.bfloat16, device="cpu"
        ),
    }
    wksd = {
        "layers.0.mlp.gate.e_score_correction_bias": torch.zeros(
            8, dtype=torch.float32, device=device
        ),
    }

    def lw(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            if name.endswith("e_score_correction_bias"):
                wksd["layers.0.mlp.gate.e_score_correction_bias"].copy_(fn())

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    load_spec = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, lw)
    spec = load_spec.src_shard_spec
    name = "model.layers.0.mlp.gate.e_score_correction_bias"
    assert spec[name] == [[(0, 8, 8)]]

    lw(hf_weights)
    verify_load_spec(hf_weights, lambda: wksd, load_spec)


@pytest.mark.skipif(
    not torch.cuda.is_available() or not torch.cuda.is_bf16_supported(),
    reason="needs CUDA with bfloat16",
)
def test_infer_load_spec_bfloat16(device: torch.device) -> None:
    """``infer_load_spec`` with bf16 HF + worker tensors at production-like sizes."""
    dt = torch.bfloat16

    # Large square weight (e.g. attention projection): 1024×1024, row-parallel TP2 on dim 1.
    H = 1024
    half_w = H // 2
    tp_rank_o = 0
    hfsd_o = {
        "model.layers.0.self_attn.o_proj.weight": torch.randn(
            H, H, dtype=dt, device="cpu"
        ),
    }
    wksd_o = {
        "layers.0.self_attn.o_proj.weight": torch.zeros(
            H, half_w, dtype=dt, device=device
        ),
    }

    def lw_o(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            if name.endswith("o_proj.weight"):
                wksd_o["layers.0.self_attn.o_proj.weight"].copy_(
                    fn()[:, tp_rank_o * half_w : (tp_rank_o + 1) * half_w]
                )

    hf_w_o, hf_s_o = _hfsd_to_fetcher(hfsd_o)
    spec_o = infer_load_spec(hf_w_o, hf_s_o, lambda: wksd_o, lw_o).src_shard_spec
    assert spec_o["model.layers.0.self_attn.o_proj.weight"] == [
        [(0, H, H), (tp_rank_o * half_w, (tp_rank_o + 1) * half_w, H)]
    ]

    # Wide matrix 16×151936 (vocab-scale last dim): column-parallel TP2 on dim 1.
    rows, cols = 16, 151_936
    half_c = cols // 2
    tp_rank_e = 1
    hfsd_e = {
        "model.embed_tokens.weight": torch.randn(rows, cols, dtype=dt, device="cpu"),
    }
    wksd_e = {
        "model.embed_tokens.weight": torch.zeros(rows, half_c, dtype=dt, device=device),
    }

    def lw_e(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            if name.endswith("embed_tokens.weight"):
                wksd_e["model.embed_tokens.weight"].copy_(
                    fn()[:, tp_rank_e * half_c : (tp_rank_e + 1) * half_c]
                )

    hf_w_e, hf_s_e = _hfsd_to_fetcher(hfsd_e)
    spec_e = infer_load_spec(hf_w_e, hf_s_e, lambda: wksd_e, lw_e).src_shard_spec
    assert spec_e["model.embed_tokens.weight"] == [
        [(0, rows, rows), (tp_rank_e * half_c, (tp_rank_e + 1) * half_c, cols)]
    ]


def test_verify_load_spec_1to1(device: torch.device) -> None:
    """After load, :func:`verify_load_spec` matches HF slices to worker slices from the LoadSpec."""
    H = 4
    hfsd = {
        "a.weight": torch.randn(H, H),
        "b.weight": torch.randn(H, H),
    }
    wksd = {
        "a.weight": torch.zeros(H, H, device=device),
        "b.weight": torch.zeros(H, H, device=device),
    }

    def lw(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            wksd[name].copy_(fn())

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    lw(hf_weights)
    load_spec = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, lw)
    verify_load_spec(hf_weights, lambda: wksd, load_spec)


def test_load_spec_src_shard_spec_multi_mapping_merge_and_split() -> None:
    """Multiple :class:`ShardMapping` for one (src, dst) pair; :attr:`LoadSpec.src_shard_spec` lists source shards.

    Cases 1–4 use **2D** tensors (axis-aligned boxes). Cases 5–6 are separate **1D** tensors with scattered
    index ranges.
    """
    H, C = 4, 16

    # --- Case 1 (2D): full row span, two non-adjacent column windows → two src shards ---
    split_entries = {
        "hf_split": {
            "wk_split": [
                ([(0, H, H), (0, 4, C)], [(0, H, H), (0, 4, C)]),
                ([(0, H, H), (8, 12, C)], [(0, H, H), (4, 8, C)]),
            ]
        }
    }
    spec_split = LoadSpec(split_entries).src_shard_spec
    assert sorted(spec_split["hf_split"], key=lambda s: (s[1][0], s[1][1])) == [
        [(0, H, H), (0, 4, C)],
        [(0, H, H), (8, 12, C)],
    ]

    # --- Case 2 (2D): full rows, two column bands separated by a gap (distinct from case 1) ---
    gap_entries = {
        "hf_gap": {
            "wk_gap": [
                ([(0, H, H), (0, 3, C)], [(0, H, H), (0, 3, C)]),
                ([(0, H, H), (10, 14, C)], [(0, H, H), (3, 7, C)]),
            ]
        }
    }
    spec_gap = LoadSpec(gap_entries).src_shard_spec
    assert sorted(spec_gap["hf_gap"], key=lambda s: (s[1][0], s[1][1])) == [
        [(0, H, H), (0, 3, C)],
        [(0, H, H), (10, 14, C)],
    ]

    # --- Case 3 (2D): scattered column intervals (gaps), three src shards ---
    scattered_entries = {
        "hf_scatter": {
            "wk_scatter": [
                ([(0, H, H), (0, 2, C)], [(0, H, H), (0, 2, C)]),
                ([(0, H, H), (5, 7, C)], [(0, H, H), (2, 4, C)]),
                ([(0, H, H), (10, 12, C)], [(0, H, H), (4, 6, C)]),
            ]
        }
    }
    spec_scatter = LoadSpec(scattered_entries).src_shard_spec
    assert sorted(spec_scatter["hf_scatter"], key=lambda s: s[1][0]) == [
        [(0, H, H), (0, 2, C)],
        [(0, H, H), (5, 7, C)],
        [(0, H, H), (10, 12, C)],
    ]

    # --- Case 4 (2D): two disjoint rectangles (different row *and* column ranges), no overlap ---
    disjoint_entries = {
        "hf_disjoint": {
            "wk_disjoint": [
                ([(0, 2, H), (0, 4, C)], [(0, 2, H), (0, 4, C)]),
                ([(2, H, H), (8, 12, C)], [(2, H, H), (4, 8, C)]),
            ]
        }
    }
    spec_disjoint = LoadSpec(disjoint_entries).src_shard_spec
    assert sorted(spec_disjoint["hf_disjoint"], key=lambda s: (s[0][0], s[1][0])) == [
        [(0, 2, H), (0, 4, C)],
        [(2, H, H), (8, 12, C)],
    ]

    # --- Case 5 (1D): pseudo-random non-adjacent intervals on one axis ---
    W1 = 32
    random_1d = {
        "hf_rand1d": {
            "wk_rand1d": [
                ([(1, 3, W1)], [(0, 2, W1)]),
                ([(11, 15, W1)], [(2, 6, W1)]),
                ([(20, 24, W1)], [(6, 10, W1)]),
            ]
        }
    }
    spec_1d = LoadSpec(random_1d).src_shard_spec
    assert sorted(spec_1d["hf_rand1d"], key=lambda s: s[0][0]) == [
        [(1, 3, W1)],
        [(11, 15, W1)],
        [(20, 24, W1)],
    ]

    # --- Case 6 (1D): second 1D tensor, different width and scattered ranges ---
    W2 = 48
    random_1d_b = {
        "hf_rand1d_b": {
            "wk_rand1d_b": [
                ([(0, 5, W2)], [(0, 5, W2)]),
                ([(7, 11, W2)], [(5, 9, W2)]),
                ([(30, 36, W2)], [(9, 15, W2)]),
            ]
        }
    }
    spec_1d_b = LoadSpec(random_1d_b).src_shard_spec
    assert sorted(spec_1d_b["hf_rand1d_b"], key=lambda s: s[0][0]) == [
        [(0, 5, W2)],
        [(7, 11, W2)],
        [(30, 36, W2)],
    ]


def test_infer_load_spec_transpose(device: torch.device) -> None:
    """Transpose mapping: HF (A, B) → wk (B, A) via t.T.contiguous()."""
    A, B = 6, 4
    hfsd = {
        "model.linear.weight": torch.randn(A, B),
    }
    wksd = {
        "linear.weight": torch.zeros(B, A, device=device),
    }

    def lw(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            if name == "model.linear.weight":
                wksd["linear.weight"].copy_(fn().T.contiguous())

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    load_spec = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, lw)

    # Verify the LoadSpec can reproduce the weights
    lw(hf_weights)
    verify_load_spec(hf_weights, lambda: wksd, load_spec)


def test_infer_load_spec_single_row_col(device: torch.device) -> None:
    """Single-row and single-column shards (width-1 or height-1 boxes)."""
    H, W = 8, 4
    hfsd = {
        "model.row_weight": torch.randn(1, W),
        "model.col_weight": torch.randn(H, 1),
    }
    wksd = {
        "row_weight": torch.zeros(1, W, device=device),
        "col_weight": torch.zeros(H, 1, device=device),
    }

    def lw(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            if name == "model.row_weight":
                wksd["row_weight"].copy_(fn())
            elif name == "model.col_weight":
                wksd["col_weight"].copy_(fn())

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    load_spec = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, lw)
    spec = load_spec.src_shard_spec
    assert "model.row_weight" in spec.entries
    assert "model.col_weight" in spec.entries

    # Verify correctness
    lw(hf_weights)
    verify_load_spec(hf_weights, lambda: wksd, load_spec)


def test_infer_load_spec_l_shape(device: torch.device) -> None:
    """L-shaped mapping: two rectangular regions from same HF tensor with different spans."""
    H, W = 8, 8
    hfsd = {
        "model.weight": torch.randn(H, W),
    }
    # Worker tensor is 6×4 — top 4 rows get cols [0:4], bottom 2 rows get cols [4:8]
    wksd = {
        "weight": torch.zeros(6, 4, device=device),
    }

    def lw(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            if name == "model.weight":
                t = fn()
                # Top 4 rows: copy cols [0:4] from HF rows [0:4]
                wksd["weight"][:4, :].copy_(t[:4, :4])
                # Bottom 2 rows: copy cols [4:8] from HF rows [4:6]
                wksd["weight"][4:, :].copy_(t[4:6, 4:8])

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    load_spec = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, lw)

    # Should decompose into 2 mappings
    wk_mappings = load_spec.entries.get("model.weight", {}).get("weight", [])
    assert len(wk_mappings) == 2, (
        f"Expected 2 mappings for L-shape, got {len(wk_mappings)}"
    )

    # Verify correctness
    lw(hf_weights)
    verify_load_spec(hf_weights, lambda: wksd, load_spec)


def test_infer_load_spec_moe_many_names_overflow(device: torch.device) -> None:
    """MoE-like: many HF tensors → one fused 3D wksd tensor.

    With many HF names, n_bits is large (≥15) so the first coordinate's
    bit offset ≥ ele_bits (16 for bf16).  The structured-index stride
    when moving right through the wksd tensor wraps in int16, which
    previously caused diff2 to produce O(numel) nonzeros instead of O(1).
    """
    N_EXPERTS = 8
    EXPERT_H, EXPERT_W = 4, 6

    # Create enough HF tensors to push n_bits high.
    # n_bits = ceil(log2(N)), so N ≥ 2^14 = 16384 → n_bits = 15.
    N_PAD = 16384
    hfsd = {}
    for i in range(N_EXPERTS):
        hfsd[f"model.experts.{i}.weight"] = torch.randn(EXPERT_H, EXPERT_W)
    for i in range(N_PAD):
        hfsd[f"model.pad.{i}.weight"] = torch.randn(1, 1)

    wksd = {
        "experts.weight": torch.zeros(N_EXPERTS, EXPERT_H, EXPERT_W, device=device),
    }

    def lw(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            if name.startswith("model.experts."):
                idx = int(name.split(".")[2])
                if idx < N_EXPERTS:
                    wksd["experts.weight"][idx].copy_(fn())

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    load_spec = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, lw)

    expert_count = 0
    for hf_name, wk_map in load_spec.entries.items():
        if "experts.weight" in wk_map:
            expert_count += len(wk_map["experts.weight"])
    assert expert_count == N_EXPERTS, (
        f"Expected {N_EXPERTS} expert mappings, got {expert_count}"
    )

    lw(hf_weights)
    verify_load_spec(hf_weights, lambda: wksd, load_spec)


def test_infer_load_spec_moe_transpose_overflow(device: torch.device) -> None:
    """MoE + transpose: HF (H, W) → wksd[i] = (W, H) via .T.

    Combines the overflow scenario (many names → large n_bits) with
    transpose — the exact pattern causing 30M diff2 nonzeros in
    Qwen3-30B-A3B before the fix.
    """
    N_EXPERTS = 4
    EXPERT_H, EXPERT_W = 4, 6
    N_PAD = 16384

    hfsd = {}
    for i in range(N_EXPERTS):
        hfsd[f"model.experts.{i}.down_proj"] = torch.randn(EXPERT_H, EXPERT_W)
    for i in range(N_PAD):
        hfsd[f"model.pad.{i}.w"] = torch.randn(1, 1)

    wksd = {
        "experts.w2_weight": torch.zeros(N_EXPERTS, EXPERT_W, EXPERT_H, device=device),
    }

    def lw(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            if name.startswith("model.experts.") and "down_proj" in name:
                idx = int(name.split(".")[2])
                if idx < N_EXPERTS:
                    wksd["experts.w2_weight"][idx].copy_(fn().T)

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    load_spec = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, lw)

    lw(hf_weights)
    verify_load_spec(hf_weights, lambda: wksd, load_spec)


def test_carry_aware_moe_down_proj(device: torch.device) -> None:
    """Carry-aware diff2 with realistic MoE down_proj dimensions.

    Simplified Qwen3-30B-A3B down_proj:
      HF: model.experts.{i}.down_proj → (768, 256) per expert
      Worker: experts.w2_weight → (8, 256, 768) — transposed!
      lw: wksd[idx].copy_(fn().T)

    With 16K+ pad names, compact n_bits = 15.  For bf16 (ele_bits=16):
      Total bits: 15 + 10 + 8 = 33 → 3 chunks of 16 bits
      Chunk 0 [0,16): name(15) + 1 bit of x_0 → carries
      Chunk 1 [16,32): 9 bits of x_0 + 7 bits of x_1 → carries
      Chunk 2 [32,48): 1 bit of x_1 + sign

    Without carry-aware probing, chunks 0 and 1 would produce O(dim) nnz.
    With carry propagation, each chunk's remainder has O(N_EXPERTS) nnz.
    """
    N_EXPERTS = 8
    EXPERT_H, EXPERT_W = 768, 256  # real 30B dims (HF shape)

    # 16K pad names → n_bits = ceil(log2(16K + 8 + 1)) = 15
    N_PAD = 16384
    hfsd: dict[str, torch.Tensor] = {}
    for i in range(N_EXPERTS):
        hfsd[f"model.experts.{i}.down_proj"] = torch.randn(EXPERT_H, EXPERT_W)
    for i in range(N_PAD):
        hfsd[f"model.pad.{i}.w"] = torch.randn(1, 1)

    wksd = {
        "experts.w2_weight": torch.zeros(
            N_EXPERTS,
            EXPERT_W,
            EXPERT_H,
            device=device,
            dtype=torch.bfloat16,
        ),
    }

    def lw(hf_weights: HFWeightFetcher) -> None:
        for name, fn in hf_weights.items():
            if name.startswith("model.experts.") and "down_proj" in name:
                idx = int(name.split(".")[2])
                if idx < N_EXPERTS:
                    wksd["experts.w2_weight"][idx].copy_(fn().T)

    hf_weights, hf_shapes = _hfsd_to_fetcher(hfsd)
    load_spec = infer_load_spec(hf_weights, hf_shapes, lambda: wksd, lw)

    # Verify correctness
    lw(hf_weights)
    verify_load_spec(hf_weights, lambda: wksd, load_spec)

    # Verify we found all expert mappings
    expert_count = 0
    for hf_name, wk_map in load_spec.entries.items():
        if "experts.w2_weight" in wk_map:
            expert_count += len(wk_map["experts.w2_weight"])
    assert expert_count == N_EXPERTS, (
        f"Expected {N_EXPERTS} expert mappings, got {expert_count}"
    )
