# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
One decoder-block Qwen2-style toy (no ``model.layers.N`` in HF keys).

**HF checkpoint** — split ``q_proj`` / ``k_proj`` / ``v_proj``, ``gate_proj`` / ``up_proj``, etc.

**Trainer Worker / actor** — Megatron ``linear_qkv`` packing (GQA): per group, Q heads then K then V
(see ``slime/.../megatron_to_hf/qwen2.py``). TP splits output rows of each parameter.

**Rollout Worker** — SGLang-style stacked tensors (same idea as ``Qwen2ForCausalLM.load_weights`` in
``sglang/srt/models/qwen2.py``): ``qkv_proj`` holds ``[Q_shard; K_shard; V_shard]`` on dim 0;
``gate_up_proj`` holds ``[gate; up]`` with column-parallel-style row sharding. TP matches
``QKVParallelLinear`` / ``MergedColumnParallelLinear`` / ``RowParallelLinear`` when ``num_heads``
and ``num_kv_heads`` divide ``tp_size``.

Loaders only **narrow + copy** for keys present in the iterable (``infer_load_spec`` subsets).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

import torch

__all__ = [
    "DEFAULT_QWEN_TINY_CONFIG",
    "QwenTinyConfig",
    "build_trainer_wksd",
    "build_qwen_tiny_hf_checkpoint",
    "build_rollout_wksd",
    "make_trainer_load_weights",
    "make_rollout_load_weights",
]


@dataclass(frozen=True)
class QwenTinyConfig:
    vocab_size: int = 64
    hidden_size: int = 32
    intermediate_size: int = 48
    num_attn_heads: int = 4
    num_kv_heads: int = 2

    def __post_init__(self) -> None:
        if self.num_attn_heads % self.num_kv_heads != 0:
            raise ValueError("num_attn_heads must be divisible by num_kv_heads")

    @property
    def head_dim(self) -> int:
        return self.hidden_size // self.num_attn_heads

    @property
    def q_out(self) -> int:
        return self.num_attn_heads * self.head_dim

    @property
    def kv_out(self) -> int:
        return self.num_kv_heads * self.head_dim

    @property
    def num_query_groups(self) -> int:
        return self.num_kv_heads

    @property
    def value_num_per_group(self) -> int:
        return self.num_attn_heads // self.num_query_groups

    @property
    def megatron_qkv_rows(self) -> int:
        g, v, d = self.num_query_groups, self.value_num_per_group, self.head_dim
        return g * (v + 2) * d


DEFAULT_QWEN_TINY_CONFIG = QwenTinyConfig()


def _assert_tp(cfg: QwenTinyConfig, tp: int) -> None:
    assert cfg.vocab_size % tp == 0
    assert cfg.q_out % tp == 0
    assert cfg.kv_out % tp == 0
    assert cfg.intermediate_size % tp == 0
    assert cfg.megatron_qkv_rows % tp == 0
    assert (2 * cfg.intermediate_size) % tp == 0


def _scatter_rows(
    dst_shard: torch.Tensor,
    shard_global_lo: int,
    shard_nrows: int,
    src: torch.Tensor,
    src_row_offset: int,
    block_global_lo: int,
    block_nrows: int,
    *,
    device: torch.device,
    dtype: torch.dtype,
) -> None:
    a = max(shard_global_lo, block_global_lo)
    b = min(shard_global_lo + shard_nrows, block_global_lo + block_nrows)
    if a >= b:
        return
    sr = src_row_offset + (a - block_global_lo)
    dr = a - shard_global_lo
    dst_shard[dr : dr + (b - a)].copy_(
        src[sr : sr + (b - a)].to(device=device, dtype=dtype)
    )


def _trainer_scatter_megatron_qkv(
    dst: torch.Tensor,
    cfg: QwenTinyConfig,
    tp_rank: int,
    tp_size: int,
    w: dict[str, torch.Tensor],
    *,
    device: torch.device,
    dtype: torch.dtype,
) -> None:
    R, part = cfg.megatron_qkv_rows, cfg.megatron_qkv_rows // tp_size
    lo, n_g = tp_rank * part, cfg.num_query_groups
    vpg, d = cfg.value_num_per_group, cfg.head_dim
    if "self_attn.q_proj.weight" in w:
        q = w["self_attn.q_proj.weight"]
        for g in range(n_g):
            gr = g * (vpg + 2) * d
            _scatter_rows(
                dst, lo, part, q, g * vpg * d, gr, vpg * d, device=device, dtype=dtype
            )
    if "self_attn.k_proj.weight" in w:
        k = w["self_attn.k_proj.weight"]
        for g in range(n_g):
            gr = g * (vpg + 2) * d + vpg * d
            _scatter_rows(dst, lo, part, k, g * d, gr, d, device=device, dtype=dtype)
    if "self_attn.v_proj.weight" in w:
        v = w["self_attn.v_proj.weight"]
        for g in range(n_g):
            gr = g * (vpg + 2) * d + (vpg + 1) * d
            _scatter_rows(dst, lo, part, v, g * d, gr, d, device=device, dtype=dtype)


def build_qwen_tiny_hf_checkpoint(
    cfg: QwenTinyConfig,
    *,
    dtype: torch.dtype = torch.float32,
    seed: int = 42,
    device: str = "cpu",
) -> dict[str, torch.Tensor]:
    g = torch.Generator(device=device).manual_seed(seed)
    h, iq, ikv = cfg.hidden_size, cfg.intermediate_size, cfg.kv_out
    qo, v = cfg.q_out, cfg.vocab_size

    def R(*s: int) -> torch.Tensor:
        return torch.randn(*s, dtype=dtype, device=device, generator=g)

    return {
        "model.embed_tokens.weight": R(v, h),
        "self_attn.q_proj.weight": R(qo, h),
        "self_attn.k_proj.weight": R(ikv, h),
        "self_attn.v_proj.weight": R(ikv, h),
        "self_attn.o_proj.weight": R(h, qo),
        "mlp.gate_proj.weight": R(iq, h),
        "mlp.up_proj.weight": R(iq, h),
        "mlp.down_proj.weight": R(h, iq),
        "input_layernorm.weight": R(h),
        "post_attention_layernorm.weight": R(h),
    }


def _actor_shapes(cfg: QwenTinyConfig, tp: int) -> dict[str, tuple[int, ...]]:
    _assert_tp(cfg, tp)
    h, iq = cfg.hidden_size, cfg.intermediate_size
    v0, qkv0 = cfg.vocab_size // tp, cfg.megatron_qkv_rows // tp
    q0, i0 = cfg.q_out // tp, cfg.intermediate_size // tp
    return {
        "embedding.word_embeddings.weight": (v0, h),
        "self_attention.linear_qkv.weight": (qkv0, h),
        "self_attention.linear_proj.weight": (h, q0),
        "mlp.linear_fc1.weight": (2 * iq // tp, h),
        "mlp.linear_fc2.weight": (h, i0),
        "self_attention.linear_qkv.layer_norm_weight": (h,),
        "mlp.linear_fc1.layer_norm_weight": (h,),
    }


def build_trainer_wksd(
    cfg: QwenTinyConfig,
    *,
    device: str,
    dtype: torch.dtype = torch.float32,
    tp_rank: int = 0,
    tp_size: int = 1,
) -> dict[str, torch.Tensor]:
    del tp_rank
    return {
        k: torch.empty(s, dtype=dtype, device=device)
        for k, s in _actor_shapes(cfg, tp_size).items()
    }


def trainer_load_weights(
    weights: Iterable[tuple[str, torch.Tensor]],
    wksd: dict[str, torch.Tensor],
    cfg: QwenTinyConfig,
    *,
    device: torch.device,
    dtype: torch.dtype,
    tp_rank: int,
    tp_size: int,
) -> None:
    _assert_tp(cfg, tp_size)
    w = dict(weights)
    iq, h = cfg.intermediate_size, cfg.hidden_size
    q0, i0 = cfg.q_out // tp_size, cfg.intermediate_size // tp_size
    fc1_part = 2 * iq // tp_size
    fc1_lo = tp_rank * fc1_part

    if "model.embed_tokens.weight" in w:
        vp = cfg.vocab_size // tp_size
        sl = slice(tp_rank * vp, (tp_rank + 1) * vp)
        wksd["embedding.word_embeddings.weight"].copy_(
            w["model.embed_tokens.weight"][sl].to(device=device, dtype=dtype)
        )

    if any(
        k in w
        for k in (
            "self_attn.q_proj.weight",
            "self_attn.k_proj.weight",
            "self_attn.v_proj.weight",
        )
    ):
        _trainer_scatter_megatron_qkv(
            wksd["self_attention.linear_qkv.weight"],
            cfg,
            tp_rank,
            tp_size,
            w,
            device=device,
            dtype=dtype,
        )

    if "self_attn.o_proj.weight" in w:
        o = w["self_attn.o_proj.weight"]
        c0, c1 = tp_rank * q0, (tp_rank + 1) * q0
        wksd["self_attention.linear_proj.weight"].copy_(
            o[:, c0:c1].to(device=device, dtype=dtype)
        )

    if "mlp.gate_proj.weight" in w:
        _scatter_rows(
            wksd["mlp.linear_fc1.weight"],
            fc1_lo,
            fc1_part,
            w["mlp.gate_proj.weight"],
            0,
            0,
            iq,
            device=device,
            dtype=dtype,
        )
    if "mlp.up_proj.weight" in w:
        _scatter_rows(
            wksd["mlp.linear_fc1.weight"],
            fc1_lo,
            fc1_part,
            w["mlp.up_proj.weight"],
            0,
            iq,
            iq,
            device=device,
            dtype=dtype,
        )

    if "mlp.down_proj.weight" in w:
        c0, c1 = tp_rank * i0, (tp_rank + 1) * i0
        wksd["mlp.linear_fc2.weight"].copy_(
            w["mlp.down_proj.weight"][:, c0:c1].to(device=device, dtype=dtype)
        )

    if "input_layernorm.weight" in w:
        wksd["self_attention.linear_qkv.layer_norm_weight"].copy_(
            w["input_layernorm.weight"].to(device=device, dtype=dtype)
        )
    if "post_attention_layernorm.weight" in w:
        wksd["mlp.linear_fc1.layer_norm_weight"].copy_(
            w["post_attention_layernorm.weight"].to(device=device, dtype=dtype)
        )


def make_trainer_load_weights(
    wksd: dict[str, torch.Tensor],
    cfg: QwenTinyConfig,
    *,
    device: str,
    dtype: torch.dtype,
    tp_rank: int,
    tp_size: int,
) -> Callable[[Iterable[tuple[str, torch.Tensor]]], None]:
    dev = torch.device(device)

    def lw(it: Iterable[tuple[str, torch.Tensor]]) -> None:
        trainer_load_weights(
            it, wksd, cfg, device=dev, dtype=dtype, tp_rank=tp_rank, tp_size=tp_size
        )

    return lw


def _rollout_shapes(cfg: QwenTinyConfig, tp: int) -> dict[str, tuple[int, ...]]:
    _assert_tp(cfg, tp)
    h, iq = cfg.hidden_size, cfg.intermediate_size
    q_loc, kv_loc = cfg.q_out // tp, cfg.kv_out // tp
    qkv_r = q_loc + 2 * kv_loc
    return {
        "model.embed_tokens.weight": (cfg.vocab_size // tp, h),
        "self_attn.qkv_proj.weight": (qkv_r, h),
        "self_attn.o_proj.weight": (h, q_loc),
        "mlp.gate_up_proj.weight": (2 * iq // tp, h),
        # down_proj stored TRANSPOSED (in, out) — mimics a Megatron RowParallel weight; forces the
        # LoadSpec to carry a transpose (negative w) so the transfer path must handle it.
        "mlp.down_proj.weight": (iq // tp, h),
        "input_layernorm.weight": (h,),
        "post_attention_layernorm.weight": (h,),
    }


def build_rollout_wksd(
    cfg: QwenTinyConfig,
    *,
    device: str,
    dtype: torch.dtype = torch.float32,
    tp_rank: int = 0,
    tp_size: int = 1,
) -> dict[str, torch.Tensor]:
    del tp_rank
    return {
        k: torch.empty(s, dtype=dtype, device=device)
        for k, s in _rollout_shapes(cfg, tp_size).items()
    }


def rollout_load_weights(
    weights: Iterable[tuple[str, torch.Tensor]],
    wksd: dict[str, torch.Tensor],
    cfg: QwenTinyConfig,
    *,
    device: torch.device,
    dtype: torch.dtype,
    tp_rank: int,
    tp_size: int,
) -> None:
    """Map HF shards into Rollout Worker ``wksd`` (stacked qkv / gate_up, TP on outputs or inputs)."""
    _assert_tp(cfg, tp_size)
    w = dict(weights)
    iq, h = cfg.intermediate_size, cfg.hidden_size
    q_loc, kv_loc = cfg.q_out // tp_size, cfg.kv_out // tp_size
    i0 = iq // tp_size

    if "model.embed_tokens.weight" in w:
        vp = cfg.vocab_size // tp_size
        sl = slice(tp_rank * vp, (tp_rank + 1) * vp)
        wksd["model.embed_tokens.weight"].copy_(
            w["model.embed_tokens.weight"][sl].to(device=device, dtype=dtype)
        )

    qkv = wksd["self_attn.qkv_proj.weight"]
    if "self_attn.q_proj.weight" in w:
        sl = slice(tp_rank * q_loc, (tp_rank + 1) * q_loc)
        qkv[:q_loc].copy_(
            w["self_attn.q_proj.weight"][sl].to(device=device, dtype=dtype)
        )
    if "self_attn.k_proj.weight" in w:
        sl = slice(tp_rank * kv_loc, (tp_rank + 1) * kv_loc)
        qkv[q_loc : q_loc + kv_loc].copy_(
            w["self_attn.k_proj.weight"][sl].to(device=device, dtype=dtype)
        )
    if "self_attn.v_proj.weight" in w:
        sl = slice(tp_rank * kv_loc, (tp_rank + 1) * kv_loc)
        qkv[q_loc + kv_loc :].copy_(
            w["self_attn.v_proj.weight"][sl].to(device=device, dtype=dtype)
        )

    if "self_attn.o_proj.weight" in w:
        c0, c1 = tp_rank * q_loc, (tp_rank + 1) * q_loc
        wksd["self_attn.o_proj.weight"].copy_(
            w["self_attn.o_proj.weight"][:, c0:c1].to(device=device, dtype=dtype)
        )

    part = 2 * iq // tp_size
    glo = tp_rank * part
    if "mlp.gate_proj.weight" in w:
        _scatter_rows(
            wksd["mlp.gate_up_proj.weight"],
            glo,
            part,
            w["mlp.gate_proj.weight"],
            0,
            0,
            iq,
            device=device,
            dtype=dtype,
        )
    if "mlp.up_proj.weight" in w:
        _scatter_rows(
            wksd["mlp.gate_up_proj.weight"],
            glo,
            part,
            w["mlp.up_proj.weight"],
            0,
            iq,
            iq,
            device=device,
            dtype=dtype,
        )

    if "mlp.down_proj.weight" in w:
        c0, c1 = tp_rank * i0, (tp_rank + 1) * i0
        # store TRANSPOSED: HF [h, iq] -> slice [h, i0] -> .t() -> param [i0, h]
        wksd["mlp.down_proj.weight"].copy_(
            w["mlp.down_proj.weight"][:, c0:c1].t().to(device=device, dtype=dtype)
        )

    if "input_layernorm.weight" in w:
        wksd["input_layernorm.weight"].copy_(
            w["input_layernorm.weight"].to(device=device, dtype=dtype)
        )
    if "post_attention_layernorm.weight" in w:
        wksd["post_attention_layernorm.weight"].copy_(
            w["post_attention_layernorm.weight"].to(device=device, dtype=dtype)
        )


def make_rollout_load_weights(
    wksd: dict[str, torch.Tensor],
    cfg: QwenTinyConfig,
    *,
    device: str,
    dtype: torch.dtype,
    tp_rank: int,
    tp_size: int,
) -> Callable[[Iterable[tuple[str, torch.Tensor]]], None]:
    dev = torch.device(device)

    def lw(it: Iterable[tuple[str, torch.Tensor]]) -> None:
        rollout_load_weights(
            it, wksd, cfg, device=dev, dtype=dtype, tp_rank=tp_rank, tp_size=tp_size
        )

    return lw
