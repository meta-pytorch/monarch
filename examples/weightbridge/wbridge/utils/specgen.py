# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Infer a :class:`~wbridge.utils.data.LoadSpec` by probing HF weight factories
through a ``load_weights``-style callable.

Algorithm overview
~~~~~~~~~~~~~~~~~~
1. **Index encoding.** Each HF tensor element gets a unique structured index
   ``name_idx | x_0 | x_1 | … | x_{d-1}`` (bit-concatenation, name in the
   lowest bits).  Name indices are 1-based so that 0 identifies unmapped
   worker elements.

2. **Dtype-grouped probing.** Worker tensors are partitioned by dtype.  For
   each dtype group the index tensor is chunked to the group's element width,
   and one full ``load_weights`` call per chunk copies the encoded indices
   into the worker state dict.  Across groups this yields O(G × C)
   ``load_weights`` calls, where *G* is the number of distinct worker dtypes
   (typically 1–2) and *C = ceil(max_index_bits / element_bits)* (typically
   1–3).

3. **Shard extraction.** From the per-element index map the name bits
   identify which HF tensor each worker element came from; the coordinate
   bits give the source position.  Axis-aligned rectangular boxes are
   detected with the same successor / corner logic used previously.

Core assumption
~~~~~~~~~~~~~~~
``lw`` must accept HF tensors in any dtype.  For each element whose HF
tensor has the **same dtype** as its destination worker tensor, ``lw`` must
preserve the element value exactly (pure bijective copy/scatter — no
arithmetic).  Elements crossing a dtype boundary may be corrupted; only
same-dtype pairs are relied upon.

The full-iterator principle
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Every ``lw`` call receives the complete dict of HF tensor name→factory
mappings (though with probe values rather than real weights).  This avoids
problems with ``load_weights`` implementations that perform inter-rank
communication (e.g. broadcasting shared embeddings when PP > 1).
"""

from __future__ import annotations

import logging
import math
import os
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any

import torch
from wbridge.utils.data import LoadSpec, logical_tensor_name, Shard, ShardMapping

HFWeightFetcher = dict[str, Callable[[], torch.Tensor]]
"""Mapping from HF tensor names to zero-arg callables that return the tensor."""

LoadWeightsFn = Callable[[HFWeightFetcher], Any]
"""A ``load_weights``-style callable that accepts an :data:`HFWeightFetcher`."""

WksdFactory = Callable[[], dict[str, torch.Tensor]]
"""Zero-arg callable returning a fresh GPU worker state dict snapshot.

Frameworks that replace parameter tensors during ``load_weights`` (e.g. SGLang MoE
expert fusion) must return a new snapshot on each call so that specgen reads correct
values and old tensors can be freed by GC."""

# Default cap on total CPU storage for HF placeholder tensors per infer batch.
DEFAULT_MAX_HF_BYTES = 20 * 1024**3

# Structured indices are represented as int64 while they are built and
# compressed.  Keep every logical/source and worker probe slice below this
# size; the actual arithmetic is tiled more finely to leave headroom for
# temporary operands used by bit packing, diff2, remainder, and carry.
DEFAULT_SPECGEN_I64_CAP_BYTES = 512 * 1024**2
SPECGEN_I64_CAP_ENV = "WBRIDGE_SPECGEN_I64_CAP_BYTES"
SPECGEN_PROBE_DEVICE_ENV = "WBRIDGE_SPECGEN_PROBE_DEVICE"
_SPECGEN_BUILD_TILE_BYTES = 64 * 1024**2
_SPECGEN_DIFF_TILE_BYTES = 64 * 1024**2
_SPECGEN_CUDA_BUILD_RESERVE_BYTES = 512 * 1024**2
# When complete probe sources cannot safely remain in HBM, keep the
# loader-visible tensor on CPU but populate it using bounded CUDA row tiles.
# This cap is based on the emitted checkpoint dtype, not the temporary int64
# representation governed by DEFAULT_SPECGEN_I64_CAP_BYTES.
_SPECGEN_CUDA_ROW_TILE_BYTES = 512 * 1024**2
# SGLang's checkpoint loader may retain the current source tensor while it
# asks the fetcher for the next one.  Budgeting only one physical source can
# therefore pass the preflight check and still OOM on the following fetch.
_SPECGEN_CUDA_MAX_LIVE_SOURCE_TENSORS = 2

logger = logging.getLogger(__name__)


def _select_probe_build_device(
    hf_names: list[str],
    source_pieces: dict[str, list["_LogicalSourcePiece"]],
    group_dtype: torch.dtype,
) -> torch.device:
    """Choose whether complete loader-visible probes may remain on CUDA.

    A model loader normally accepts CPU checkpoint tensors and copies only the
    local shard to GPU.  Prefer complete CUDA probes when physical sources fit
    with conservative scratch headroom.  A CPU result selects bounded staging:
    probes up to :data:`_SPECGEN_CUDA_ROW_TILE_BYTES` still remain on CUDA, and
    larger loader-visible CPU tensors are populated one CUDA-built row tile at
    a time.  Probe arithmetic therefore remains on CUDA.  The loader is allowed
    to retain one source while fetching the next, so auto placement budgets two
    largest-source tensors.

    ``WBRIDGE_SPECGEN_PROBE_DEVICE=cpu|cuda`` is an explicit diagnostic
    override; the default is ``auto``.
    """
    requested = os.environ.get(SPECGEN_PROBE_DEVICE_ENV, "auto").strip().lower()
    if requested not in {"auto", "cpu", "cuda"}:
        raise ValueError(
            f"{SPECGEN_PROBE_DEVICE_ENV} must be auto, cpu, or cuda, got {requested!r}"
        )
    if requested == "cpu" or not torch.cuda.is_available():
        return torch.device("cpu")
    if requested == "cuda":
        return torch.device("cuda")

    max_source_bytes = max(
        (
            math.prod(source_pieces[name][0].full_shape) * group_dtype.itemsize
            for name in hf_names
        ),
        default=0,
    )
    # Release only unused allocator cache before measuring.  Live model and
    # KV-cache allocations remain accounted for by mem_get_info().
    torch.cuda.empty_cache()
    free_bytes, _total_bytes = torch.cuda.mem_get_info()
    required_bytes = (
        _SPECGEN_CUDA_MAX_LIVE_SOURCE_TENSORS * max_source_bytes
        + _SPECGEN_CUDA_BUILD_RESERVE_BYTES
    )
    device = torch.device("cuda" if required_bytes <= free_bytes else "cpu")
    logger.info(
        "specgen probe source mode=%s for %s: largest source=%.2f GiB, "
        "CUDA free=%.2f GiB, required for %d live sources with reserve=%.2f GiB",
        "cuda-resident" if device.type == "cuda" else "cuda-row-staged",
        group_dtype,
        max_source_bytes / 1024**3,
        free_bytes / 1024**3,
        _SPECGEN_CUDA_MAX_LIVE_SOURCE_TENSORS,
        required_bytes / 1024**3,
    )
    return device


@dataclass(frozen=True)
class _LogicalSourcePiece:
    """One dim-0 interval in the structured-index namespace.

    ``logical_name`` gets its own encoded tensor id, but coordinates retain
    the physical tensor's coordinate system.  This lets the load-weights
    callable continue receiving the original tensor name and shape while the
    final LoadSpec can be canonicalised by renaming and merging only.
    """

    physical_name: str
    logical_name: str
    start: int
    end: int
    full_shape: tuple[int, ...]


@dataclass
class _SparseWorkerSlice:
    """Sparse probe chunks for one bounded flattened-row worker slice.

    Worker tensors are viewed as ``(-1, shape[-1])`` for ndim >= 2.  This
    keeps a fused expert tensor such as ``(experts, rows, cols)`` tileable
    even when one dim-0 slab is larger than the execution-memory cap.
    """

    row_start: int
    row_end: int
    shape: tuple[int, ...]
    chunks: list[torch.Tensor] = field(default_factory=list)


def specgen_i64_cap_bytes() -> int | float:
    """Return the configured int64 probe cap; ``inf`` disables splitting."""
    raw = (
        os.environ.get(
            SPECGEN_I64_CAP_ENV,
            str(DEFAULT_SPECGEN_I64_CAP_BYTES),
        )
        .strip()
        .lower()
    )
    if raw in {"inf", "+inf", "infinity", "+infinity"}:
        return math.inf
    try:
        cap = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{SPECGEN_I64_CAP_ENV} must be a positive integer or 'inf', got {raw!r}"
        ) from exc
    if cap <= 0:
        raise ValueError(f"{SPECGEN_I64_CAP_ENV} must be positive or 'inf', got {cap}")
    return cap


def _resolve_i64_cap(cap: int | float | None) -> int | float:
    resolved = specgen_i64_cap_bytes() if cap is None else cap
    if isinstance(resolved, float) and math.isinf(resolved):
        return math.inf
    try:
        resolved_int = int(resolved)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            f"specgen int64 cap must be a positive integer or inf, got {resolved!r}"
        ) from exc
    if resolved_int <= 0:
        raise ValueError(
            f"specgen int64 cap must be positive or inf, got {resolved_int}"
        )
    return resolved_int


def _plan_logical_sources(
    hf_shapes: dict[str, tuple[int, ...]],
    i64_cap_bytes: int | float,
) -> tuple[dict[str, list[_LogicalSourcePiece]], dict[str, tuple[int, ...]]]:
    """Split oversized encoded HF tensors on dim 0.

    The logical tensor's decoded coordinates deliberately use ``full_shape``
    rather than a piece-local shape.  Therefore its inferred source shards are
    already expressed in physical checkpoint coordinates.
    """
    by_physical: dict[str, list[_LogicalSourcePiece]] = {}
    logical_shapes: dict[str, tuple[int, ...]] = {}

    for name in sorted(hf_shapes):
        shape = hf_shapes[name]
        numel = math.prod(shape)
        should_split = (
            not math.isinf(i64_cap_bytes)
            and shape
            and numel * torch.int64.itemsize > i64_cap_bytes
        )
        if not should_split:
            piece = _LogicalSourcePiece(name, name, 0, shape[0] if shape else 1, shape)
            by_physical[name] = [piece]
            logical_shapes[name] = shape
            continue

        row_i64_bytes = math.prod(shape[1:]) * torch.int64.itemsize
        if row_i64_bytes > i64_cap_bytes:
            raise ValueError(
                f"cannot cap structured index for {name!r} at {i64_cap_bytes} bytes by "
                f"splitting dim 0: one row is {row_i64_bytes} bytes (shape={shape})"
            )
        rows_per_piece = max(1, int(i64_cap_bytes) // row_i64_bytes)
        pieces: list[_LogicalSourcePiece] = []
        for start in range(0, shape[0], rows_per_piece):
            end = min(start + rows_per_piece, shape[0])
            lname = logical_tensor_name(name, start, end)
            piece = _LogicalSourcePiece(name, lname, start, end, shape)
            pieces.append(piece)
            logical_shapes[lname] = shape
        by_physical[name] = pieces

    return by_physical, logical_shapes


def _plan_worker_slices(
    shape: tuple[int, ...],
    i64_cap_bytes: int | float,
) -> list[tuple[int, int, tuple[int, ...]]]:
    """Return bounded flattened-row slices for sparse compression.

    Diff2 already treats every ndim >= 2 tensor as a matrix whose columns
    are the physical last dimension.  Tile that same matrix by rows instead
    of requiring a complete dim-0 slab to fit.  For fused MoE tensors this
    changes the indivisible unit from ``rows * cols`` to only ``cols``.
    """
    if not shape:
        return [(0, 1, shape)]
    # Diff/remainder/carry have overlapping live temporaries.  A smaller
    # execution tile than the logical cap keeps peak GPU memory predictable.
    exec_cap = _SPECGEN_DIFF_TILE_BYTES
    if not math.isinf(i64_cap_bytes):
        exec_cap = min(exec_cap, int(i64_cap_bytes))
    if len(shape) == 1:
        rows = shape[0]
        row_width = 1
    else:
        rows = math.prod(shape[:-1])
        row_width = shape[-1]
    row_i64_bytes = row_width * torch.int64.itemsize
    if row_i64_bytes > exec_cap:
        raise ValueError(
            f"cannot bound worker structured index at {exec_cap} bytes by splitting "
            f"flattened rows: the last dimension is {row_i64_bytes} bytes "
            f"(shape={shape})"
        )
    rows_per_slice = max(1, exec_cap // row_i64_bytes)
    return [
        (
            start,
            min(start + rows_per_slice, rows),
            (
                (min(start + rows_per_slice, rows) - start,)
                if len(shape) == 1
                else (min(start + rows_per_slice, rows) - start, shape[-1])
            ),
        )
        for start in range(0, rows, rows_per_slice)
    ]


def verify_load_spec(
    hf_weights: HFWeightFetcher,
    wksd_factory: WksdFactory,
    load_spec: LoadSpec,
) -> None:
    """Assume ``lw`` was already run so *wksd* holds the loaded weights.

    For each worker key, build a zero tensor matching *wksd*[*key*], copy *hfsd*
    slices into it according to *load_spec*, then ``torch.equal`` against *wksd*.
    """
    wksd = wksd_factory()
    assert wksd, "wksd must be non-empty"
    expected = {k: torch.zeros_like(v, device="cpu") for k, v in wksd.items()}

    load_spec.load_from_full(hf_weights, expected)

    for k, v in wksd.items():
        assert torch.equal(expected[k].to(v.device), v), (
            f"verify_load_spec: mismatch on worker key {k}"
        )
    logger.info("verify_load_spec: LoadSpec verification succeeded")


# ---------------------------------------------------------------------------
# Index encoding helpers
# ---------------------------------------------------------------------------


def _build_chunk_gpu(
    name_idx: int,
    shape: tuple[int, ...],
    n_bits: int,
    chunk_k: int,
    ele_bits: int,
    target_dtype: torch.dtype,
    device: torch.device,
    *,
    dim0_start: int = 0,
    dim0_end: int | None = None,
) -> torch.Tensor:
    """Build one physical dim-0 slice of a structured-index chunk.

    Equivalent to ``_extract_chunk(_build_index_tensor(name_idx, shape, n_bits),
    chunk_k, ele_bits, target_dtype)`` but avoids materialising the full
    int64 tensor.  Coordinates remain relative to the full physical ``shape``;
    only the returned tensor is sliced.  The bit-window we keep is
    ``[chunk_k*ele_bits, (chunk_k+1)*ele_bits)``.

    Construction itself is tiled so the int64 ``flat``/coordinate/packed
    temporaries stay small even when an explicitly infinite logical cap is
    used for equivalence testing.
    """
    if shape:
        end = shape[0] if dim0_end is None else dim0_end
        if not 0 <= dim0_start <= end <= shape[0]:
            raise ValueError(
                f"invalid dim-0 probe interval [{dim0_start}, {end}) for shape {shape}"
            )
        slice_shape = (end - dim0_start, *shape[1:])
        row_numel = math.prod(shape[1:])
        global_flat_start = dim0_start * row_numel
    else:
        if dim0_start != 0 or dim0_end not in (None, 1):
            raise ValueError(
                f"invalid scalar probe interval [{dim0_start}, {dim0_end})"
            )
        slice_shape = shape
        global_flat_start = 0

    numel = math.prod(slice_shape)
    if numel == 0:
        return torch.zeros(slice_shape, dtype=target_dtype, device=device)

    int_dtype = getattr(torch, f"int{ele_bits}")
    strides: list[int] = []
    s = 1
    for i in range(len(shape) - 1, -1, -1):
        strides.insert(0, s)
        s *= shape[i]
    chunk_lo = chunk_k * ele_bits
    chunk_hi = chunk_lo + ele_bits
    out = torch.empty(numel, dtype=target_dtype, device=device)
    tile_elements = max(1, _SPECGEN_BUILD_TILE_BYTES // torch.int64.itemsize)

    for local_start in range(0, numel, tile_elements):
        local_end = min(local_start + tile_elements, numel)
        tile_numel = local_end - local_start
        chunk = torch.zeros(tile_numel, dtype=torch.int64, device=device)

        # Bind chunk as a default so the closure captures this iteration's
        # tensor rather than the loop variable.
        def _add(
            value: int | torch.Tensor,
            bit_offset: int,
            chunk: torch.Tensor = chunk,
        ) -> None:
            rel = bit_offset - chunk_lo
            if isinstance(value, int):
                shifted = value << rel if rel >= 0 else value >> (-rel)
                chunk.bitwise_or_(shifted)
                return
            shifted = value << rel if rel >= 0 else value >> (-rel)
            chunk.bitwise_or_(shifted)

        if n_bits > 0 and chunk_lo < n_bits:
            _add(name_idx, 0)
        flat = torch.arange(
            global_flat_start + local_start,
            global_flat_start + local_end,
            dtype=torch.int64,
            device=device,
        )
        bit_offset = n_bits
        for d, dim_size in enumerate(shape):
            if dim_size <= 1:
                continue
            dim_bits = (dim_size - 1).bit_length()
            if chunk_lo < bit_offset + dim_bits and chunk_hi > bit_offset:
                coord = (flat // strides[d]) % dim_size
                _add(coord, bit_offset)
                del coord
            bit_offset += dim_bits

        if ele_bits < 64:
            chunk.bitwise_and_((1 << ele_bits) - 1)
        encoded = chunk.to(int_dtype).view(target_dtype)
        out[local_start:local_end].copy_(encoded)
        del flat, chunk, encoded

    return out.reshape(slice_shape)


def _build_physical_chunk_on_device(
    pieces: list[_LogicalSourcePiece],
    name_to_idx: dict[str, int],
    n_bits: int,
    chunk_k: int,
    ele_bits: int,
    target_dtype: torch.dtype,
    device: torch.device,
) -> torch.Tensor:
    """Assemble one loader-visible physical tensor directly on *device*."""
    if not pieces:
        raise ValueError("a physical source must contain at least one logical piece")
    shape = pieces[0].full_shape
    if len(pieces) == 1 and pieces[0].logical_name == pieces[0].physical_name:
        piece = pieces[0]
        return _build_chunk_gpu(
            name_to_idx[piece.logical_name],
            shape,
            n_bits,
            chunk_k,
            ele_bits,
            target_dtype,
            device,
            dim0_start=piece.start,
            dim0_end=piece.end,
        )
    out = torch.empty(shape, dtype=target_dtype, device=device)
    for piece in pieces:
        encoded = _build_chunk_gpu(
            name_to_idx[piece.logical_name],
            shape,
            n_bits,
            chunk_k,
            ele_bits,
            target_dtype,
            device,
            dim0_start=piece.start,
            dim0_end=piece.end,
        )
        if shape:
            out[piece.start : piece.end].copy_(encoded)
        else:
            out.copy_(encoded)
        del encoded
    return out


def _build_physical_chunk(
    pieces: list[_LogicalSourcePiece],
    name_to_idx: dict[str, int],
    n_bits: int,
    chunk_k: int,
    ele_bits: int,
    target_dtype: torch.dtype,
    device: torch.device,
) -> torch.Tensor:
    """Build a physical probe without performing probe arithmetic on CPU.

    ``device=cpu`` means a complete source was too large for the conservative
    HBM preflight.  Sources no larger than the row-tile cap are still returned
    directly from CUDA.  Larger sources get a CPU backing tensor, while each
    logical piece is subdivided along dim 0, built on CUDA, and copied into its
    corresponding CPU interval.  Tile boundaries never cross logical pieces,
    since each piece has its own encoded name id.

    The direct CPU path is retained only for CUDA-less utility use; normal
    inference requires CUDA worker tensors.
    """
    if not pieces:
        raise ValueError("a physical source must contain at least one logical piece")
    if device.type != "cpu" or not torch.cuda.is_available():
        return _build_physical_chunk_on_device(
            pieces,
            name_to_idx,
            n_bits,
            chunk_k,
            ele_bits,
            target_dtype,
            device,
        )

    shape = pieces[0].full_shape
    source_bytes = math.prod(shape) * target_dtype.itemsize
    cuda_device = torch.device("cuda", torch.cuda.current_device())
    if source_bytes <= _SPECGEN_CUDA_ROW_TILE_BYTES:
        return _build_physical_chunk_on_device(
            pieces,
            name_to_idx,
            n_bits,
            chunk_k,
            ele_bits,
            target_dtype,
            cuda_device,
        )

    # A scalar cannot reach the default cap, but handle a test/configuration
    # that lowers it below one element without falling back to CPU arithmetic.
    if not shape:
        return _build_physical_chunk_on_device(
            pieces,
            name_to_idx,
            n_bits,
            chunk_k,
            ele_bits,
            target_dtype,
            cuda_device,
        ).cpu()

    out = torch.empty(shape, dtype=target_dtype, device="cpu")
    row_bytes = math.prod(shape[1:]) * target_dtype.itemsize
    # If one row itself exceeds the cap, one row is the smallest legal tile.
    rows_per_tile = max(1, _SPECGEN_CUDA_ROW_TILE_BYTES // max(1, row_bytes))
    for piece in pieces:
        for row_start in range(piece.start, piece.end, rows_per_tile):
            row_end = min(row_start + rows_per_tile, piece.end)
            encoded = _build_chunk_gpu(
                name_to_idx[piece.logical_name],
                shape,
                n_bits,
                chunk_k,
                ele_bits,
                target_dtype,
                cuda_device,
                dim0_start=row_start,
                dim0_end=row_end,
            )
            out[row_start:row_end].copy_(encoded)
            del encoded
    return out


def _build_index_tensor(
    name_idx: int, shape: tuple[int, ...], n_bits: int
) -> torch.Tensor:
    """Build a structured-index tensor for one HF tensor.

    Each element at coordinates ``(x_0, x_1, …, x_{d-1})`` is assigned::

        name_idx  |  x_0  |  x_1  |  …  |  x_{d-1}

    ``|`` is bit concatenation; *name_idx* occupies the **lowest** *n_bits*.
    """
    numel = 1
    for s in shape:
        numel *= s
    if numel == 0:
        return torch.zeros(shape, dtype=torch.int64)

    # Row-major strides
    strides: list[int] = []
    s = 1
    for i in range(len(shape) - 1, -1, -1):
        strides.insert(0, s)
        s *= shape[i]

    flat = torch.arange(numel, dtype=torch.int64)
    result = torch.full((numel,), name_idx, dtype=torch.int64)

    bit_offset = n_bits
    for d, dim_size in enumerate(shape):
        if dim_size <= 1:
            continue
        dim_bits = (dim_size - 1).bit_length()
        coord = (flat // strides[d]) % dim_size
        result |= coord << bit_offset
        bit_offset += dim_bits

    return result.reshape(shape)


def _extract_chunk(
    full_index: torch.Tensor, chunk_k: int, ele_bits: int, target_dtype: torch.dtype
) -> torch.Tensor:
    """Return bits ``[k*B, (k+1)*B)`` of *full_index*, reinterpreted as *target_dtype*."""
    int_dtype = getattr(torch, f"int{ele_bits}")
    mask = (1 << ele_bits) - 1
    chunk = ((full_index >> (chunk_k * ele_bits)) & mask).to(int_dtype)
    return chunk.view(target_dtype)


def _coord_bits_for_shape(shape: tuple[int, ...]) -> int:
    """Total coordinate bits needed for one tensor shape."""
    return sum((d - 1).bit_length() if d > 1 else 0 for d in shape)


def _decode_coords(val: int, hf_shape: tuple[int, ...], n_bits: int) -> tuple[int, ...]:
    """Decode an int64 structured index into (name_idx, coord_0, coord_1, ...)."""
    name_idx = val & ((1 << n_bits) - 1)
    coords: list[int] = []
    bit_offset = n_bits
    for dim_size in hf_shape:
        if dim_size <= 1:
            coords.append(0)
        else:
            dim_bits = (dim_size - 1).bit_length()
            coords.append((val >> bit_offset) & ((1 << dim_bits) - 1))
            bit_offset += dim_bits
    return (name_idx, *coords)


def _shard_diff2(
    r0: int,
    r1: int,
    c0: int,
    c1: int,
    val0: int,
    stride_down: int,
    stride_right: int,
    H: int,
    W: int,
) -> list[tuple[int, int, int]]:
    """Analytically compute the diff2 entries of a rectangular shard.

    Returns sorted ``(row, col, value)`` triples for the separable 2nd-order
    difference of ``f(r, c) = val0 + stride_down*(r-r0) + stride_right*(c-c0)``
    over the region ``[r0:r1, c0:c1]``, with zero-padding outside.
    At most 16 entries (4 row positions × 4 column positions).

    Operates in exact int64 arithmetic — matches the probing path which
    also diff2s in int64 (zero-extended, unshifted).
    """
    A, B, C = val0, stride_down, stride_right
    span_r, span_c = r1 - r0, c1 - c0

    col_params: list[tuple[int, int, int]] = [(c0, A, B)]
    if c0 + 1 < W:
        col_params.append((c0 + 1, C - A, -B))
    if c1 < W:
        col_params.append((c1, -(A + C * span_c), -B))
    if c1 + 1 < W:
        col_params.append((c1 + 1, A + C * (span_c - 1), B))

    accum: dict[tuple[int, int], int] = {}
    for col, alpha, beta in col_params:
        pairs: list[tuple[int, int]] = [(r0, alpha)]
        if r0 + 1 < H:
            pairs.append((r0 + 1, beta - alpha))
        if r1 < H:
            pairs.append((r1, -(alpha + beta * span_r)))
        if r1 + 1 < H:
            pairs.append((r1 + 1, alpha + beta * (span_r - 1)))
        for row, v in pairs:
            key = (row, col)
            accum[key] = accum.get(key, 0) + v

    return [(r, c, v) for (r, c), v in sorted(accum.items()) if v != 0]


def _extract_shards_greedy(
    entries: list[list[int]],
    wk_shape: tuple[int, ...],
    H: int,
    W: int,
    n_bits: int,
    idx_to_name: dict[int, str],
    hf_shapes: dict[str, tuple[int, ...]],
) -> dict[str, list[ShardMapping]]:
    """Extract shard mappings from a sorted diff2 entry list.

    *entries* is a mutable ``[[row, col, value], ...]`` list sorted by
    ``(row, col)``.  Entries are consumed (removed / updated) as shards are
    extracted.

    Under the 2D-prefix invariant the first entry is the top-left corner of
    the next shard.  Its value equals the original structured index there
    (no contributions from above or left).  Strides are recovered from the
    adjacent entries, boundaries from the next entry along the same row/col.

    Complexity: O(n²) per tensor where n = total entries.
    """
    name_mask = (1 << n_bits) - 1
    orig_ndim = len(wk_shape)
    mappings: dict[str, list[ShardMapping]] = {}

    while entries:
        r0, c0, val0 = entries[0]
        nid = val0 & name_mask
        if nid == 0:
            entries.pop(0)
            continue

        hf_name = idx_to_name[nid]
        hf_shape = hf_shapes[hf_name]
        hf_ndim = len(hf_shape)
        coords0 = _decode_coords(val0, hf_shape, n_bits)

        # ---- Find v_right (entry at (r0, c0+1)) and v_down (at (r0+1, c0)) ----
        v_right = 0
        v_down = 0
        for e in entries:
            if e[0] == r0 and e[1] == c0 + 1:
                v_right = e[2]
            elif e[0] == r0 + 1 and e[1] == c0:
                v_down = e[2]
            elif e[0] > r0 + 1:
                break

        stride_right = val0 + v_right
        stride_down = val0 + v_down

        # ---- Validate strides via coordinate decoding ----
        right_valid = False
        transposed = False
        if hf_ndim == 1 and stride_right != 0:
            cr = _decode_coords(val0 + stride_right, hf_shape, n_bits)
            right_valid = cr[0] == coords0[0] and cr[1] == coords0[1] + 1
        elif hf_ndim >= 2 and stride_right != 0:
            cr = _decode_coords(val0 + stride_right, hf_shape, n_bits)
            exp_normal = list(coords0[1:])
            exp_normal[-1] += 1
            exp_trans = list(coords0[1:])
            exp_trans[-2] += 1
            if list(cr[1:]) == exp_normal:
                right_valid = True
            elif list(cr[1:]) == exp_trans:
                right_valid = True
                transposed = True

        down_valid = False
        if hf_ndim == 1:
            down_valid = True  # 1D always extends if stride is valid
        elif hf_ndim >= 2 and stride_down != 0:
            cd = _decode_coords(val0 + stride_down, hf_shape, n_bits)
            if not transposed:
                exp_down = list(coords0[1:])
                exp_down[-2] += 1
                exp_trans_down = list(coords0[1:])
                exp_trans_down[-1] += 1
                if r0 + 1 < H and list(cd[1:]) == exp_trans_down:
                    # A transposed shard only one destination column wide has
                    # no right-neighbour from which to infer orientation.
                    # Its downward stride still identifies the transpose.
                    transposed = True
                    down_valid = True
                else:
                    down_valid = list(cd[1:]) == exp_down
            else:
                exp_down = list(coords0[1:])
                exp_down[-1] += 1
                down_valid = list(cd[1:]) == exp_down

        # ---- Determine c1 ----
        if right_valid:
            # c1 = next entry in row r0 beyond c0+1, or W
            c1 = W
            for e in entries:
                if e[0] == r0 and e[1] > c0 + 1:
                    c1 = e[1]
                    break
                if e[0] > r0:
                    break
        else:
            c1 = c0 + 1

        col_span = c1 - c0

        # ---- Determine r1 ----
        if down_valid:
            # r1 = next entry at col c0 beyond the stride entry, or H.
            # If there's an entry at (r0+1, c0), skip it (it's the stride).
            # The next entry at col c0 after that is the boundary.
            skip_row = r0 + 1 if v_down != 0 else r0
            r1 = H
            for e in entries:
                if e[1] == c0 and e[0] > skip_row:
                    r1 = e[0]
                    break
        else:
            r1 = r0 + 1

        # For 1D HF into 2D wk, stride_down should advance by col_span coords
        if hf_ndim == 1 and down_valid:
            cd = _decode_coords(val0 + stride_down, hf_shape, n_bits)
            if cd[0] != coords0[0] or cd[1] != coords0[1] + col_span:
                down_valid = False
                r1 = r0 + 1

        # ---- Subtract shard's diff2 contribution ----
        shard_d2 = _shard_diff2(
            r0,
            r1,
            c0,
            c1,
            val0,
            stride_down if down_valid else 0,
            stride_right if right_valid else 0,
            H,
            W,
        )
        for sr, sc, sv in shard_d2:
            found = False
            for i in range(len(entries)):
                if entries[i][0] == sr and entries[i][1] == sc:
                    entries[i][2] -= sv
                    if entries[i][2] == 0:
                        entries.pop(i)
                    found = True
                    break
                if entries[i][0] > sr or (entries[i][0] == sr and entries[i][1] > sc):
                    break
            if not found and sv != 0:
                # Insert maintaining sort order
                new_e = [sr, sc, -sv]
                for i in range(len(entries)):
                    if (entries[i][0] > sr) or (
                        entries[i][0] == sr and entries[i][1] > sc
                    ):
                        entries.insert(i, new_e)
                        break
                else:
                    entries.append(new_e)

        # ---- Build shard mapping ----
        hf_r0 = coords0[-2] if hf_ndim >= 2 else coords0[1]
        hf_c0 = coords0[-1] if hf_ndim >= 2 else coords0[1]

        if hf_ndim == 1:
            hf_start = coords0[1]
            hf_end = hf_start + (r1 - r0) * col_span
            hf_shard = [(hf_start, hf_end, hf_shape[0])]
            if orig_ndim == 1:
                wk_shard = [(c0, c0 + (r1 - r0) * col_span, wk_shape[0])]
            else:
                wk_shard = [(r0, r1, H), (c0, c1, W)]
        elif hf_ndim == 2:
            if not transposed:
                hf_shard = [
                    (hf_r0, hf_r0 + (r1 - r0), hf_shape[0]),
                    (hf_c0, hf_c0 + col_span, hf_shape[1]),
                ]
            else:
                hf_shard = [
                    (hf_r0, hf_r0 + col_span, -hf_shape[0]),
                    (hf_c0, hf_c0 + (r1 - r0), hf_shape[1]),
                ]
            wk_shard = [(r0, r1, H), (c0, c1, W)]
        else:
            if not transposed:
                hf_shard = [
                    (
                        coords0[d + 1],
                        coords0[d + 1]
                        + (
                            (r1 - r0)
                            if d == hf_ndim - 2
                            else col_span
                            if d == hf_ndim - 1
                            else 1
                        ),
                        hf_shape[d],
                    )
                    for d in range(hf_ndim)
                ]
            else:
                hf_shard = [
                    (
                        coords0[d + 1],
                        coords0[d + 1]
                        + (
                            col_span
                            if d == hf_ndim - 2
                            else (r1 - r0)
                            if d == hf_ndim - 1
                            else 1
                        ),
                        -hf_shape[d] if d == hf_ndim - 2 else hf_shape[d],
                    )
                    for d in range(hf_ndim)
                ]
            wk_shard = [(r0, r1, H), (c0, c1, W)]

        if orig_ndim == 1 and len(wk_shard) == 2:
            flat_start = r0 * W + c0
            flat_end = flat_start + (r1 - r0) * col_span
            wk_shard = [(flat_start, flat_end, wk_shape[0])]

        mappings.setdefault(hf_name, []).append((hf_shard, wk_shard))

    return mappings


def _diff2_inplace(t: torch.Tensor) -> torch.Tensor:
    """Compute 2D separable 2nd-order difference in-place.

    For a 2D tensor of shape ``(H, W)``::

        d2[r, c] = t[r,c] - 2*t[r,c-1] + t[r,c-2]   (along cols first)

    then the same along rows.  Edges assume zero-padding.

    For ≥3D, the leading dimensions are flattened to rows (``(-1, last_dim)``).
    For 1D, a row dimension of 1 is prepended.

    A contiguous rectangular shard whose structured-index values are affine
    in ``(row, col)`` produces **O(1) nonzeros** — at most 16 at the 4
    corners of each box.  This lets us compress each chunk to a sparse COO
    representation on CPU.
    """
    orig_shape = t.shape
    if t.ndim == 0 or t.numel() == 0:
        return t
    if t.ndim == 1:
        t = t.unsqueeze(0)
    elif t.ndim > 2:
        t = t.reshape(-1, t.shape[-1])

    H, W = t.shape

    # A separable diff2 is two strided diff1 passes per axis.  The vectorised
    # diff1 helper materialises only one bounded-slice temporary per pass,
    # rather than the multiple full-size operands in the expanded
    # ``x - 2*x_prev + x_prev2`` expression.
    if W >= 2:
        _strided_diff1_2d(t, axis=1, stride=1)
        _strided_diff1_2d(t, axis=1, stride=1)
    if H >= 2:
        _strided_diff1_2d(t, axis=0, stride=1)
        _strided_diff1_2d(t, axis=0, stride=1)

    return t.reshape(orig_shape)


def _prefix_sum_2d(t: torch.Tensor) -> torch.Tensor:
    """Reconstruct a 2D tensor from its 2nd-order difference (inverse of _diff2_inplace).

    Applies prefix-sum twice along columns, then twice along rows.
    """
    orig_shape = t.shape
    if t.ndim == 0 or t.numel() == 0:
        return t
    if t.ndim == 1:
        t = t.unsqueeze(0)
    elif t.ndim > 2:
        t = t.reshape(-1, t.shape[-1])

    # Undo rows first (diff2 did rows last), then columns
    t.cumsum_(dim=0)
    t.cumsum_(dim=0)
    # Then undo columns
    t.cumsum_(dim=1)
    t.cumsum_(dim=1)

    return t.reshape(orig_shape)


def _strided_diff1_2d(t: torch.Tensor, axis: int, stride: int) -> torch.Tensor:
    """Strided 1st-order difference along *axis*.

    ``d[i] = t[i] − t[i − stride]`` for ``i >= stride``; ``d[i] = t[i]``
    otherwise.  Operates in-place using one vectorised difference temporary.
    """
    if t.ndim != 2:
        raise ValueError(
            f"strided diff1 expects a 2D tensor, got shape {tuple(t.shape)}"
        )
    if axis not in (0, 1):
        raise ValueError(f"strided diff1 axis must be 0 or 1, got {axis}")
    if stride <= 0:
        raise ValueError(f"strided diff1 stride must be positive, got {stride}")
    if axis == 1:
        if stride < t.shape[1]:
            t[:, stride:] = t[:, stride:] - t[:, :-stride]
    else:
        if stride < t.shape[0]:
            t[stride:, :] = t[stride:, :] - t[:-stride, :]
    return t


def _strided_prefix_sum_2d(t: torch.Tensor, axis: int, stride: int) -> torch.Tensor:
    """Inverse of :func:`_strided_diff1_2d` — prefix sum with stride.

    For each independent sub-sequence (offset 0, 1, …, stride-1) along
    *axis*, computes cumulative sum.  Operates **in-place**.
    """
    if axis == 1:
        for start in range(min(stride, t.shape[1])):
            t[:, start::stride] = t[:, start::stride].cumsum(dim=1)
    else:
        for start in range(min(stride, t.shape[0])):
            t[start::stride, :] = t[start::stride, :].cumsum(dim=0)
    return t


# ---------------------------------------------------------------------------
# Dtype-grouped probing
# ---------------------------------------------------------------------------


def _probe_dtype_group(
    hf_names: list[str],
    source_pieces: dict[str, list[_LogicalSourcePiece]],
    name_to_idx: dict[str, int],
    n_bits: int,
    max_total_bits: int,
    wksd_factory: WksdFactory,
    wk_names: list[str],
    group_dtype: torch.dtype,
    lw: LoadWeightsFn,
    i64_cap_bytes: int | float,
) -> dict[str, list[_SparseWorkerSlice]]:
    """Probe one wksd dtype group with carry-aware chunked diff2.

    Processes chunks low-to-high.  After diff2 of each chunk the result
    is split into a **remainder** (``raw mod M``, O(S) nnz per shard)
    and a **carry** (``raw // M``).  The carry stays on GPU as a dense
    tensor and is added to the next chunk's diff2 before splitting.
    Only the remainder is transferred to CPU as sparse COO.

    After the last chunk, the sign-extension carry (also O(S) nnz) is
    stored as an extra sparse entry.

    Worker tensors are independently split on flattened leading rows before
    int64 conversion.  Each slice has its own diff2/carry stream and is
    extracted in local coordinates later, avoiding the old full-worker int64
    allocation.

    Returns ``{wk_name: [_SparseWorkerSlice(...), ...]}``.
    """
    ele_bits = group_dtype.itemsize * 8
    num_chunks = max(1, (max_total_bits + ele_bits - 1) // ele_bits)
    M = 1 << ele_bits

    logger.info(
        "probing dtype group %s: %d wk tensors, %d chunks "
        "(ele_bits=%d, max_idx_bits=%d)",
        group_dtype,
        len(wk_names),
        num_chunks,
        ele_bits,
        max_total_bits,
    )

    cuda_avail = torch.cuda.is_available()
    device = _select_probe_build_device(hf_names, source_pieces, group_dtype)
    int_dtype = getattr(torch, f"int{ele_bits}")

    initial_wksd = wksd_factory()
    sparse_slices: dict[str, list[_SparseWorkerSlice]] = {
        wk: [
            _SparseWorkerSlice(start, end, slice_shape)
            for start, end, slice_shape in _plan_worker_slices(
                tuple(initial_wksd[wk].shape),
                i64_cap_bytes,
            )
        ]
        for wk in wk_names
    }
    del initial_wksd
    # Carry tensors stored as sparse COO on CPU between chunks to avoid
    # holding dense int64 GPU tensors for all worker-state keys simultaneously, which can exceed available
    # memory for large MoE models.
    prev_carry_sparse: dict[tuple[str, int], torch.Tensor | None] = {
        (wk, slice_idx): None
        for wk, slices in sparse_slices.items()
        for slice_idx in range(len(slices))
    }
    total_nnz = 0

    for chunk_k in range(num_chunks):
        t_chunk0 = time.time()

        def _make_factory(name, ck=chunk_k):
            def _factory():
                return _build_physical_chunk(
                    source_pieces[name],
                    name_to_idx,
                    n_bits,
                    ck,
                    ele_bits,
                    group_dtype,
                    device,
                )

            return _factory

        chunk_fetcher: HFWeightFetcher = {
            name: _make_factory(name) for name in hf_names
        }
        t_build = time.time()

        # Fresh snapshot: model.load_weights may have replaced parameter
        # tensors on the previous chunk.  Re-snapshotting drops references
        # to old tensors so GC can free them.
        wksd = wksd_factory()
        for wk in wk_names:
            wksd[wk].zero_()
        t_zero = time.time()

        if cuda_avail:
            _pre = torch.cuda.memory_allocated() / 1e9
        lw(chunk_fetcher)
        if torch.cuda.is_available():
            torch.cuda.synchronize()
        t_lw = time.time()
        if cuda_avail:
            _post = torch.cuda.memory_allocated() / 1e9
            logger.info(
                "  chunk %d/%d GPU mem: before_lw=%.2fG after_lw=%.2fG delta=%.2fG",
                chunk_k + 1,
                num_chunks,
                _pre,
                _post,
                _post - _pre,
            )

        # Re-snapshot after lw(): model.load_weights may have replaced
        # parameter tensors with new fused buffers (e.g. MoE experts).
        wksd = wksd_factory()

        # ── Carry-aware compression ──────────────────────────────
        chunk_nnz = 0
        for wk in wk_names:
            wk_tensor = wksd[wk].detach()
            for slice_idx, probe_slice in enumerate(sparse_slices[wk]):
                if wk_tensor.ndim == 0:
                    tensor_slice = wk_tensor
                elif wk_tensor.ndim == 1:
                    tensor_slice = wk_tensor[
                        probe_slice.row_start : probe_slice.row_end
                    ]
                else:
                    tensor_slice = wk_tensor.reshape(-1, wk_tensor.shape[-1])[
                        probe_slice.row_start : probe_slice.row_end
                    ]
                if not tensor_slice.is_contiguous():
                    tensor_slice = tensor_slice.contiguous()
                raw = tensor_slice.view(dtype=int_dtype).to(torch.int64)
                if ele_bits < 64:
                    raw.bitwise_and_((1 << ele_bits) - 1)  # unsigned zero-extension

                # A slice is an independent logical destination during sparse
                # extraction.  Its local diff2 is translated and merged back
                # into the physical worker tensor after extraction.
                _diff2_inplace(raw)

                carry_key = (wk, slice_idx)
                prev_sp = prev_carry_sparse[carry_key]
                if prev_sp is not None:
                    raw.add_(prev_sp.to(raw.device).to_dense())
                    prev_carry_sparse[carry_key] = None

                # Keep two dense int64 buffers rather than raw+remainder+carry:
                # raw is converted in-place into carry after remainder is made.
                remainder = torch.remainder(raw, M)
                raw.sub_(remainder).floor_divide_(M)

                sp = remainder.to_sparse().cpu()
                chunk_nnz += sp._nnz()
                probe_slice.chunks.append(sp)

                if chunk_k < num_chunks - 1:
                    prev_carry_sparse[carry_key] = raw.to_sparse().cpu()
                else:
                    sign_sp = raw.to_sparse().cpu()
                    if sign_sp._nnz() > 0:
                        chunk_nnz += sign_sp._nnz()
                        probe_slice.chunks.append(sign_sp)

                del raw, remainder, tensor_slice

        total_nnz += chunk_nnz
        t_compress = time.time()

        logger.info(
            "  chunk %d/%d: build=%.2fs zero=%.2fs lw=%.2fs "
            "compress=%.2fs nnz=%d total=%.2fs",
            chunk_k + 1,
            num_chunks,
            t_build - t_chunk0,
            t_zero - t_build,
            t_lw - t_zero,
            t_compress - t_lw,
            chunk_nnz,
            t_compress - t_chunk0,
        )

    logger.info(
        "probing complete: total_nnz=%d across %d tensors × %d chunks (%.2f MB on CPU)",
        total_nnz,
        len(wk_names),
        num_chunks,
        total_nnz * 24 / 1e6,
    )

    return sparse_slices


def _translate_worker_shard(
    shard: Shard,
    full_shape: tuple[int, ...],
    flat_row_start: int,
) -> Shard:
    """Translate a worker-slice-local destination shard to physical coordinates."""
    if not full_shape:
        return list(shard)
    if len(full_shape) == 1:
        if len(shard) != 1:
            raise ValueError(
                f"expected 1D worker shard for shape {full_shape}, got {shard}"
            )
        left, right, _width = shard[0]
        return [(left + flat_row_start, right + flat_row_start, full_shape[0])]

    if len(shard) != 2:
        raise ValueError(
            f"expected flattened 2D worker shard for shape {full_shape}, got {shard}"
        )
    global_rows = math.prod(full_shape[:-1])
    row_left, row_right, _row_width = shard[0]
    col_left, col_right, col_width = shard[1]
    return [
        (row_left + flat_row_start, row_right + flat_row_start, global_rows),
        (col_left, col_right, col_width),
    ]


def _merge_adjacent_shards(a: Shard, b: Shard) -> Shard | None:
    """Merge boxes that are identical except for one exactly adjacent axis."""
    if len(a) != len(b):
        return None
    differing: int | None = None
    for axis, ((la, ra, wa), (lb, rb, wb)) in enumerate(zip(a, b)):
        if wa != wb:
            return None
        if (la, ra) != (lb, rb):
            if differing is not None:
                return None
            differing = axis
    if differing is None:
        return list(a)
    la, ra, width = a[differing]
    lb, rb, _ = b[differing]
    if ra != lb and rb != la:
        return None
    merged = list(a)
    merged[differing] = (min(la, lb), max(ra, rb), width)
    return merged


def _merge_consecutive_mappings(mappings: list[ShardMapping]) -> list[ShardMapping]:
    """Coalesce consecutive source and destination boxes where both agree."""
    pending: list[ShardMapping] = []
    seen: set[tuple] = set()
    for source, destination in mappings:
        key = (tuple(source), tuple(destination))
        if key not in seen:
            seen.add(key)
            pending.append((list(source), list(destination)))

    while True:
        changed = True
        while changed:
            changed = False
            for i in range(len(pending)):
                for j in range(i + 1, len(pending)):
                    source = _merge_adjacent_shards(pending[i][0], pending[j][0])
                    destination = _merge_adjacent_shards(pending[i][1], pending[j][1])
                    if source is None or destination is None:
                        continue
                    # Identical source or destination boxes represent replication,
                    # not consecutive shards, and must remain separate.
                    if source == pending[i][0] or destination == pending[i][1]:
                        continue
                    if math.prod(r - l for l, r, _ in source) != math.prod(
                        r - l for l, r, _ in destination
                    ):
                        continue
                    pending[i] = (source, destination)
                    pending.pop(j)
                    changed = True
                    break
                if changed:
                    break

        # A one-element-wide logical remainder can make transpose impossible
        # to identify during local extraction.  Once consecutive 1x1 boxes
        # have coalesced, their rectangular shapes make it unambiguous.
        orientation_changed = False
        for index, (source, destination) in enumerate(pending):
            if len(source) == len(destination) == 2 and all(
                width > 0 for _left, _right, width in source
            ):
                source_shape = tuple(right - left for left, right, _width in source)
                destination_shape = tuple(
                    right - left for left, right, _width in destination
                )
                if (
                    source_shape != destination_shape
                    and source_shape[::-1] == destination_shape
                ):
                    source = list(source)
                    left, right, width = source[0]
                    source[0] = (left, right, -abs(width))
                    pending[index] = (source, destination)
                    orientation_changed = True

        if not orientation_changed:
            break

    pending.sort(key=lambda mapping: (tuple(mapping[0]), tuple(mapping[1])))
    return pending


def _canonicalize_logical_sources(
    entries: dict[str, dict[str, list[ShardMapping]]],
    logical_to_physical: dict[str, str],
) -> LoadSpec:
    """Rename probe-only logical sources and merge their consecutive shards."""
    physical_entries: dict[str, dict[str, list[ShardMapping]]] = {}
    for logical_name, destinations in entries.items():
        physical_name = logical_to_physical[logical_name]
        target = physical_entries.setdefault(physical_name, {})
        for destination_name, mappings in destinations.items():
            target.setdefault(destination_name, []).extend(mappings)

    canonical: dict[str, dict[str, list[ShardMapping]]] = {}
    for physical_name in sorted(physical_entries):
        destinations: dict[str, list[ShardMapping]] = {}
        for destination_name in sorted(physical_entries[physical_name]):
            destinations[destination_name] = _merge_consecutive_mappings(
                physical_entries[physical_name][destination_name]
            )
        canonical[physical_name] = destinations
    return LoadSpec(canonical)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def infer_load_spec(
    hf_weights: HFWeightFetcher,
    hf_shapes: dict[str, tuple[int, ...]],
    wksd_factory: WksdFactory,
    lw: LoadWeightsFn,
    *,
    i64_cap_bytes: int | float | None = None,
) -> LoadSpec:
    """Infer a :class:`~wbridge.utils.data.LoadSpec` from *hf_weights* and *wksd*.

    Uses dtype-grouped, full-iterator probing with chunked index encoding.
    ``O(G × C)`` ``load_weights`` calls, where *G* is the number of distinct
    worker dtypes (usually 1–2) and *C = ceil(max_index_bits / element_bits)*
    (usually 1–3).

    Oversized source int64 probe tensors are split on dim 0, while worker
    tensors are split on flattened leading rows, according to
    ``i64_cap_bytes`` (or :data:`SPECGEN_I64_CAP_ENV`).  Probe-only logical
    source names and worker slices are translated back to physical coordinates
    and consecutive mappings are merged before this function returns.  If a
    complete loader-visible source cannot remain in HBM, sources larger than
    512 MiB are backed by CPU memory but populated exclusively from CUDA-built
    row tiles.

    Overwrites *wksd* during probing, then restores via
    :meth:`~wbridge.utils.data.LoadSpec.load_from_full`.
    Call :func:`verify_load_spec` after to validate the mapping.
    """
    resolved_i64_cap = _resolve_i64_cap(i64_cap_bytes)
    wksd = wksd_factory()
    assert wksd, "wksd must be non-empty"
    assert all(v.is_cuda for v in wksd.values()), "wksd tensors must be on CUDA"

    hf_names = sorted(hf_shapes.keys())
    if not hf_names:
        return LoadSpec({})

    source_pieces, logical_shapes = _plan_logical_sources(
        hf_shapes,
        resolved_i64_cap,
    )
    logical_names = sorted(logical_shapes)
    N = len(logical_names)

    # 1-based name indices: 0 = unmapped sentinel.
    # Compact packing: n_bits = ceil(log2(N+1)).  Cross-chunk carries
    # from field-straddling are handled by carry-aware diff2 in
    # _probe_dtype_group (propagation from low to high chunks).
    n_bits = max(1, N.bit_length())
    name_to_idx = {name: i + 1 for i, name in enumerate(logical_names)}

    # Max total index bits across all HF tensors
    max_total_bits = max(
        n_bits + _coord_bits_for_shape(logical_shapes[name]) for name in logical_names
    )

    logger.info(
        "infer_load_spec: %d physical/%d logical HF tensors, %d wksd tensors, "
        "i64_cap=%s, n_bits=%d, max_total_bits=%d",
        len(hf_names),
        N,
        len(wksd),
        "inf" if math.isinf(resolved_i64_cap) else resolved_i64_cap,
        n_bits,
        max_total_bits,
    )

    # Free GPU cache before probing — large MoE models may leave very
    # little GPU headroom after model + optimizer + KV cache allocation.
    if torch.cuda.is_available():
        import gc

        gc.collect()
        torch.cuda.empty_cache()

    # Log wksd tensor shapes for debugging (especially MoE 2D vs 3D)
    ndim_counts: dict[int, int] = {}
    for wk_name, wk_tensor in wksd.items():
        nd = wk_tensor.ndim
        ndim_counts[nd] = ndim_counts.get(nd, 0) + 1
    logger.info(
        "wksd ndim distribution: %s, largest: %s",
        ndim_counts,
        max(((v.numel(), k) for k, v in wksd.items()), key=lambda x: x[0]),
    )

    # ---- Probe and extract (corrupts wksd, restored afterward) ----
    # ---- Group wksd by dtype ----
    dtype_groups: dict[torch.dtype, list[str]] = defaultdict(list)
    for wk_name, wk_tensor in wksd.items():
        dtype_groups[wk_tensor.dtype].append(wk_name)

    # Drop initial snapshot — _probe_dtype_group re-snapshots per chunk
    del wksd

    # ---- Probe each dtype group (sparse) ----
    t0 = time.time()
    # sparse_indices: {wk_name: [sparse_coo_chunk_0, ..., chunk_{C-1}]}
    sparse_indices: dict[str, list[_SparseWorkerSlice]] = {}

    for group_dtype, wk_names in dtype_groups.items():
        group_sparse = _probe_dtype_group(
            hf_names,
            source_pieces,
            name_to_idx,
            n_bits,
            max_total_bits,
            wksd_factory,
            wk_names,
            group_dtype,
            lw,
            resolved_i64_cap,
        )
        sparse_indices.update(group_sparse)

    t1 = time.time()
    logger.info("probing completed in %.2fs", t1 - t0)

    # ---- Extract shards from sparse diff2 entries (CPU, no dense tensors) ----
    wksd = wksd_factory()
    wk_shapes = {k: tuple(v.shape) for k, v in wksd.items()}
    idx_to_name = {i + 1: name for i, name in enumerate(logical_names)}
    entries: dict[str, dict[str, list[ShardMapping]]] = {}

    for wk_name, worker_slices in sparse_indices.items():
        full_wk_shape = wk_shapes[wk_name]
        group_ele_bits = wksd[wk_name].dtype.itemsize * 8
        for worker_slice in worker_slices:
            wk_shape = worker_slice.shape
            orig_ndim = len(wk_shape)
            if orig_ndim == 0:
                H, W = 1, 1
            elif orig_ndim == 1:
                H, W = 1, wk_shape[0]
            elif orig_ndim == 2:
                H, W = wk_shape
            else:
                W = wk_shape[-1]
                H = math.prod(wk_shape) // W

            # Collect sparse entries across chunks.  Each chunk stores diff2
            # of unshifted values in this worker slice's local coordinates.
            elist: list[list[int]] = []
            for chunk_k, sp in enumerate(worker_slice.chunks):
                shift = chunk_k * group_ele_bits
                sp_cpu = sp.coalesce()
                indices = sp_cpu.indices()
                values = sp_cpu.values()
                nnz = values.shape[0]
                if nnz == 0:
                    continue
                if sp_cpu.ndim == 0:
                    rows_l = [0] * nnz
                    cols_l = [0] * nnz
                elif sp_cpu.ndim == 1:
                    rows_l = [0] * nnz
                    cols_l = indices[0].tolist()
                elif sp_cpu.ndim == 2:
                    rows_l = indices[0].tolist()
                    cols_l = indices[1].tolist()
                else:
                    rows_t = torch.zeros(nnz, dtype=torch.long)
                    stride = 1
                    for d in range(sp_cpu.ndim - 2, -1, -1):
                        rows_t += indices[d] * stride
                        stride *= sp_cpu.shape[d]
                    rows_l = rows_t.tolist()
                    cols_l = indices[-1].tolist()
                vals_l = values.tolist()
                for r, c, value in zip(rows_l, cols_l, vals_l):
                    elist.append([r, c, value << shift])

            elist.sort()
            merged: list[list[int]] = []
            for entry in elist:
                if merged and merged[-1][:2] == entry[:2]:
                    merged[-1][2] += entry[2]
                else:
                    merged.append(entry)
            merged = [entry for entry in merged if entry[2] != 0]
            if not merged:
                continue

            wk_mappings = _extract_shards_greedy(
                merged,
                wk_shape,
                H,
                W,
                n_bits,
                idx_to_name,
                logical_shapes,
            )
            for logical_name, shard_list in wk_mappings.items():
                translated = [
                    (
                        source_shard,
                        _translate_worker_shard(
                            destination_shard,
                            full_wk_shape,
                            worker_slice.row_start,
                        ),
                    )
                    for source_shard, destination_shard in shard_list
                ]
                entries.setdefault(logical_name, {}).setdefault(wk_name, []).extend(
                    translated
                )

    t2 = time.time()
    logger.info("shard extraction completed in %.2fs", t2 - t1)

    logical_to_physical = {
        piece.logical_name: physical_name
        for physical_name, pieces in source_pieces.items()
        for piece in pieces
    }
    result = _canonicalize_logical_sources(entries, logical_to_physical)

    # Restore wksd from real weights (specgen probing corrupted them).
    # Use LoadSpec.load_from_full instead of lw(hf_weights) — the latter
    # calls model.load_weights which for large MoE models
    # allocates GPU temporaries for expert routing that are not freed.
    # load_from_full takes an HFWeightFetcher and only materialises the
    # tensors it needs, keeping only a small fraction of the checkpoint resident for a typical rank.
    t3 = time.time()
    wksd = wksd_factory()
    result.load_from_full(hf_weights, wksd)
    logger.info("wksd restore via LoadSpec completed in %.2fs", time.time() - t3)

    return result


# ---------------------------------------------------------------------------
# Checkpoint → HFWeightFetcher builder
# ---------------------------------------------------------------------------


def hf_weights_from_checkpoint(
    hf_path: str,
) -> tuple[HFWeightFetcher, dict[str, tuple[int, ...]]]:
    """Build ``(hf_weights, hf_shapes)`` from a HF checkpoint directory.

    Scans safetensors metadata to discover names and shapes (no tensor data
    loaded).  Each factory does a single targeted ``safe_open`` /
    ``get_tensor`` call — O(1) per invocation, no full-checkpoint scans.

    Falls back to ``pytorch_model*.bin`` if no safetensors files are found.
    """
    from pathlib import Path

    root = Path(hf_path)
    st_files = sorted(root.glob("*.safetensors"))

    if st_files:
        from safetensors import safe_open

        # Build name→file index and collect shapes in one pass (metadata only)
        name_to_file: dict[str, str] = {}
        shapes: dict[str, tuple[int, ...]] = {}
        for fp in st_files:
            with safe_open(str(fp), framework="pt", device="cpu") as sf:
                for k in sf.keys():
                    name_to_file[k] = str(fp)
                    shapes[k] = tuple(sf.get_slice(k).get_shape())

        def _make_st_factory(name: str, filepath: str):
            def _load() -> torch.Tensor:
                with safe_open(filepath, framework="pt", device="cpu") as sf:
                    return sf.get_tensor(name).contiguous()

            return _load

        fetcher: HFWeightFetcher = {
            name: _make_st_factory(name, name_to_file[name]) for name in shapes
        }
        return fetcher, shapes

    # Fallback: pytorch_model*.bin
    bins = sorted(root.glob("pytorch_model*.bin"))
    if len(bins) == 1:
        try:
            blob = torch.load(bins[0], map_location="cpu", weights_only=True)
        except TypeError:
            blob = torch.load(bins[0], map_location="cpu")
        if not isinstance(blob, dict):
            raise TypeError(f"Expected state_dict in {bins[0]}")
        shapes = {}
        fetcher = {}
        for k, v in blob.items():
            if torch.is_tensor(v):
                shapes[k] = tuple(v.shape)
                fetcher[k] = lambda t=v: t.clone().contiguous()
        return fetcher, shapes

    raise FileNotFoundError(
        f"No *.safetensors or single pytorch_model*.bin under {hf_path!r} "
        "(needed for LoadSpec inference)."
    )


__all__ = [
    "DEFAULT_MAX_HF_BYTES",
    "DEFAULT_SPECGEN_I64_CAP_BYTES",
    "HFWeightFetcher",
    "LoadWeightsFn",
    "SPECGEN_I64_CAP_ENV",
    "WksdFactory",
    "hf_weights_from_checkpoint",
    "infer_load_spec",
    "specgen_i64_cap_bytes",
    "verify_load_spec",
]
