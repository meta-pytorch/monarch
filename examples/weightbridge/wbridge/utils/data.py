# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import copy
import math
import os
from typing import Callable, Iterable, Iterator, TypeAlias

import torch

Shard: TypeAlias = list[tuple[int, int, int]]
Shards: TypeAlias = list[Shard]
# One HF↔worker region pair: source box and destination box (same flattened numel).
ShardMapping: TypeAlias = tuple[Shard, Shard]


DEFAULT_LOGICAL_TENSOR_CAP_BYTES = 128 * 1024**2
LOGICAL_TENSOR_SPLIT_ENV = "WBRIDGE_LOGICAL_TENSOR_CAP_BYTES"
_LOGICAL_TENSOR_SPLIT_MARKER = ".__wbridge_dim0_"


def logical_tensor_cap_bytes() -> int:
    """Configured maximum full logical-source size in bytes; zero disables splitting."""
    raw = os.environ.get(
        LOGICAL_TENSOR_SPLIT_ENV,
        str(DEFAULT_LOGICAL_TENSOR_CAP_BYTES),
    ).strip()
    try:
        cap = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{LOGICAL_TENSOR_SPLIT_ENV} must be an integer, got {raw!r}"
        ) from exc
    if cap < 0:
        raise ValueError(f"{LOGICAL_TENSOR_SPLIT_ENV} must be non-negative, got {cap}")
    return cap


def logical_tensor_name(original: str, start: int, end: int) -> str:
    """Deterministic virtual name for ``original[start:end, ...]``."""
    if _LOGICAL_TENSOR_SPLIT_MARKER in original:
        raise ValueError(
            f"checkpoint tensor name {original!r} contains reserved marker "
            f"{_LOGICAL_TENSOR_SPLIT_MARKER!r}"
        )
    if not 0 <= start < end:
        raise ValueError(f"invalid logical tensor dim-0 interval [{start}, {end})")
    return f"{original}{_LOGICAL_TENSOR_SPLIT_MARKER}{start:012d}_{end:012d}"


def parse_logical_tensor_name(name: str) -> tuple[str, int, int] | None:
    """Return ``(physical_name, start, end)`` for a virtual source name."""
    original, marker, suffix = name.rpartition(_LOGICAL_TENSOR_SPLIT_MARKER)
    if not marker:
        return None
    fields = suffix.split("_")
    if not original or len(fields) != 2:
        raise ValueError(f"malformed logical tensor name {name!r}")
    try:
        start, end = map(int, fields)
    except ValueError as exc:
        raise ValueError(f"malformed logical tensor interval in {name!r}") from exc
    if not 0 <= start < end:
        raise ValueError(f"invalid logical tensor interval in {name!r}")
    return original, start, end


def shard_shape(shard: Shard) -> tuple[int, ...]:
    return tuple(r - l for l, r, _ in shard)


def shard_numel(shard: Shard) -> int:
    return math.prod(shard_shape(shard))


def shards_numel(shards: Shards) -> int:
    return sum(shard_numel(shard) for shard in shards)


def shards_nbytes(shards: Shards, dtype: torch.dtype) -> int:
    return shards_numel(shards) * dtype.itemsize


def split_shard_evenly(shard: Shard, k: int, j: int) -> Shard | None:
    """Sub-shard *j* of *k* from splitting *shard* evenly along its longest axis.

    Used for sender-side de-replication: *k* workers holding an identical *shard* are each viewed as
    holding one disjoint sub-range, so the shard is sent once instead of *k* times. The remainder is
    spread over the first ``len % k`` parts, so the *k* sub-shards partition *shard* exactly. ``w``
    (incl. transpose sign) is preserved. Returns ``None`` when sub-shard *j* is empty (``len < k`` and
    *j* past the remainder — that member then sends nothing).
    """
    if k <= 1:
        return list(shard)
    shape = shard_shape(shard)
    # Split the OUTERMOST axis with extent >= k so each sub-shard stays a CONTIGUOUS block (the axes before
    # it are then singletons). A strided sub-shard (e.g. splitting the column axis of a row-major tensor)
    # makes the wire<->model copy fall on the much slower coordinate Triton kernel instead of the
    # contiguous fast path — the dominant cost in the receiver consume. Byte-balance per part is axis-independent
    # for an even k-way split, so this costs nothing there. Fall back to the longest axis for a tiny tensor
    # with no axis >= k.
    d = next(
        (i for i in range(len(shape)) if shape[i] >= k),
        max(range(len(shape)), key=lambda i: shape[i]),
    )
    l, r, w = shard[d]
    base, rem = divmod(r - l, k)
    start = l + j * base + min(j, rem)
    end = start + base + (1 if j < rem else 0)
    if start >= end:
        return None
    out = list(shard)
    out[d] = (start, end, w)
    return out


def _sanity_check(name: str, shards: Shards) -> None:
    numel = None
    assert len(shards) > 0, f"Empty shard list for {name}"
    for shard in shards:
        assert len(shard) > 0, f"Empty shard in list for {name}"
        for l, r, w in shard:
            # Negative w signals a transposed axis (see specgen transpose detection).
            assert 0 <= l < r <= abs(w), f"Invalid shard: {l, r, w} for {name}"
        cur_numel = math.prod(abs(w) for _, _, w in shard)
        if numel is None:
            numel = cur_numel
        else:
            assert numel == cur_numel, (
                f"Shard {shard} does not match original total numel: {numel} != {cur_numel}!"
            )


def shards_iterator(
    shards: Shards, tensor: torch.Tensor
) -> Iterator[tuple[Shard, torch.Tensor]]:
    """Iterate over shards and yield the corresponding tensor slices."""
    offset = 0
    for shard in shards:
        length = shard_numel(shard)
        yield shard, tensor[offset : offset + length].view(shard_shape(shard))
        offset += length


class ShardSpec:
    """
    Per-tensor shard layout and dtype for weight transfer (no tensor storage).

    Format::

        {
            "name": [[(l, r, w), ...], ...],  # each value is :class:`Shards`
            ...
        }

    Each entry value must be full multi-shard form (a :class:`Shards` list).
    ``dtype`` may be :class:`torch.dtype` or a string (for JSON / legacy).

    Tensor values are passed separately as ``dict[str, torch.Tensor]`` to
    :meth:`__call__` (returns a :class:`BoundShardSpec`) or
    """

    def __init__(self, entries: dict[str, Shards]):
        self.entries = dict(entries)

        for name, shards in self:
            _sanity_check(name, shards)

    def __call__(self, tensors: dict[str, torch.Tensor]) -> "BoundShardSpec":
        """Bind *tensors* to this shard spec for overlap packing / unpacking.

        Returns a :class:`BoundShardSpec` for ``f[overlaps]`` /
        ``f[overlaps] = chunks`` (see :class:`BoundShardSpec`).
        """
        return BoundShardSpec(self, tensors)

    def __bool__(self) -> bool:
        return bool(self.entries)

    def __iter__(self) -> Iterator[tuple[str, Shards]]:
        return iter(sorted(self.entries.items()))

    def __len__(self) -> int:
        return len(self.entries)

    def __contains__(self, key: str) -> bool:
        return key in self.entries

    def __delitem__(self, key: str) -> None:
        del self.entries[key]

    def __getitem__(self, key: str) -> Shards:
        return self.entries[key]

    def __setitem__(self, key: str, value: Shards) -> None:
        _sanity_check(key, value)
        self.entries[key] = value

    def nbytes(self, dtype_spec: dict[str, torch.dtype]) -> int:
        return sum(shards_nbytes(shards, dtype_spec[name]) for name, shards in self)

    def iter_with_intv(
        self, tensors: dict[str, torch.Tensor]
    ) -> Iterator[tuple[int, int, str, torch.dtype]]:
        offset = 0
        for name, shards in self:
            length = shards_nbytes(shards, tensors[name].dtype)
            yield offset, offset + length, name, tensors[name].dtype
            offset += length

    def clone(self) -> "ShardSpec":
        return ShardSpec(copy.deepcopy(self.entries))

    def subset(self, names: set[str]) -> "ShardSpec":
        names &= self.entries.keys()
        return ShardSpec({name: self[name] for name in names})

    def make_named_buffer(
        self, dtype_spec: dict[str, torch.dtype], device: str
    ) -> dict[str, torch.Tensor]:
        """Make a dictionary of tensors for each name in the shard spec."""
        return {
            name: torch.empty(
                shards_numel(shards), dtype=dtype_spec[name], device=device
            )
            for name, shards in self
        }

    def make_byte_chunk(
        self, dtype_spec: dict[str, torch.dtype], device: str
    ) -> torch.Tensor:
        """Return a uint8 P2P buffer covering this spec (all tensor names, layout order in :meth:`iter_with_intv`)."""
        return torch.zeros(self.nbytes(dtype_spec), dtype=torch.uint8, device=device)

    @staticmethod
    def compute_overlap(
        sender: "ShardSpec",
        receiver: "ShardSpec",
        names: Iterable[str] | None = None,
    ) -> "ShardSpec":
        """Return a new :class:`ShardSpec` whose entries describe the shard regions
        where *sender* and *receiver* overlap (spec only, no tensor data).

        Sender and receiver specs must already store entries as multi-box :class:`Shards`.
        Every sender shard is paired against every
        receiver shard and all non-empty overlaps are collected.  When *names* is
        provided, only those tensor names are considered; names absent from either
        spec are ignored.  This lets callers with a precomputed name intersection
        avoid scanning the whole sender map for every rank pair.
        """
        result: dict[str, Shards] = {}
        sender_items = (
            sender
            if names is None
            else ((name, sender[name]) for name in names if name in sender)
        )
        for name, s_shards in sender_items:
            if name not in receiver:
                continue
            r_shards = receiver[name]

            overlap_shards: Shards = []
            for s_shard in s_shards:
                for r_shard in r_shards:
                    alignment = _check_shard_compatibility(s_shard, r_shard)
                    if alignment is None:
                        raise ValueError(f"Shard compatibility check failed for {name}")

                    overlap_dims = [
                        (max(ls, lr), min(rs, rr), w) for ls, rs, lr, rr, w in alignment
                    ]

                    if all(lo < hi for lo, hi, _ in overlap_dims):
                        overlap_shards.append(overlap_dims)

            if overlap_shards:
                result[name] = overlap_shards

        return ShardSpec(result)


class BoundShardSpec:
    """``f = spec(tensors)``: overlap-indexed pack/unpack into *tensors*.

    * ``v = f[c]`` — *c* maps ranks to overlap :class:`ShardSpec` entries.
      Returns a ``dict`` of one-dimensional ``uint8`` tensors (wire layout).
    * ``f[c] = v`` — *v* maps ranks to matching ``uint8`` flat tensors, copied
      into *tensors* at the overlap regions (receiver layout; *shard_spec* must
      describe *tensors*).
    """

    def __init__(self, shard_spec: ShardSpec, tensors: dict[str, torch.Tensor]) -> None:
        self._shard_spec = shard_spec
        self._tensors = dict(tensors)
        self.device = tensors[list(tensors.keys())[0]].device

        # Sanity check and flatten
        for name, shards in self._shard_spec:
            assert name in self._tensors, f"Missing tensor {name} for overlap entry"
            tensor = self._tensors[name]
            assert tensor.is_contiguous(), f"Tensor {name} is not contiguous"
            assert tensor.dim() == 1, f"Tensor {name} must be 1D"
            assert shards_numel(shards) == tensor.numel(), (
                f"spec and tensor numel mismatch for {name}, {shards_numel(shards)} vs {tensor.numel()}"
            )
            self._tensors[name] = tensor.flatten()
            assert tensor.device == self.device, (
                f"Tensor {name} is not on the same device as other tensors"
            )

        # Drop tensors not listed in shard_spec
        for name in list(self._tensors):
            if name not in self._shard_spec:
                del self._tensors[name]

    @staticmethod
    def slice_copy(
        large: "BoundShardSpec", small: "BoundShardSpec", big2small: bool = True
    ) -> None:
        """
        Copy data between two flattened buffers (resharding; no transpose here).
        The direction is set by ``big2small``. "small" is a subset of "large".
        Executed in O(1) CUDA kernel launches via :func:`batched_copy`.
        """
        batched_copy(BoundShardSpec._slice_copy_pairs(large, small, big2small))

    @staticmethod
    def _slice_copy_pairs(
        large: "BoundShardSpec",
        small: "BoundShardSpec",
        big2small: bool = True,
    ) -> list[tuple[torch.Tensor, torch.Tensor]]:
        """Return (dst_view, src_view) pairs for :meth:`slice_copy` without executing the copies."""
        pairs: list[tuple[torch.Tensor, torch.Tensor]] = []
        for name, _ in small._shard_spec:
            assert name in large._shard_spec, f"Missing tensor {name} for large entry"

            for s_shard, s_tensor in shards_iterator(
                small._shard_spec[name], small._tensors[name]
            ):
                for b_shard, b_tensor in shards_iterator(
                    large._shard_spec[name], large._tensors[name]
                ):
                    assert b_tensor.dtype == s_tensor.dtype, (
                        f"Tensor dtype mismatch for {name}: {b_tensor.dtype} vs {s_tensor.dtype}"
                    )
                    alignment = _check_shard_compatibility(b_shard, s_shard)

                    if not alignment or not all(
                        bl <= sl and sr <= br for bl, br, sl, sr, _ in alignment
                    ):
                        continue

                    slices = tuple(
                        slice(sl - bl, sr - bl) for bl, _, sl, sr, _ in alignment
                    )
                    if big2small:
                        pairs.append((s_tensor, b_tensor[slices]))
                    else:
                        pairs.append((b_tensor[slices], s_tensor))
                    break
        return pairs

    def __getitem__(self, dst_specs: dict[int, ShardSpec]) -> dict[int, torch.Tensor]:
        # Wire layout uses dtypes from the *full* bound tensors (per-name), not the tensor objects themselves.
        dtype_by_name = {n: t.dtype for n, t in self._tensors.items()}
        dst_tensors = {
            rank: dst_spec.make_byte_chunk(dtype_by_name, str(self.device))
            for rank, dst_spec in dst_specs.items()
        }
        all_pairs: list[tuple[torch.Tensor, torch.Tensor]] = []
        for rank, dst_spec in dst_specs.items():
            dst_tensor = dst_tensors[rank]
            state_dict = {
                name: dst_tensor[start:end].view(dtype)
                for start, end, name, dtype in dst_spec.iter_with_intv(self._tensors)
            }
            all_pairs += BoundShardSpec._slice_copy_pairs(
                self, dst_spec(state_dict), big2small=True
            )
        batched_copy(all_pairs)  # all ranks in O(1) launches
        return dst_tensors

    def setitem_pairs(
        self,
        src_specs: dict[int, ShardSpec],
        src_tensors: dict[int, torch.Tensor],
    ) -> list[tuple[torch.Tensor, torch.Tensor]]:
        """(dst_view, src_view) pairs for ``f[src_specs] = src_tensors`` without executing (for CopyPlan)."""
        named_tensors: dict[int, dict[str, torch.Tensor]] = {}
        for rank, src_spec in src_specs.items():
            src_tensor = src_tensors[rank]
            named_tensors[rank] = {
                name: src_tensor[start:end].view(dtype)
                for start, end, name, dtype in src_spec.iter_with_intv(self._tensors)
            }
        return self.setitem_named_pairs(src_specs, named_tensors)

    def setitem_named_pairs(
        self,
        src_specs: dict[int, ShardSpec],
        src_tensors: dict[int, dict[str, torch.Tensor]],
    ) -> list[tuple[torch.Tensor, torch.Tensor]]:
        """Like :meth:`setitem_pairs`, with already-carved per-name source tensors.

        This is useful when one packed source feeds multiple destination layouts: callers carve its original
        wire layout once, then select whole names without pretending the filtered names start at byte zero.
        The returned pairs still describe direct source-to-destination copies, so all destinations can be
        executed by one :class:`CopyPlan` kernel with no intermediate dependency.
        """
        all_pairs: list[tuple[torch.Tensor, torch.Tensor]] = []
        for rank, src_spec in src_specs.items():
            all_pairs += BoundShardSpec._slice_copy_pairs(
                self,
                src_spec(src_tensors[rank]),
                big2small=False,
            )
        return all_pairs

    def __setitem__(
        self,
        src_specs: dict[int, ShardSpec],
        src_tensors: dict[int, torch.Tensor],
    ) -> None:
        batched_copy(
            self.setitem_pairs(src_specs, src_tensors)
        )  # all ranks in O(1) launches

    def pack_into_pairs(self, dst_spec: ShardSpec, out: torch.Tensor):
        """Like :meth:`pack_into` but returns ``(nbytes, pairs)`` without executing the copies.

        Lets the sender collect pairs across all peers of a round and run a single :func:`batched_copy`.
        """
        dtype_by_name = {name: t.dtype for name, t in self._tensors.items()}
        nbytes = dst_spec.nbytes(dtype_by_name)
        assert out.dtype == torch.uint8 and out.dim() == 1 and out.numel() >= nbytes, (
            f"pack_into needs a 1-D uint8 buffer of >= {nbytes} bytes, got {tuple(out.shape)} {out.dtype}"
        )
        region = out[:nbytes]
        state_dict = {
            name: region[start:end].view(dtype)
            for start, end, name, dtype in dst_spec.iter_with_intv(self._tensors)
        }
        return nbytes, BoundShardSpec._slice_copy_pairs(
            self, dst_spec(state_dict), big2small=True
        )

    def pack_into(self, dst_spec: ShardSpec, out: torch.Tensor) -> int:
        """Pack the overlap region described by *dst_spec* into a caller-owned uint8 buffer *out*.

        Equivalent to ``self[{r: dst_spec}][r]`` but writes into a pre-allocated (e.g. RDMA-registered)
        buffer. Executes the copies in O(1) launches via :func:`batched_copy`. Returns bytes packed.
        """
        nbytes, pairs = self.pack_into_pairs(dst_spec, out)
        batched_copy(pairs)
        return nbytes


def _check_shard_compatibility(
    s_shard: Shard, r_shard: Shard
) -> list[tuple[int, int, int, int, int]] | None:
    """Check whether two shard specs can be aligned to a common dimensionality.

    A dimension ``(l*a, r*a, w*a)`` is equivalent to
    ``[(l, r, w), (0, a, a)]`` — trailing full dimensions can be split off
    or merged.  More generally, a contiguous range ``[l, r)`` within a
    single "row" of an outer dimension can also be split.

    Returns a list of ``(l1, r1, l2, r2, w)`` pairs: sender interval
    ``[l1, r1)``, receiver interval ``[l2, r2)``, shared width ``w`` per
    aligned dimension, or ``None`` if no valid alignment exists.
    """
    s_cur = next(iter(s_shard), None)
    r_cur = next(iter(r_shard), None)
    s_iter = iter(s_shard[1:])
    r_iter = iter(r_shard[1:])
    aligned: list[tuple[int, int, int, int, int]] = []

    while s_cur is not None and r_cur is not None:
        sl, sr, sw = s_cur
        rl, rr, rw = r_cur
        # Use absolute widths for compatibility arithmetic; sign encodes transpose.
        asw, arw = abs(sw), abs(rw)

        if asw == arw:
            aligned.append((sl, sr, rl, rr, asw))
            s_cur = next(s_iter, None)
            r_cur = next(r_iter, None)

        elif asw > arw:
            if asw % arw != 0:
                return None
            tail = asw // arw
            if sl % tail == 0 and sr % tail == 0:
                aligned.append((sl // tail, sr // tail, rl, rr, arw))
                s_cur = (0, tail, tail)
            elif sl // tail == (sr - 1) // tail:
                row = sl // tail
                aligned.append((row, row + 1, rl, rr, arw))
                s_cur = (sl - row * tail, sr - row * tail, tail)
            else:
                return None
            r_cur = next(r_iter, None)

        else:  # asw < arw
            if arw % asw != 0:
                return None
            tail = arw // asw
            if rl % tail == 0 and rr % tail == 0:
                aligned.append((sl, sr, rl // tail, rr // tail, asw))
                r_cur = (0, tail, tail)
            elif rl // tail == (rr - 1) // tail:
                row = rl // tail
                aligned.append((sl, sr, row, row + 1, asw))
                r_cur = (rl - row * tail, rr - row * tail, tail)
            else:
                return None
            s_cur = next(s_iter, None)

    if s_cur is not None or r_cur is not None:
        return None
    return aligned


def _shard_contained_in(inner: Shard, outer: Shard) -> bool:
    """True if *inner*'s box lies inside *outer* (same dimensionality and width metadata)."""
    if len(inner) != len(outer):
        return False
    for (li, ri, wi), (lo, ro, wo) in zip(inner, outer):
        if abs(wi) != abs(wo):
            return False
        if not (lo <= li < ri <= ro):
            return False
    return True


def _try_merge_two_shards(a: Shard, b: Shard) -> Shard | None:
    """If *a* and *b* match on all but one axis, and on that axis the intervals touch or overlap, return the union."""
    if len(a) != len(b):
        return None
    diff_dim: int | None = None
    for d, ((la, ra, wa), (lb, rb, wb)) in enumerate(zip(a, b)):
        if abs(wa) != abs(wb) or (wa < 0) != (wb < 0):
            return None
        if (la, ra) != (lb, rb):
            if diff_dim is not None:
                return None
            diff_dim = d
    if diff_dim is None:
        return None
    la, ra, w = a[diff_dim]
    lb, rb, _ = b[diff_dim]
    if ra < lb or rb < la:
        return None
    ml, mr = min(la, lb), max(ra, rb)
    if ml >= mr:
        return None
    out = list(a)
    out[diff_dim] = (ml, mr, w)
    return out


def _merge_shards_for_src_shard_spec(shards: Shards) -> Shards:
    """Collapse duplicate / contained shards and merge axis-aligned neighbors that differ on one axis only."""
    keep = [True] * len(shards)
    i = 0
    while i < len(keep):
        if not keep[i]:
            i += 1
            continue
        cur_shard = shards[i]
        for j in range(0, i):
            if keep[j]:
                shard = shards[j]
                if _shard_contained_in(shard, cur_shard):
                    keep[j] = False
                elif _shard_contained_in(cur_shard, shard):
                    keep[i] = False
                    break
                elif c := _try_merge_two_shards(shard, cur_shard):
                    shards.append(c)
                    keep.append(True)
                    keep[i] = False
                    keep[j] = False
                    break
        i += 1
    return [shard for i, shard in enumerate(shards) if keep[i]]


def _clip_shard_dim0(shard: Shard, start: int, end: int) -> Shard | None:
    """Intersect *shard* with the full-tensor dim-0 interval ``[start, end)``."""
    if not shard:
        return None
    left, right, width = shard[0]
    clipped_left, clipped_right = max(left, start), min(right, end)
    if clipped_left >= clipped_right:
        return None
    out = list(shard)
    out[0] = (clipped_left, clipped_right, width)
    return out


def _source_full_shape(
    entry: dict[str, list[ShardMapping]], source_name: str
) -> tuple[int, ...]:
    shapes = {
        tuple(abs(width) for _left, _right, width in source_shard)
        for mappings in entry.values()
        for source_shard, _destination_shard in mappings
    }
    if len(shapes) != 1:
        raise ValueError(
            f"source tensor {source_name!r} has inconsistent full shapes: {sorted(shapes)}"
        )
    shape = next(iter(shapes))
    if not shape:
        raise ValueError(f"cannot logically split scalar source tensor {source_name!r}")
    return shape


def _mapping_permutation(source_shard: Shard) -> list[int]:
    negative = [i for i, (_left, _right, width) in enumerate(source_shard) if width < 0]
    positive = [
        i for i, (_left, _right, width) in enumerate(source_shard) if width >= 0
    ]
    return positive + negative


def _validate_dim0_split_mapping(
    source_name: str,
    source_shard: Shard,
    destination_name: str,
    destination_shard: Shard,
) -> None:
    """Prove a dim-0 source cut maps to one rectangular destination cut."""
    if len(source_shard) != len(destination_shard):
        raise ValueError(
            f"cannot dim-0 split {source_name}->{destination_name}: source/destination dimensions "
            f"differ ({len(source_shard)} vs {len(destination_shard)})"
        )
    source_shape = shard_shape(source_shard)
    destination_shape = shard_shape(destination_shard)
    permutation = _mapping_permutation(source_shard)
    mapped_shape = tuple(source_shape[source_dim] for source_dim in permutation)
    if mapped_shape != destination_shape:
        raise ValueError(
            f"cannot dim-0 split {source_name}->{destination_name}: mapped source shape "
            f"{mapped_shape} != destination shape {destination_shape}"
        )


def split_large_load_spec_sources(
    load_spec: "LoadSpec",
    dtype_spec: dict[str, torch.dtype],
    max_bytes: int | None = None,
) -> tuple["LoadSpec", dict[str, torch.dtype], list[dict]]:
    """Replace oversized checkpoint names with row-aligned logical source names.

    The underlying parameter tensors and mappings are unchanged.  A logical name retains the physical
    checkpoint coordinate system but exposes only ``physical[start:end, ...]`` through
    :attr:`LoadSpec.src_shard_spec`.  Consequently routing treats pieces as independent tensors while fused
    model↔wire copies continue to address the original model parameters directly.

    Returns ``(translated_load_spec, translated_dtype_spec, split_report)``.  The operation is idempotent:
    records that already contain logical names pass through unchanged.
    """
    cap = logical_tensor_cap_bytes() if max_bytes is None else int(max_bytes)
    if cap < 0:
        raise ValueError(f"logical tensor cap must be non-negative, got {cap}")
    if cap == 0:
        return load_spec, dict(dtype_spec), []

    translated: dict[str, dict[str, list[ShardMapping]]] = {}
    translated_dtypes: dict[str, torch.dtype] = {}
    report: list[dict] = []

    for source_name, entry in load_spec.entries.items():
        if source_name not in dtype_spec:
            raise KeyError(f"missing dtype for LoadSpec source {source_name!r}")
        dtype = dtype_spec[source_name]
        already_logical = parse_logical_tensor_name(source_name)
        if already_logical is not None:
            translated[source_name] = copy.deepcopy(entry)
            translated_dtypes[source_name] = dtype
            continue
        if _LOGICAL_TENSOR_SPLIT_MARKER in source_name:
            raise ValueError(
                f"checkpoint tensor name {source_name!r} contains reserved marker "
                f"{_LOGICAL_TENSOR_SPLIT_MARKER!r}"
            )

        full_shape = _source_full_shape(entry, source_name)
        full_bytes = math.prod(full_shape) * dtype.itemsize
        if full_bytes <= cap:
            translated[source_name] = copy.deepcopy(entry)
            translated_dtypes[source_name] = dtype
            continue

        row_bytes = math.prod(full_shape[1:]) * dtype.itemsize
        if row_bytes > cap:
            raise ValueError(
                f"cannot cap {source_name!r} at {cap} bytes by splitting dim 0: one row is "
                f"{row_bytes} bytes (shape={full_shape}, dtype={dtype})"
            )
        for destination_name, mappings in entry.items():
            for source_shard, destination_shard in mappings:
                _validate_dim0_split_mapping(
                    source_name,
                    source_shard,
                    destination_name,
                    destination_shard,
                )

        rows_per_piece = max(1, cap // row_bytes)
        logical_names: list[str] = []
        max_piece_bytes = 0
        for start in range(0, full_shape[0], rows_per_piece):
            end = min(start + rows_per_piece, full_shape[0])
            # Pipeline stages / tensor-parallel ranks need only the logical pieces intersecting their local
            # mappings.  All workers still derive identical names for any shared interval.
            intersects_local_mapping = any(
                max(source_shard[0][0], start) < min(source_shard[0][1], end)
                for mappings in entry.values()
                for source_shard, _destination_shard in mappings
            )
            if not intersects_local_mapping:
                continue
            logical_name = logical_tensor_name(source_name, start, end)
            if logical_name in translated:
                raise ValueError(f"logical tensor name collision: {logical_name!r}")
            translated[logical_name] = copy.deepcopy(entry)
            translated_dtypes[logical_name] = dtype
            logical_names.append(logical_name)
            max_piece_bytes = max(max_piece_bytes, (end - start) * row_bytes)

        if not logical_names:
            raise RuntimeError(
                f"logical split of {source_name!r} produced no local pieces"
            )
        report.append(
            {
                "source": source_name,
                "shape": full_shape,
                "dtype": str(dtype),
                "full_bytes": full_bytes,
                "row_bytes": row_bytes,
                "logical_names": tuple(logical_names),
                "max_piece_bytes": max_piece_bytes,
            }
        )

    return LoadSpec(translated), translated_dtypes, report


def validate_logical_tensor_partitions(shard_specs: Iterable[ShardSpec]) -> None:
    """Reject inconsistent logical split grids across workers.

    Workers may own disjoint subsets of a physical tensor, so they need not list every logical piece.  But
    any intervals visible globally must be non-overlapping (except exact duplicates), and a physical source
    cannot appear both split and unsplit.  This catches mismatched caps before routing could silently treat
    incompatible logical names as unrelated tensors; dtype consistency is validated separately at gather.
    """
    plain_names: set[str] = set()
    intervals: dict[str, set[tuple[int, int]]] = {}
    for spec in shard_specs:
        for name, _shards in spec:
            logical = parse_logical_tensor_name(name)
            if logical is None:
                plain_names.add(name)
                continue
            physical, start, end = logical
            intervals.setdefault(physical, set()).add((start, end))

    mixed = sorted(physical for physical in intervals if physical in plain_names)
    if mixed:
        raise ValueError(
            "inconsistent logical tensor splitting: physical and logical names coexist for "
            f"{mixed[:8]}"
        )
    for physical, unique_intervals in intervals.items():
        ordered = sorted(unique_intervals)
        for (left_start, left_end), (right_start, right_end) in zip(
            ordered, ordered[1:]
        ):
            if right_start < left_end:
                raise ValueError(
                    f"inconsistent logical tensor split grid for {physical!r}: "
                    f"[{left_start}, {left_end}) overlaps [{right_start}, {right_end})"
                )


class LoadSpec:
    """
    A transfer spec describes the correspondence between 2 sets of tensors.

    The format is::

        {
            "src_name": {
                "dst_name": [
                    (src_shard, dst_shard),  # each is a :class:`Shard`
                    ...
                ],
                ...
            },
            ...
        }

    Each ``(src_shard, dst_shard)`` pair maps one axis-aligned box in the source
    tensor to one box in the destination; ``shard_numel`` must match on both
    sides. Multiple pairs per ``(src_name, dst_name)`` are allowed (e.g. split
    QKV blocks).
    """

    def __init__(self, entries: dict[str, dict[str, list[ShardMapping]]]) -> None:
        self.entries = entries
        for sname, dname, s_shard, d_shard in self:
            _sanity_check(sname, [s_shard])
            _sanity_check(dname, [d_shard])
            assert shard_numel(s_shard) == shard_numel(d_shard), (
                f"numel mismatch for {sname}->{dname}: "
                f"{shard_numel(s_shard)} vs {shard_numel(d_shard)}"
            )

    def __len__(self) -> int:
        return len(self.entries)

    @property
    def src_shard_spec(self) -> ShardSpec:
        if not hasattr(self, "_src_spec"):
            self._src_spec = self._compute_src_shard_spec()
        return self._src_spec

    def _compute_src_shard_spec(self) -> ShardSpec:
        def _unique_shards_for_src(entry: dict[str, list[ShardMapping]]) -> Shards:
            seen: set[tuple[tuple[int, int, int], ...]] = set()
            out: Shards = []
            for mappings in entry.values():
                for s_shard, _ in mappings:
                    key = tuple(s_shard)
                    if key not in seen:
                        seen.add(key)
                        out.append(list(s_shard))
            return _merge_shards_for_src_shard_spec(out)

        result: dict[str, Shards] = {}
        for src_name, entry in self.entries.items():
            shards = _unique_shards_for_src(entry)
            logical = parse_logical_tensor_name(src_name)
            if logical is not None:
                _physical_name, start, end = logical
                shards = [
                    clipped
                    for shard in shards
                    if (clipped := _clip_shard_dim0(shard, start, end)) is not None
                ]
                if not shards:
                    raise ValueError(
                        f"logical source {src_name!r} does not intersect any LoadSpec mapping"
                    )
                shards = _merge_shards_for_src_shard_spec(shards)
            result[src_name] = shards
        return ShardSpec(result)

    def __iter__(self) -> Iterator[tuple[str, str, Shard, Shard]]:
        for sname, entry in self.entries.items():
            for dname, mappings in entry.items():
                for s_shard, d_shard in mappings:
                    yield sname, dname, s_shard, d_shard

    def load_from_full(
        self,
        hf_fetcher: dict[str, Callable[[], torch.Tensor]],
        dst_tensors: dict[str, torch.Tensor],
    ) -> None:
        """Restore *dst_tensors* from HF checkpoint via the inferred spec.

        Only tensors whose HF source name appears in :attr:`entries` are
        materialised — the rest of *hf_fetcher* is untouched.
        """
        physical_sources: dict[str, list[str]] = {}
        for source_name in self.entries:
            logical = parse_logical_tensor_name(source_name)
            physical_name = logical[0] if logical is not None else source_name
            physical_sources.setdefault(physical_name, []).append(source_name)

        for physical_name, source_names in physical_sources.items():
            if physical_name not in hf_fetcher:
                continue
            src_tensor = hf_fetcher[physical_name]()
            for source_name in source_names:
                logical = parse_logical_tensor_name(source_name)
                logical_bounds = None if logical is None else logical[1:]
                for dname, mappings in self.entries[source_name].items():
                    assert dname in dst_tensors, f"Missing tensor {dname} for load"
                    dst_tensor = dst_tensors[dname]
                    for s_shard, d_shard in mappings:
                        source_box = list(s_shard)
                        if logical_bounds is not None:
                            clipped = _clip_shard_dim0(source_box, *logical_bounds)
                            if clipped is None:
                                continue
                            source_box = clipped
                            _validate_dim0_split_mapping(
                                source_name, s_shard, dname, d_shard
                            )

                        src_slices = tuple(slice(l, r) for l, r, _ in source_box)
                        src_block = src_tensor[src_slices]
                        if logical_bounds is None:
                            dst_slices = tuple(slice(l, r) for l, r, _ in d_shard)
                        else:
                            permutation = _mapping_permutation(s_shard)
                            worker_slices = []
                            for worker_dim, (
                                dst_left,
                                _dst_right,
                                _dst_width,
                            ) in enumerate(d_shard):
                                source_dim = permutation[worker_dim]
                                source_left = s_shard[source_dim][0]
                                box_left, box_right, _box_width = source_box[source_dim]
                                worker_slices.append(
                                    slice(
                                        dst_left + box_left - source_left,
                                        dst_left + box_right - source_left,
                                    )
                                )
                            dst_slices = tuple(worker_slices)
                        if any(width < 0 for _, _, width in s_shard):
                            src_block = src_block.permute(
                                _mapping_permutation(s_shard)
                            ).contiguous()
                        shard_full_shape = tuple(abs(width) for _, _, width in d_shard)
                        dst_tensor.reshape(shard_full_shape)[dst_slices].copy_(
                            src_block
                        )

    def copy_fromto_params(
        self,
        src_shard_spec: ShardSpec,
        src_tensors: dict[
            str, torch.Tensor
        ],  # 1D comm buffers associated with src_shard_spec
        dst_tensors: dict[str, torch.Tensor],  # parameter tensors (2D+)
        *,
        src_to_dst: bool,
    ) -> None:
        """
        Copy data between communication buffer and parameter tensors for compute.

        Only a subset of names indicated by `shard_spec` are copied.

        NOTE: Only mapping whose source fully contained in shard_spec are copied. Partial overlaps are not supported.

        Transpose-aware (shards with negative ``w``) and batched: the actual copies are executed in O(1)
        CUDA kernel launches via :func:`batched_copy`.
        """
        batched_copy(
            _copy_fromto_pairs(
                self, src_shard_spec, src_tensors, dst_tensors, src_to_dst=src_to_dst
            )
        )

    def copy_fromto_pairs(
        self, src_shard_spec, src_tensors, dst_tensors, *, src_to_dst
    ):
        """(dst_view, src_view) pairs for :meth:`copy_fromto_params` without executing (for CopyPlan)."""
        return _copy_fromto_pairs(
            self, src_shard_spec, src_tensors, dst_tensors, src_to_dst=src_to_dst
        )

    def fuse_copy_pairs(self, ospec, wire_buf, dst_tensors, dtype_spec, *, src_to_dst):
        """(dst_view, src_view) pairs mapping parameter tensors **directly** to a wire buffer.

        Fuses the two packing stages — ``copy_fromto_params`` (model↔logical, transpose) and
        ``pack_into``/``__setitem__`` (logical↔wire overlap reshard) — into one set of pairs, so no
        intermediate logical buffer is needed. *ospec* is the per-peer overlap :class:`ShardSpec` (HF
        coordinates); *wire_buf* is the peer's 1-D uint8 RDMA buffer, laid out per *ospec* using
        *dtype_spec* (identical byte layout to the 2-stage path). ``src_to_dst=True`` loads wire→model
        (receiver); ``False`` saves model→wire (sender). Raises :class:`FuseUnsupported` for shard shapes
        the fast path can't map (caller falls back to the 2-stage path).
        """
        return _fuse_copy_pairs(
            self, ospec, wire_buf, dst_tensors, dtype_spec, src_to_dst=src_to_dst
        )


# =========================================================================================
# Batched copy: O(1) CUDA kernel launches for many strided / transposed segment copies.
#
# Each copy is one (dst_view, src_view) pair with matching shape/dtype; src/dst may be strided or
# permuted views (a transpose is a permuted src view). A single Triton kernel copies all segments of a
# given dtype in one launch, gathering/scattering across many base tensors via int64 address arrays and
# per-segment strides. Falls back to a per-pair ``copy_`` loop when Triton/CUDA is unavailable.
# =========================================================================================
try:
    import triton
    import triton.language as tl

    _HAS_TRITON = True
except Exception:  # pragma: no cover - triton optional
    _HAS_TRITON = False

_MAXD = 6  # max dims a single copy segment may have

_TL_DTYPE: dict = {}
if _HAS_TRITON:
    _TL_DTYPE = {
        torch.float32: tl.float32,
        torch.float16: tl.float16,
        torch.bfloat16: tl.bfloat16,
        torch.uint8: tl.uint8,
        torch.int8: tl.int8,
        torch.int32: tl.int32,
        torch.int64: tl.int64,
    }

    @triton.jit
    def _batched_copy_kernel(
        tile_seg,
        tile_start,
        seg_src_base,
        seg_dst_base,
        seg_numel,
        seg_shape,
        seg_src_stride,
        seg_dst_stride,
        ELSIZE: tl.constexpr,
        DTYPE: tl.constexpr,
        BLOCK: tl.constexpr,
        MAXD: tl.constexpr,
    ):
        pid = tl.program_id(0)
        seg = tl.load(tile_seg + pid)
        start = tl.load(tile_start + pid)
        numel = tl.load(seg_numel + seg)
        src_base = tl.load(seg_src_base + seg)
        dst_base = tl.load(seg_dst_base + seg)
        e = start + tl.arange(0, BLOCK).to(tl.int64)
        mask = e < numel
        rem = e
        src_off = tl.zeros([BLOCK], tl.int64)
        dst_off = tl.zeros([BLOCK], tl.int64)
        for d in tl.static_range(MAXD - 1, -1, -1):
            sh = tl.load(seg_shape + seg * MAXD + d)
            sst = tl.load(seg_src_stride + seg * MAXD + d)
            dst_st = tl.load(seg_dst_stride + seg * MAXD + d)
            coord = rem % sh
            rem = rem // sh
            src_off += coord * sst
            dst_off += coord * dst_st
        src_addr = (src_base + src_off * ELSIZE).to(tl.pointer_type(DTYPE))
        dst_addr = (dst_base + dst_off * ELSIZE).to(tl.pointer_type(DTYPE))
        val = tl.load(src_addr, mask=mask)
        tl.store(dst_addr, val, mask=mask)

    @triton.jit
    def _flat_copy_kernel(
        tile_seg,
        tile_start,
        seg_src_base,
        seg_dst_base,
        seg_numel,
        ELSIZE: tl.constexpr,
        DTYPE: tl.constexpr,
        BLOCK: tl.constexpr,
    ):
        # Fast path for segments that are contiguous in BOTH src and dst: element i of the flattened
        # tensor is at base + i*ELSIZE, so no per-element coordinate decomposition is needed. This runs
        # near memcpy bandwidth, unlike the substantially slower strided kernel's int64 %/​// over MAXD dims.
        pid = tl.program_id(0)
        seg = tl.load(tile_seg + pid)
        start = tl.load(tile_start + pid)
        numel = tl.load(seg_numel + seg)
        src_base = tl.load(seg_src_base + seg)
        dst_base = tl.load(seg_dst_base + seg)
        e = start + tl.arange(0, BLOCK).to(tl.int64)
        mask = e < numel
        src_addr = (src_base + e * ELSIZE).to(tl.pointer_type(DTYPE))
        dst_addr = (dst_base + e * ELSIZE).to(tl.pointer_type(DTYPE))
        val = tl.load(src_addr, mask=mask)
        tl.store(dst_addr, val, mask=mask)

    @triton.jit
    def _unified_copy_kernel(
        tile_seg,
        tile_start,
        seg_src_base,
        seg_dst_base,
        seg_numel,
        seg_shape,
        seg_src_stride,
        seg_dst_stride,
        seg_flat,
        ELSIZE: tl.constexpr,
        DTYPE: tl.constexpr,
        BLOCK: tl.constexpr,
        MAXD: tl.constexpr,
    ):
        """Copy flat and transformed segments in one launch.

        ``seg_flat`` is uniform for every lane in a program, so the runtime branch does not diverge. This
        preserves the memcpy-like address path for ordinary tensors while allowing a peer's transposed or
        strided model mappings to share the same launch. It is used by topology's per-peer internal-consume
        plan, where launch count is part of the protocol rather than merely a local optimization.
        """
        pid = tl.program_id(0)
        seg = tl.load(tile_seg + pid)
        start = tl.load(tile_start + pid)
        numel = tl.load(seg_numel + seg)
        src_base = tl.load(seg_src_base + seg)
        dst_base = tl.load(seg_dst_base + seg)
        e = start + tl.arange(0, BLOCK).to(tl.int64)
        mask = e < numel
        if tl.load(seg_flat + seg):
            src_off = e
            dst_off = e
        else:
            rem = e
            src_off = tl.zeros([BLOCK], tl.int64)
            dst_off = tl.zeros([BLOCK], tl.int64)
            for d in tl.static_range(MAXD - 1, -1, -1):
                sh = tl.load(seg_shape + seg * MAXD + d)
                sst = tl.load(seg_src_stride + seg * MAXD + d)
                dst_st = tl.load(seg_dst_stride + seg * MAXD + d)
                coord = rem % sh
                rem = rem // sh
                src_off += coord * sst
                dst_off += coord * dst_st
        src_addr = (src_base + src_off * ELSIZE).to(tl.pointer_type(DTYPE))
        dst_addr = (dst_base + dst_off * ELSIZE).to(tl.pointer_type(DTYPE))
        val = tl.load(src_addr, mask=mask)
        tl.store(dst_addr, val, mask=mask)


def _pad(seq, n, fill):
    seq = list(seq)[:n]
    return seq + [fill] * (n - len(seq))


def batched_copy(pairs, *, block: int = 1024) -> None:
    """One-shot element-wise ``dst <- src`` for every (dst_view, src_view) pair (builds a plan, runs it).

    For repeated transfers over *static* buffers, build a :class:`CopyPlan` once and call ``.run()`` each
    time instead — that caches the alignment + descriptors so per-transfer work is O(1), not O(segments).
    """
    CopyPlan(pairs, block=block).run()


class CopyPlan:
    """Precomputed descriptors for many strided/transposed segment copies, replayable in O(1) launches.

    Building does the (Python) grouping + descriptor construction once; :meth:`run` only launches the
    Triton kernel per dtype group (no Python scan, no descriptor rebuild). The pair views must stay valid
    across ``run`` calls — i.e. bound to buffers whose addresses don't change (model params updated
    in-place, reused logical/wire buffers). Falls back to a per-pair ``copy_`` loop off-CUDA/without Triton.
    """

    def __init__(
        self,
        pairs,
        *,
        block: int = 1024,
        single_kernel: bool = False,
        unified_dtype_kernels: bool = False,
        source_ptrs: list[int] | None = None,
        source_region: tuple[int, int] | None = None,
    ) -> None:
        self.block = block
        self._groups: list = []  # strided/transposed segments -> coordinate kernel
        self._flat_groups: list = []  # contiguous on both sides -> flat linear-index kernel (memcpy-speed)
        self._unified_groups: list = []  # flat + transformed segments, exactly one supported dtype/launch
        self._fallback: list = []
        if single_kernel and unified_dtype_kernels:
            raise ValueError(
                "single_kernel and unified_dtype_kernels are mutually exclusive"
            )
        pairs = list(pairs)
        if source_ptrs is not None and len(source_ptrs) != len(pairs):
            raise ValueError(
                f"source_ptrs has {len(source_ptrs)} entries for {len(pairs)} copy pairs"
            )
        triples = [
            (d, s, int(source_ptrs[i]) if source_ptrs is not None else s.data_ptr())
            for i, (d, s) in enumerate(pairs)
            if d.numel() > 0
        ]
        self._source_base = int(source_region[0]) if source_region is not None else None
        self._source_extent = int(source_region[1]) if source_region is not None else 0
        if source_region is not None:
            if self._source_extent < 0:
                raise ValueError(f"negative source-region extent {self._source_extent}")
            limit = self._source_base + self._source_extent
            outside = [p for _d, _s, p in triples if not self._source_base <= p < limit]
            if outside:
                raise ValueError(
                    f"CopyPlan source pointer outside rebindable region "
                    f"[{self._source_base:#x}, {limit:#x}): {outside[0]:#x}"
                )
        if not triples:
            return
        if not (_HAS_TRITON and all(d.is_cuda and s.is_cuda for d, s, _ in triples)):
            if source_ptrs is not None or source_region is not None:
                raise ValueError(
                    "raw/rebindable source pointers require CUDA Triton CopyPlan execution"
                )
            self._fallback = [(d, s) for d, s, _ in triples]
            return
        from collections import defaultdict

        flat: dict = defaultdict(list)
        strided: dict = defaultdict(list)
        supported: dict = defaultdict(list)
        for d, s, source_ptr in triples:
            assert d.shape == s.shape, (
                f"CopyPlan shape mismatch {tuple(d.shape)} vs {tuple(s.shape)}"
            )
            if d.dtype != s.dtype:
                # Frameworks may deliberately keep a numerically identical checkpoint tensor in a wider
                # runtime dtype (GLM-5's rollout DSA k_norm is FP32 while Megatron stores BF16). Triton copy
                # descriptors are byte-preserving and therefore cannot implement this cast. Local model<->
                # logical copies can safely use torch.copy_, which performs the required conversion. Raw
                # pointer overrides identify peer IPC memory and cannot be represented by a normal Tensor
                # copy, so keep rejecting that unsupported case explicitly.
                if source_ptr != s.data_ptr() or source_region is not None:
                    raise ValueError(
                        f"raw/rebindable source pointer dtype conversion unsupported: "
                        f"{s.dtype} -> {d.dtype}"
                    )
                self._fallback.append((d, s))
                continue
            assert d.dim() <= _MAXD, f"CopyPlan: {d.dim()} dims exceeds MAXD={_MAXD}"
            if d.dtype not in _TL_DTYPE:
                if source_ptr != s.data_ptr():
                    raise ValueError(
                        f"raw source pointer override unsupported for dtype {d.dtype}"
                    )
                self._fallback.append((d, s))
            elif single_kernel or unified_dtype_kernels:
                supported[d.dtype].append((d, s, source_ptr))
            elif d.is_contiguous() and s.is_contiguous():
                flat[d.dtype].append(
                    (d, s, source_ptr)
                )  # linear copy, no coordinate math
            else:
                strided[d.dtype].append((d, s, source_ptr))
        if source_region is not None and self._fallback:
            raise ValueError(
                "rebindable CopyPlan sources do not support fallback copy segments"
            )
        if single_kernel or unified_dtype_kernels:
            if self._fallback or (single_kernel and len(supported) != 1):
                raise ValueError(
                    "unified-kernel CopyPlan requires Triton-supported dtype groups and no fallback pairs; "
                    f"dtypes={list(supported)} fallback={len(self._fallback)}"
                )
            for dtype, gp in supported.items():
                self._unified_groups.append(self._build_unified(gp, dtype))
            return
        for dtype, gp in flat.items():
            self._flat_groups.append(self._build_flat(gp, dtype))
        for dtype, gp in strided.items():
            self._groups.append(self._build_group(gp, dtype))

    def _tile_table(self, seg_numel, dev):
        """Map a flat program grid onto variable-size segments: BLOCK elements per program."""
        n = seg_numel.numel()
        ntiles = (seg_numel + self.block - 1) // self.block
        tile_seg = torch.repeat_interleave(
            torch.arange(n, device=dev, dtype=torch.int32), ntiles
        )
        seg_first_tile = torch.cumsum(ntiles, 0) - ntiles
        within = (
            torch.arange(tile_seg.numel(), device=dev, dtype=torch.int64)
            - seg_first_tile[tile_seg.to(torch.int64)]
        )
        return tile_seg, within * self.block

    def _build_flat(self, gp, dtype):
        dev = gp[0][0].device
        seg_src_base = torch.tensor(
            [source_ptr for _, _, source_ptr in gp], device=dev, dtype=torch.int64
        )
        seg_dst_base = torch.tensor(
            [d.data_ptr() for d, _, _ in gp], device=dev, dtype=torch.int64
        )
        seg_numel = torch.tensor(
            [d.numel() for d, _, _ in gp], device=dev, dtype=torch.int64
        )
        tile_seg, tile_start = self._tile_table(seg_numel, dev)
        elsize = torch.empty((), dtype=dtype).element_size()
        return (
            dtype,
            elsize,
            seg_src_base,
            seg_dst_base,
            seg_numel,
            tile_seg,
            tile_start,
        )

    def _build_group(self, gp, dtype):
        dev = gp[0][0].device
        seg_src_base = torch.tensor(
            [source_ptr for _, _, source_ptr in gp], device=dev, dtype=torch.int64
        )
        seg_dst_base = torch.tensor(
            [d.data_ptr() for d, _, _ in gp], device=dev, dtype=torch.int64
        )
        seg_numel = torch.tensor(
            [d.numel() for d, _, _ in gp], device=dev, dtype=torch.int64
        )
        seg_shape = torch.tensor(
            [_pad(d.shape, _MAXD, 1) for d, _, _ in gp], device=dev, dtype=torch.int64
        )
        seg_src_stride = torch.tensor(
            [_pad(s.stride(), _MAXD, 0) for _, s, _ in gp],
            device=dev,
            dtype=torch.int64,
        )
        seg_dst_stride = torch.tensor(
            [_pad(d.stride(), _MAXD, 0) for d, _, _ in gp],
            device=dev,
            dtype=torch.int64,
        )
        tile_seg, tile_start = self._tile_table(seg_numel, dev)
        elsize = torch.empty((), dtype=dtype).element_size()
        return (
            dtype,
            elsize,
            seg_src_base,
            seg_dst_base,
            seg_numel,
            seg_shape,
            seg_src_stride,
            seg_dst_stride,
            tile_seg,
            tile_start,
        )

    def _build_unified(self, gp, dtype):
        dev = gp[0][0].device
        seg_src_base = torch.tensor(
            [source_ptr for _, _, source_ptr in gp], device=dev, dtype=torch.int64
        )
        seg_dst_base = torch.tensor(
            [d.data_ptr() for d, _, _ in gp], device=dev, dtype=torch.int64
        )
        seg_numel = torch.tensor(
            [d.numel() for d, _, _ in gp], device=dev, dtype=torch.int64
        )
        seg_shape = torch.tensor(
            [_pad(d.shape, _MAXD, 1) for d, _, _ in gp], device=dev, dtype=torch.int64
        )
        seg_src_stride = torch.tensor(
            [_pad(s.stride(), _MAXD, 0) for _, s, _ in gp],
            device=dev,
            dtype=torch.int64,
        )
        seg_dst_stride = torch.tensor(
            [_pad(d.stride(), _MAXD, 0) for d, _, _ in gp],
            device=dev,
            dtype=torch.int64,
        )
        seg_flat = torch.tensor(
            [d.is_contiguous() and s.is_contiguous() for d, s, _ in gp],
            device=dev,
            dtype=torch.int8,
        )
        tile_seg, tile_start = self._tile_table(seg_numel, dev)
        elsize = torch.empty((), dtype=dtype).element_size()
        return (
            dtype,
            elsize,
            seg_src_base,
            seg_dst_base,
            seg_numel,
            seg_shape,
            seg_src_stride,
            seg_dst_stride,
            seg_flat,
            tile_seg,
            tile_start,
        )

    @property
    def launch_count(self) -> int:
        """Number of CUDA launches made by :meth:`run` (fallback copies count individually)."""
        return (
            len(self._fallback)
            + len(self._flat_groups)
            + len(self._groups)
            + len(self._unified_groups)
        )

    def rebase_sources(self, new_base: int) -> None:
        """Retarget every source descriptor to the same offsets under *new_base*.

        Plans created with ``source_region=(base, size)`` can therefore outlive an epoch-scoped scratch
        allocation. Descriptor updates are enqueued on the current CUDA stream; a subsequent :meth:`run`
        on that stream observes them without a host synchronization.
        """
        if self._source_base is None:
            raise RuntimeError("CopyPlan was not built with a rebindable source_region")
        new_base = int(new_base)
        delta = new_base - self._source_base
        if delta:
            for group in (*self._flat_groups, *self._groups, *self._unified_groups):
                group[2].add_(delta)
            self._source_base = new_base

    def run(self) -> None:
        for d, s in self._fallback:
            d.copy_(s)
        for g in self._flat_groups:
            (dtype, elsize, ssb, sdb, sn, tseg, tstart) = g
            _flat_copy_kernel[(tseg.numel(),)](
                tseg,
                tstart,
                ssb,
                sdb,
                sn,
                ELSIZE=elsize,
                DTYPE=_TL_DTYPE[dtype],
                BLOCK=self.block,
            )
        for g in self._groups:
            (dtype, elsize, ssb, sdb, sn, sh, sss, sds, tseg, tstart) = g
            _batched_copy_kernel[(tseg.numel(),)](
                tseg,
                tstart,
                ssb,
                sdb,
                sn,
                sh,
                sss,
                sds,
                ELSIZE=elsize,
                DTYPE=_TL_DTYPE[dtype],
                BLOCK=self.block,
                MAXD=_MAXD,
            )
        for g in self._unified_groups:
            (dtype, elsize, ssb, sdb, sn, sh, sss, sds, flat, tseg, tstart) = g
            _unified_copy_kernel[(tseg.numel(),)](
                tseg,
                tstart,
                ssb,
                sdb,
                sn,
                sh,
                sss,
                sds,
                flat,
                ELSIZE=elsize,
                DTYPE=_TL_DTYPE[dtype],
                BLOCK=self.block,
                MAXD=_MAXD,
            )


def _argsort_perm(perm):
    inv = [0] * len(perm)
    for k, p in enumerate(perm):
        inv[p] = k
    return inv


def _copy_fromto_pairs(
    load_spec, src_shard_spec, src_tensors, dst_tensors, *, src_to_dst
):
    """Build (dst_view, src_view) pairs for ``LoadSpec.copy_fromto_params``, applying transpose.

    A transposed mapping (some src dim has ``w<0``) is realised by permuting the *source* view so that
    element-wise ``dst[coord] = src[coord]`` performs the transpose — mirroring ``LoadSpec.load_from_full``.
    """
    pairs: list[tuple[torch.Tensor, torch.Tensor]] = []
    for sname, dname, s_shard, d_shard in load_spec:
        if sname not in src_shard_spec or dname not in dst_tensors:
            continue
        dst_tensor = dst_tensors[dname]
        neg_dims = [i for i, (_, _, w) in enumerate(s_shard) if w < 0]
        pos_dims = [i for i, (_, _, w) in enumerate(s_shard) if w >= 0]
        perm = pos_dims + neg_dims  # HF dim order -> worker dim order
        inv = _argsort_perm(perm)  # worker dim order -> HF
        logical = parse_logical_tensor_name(sname)
        for shard, src_tensor in shards_iterator(
            src_shard_spec[sname], src_tensors[sname]
        ):
            # Sender/receiver de-duplication can make ``shard`` a strict sub-box of a normal (non-logical)
            # LoadSpec mapping.  The old two-stage fallback handled intersections only for explicitly
            # dim-0-split logical names; for ordinary de-duplicated names it silently skipped the mapping and
            # packed uninitialized logical bytes.  Use the same rectangular intersection path whenever the
            # source and destination mapping dimensions are related by the recorded transpose permutation.
            # Keep the legacy containment path below for unusual reshape-only mappings whose dimensions differ.
            rectangular = (
                len(shard) == len(s_shard) == len(d_shard)
                and all(
                    abs(sw) == abs(mw)
                    for (_sl, _sr, sw), (_ml, _mr, mw) in zip(shard, s_shard)
                )
                and tuple(shard_shape(s_shard)[source_dim] for source_dim in perm)
                == shard_shape(d_shard)
            )
            if logical is not None:
                _validate_dim0_split_mapping(sname, s_shard, dname, d_shard)
                rectangular = True
            if rectangular:
                inter: list[tuple[int, int]] = []
                for (source_left, source_right, source_width), (
                    map_left,
                    map_right,
                    map_width,
                ) in zip(
                    shard,
                    s_shard,
                ):
                    if abs(source_width) != abs(map_width):
                        raise ValueError(
                            f"source width mismatch for {sname}->{dname}: "
                            f"{source_width} vs {map_width}"
                        )
                    left, right = (
                        max(source_left, map_left),
                        min(source_right, map_right),
                    )
                    if left >= right:
                        inter = []
                        break
                    inter.append((left, right))
                if not inter:
                    continue
                src_slices = tuple(
                    slice(left - shard_left, right - shard_left)
                    for (left, right), (shard_left, _shard_right, _shard_width) in zip(
                        inter, shard
                    )
                )
                worker_slices = []
                for worker_dim, (dst_left, _dst_right, _dst_width) in enumerate(
                    d_shard
                ):
                    source_dim = perm[worker_dim]
                    left, right = inter[source_dim]
                    map_left = s_shard[source_dim][0]
                    worker_slices.append(
                        slice(
                            dst_left + left - map_left,
                            dst_left + right - map_left,
                        )
                    )
                shard_full_shape = tuple(abs(width) for _, _, width in d_shard)
                worker_block = dst_tensor.reshape(shard_full_shape)[
                    tuple(worker_slices)
                ]
                hf_block = src_tensor[src_slices]
                if neg_dims:
                    if src_to_dst:
                        pairs.append((worker_block, hf_block.permute(perm)))
                    else:
                        pairs.append((hf_block, worker_block.permute(inv)))
                else:
                    pairs.append(
                        (worker_block, hf_block)
                        if src_to_dst
                        else (hf_block, worker_block)
                    )
                continue
            if _shard_contained_in(s_shard, shard):
                src_slices = tuple(
                    slice(l - l0, r - l0)
                    for (l, r, _), (l0, _, _) in zip(s_shard, shard)
                )
                dst_slices = tuple(slice(l, r) for l, r, _ in d_shard)
                shard_full_shape = tuple(abs(w) for _, _, w in d_shard)
                worker_block = dst_tensor.reshape(shard_full_shape)[dst_slices]
                hf_block = src_tensor[src_slices]
                if neg_dims:
                    if src_to_dst:  # worker <- HF: worker = HF.permute(perm)
                        pairs.append((worker_block, hf_block.permute(perm)))
                    else:  # HF <- worker: HF = worker.permute(inv)
                        pairs.append((hf_block, worker_block.permute(inv)))
                else:
                    pairs.append(
                        (worker_block, hf_block)
                        if src_to_dst
                        else (hf_block, worker_block)
                    )
                break
    return pairs


class FuseUnsupported(Exception):
    """A fused model↔wire mapping can't be built for a shard shape (caller falls back to the 2-stage path)."""


def _fuse_copy_pairs(
    load_spec, ospec, wire_buf, dst_tensors, dtype_spec, *, src_to_dst
):
    """Build (dst_view, src_view) pairs mapping parameter tensors **directly** to a wire buffer.

    Composes the LoadSpec transpose/TP-remap (model↔HF, :func:`_copy_fromto_pairs`) with the overlap
    reshard (HF↔wire, :meth:`BoundShardSpec._slice_copy_pairs`) without an intermediate logical buffer.

    For each HF name, each overlap box ``o_box`` of *ospec* (a 1-D wire region viewed in the name's dtype,
    HF coordinates), and each LoadSpec mapping ``(s_shard, d_shard)``: the transferred region is the
    **intersection** of ``o_box`` with ``s_shard`` (vs. containment in :func:`_copy_fromto_pairs`). The
    wire sub-view is sliced from ``o_box``; the model sub-view is the corresponding slice of the worker
    tensor, permuted for transpose exactly as :func:`_copy_fromto_pairs`. Reduces to
    :func:`_copy_fromto_pairs` when ``o_box`` contains ``s_shard``.

    Only handles shards where ``o_box`` and ``s_shard`` share dimensionality and per-axis widths (all
    real cases: TP row/col, QKV/gate_up fusion, transpose). Anything else → :class:`FuseUnsupported`.
    """
    pairs: list[tuple[torch.Tensor, torch.Tensor]] = []
    # Wire byte offsets per name, identical layout to ShardSpec.iter_with_intv (uses dtype_spec).
    offset = 0
    for name, shards in ospec:
        length = shards_nbytes(shards, dtype_spec[name])
        if name in load_spec.entries:
            dt = dtype_spec[name]
            name_region = wire_buf[offset : offset + length].view(dt)
            for o_box, o_view in shards_iterator(shards, name_region):
                for dname, mappings in load_spec.entries[name].items():
                    if dname not in dst_tensors:
                        continue
                    dst_tensor = dst_tensors[dname]
                    assert dst_tensor.dtype == dt, (
                        f"fuse dtype mismatch for {name}->{dname}: model {dst_tensor.dtype} vs wire {dt}"
                    )
                    neg_dims = [
                        i for i, (_, _, w) in enumerate(mappings[0][0]) if w < 0
                    ]
                    pos_dims = [
                        i for i, (_, _, w) in enumerate(mappings[0][0]) if w >= 0
                    ]
                    perm = pos_dims + neg_dims  # HF dim order -> worker dim order
                    inv = _argsort_perm(perm)  # worker dim order -> HF
                    for s_shard, d_shard in mappings:
                        if len(s_shard) != len(o_box):
                            raise FuseUnsupported(
                                f"{name}->{dname}: dim mismatch {len(s_shard)} vs {len(o_box)}"
                            )
                        # Intersect o_box with s_shard per (HF) dim; o_box widths are abs, s may be signed.
                        inter: list[tuple[int, int]] = []
                        empty = False
                        for (ol, orr, ow), (sl, sr, sw) in zip(o_box, s_shard):
                            if abs(ow) != abs(sw):
                                raise FuseUnsupported(
                                    f"{name}->{dname}: width mismatch {ow} vs {sw}"
                                )
                            il, ir = max(ol, sl), min(orr, sr)
                            if il >= ir:
                                empty = True
                                break
                            inter.append((il, ir))
                        if empty:
                            continue
                        # Wire sub-view (HF order): slice o_view to the intersection, relative to o_box.
                        wire_sub = o_view[
                            tuple(
                                slice(il - ol, ir - ol)
                                for (il, ir), (ol, _, _) in zip(inter, o_box)
                            )
                        ]
                        # Model sub-view: worker dim k is fed by HF dim perm[k]; place at d_shard[k] offset.
                        shard_full_shape = tuple(abs(w) for _, _, w in d_shard)
                        worker_slices = []
                        for k, (dl, _dr, _dw) in enumerate(d_shard):
                            j = perm[k]
                            il_j, ir_j = inter[j]
                            sl_j = s_shard[j][0]
                            worker_slices.append(
                                slice(dl + (il_j - sl_j), dl + (ir_j - sl_j))
                            )
                        worker_full = dst_tensor.reshape(shard_full_shape)
                        if worker_full.data_ptr() != dst_tensor.data_ptr():
                            # Cached fused plans retain raw addresses, not the temporary Tensor produced by
                            # a copying reshape. Such an address becomes stale after plan construction and,
                            # on later epochs, would also read a frozen snapshot rather than the live model.
                            # Let the sender select its immediate two-stage path instead.
                            raise FuseUnsupported(
                                f"{name}->{dname}: reshape {tuple(dst_tensor.shape)} "
                                f"stride={tuple(dst_tensor.stride())} to {shard_full_shape} allocates"
                            )
                        worker_block = worker_full[tuple(worker_slices)]
                        if neg_dims:
                            if src_to_dst:  # wire -> model: model = wire.permute(perm)
                                pairs.append((worker_block, wire_sub.permute(perm)))
                            else:  # model -> wire: wire = model.permute(inv)
                                pairs.append((wire_sub, worker_block.permute(inv)))
                        else:
                            pairs.append(
                                (worker_block, wire_sub)
                                if src_to_dst
                                else (wire_sub, worker_block)
                            )
        offset += length
    return pairs
