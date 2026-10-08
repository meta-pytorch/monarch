# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import pytest
import torch
from wbridge.utils.data import (
    batched_copy,
    LoadSpec,
    logical_tensor_name,
    parse_logical_tensor_name,
    ShardSpec,
    split_large_load_spec_sources,
    validate_logical_tensor_partitions,
)


def _mapping(source_shape, destination_shape=None, *, transpose=False):
    destination_shape = destination_shape or source_shape
    source = [
        (0, extent, -extent if transpose and dim == 0 else extent)
        for dim, extent in enumerate(source_shape)
    ]
    destination = [(0, extent, extent) for extent in destination_shape]
    return source, destination


def test_split_large_source_is_row_aligned_and_bounded():
    source, destination = _mapping((10, 4))
    load = LoadSpec({"weight": {"model.weight": [(source, destination)]}})

    translated, dtypes, report = split_large_load_spec_sources(
        load,
        {"weight": torch.float32},
        max_bytes=64,
    )

    assert len(report) == 1
    assert report[0]["full_bytes"] == 160
    assert report[0]["max_piece_bytes"] == 64
    assert list(translated.entries) == [
        logical_tensor_name("weight", 0, 4),
        logical_tensor_name("weight", 4, 8),
        logical_tensor_name("weight", 8, 10),
    ]
    assert set(dtypes) == set(translated.entries)
    assert [spec[0][0][:2] for _name, spec in translated.src_shard_spec] == [
        (0, 4),
        (4, 8),
        (8, 10),
    ]
    assert all(
        translated.src_shard_spec.subset({name}).nbytes(dtypes) <= 64
        for name in translated.entries
    )


def test_partial_mapping_roundtrips_through_logical_buffers():
    # This worker owns physical checkpoint rows [3, 9), so its first/last logical pieces are partial.
    source = [(3, 9, 10), (0, 4, 4)]
    destination = [(0, 6, 6), (0, 4, 4)]
    load = LoadSpec({"weight": {"model.weight": [(source, destination)]}})
    translated, dtypes, _report = split_large_load_spec_sources(
        load,
        {"weight": torch.float32},
        max_bytes=64,
    )
    spec = translated.src_shard_spec

    model = {"model.weight": torch.arange(24, dtype=torch.float32).reshape(6, 4)}
    logical = spec.make_named_buffer(dtypes, "cpu")
    translated.copy_fromto_params(spec, logical, model, src_to_dst=False)

    restored = {"model.weight": torch.zeros_like(model["model.weight"])}
    translated.copy_fromto_params(spec, logical, restored, src_to_dst=True)
    assert torch.equal(restored["model.weight"], model["model.weight"])


@pytest.mark.parametrize("start,end", [(0, 8), (8, 16)])
def test_normal_deduplicated_partial_mapping_roundtrips(start, end):
    """The two-stage fallback must support strict sub-shards created by sender de-duplication.

    These are ordinary checkpoint names, not the explicitly split logical names covered by the tests above.
    GLM-5 hits this path when its BF16 trainer k_norm is converted into an FP32 rollout wire tensor.
    """
    source, destination = _mapping((16,))
    load = LoadSpec({"weight": {"model.weight": [(source, destination)]}})
    partial = ShardSpec({"weight": [[(start, end, 16)]]})
    dtypes = {"weight": torch.float32}
    model = {"model.weight": torch.arange(16, dtype=torch.bfloat16)}

    logical = partial.make_named_buffer(dtypes, "cpu")
    load.copy_fromto_params(partial, logical, model, src_to_dst=False)
    assert torch.equal(logical["weight"], model["model.weight"][start:end].float())

    restored = {"model.weight": torch.zeros_like(model["model.weight"])}
    load.copy_fromto_params(partial, logical, restored, src_to_dst=True)
    expected = torch.zeros_like(model["model.weight"])
    expected[start:end] = model["model.weight"][start:end]
    assert torch.equal(restored["model.weight"], expected)


def test_transposed_mapping_roundtrips_after_dim0_split():
    source, destination = _mapping((6, 4), (4, 6), transpose=True)
    load = LoadSpec({"weight": {"model.weight": [(source, destination)]}})
    translated, dtypes, _report = split_large_load_spec_sources(
        load,
        {"weight": torch.float32},
        max_bytes=32,
    )
    spec = translated.src_shard_spec

    model = {"model.weight": torch.arange(24, dtype=torch.float32).reshape(4, 6)}
    logical = spec.make_named_buffer(dtypes, "cpu")
    translated.copy_fromto_params(spec, logical, model, src_to_dst=False)
    restored = {"model.weight": torch.zeros_like(model["model.weight"])}
    translated.copy_fromto_params(spec, logical, restored, src_to_dst=True)
    assert torch.equal(restored["model.weight"], model["model.weight"])


def test_translated_load_from_full_uses_physical_source_once():
    source, destination = _mapping((10, 4))
    load = LoadSpec({"weight": {"model.weight": [(source, destination)]}})
    translated, _dtypes, _report = split_large_load_spec_sources(
        load,
        {"weight": torch.float32},
        max_bytes=64,
    )
    calls = 0
    checkpoint = torch.arange(40, dtype=torch.float32).reshape(10, 4)

    def fetch():
        nonlocal calls
        calls += 1
        return checkpoint.clone()

    restored = {"model.weight": torch.zeros_like(checkpoint)}
    translated.load_from_full({"weight": fetch}, restored)
    assert calls == 1
    assert torch.equal(restored["model.weight"], checkpoint)


def test_split_is_idempotent_and_names_are_reversible():
    source, destination = _mapping((10, 4))
    load = LoadSpec({"weight": {"model.weight": [(source, destination)]}})
    once, dtypes, _report = split_large_load_spec_sources(
        load,
        {"weight": torch.float32},
        max_bytes=64,
    )
    twice, twice_dtypes, second_report = split_large_load_spec_sources(
        once,
        dtypes,
        max_bytes=64,
    )
    assert second_report == []
    assert twice.entries == once.entries
    assert twice_dtypes == dtypes
    for name in twice.entries:
        physical, start, end = parse_logical_tensor_name(name)
        assert physical == "weight"
        assert 0 <= start < end <= 10


def test_row_larger_than_cap_is_rejected():
    source, destination = _mapping((2, 64))
    load = LoadSpec({"weight": {"model.weight": [(source, destination)]}})
    try:
        split_large_load_spec_sources(load, {"weight": torch.float32}, max_bytes=128)
    except ValueError as exc:
        assert "one row is 256 bytes" in str(exc)
    else:
        raise AssertionError("expected unsplittable first dimension to be rejected")


def _logical_spec(*intervals):
    return ShardSpec(
        {
            logical_tensor_name("weight", start, end): [[(start, end, 16)]]
            for start, end in intervals
        }
    )


def test_partition_validation_allows_duplicate_and_disjoint_pieces():
    validate_logical_tensor_partitions(
        [
            _logical_spec((0, 4), (4, 8)),
            _logical_spec((0, 4), (8, 12)),
            _logical_spec((12, 16)),
        ]
    )


def test_partition_validation_rejects_mismatched_split_grids():
    with pytest.raises(ValueError, match="overlaps"):
        validate_logical_tensor_partitions(
            [
                _logical_spec((0, 8)),
                _logical_spec((0, 4), (4, 8)),
            ]
        )


def test_partition_validation_rejects_split_and_unsplit_names():
    unsplit = ShardSpec({"weight": [[(0, 16, 16)]]})
    with pytest.raises(ValueError, match="physical and logical names coexist"):
        validate_logical_tensor_partitions([unsplit, _logical_spec((0, 16))])


@pytest.mark.skipif(not torch.cuda.is_available(), reason="fused copy requires CUDA")
@pytest.mark.parametrize("transpose", [False, True])
def test_fused_model_wire_copy_with_logical_names(transpose):
    source_shape = (6, 4)
    destination_shape = (4, 6) if transpose else source_shape
    source, destination = _mapping(
        source_shape,
        destination_shape,
        transpose=transpose,
    )
    load = LoadSpec({"weight": {"model.weight": [(source, destination)]}})
    translated, dtypes, _report = split_large_load_spec_sources(
        load,
        {"weight": torch.float32},
        max_bytes=32,
    )
    spec = translated.src_shard_spec
    model = {
        "model.weight": torch.arange(24, dtype=torch.float32, device="cuda").reshape(
            destination_shape
        ),
    }

    wire = spec.make_byte_chunk(dtypes, "cuda")
    batched_copy(
        translated.fuse_copy_pairs(
            spec,
            wire,
            model,
            dtypes,
            src_to_dst=False,
        )
    )
    restored = {"model.weight": torch.zeros_like(model["model.weight"])}
    batched_copy(
        translated.fuse_copy_pairs(
            spec,
            wire,
            restored,
            dtypes,
            src_to_dst=True,
        )
    )
    torch.cuda.synchronize()
    assert torch.equal(restored["model.weight"], model["model.weight"])
