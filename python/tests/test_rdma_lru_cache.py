# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""Unit tests for the local-memory-handle LRU cache in
``monarch._src.rdma.rdma``. These tests construct handles directly
through ``_make_local_memory_handle`` and inspect the module-level
``_local_memory_cache`` — they do not require an RDMA backend, an
``RdmaManager``, or any actor mesh."""

import gc

import pytest
import torch
from monarch._src.rdma import rdma as rdma_mod


@pytest.fixture(autouse=True)
def _reset_cache():
    rdma_mod._local_memory_cache.clear()
    yield
    rdma_mod._local_memory_cache.clear()


def _only_cache_entry():
    items = list(rdma_mod._local_memory_cache.items())
    assert len(items) == 1
    return items[0]


def test_same_tensor_repeated_hits():
    t = torch.empty(1024, dtype=torch.uint8)
    h1 = rdma_mod._make_local_memory_handle(t)
    key1, weak1 = _only_cache_entry()
    h2 = rdma_mod._make_local_memory_handle(t)
    key2, weak2 = _only_cache_entry()
    # Same key, same weak handle — the second call upgraded the
    # cached entry instead of inserting a new one.
    assert key1 == key2
    assert weak1 is weak2
    assert (h1.addr, h1.size) == (h2.addr, h2.size)


def test_transient_view_of_same_storage_hits():
    t = torch.empty(1024 // 4, dtype=torch.float32)
    h1 = rdma_mod._make_local_memory_handle(t.view(dtype=torch.uint8).flatten())
    _, weak1 = _only_cache_entry()
    h2 = rdma_mod._make_local_memory_handle(t.view(dtype=torch.uint8).flatten())
    _, weak2 = _only_cache_entry()
    # Each `.view(...).flatten()` is a fresh Python object, but they
    # share `t`'s underlying storage so the cache must reuse the same
    # weak entry.
    assert weak1 is weak2
    assert (h1.addr, h1.size) == (h2.addr, h2.size)


def test_different_tensors_different_entries():
    a = torch.empty(128, dtype=torch.uint8)
    b = torch.empty(128, dtype=torch.uint8)
    rdma_mod._make_local_memory_handle(a)
    rdma_mod._make_local_memory_handle(b)
    assert len(rdma_mod._local_memory_cache) == 2


def test_disjoint_slices_of_same_storage_different_entries():
    t = torch.empty(2048, dtype=torch.uint8)
    head = t[:1024]
    tail = t[1024:]
    h_head = rdma_mod._make_local_memory_handle(head)
    h_tail = rdma_mod._make_local_memory_handle(tail)
    assert len(rdma_mod._local_memory_cache) == 2
    assert h_head.addr != h_tail.addr


def test_same_memoryview_hits():
    buf = bytearray(1024)
    mv = memoryview(buf)
    rdma_mod._make_local_memory_handle(mv)
    _, weak1 = _only_cache_entry()
    rdma_mod._make_local_memory_handle(mv)
    _, weak2 = _only_cache_entry()
    assert weak1 is weak2


def test_different_memoryviews_different_entries():
    buf = bytearray(1024)
    mv1 = memoryview(buf)
    mv2 = memoryview(buf)
    rdma_mod._make_local_memory_handle(mv1)
    rdma_mod._make_local_memory_handle(mv2)
    # Memoryviews are keyed by `id(mv)`. Holding both views alive
    # keeps their ids distinct (CPython only reuses ids of freed
    # objects), so each call gets its own cache entry.
    assert len(rdma_mod._local_memory_cache) == 2


def test_lru_capacity_evicts_oldest():
    capacity = rdma_mod._LOCAL_MEMORY_CACHE_CAPACITY
    tensors = [torch.empty(64, dtype=torch.uint8) for _ in range(capacity + 5)]
    keys = []
    for t in tensors:
        rdma_mod._make_local_memory_handle(t)
        keys.append((id(t.untyped_storage()), t.data_ptr(), 64))
    assert len(rdma_mod._local_memory_cache) == capacity
    # The first 5 inserts should have been evicted; everything from
    # index 5 onward should still be present.
    for evicted in keys[:5]:
        assert evicted not in rdma_mod._local_memory_cache
    for kept in keys[5:]:
        assert kept in rdma_mod._local_memory_cache


def test_hit_promotes_to_most_recent():
    capacity = rdma_mod._LOCAL_MEMORY_CACHE_CAPACITY
    tensors = [torch.empty(64, dtype=torch.uint8) for _ in range(capacity)]
    for t in tensors:
        rdma_mod._make_local_memory_handle(t)
    # Touch the oldest entry so it should not be the next eviction
    # victim.
    oldest = tensors[0]
    oldest_key = (id(oldest.untyped_storage()), oldest.data_ptr(), 64)
    rdma_mod._make_local_memory_handle(oldest)
    # Insert one more entry to force a single eviction.
    rdma_mod._make_local_memory_handle(torch.empty(64, dtype=torch.uint8))
    second_oldest = tensors[1]
    second_oldest_key = (
        id(second_oldest.untyped_storage()),
        second_oldest.data_ptr(),
        64,
    )
    assert oldest_key in rdma_mod._local_memory_cache
    assert second_oldest_key not in rdma_mod._local_memory_cache


def test_weak_upgrade_returns_none_after_storage_collected():
    # The cache holds only weak references, so dropping the strong
    # reference must invalidate `upgrade()` even while the entry is
    # still indexed in the cache. The entry itself is cleaned up
    # lazily — on the next lookup at the same key.
    t = torch.empty(1024, dtype=torch.uint8)
    rdma_mod._make_local_memory_handle(t)
    _, weak = _only_cache_entry()
    del t
    gc.collect()
    assert weak.upgrade() is None
