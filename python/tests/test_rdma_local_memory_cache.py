# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""Unit tests for the local-memory-handle cache in
``monarch._src.rdma.rdma``.

Two layers are covered:

- Direct tests that drive ``_make_local_memory_handle`` and inspect the
  module-level ``_local_memory_cache`` to verify keying and
  weakref-driven eviction.
- Patched tests that drive the public ``RDMABuffer`` / ``RDMAAction``
  entry points with the RDMA boundary calls
  (``_RdmaBuffer.create_rdma_buffer_blocking``,
  ``_RdmaAction.add_{read_into,write_from}_local``) mocked out, asserting
  the cached handle is reused rather than re-registered.

Neither layer requires an RDMA backend, an ``RdmaManager``, or an actor
mesh."""

import gc
from unittest.mock import MagicMock, patch

import pytest
import torch
from monarch._src.rdma import rdma as rdma_mod


@pytest.fixture(autouse=True)
def _reset_cache():
    rdma_mod._local_memory_cache.clear()
    rdma_mod._local_memory_cache_refs.clear()
    yield
    rdma_mod._local_memory_cache.clear()
    rdma_mod._local_memory_cache_refs.clear()


def _only_cache_entry():
    items = list(rdma_mod._local_memory_cache.items())
    assert len(items) == 1
    return items[0]


def _tensor_key(t):
    addr, size = rdma_mod._get_tensor_addr_and_size(t)
    return (id(t.untyped_storage()), addr, size)


def _memoryview_key(mv):
    addr, size = rdma_mod._get_memoryview_addr_and_size(mv)
    return (id(mv), addr, size)


def test_same_tensor_repeated_hits():
    t = torch.empty(1024, dtype=torch.uint8)
    h1 = rdma_mod._make_local_memory_handle(t)
    key1, weak1 = _only_cache_entry()
    h2 = rdma_mod._make_local_memory_handle(t)
    key2, weak2 = _only_cache_entry()
    # Same key, same weak handle — the second call upgraded the cached
    # entry instead of inserting a new one.
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
    # share `t`'s storage (same id, addr, size) so they reuse the same
    # weak entry.
    assert weak1 is weak2
    assert (h1.addr, h1.size) == (h2.addr, h2.size)


def test_different_tensors_different_entries():
    a = torch.empty(128, dtype=torch.uint8)
    b = torch.empty(128, dtype=torch.uint8)
    # `a` and `b` stay alive below, so both storages pin their entries.
    rdma_mod._make_local_memory_handle(a)
    rdma_mod._make_local_memory_handle(b)
    assert len(rdma_mod._local_memory_cache) == 2


def test_disjoint_slices_of_same_storage_different_entries():
    t = torch.empty(2048, dtype=torch.uint8)
    head = t[:1024]
    tail = t[1024:]
    h_head = rdma_mod._make_local_memory_handle(head)
    h_tail = rdma_mod._make_local_memory_handle(tail)
    # Same storage id but different addresses, so different keys.
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


def test_memoryviews_over_same_buffer_different_entries():
    buf = bytearray(1024)
    mv1 = memoryview(buf)
    mv2 = memoryview(buf)
    rdma_mod._make_local_memory_handle(mv1)
    rdma_mod._make_local_memory_handle(mv2)
    # Distinct memoryview objects have distinct ids, so they do not share
    # an entry even when they view the same buffer at the same offset.
    assert len(rdma_mod._local_memory_cache) == 2


def test_memoryviews_over_different_buffers_different_entries():
    buf1 = bytearray(1024)
    buf2 = bytearray(1024)
    mv1 = memoryview(buf1)
    mv2 = memoryview(buf2)
    rdma_mod._make_local_memory_handle(mv1)
    rdma_mod._make_local_memory_handle(mv2)
    assert len(rdma_mod._local_memory_cache) == 2


def test_evicts_when_tensor_deleted():
    t = torch.empty(1024, dtype=torch.uint8)
    key = _tensor_key(t)
    # Do not keep the strong handle — only `t` should pin the storage.
    rdma_mod._make_local_memory_handle(t)
    assert key in rdma_mod._local_memory_cache
    del t
    gc.collect()
    # The storage's weakref callback removes both the entry and its ref.
    assert key not in rdma_mod._local_memory_cache
    assert key not in rdma_mod._local_memory_cache_refs


def test_survives_when_view_still_alive():
    t = torch.empty(1024, dtype=torch.uint8)
    view = t[:]  # shares storage; same (id, addr, size)
    key = _tensor_key(t)
    rdma_mod._make_local_memory_handle(t)
    del t
    gc.collect()
    # The storage is still alive through `view`, so the entry persists.
    assert key in rdma_mod._local_memory_cache
    del view
    gc.collect()
    # The storage is now freed and the entry is evicted.
    assert key not in rdma_mod._local_memory_cache


def test_evicts_when_memoryview_deleted():
    buf = bytearray(1024)
    mv = memoryview(buf)
    key = _memoryview_key(mv)
    rdma_mod._make_local_memory_handle(mv)
    assert key in rdma_mod._local_memory_cache
    del mv
    gc.collect()
    # The memoryview's weakref callback evicts the entry even though the
    # underlying `buf` is still alive.
    assert key not in rdma_mod._local_memory_cache
    assert key not in rdma_mod._local_memory_cache_refs


# Reuse tests at the RDMABuffer / RDMAAction boundary.
#
# A cache hit returns a fresh `_LocalMemoryHandle` wrapper each time (the
# native `upgrade()` builds a new object), so handle identity is not a
# usable signal. Instead these spy on the registration function
# `_make_local_memory_handle_from_tensor`: on a hit it is NOT called, so
# a `call_count` of 1 across two operations proves the cached handle was
# reused. The RDMA boundary calls are mocked so the entry points run
# without an RDMA backend or actor context, and we capture the `local`
# handle each receives to confirm both operations act on the same region.


@patch.object(rdma_mod, "_RdmaBuffer")
@patch.object(rdma_mod, "context")
@patch.object(rdma_mod, "_ensure_init_rdma_manager")
@patch.object(rdma_mod, "get_rdma_backend", return_value="ibverbs")
@patch.object(
    rdma_mod,
    "_make_local_memory_handle_from_tensor",
    wraps=rdma_mod._make_local_memory_handle_from_tensor,
)
def test_rdmabuffer_same_tensor_reuses_cached_handle(
    register_spy, _backend, _ensure, _context, rdma_buffer_cls
):
    t = torch.zeros(1024, dtype=torch.uint8)
    rdma_mod.RDMABuffer(t)
    rdma_mod.RDMABuffer(t)

    # The second RDMABuffer hit the cache: the handle was registered once.
    assert register_spy.call_count == 1
    locals_ = [
        c.kwargs["local"]
        for c in rdma_buffer_cls.create_rdma_buffer_blocking.call_args_list
    ]
    assert len(locals_) == 2
    assert (locals_[0].addr, locals_[0].size) == (locals_[1].addr, locals_[1].size)


@patch.object(rdma_mod, "_RdmaBuffer")
@patch.object(rdma_mod, "_RdmaAction")
@patch.object(rdma_mod, "context")
@patch.object(rdma_mod, "_ensure_init_rdma_manager")
@patch.object(rdma_mod, "get_rdma_backend", return_value="ibverbs")
@patch.object(
    rdma_mod,
    "_make_local_memory_handle_from_tensor",
    wraps=rdma_mod._make_local_memory_handle_from_tensor,
)
def test_rdma_op_then_rdmabuffer_reuses_cached_handle(
    register_spy, _backend, _ensure, _context, rdma_action_cls, rdma_buffer_cls
):
    t = torch.zeros(1024, dtype=torch.uint8)
    remote = MagicMock()  # stand-in RDMABuffer; only `._buffer` is read

    # An RDMA op that uses `t` as local memory caches its handle.
    rdma_mod.RDMAAction().read_remote(t, remote)
    # A buffer over the same tensor must reuse the cached handle.
    rdma_mod.RDMABuffer(t)

    assert register_spy.call_count == 1
    op_local = rdma_action_cls.return_value.add_read_into_local.call_args.kwargs[
        "local"
    ]
    buf_local = rdma_buffer_cls.create_rdma_buffer_blocking.call_args.kwargs["local"]
    assert (op_local.addr, op_local.size) == (buf_local.addr, buf_local.size)


@patch.object(rdma_mod, "_RdmaBuffer")
@patch.object(rdma_mod, "_RdmaAction")
@patch.object(rdma_mod, "context")
@patch.object(rdma_mod, "_ensure_init_rdma_manager")
@patch.object(rdma_mod, "get_rdma_backend", return_value="ibverbs")
@patch.object(
    rdma_mod,
    "_make_local_memory_handle_from_tensor",
    wraps=rdma_mod._make_local_memory_handle_from_tensor,
)
def test_rdmabuffer_then_rdma_op_reuses_cached_handle(
    register_spy, _backend, _ensure, _context, rdma_action_cls, rdma_buffer_cls
):
    t = torch.zeros(1024, dtype=torch.uint8)
    remote = MagicMock()  # stand-in RDMABuffer; only `._buffer` is read

    # A buffer over `t` caches its handle.
    rdma_mod.RDMABuffer(t)
    # A later op that writes from the same tensor must reuse it.
    rdma_mod.RDMAAction().write_remote(remote, t)

    assert register_spy.call_count == 1
    buf_local = rdma_buffer_cls.create_rdma_buffer_blocking.call_args.kwargs["local"]
    op_local = rdma_action_cls.return_value.add_write_from_local.call_args.kwargs[
        "local"
    ]
    assert (buf_local.addr, buf_local.size) == (op_local.addr, op_local.size)
