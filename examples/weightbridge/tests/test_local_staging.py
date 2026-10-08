# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Tests for :class:`~wbridge.backend.rdma.local.LocalStagingEngine`.

Exercises the register_buffer + interval-map ``_view`` reconstruction and the copy path behind the
``RdmaEngine`` interface. Runs on CPU (no GPU needed): where CUDA is unavailable the engine copies
synchronously and ``write_async`` returns ``None`` (the ABC no-op-handle convention). Skipped without torch.
"""

import pytest

torch = pytest.importorskip("torch")

from wbridge.backend.rdma.local import LocalStagingEngine  # noqa: E402


def _u8(n: int, fill: int = 0) -> "torch.Tensor":
    return torch.full((n,), fill, dtype=torch.uint8)


def _engine() -> LocalStagingEngine:
    eng = LocalStagingEngine()
    eng.init("localhost", "tcp")
    return eng


def test_whole_buffer_roundtrip():
    eng = _engine()
    src = torch.arange(256, dtype=torch.uint8)
    dst = _u8(256)
    eng.register_buffer(src)
    eng.register_buffer(dst)
    eng.write(eng.session_id(), [src.data_ptr()], [dst.data_ptr()], [256])
    assert torch.equal(dst, src)


def test_offset_slice_view():
    eng = _engine()
    src = torch.arange(256, dtype=torch.uint8)
    dst = _u8(256)
    eng.register_buffer(src)
    eng.register_buffer(dst)
    # src[64:192] -> dst[0:128] via raw offset pointers (interval-map reconstruction).
    eng.write(eng.session_id(), [src.data_ptr() + 64], [dst.data_ptr()], [128])
    assert torch.equal(dst[:128], src[64:192])
    assert torch.equal(dst[128:], _u8(128))  # remainder untouched


def test_multi_segment_batch():
    eng = _engine()
    src = torch.arange(64, dtype=torch.uint8)
    a = _u8(32)
    b = _u8(32)
    for t in (src, a, b):
        eng.register_buffer(t)
    eng.write(
        eng.session_id(),
        [src.data_ptr(), src.data_ptr() + 32],
        [a.data_ptr(), b.data_ptr()],
        [32, 32],
    )
    assert torch.equal(a, src[:32]) and torch.equal(b, src[32:])


def test_unregistered_ptr_raises():
    eng = _engine()
    t = _u8(16)
    eng.register_buffer(t)
    with pytest.raises(KeyError):
        eng.write(eng.session_id(), [t.data_ptr() + 4096], [t.data_ptr()], [8])


def test_write_async_then_wait():
    eng = _engine()
    a = torch.arange(64, dtype=torch.uint8)
    b = _u8(64)
    eng.register_buffer(a)
    eng.register_buffer(b)
    h = eng.write_async(eng.session_id(), [a.data_ptr()], [b.data_ptr()], [64])
    eng.wait([h])  # a torch.cuda.Event on GPU, or a None no-op on CPU
    assert torch.equal(b, a)


def test_register_buffer_rejects_non_uint8():
    eng = _engine()
    with pytest.raises(AssertionError):
        eng.register_buffer(torch.zeros(8, dtype=torch.float32))


if __name__ == "__main__":
    for fn in [
        test_whole_buffer_roundtrip,
        test_offset_slice_view,
        test_multi_segment_batch,
        test_unregistered_ptr_raises,
        test_write_async_then_wait,
        test_register_buffer_rejects_non_uint8,
    ]:
        fn()
    print("LocalStagingEngine tests passed")
