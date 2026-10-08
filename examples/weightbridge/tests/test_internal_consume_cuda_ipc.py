# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Focused two-GPU test for the fused internal-consume data path."""

from __future__ import annotations

import multiprocessing.connection

import pytest

torch = pytest.importorskip("torch")


def _ipc_source(send: multiprocessing.connection.Connection, release) -> None:
    from torch.multiprocessing.reductions import reduce_tensor

    torch.cuda.set_device(1)
    flat_numel = 1024 * 1024
    storage = torch.arange(flat_numel + 200, dtype=torch.int32, device="cuda:1")
    _rebuild, args = reduce_tensor(storage)
    metadata = {
        "handle": bytes(args[7])[-64:],
        "tensor_offset_bytes": int(args[9]) + int(args[3]) * storage.element_size(),
    }
    send.send((storage, flat_numel, metadata))
    release.wait(timeout=60)


def _ipc_consumer(recv: multiprocessing.connection.Connection, result, release) -> None:
    import gc

    from wbridge.backend.router import (
        _close_cuda_ipc_mapping,
        _enable_cuda_peer_access,
        _open_cuda_ipc_mapping,
    )
    from wbridge.utils.data import CopyPlan

    torch.cuda.set_device(0)
    _enable_cuda_peer_access(0, 1)
    storage, flat_numel, metadata = recv.recv()
    mapped_base, allocation_base = _open_cuda_ipc_mapping(0, metadata)
    flat = storage[:flat_numel]
    matrix = storage[flat_numel:].reshape(20, 10)
    dst_flat = torch.zeros_like(flat, device="cuda:0")
    dst_transposed = torch.zeros(10, 20, dtype=torch.int32, device="cuda:0")
    pairs = [(dst_flat, flat), (dst_transposed, matrix.t())]
    plan = CopyPlan(
        pairs,
        single_kernel=True,
        source_ptrs=[
            mapped_base + (source.data_ptr() - storage.data_ptr())
            for _destination, source in pairs
        ],
    )
    plan.run()
    torch.cuda.synchronize()
    expected_transposed = (
        torch.arange(
            flat_numel,
            flat_numel + 200,
            dtype=torch.int32,
        )
        .reshape(20, 10)
        .t()
        .contiguous()
    )
    ok = bool(
        torch.equal(dst_flat.cpu(), torch.arange(dst_flat.numel(), dtype=torch.int32))
        and torch.equal(dst_transposed.cpu(), expected_transposed)
    )
    source_device = str(flat.device)
    _close_cuda_ipc_mapping(0, allocation_base)
    launches = plan.launch_count
    del pairs, plan, flat, matrix, storage
    gc.collect()
    result.send((ok, launches, source_device))
    release.set()


def _mixed_ipc_source(send: multiprocessing.connection.Connection, release) -> None:
    """Export one allocation containing both BF16 and FP32 source views."""
    from torch.multiprocessing.reductions import reduce_tensor

    torch.cuda.set_device(1)
    bf16_numel = 1024 * 1024
    fp32_shape = (20, 10)
    bf16_nbytes = bf16_numel * torch.empty((), dtype=torch.bfloat16).element_size()
    fp32_nbytes = (
        fp32_shape[0]
        * fp32_shape[1]
        * torch.empty((), dtype=torch.float32).element_size()
    )
    storage = torch.empty(bf16_nbytes + fp32_nbytes, dtype=torch.uint8, device="cuda:1")
    bf16 = storage[:bf16_nbytes].view(torch.bfloat16)
    fp32 = storage[bf16_nbytes:].view(torch.float32).reshape(fp32_shape)
    bf16.copy_(
        torch.arange(bf16_numel, dtype=torch.int32, device="cuda:1").remainder(251)
    )
    fp32.copy_(
        torch.arange(fp32.numel(), dtype=torch.float32, device="cuda:1").reshape(
            fp32_shape
        )
        + 0.25
    )
    _rebuild, args = reduce_tensor(storage)
    metadata = {
        "handle": bytes(args[7])[-64:],
        "tensor_offset_bytes": int(args[9]) + int(args[3]) * storage.element_size(),
    }
    send.send((storage, bf16_numel, fp32_shape, metadata))
    release.wait(timeout=60)


def _mixed_ipc_consumer(
    recv: multiprocessing.connection.Connection, result, release
) -> None:
    """Read mixed-dtype peer views with the production unified-dtype plan."""
    import gc

    from wbridge.backend.router import (
        _close_cuda_ipc_mapping,
        _enable_cuda_peer_access,
        _open_cuda_ipc_mapping,
    )
    from wbridge.utils.data import CopyPlan

    torch.cuda.set_device(0)
    _enable_cuda_peer_access(0, 1)
    storage, bf16_numel, fp32_shape, metadata = recv.recv()
    mapped_base, allocation_base = _open_cuda_ipc_mapping(0, metadata)
    bf16_nbytes = bf16_numel * torch.empty((), dtype=torch.bfloat16).element_size()
    bf16 = storage[:bf16_nbytes].view(torch.bfloat16)
    fp32 = storage[bf16_nbytes:].view(torch.float32).reshape(fp32_shape)
    dst_bf16 = torch.zeros_like(bf16, device="cuda:0")
    dst_fp32 = torch.zeros(
        fp32_shape[1], fp32_shape[0], dtype=torch.float32, device="cuda:0"
    )
    pairs = [(dst_bf16, bf16), (dst_fp32, fp32.t())]
    plan = CopyPlan(
        pairs,
        unified_dtype_kernels=True,
        source_ptrs=[
            mapped_base + (source.data_ptr() - storage.data_ptr())
            for _destination, source in pairs
        ],
    )
    plan.run()
    torch.cuda.synchronize()
    ok = bool(
        torch.equal(dst_bf16.cpu(), bf16.cpu())
        and torch.equal(dst_fp32.cpu(), fp32.t().contiguous().cpu())
    )
    source_device = str(bf16.device)
    _close_cuda_ipc_mapping(0, allocation_base)
    launches = plan.launch_count
    del pairs, plan, bf16, fp32, storage
    gc.collect()
    result.send((ok, launches, source_device))
    release.set()


def _event_ipc_source(
    send: multiprocessing.connection.Connection,
    go,
    published,
    release,
) -> None:
    """Publish a peer allocation only after a delayed copy and reusable IPC event record."""
    from torch.multiprocessing.reductions import reduce_tensor

    torch.cuda.set_device(1)
    flat_numel = 1024 * 1024
    storage = torch.zeros(flat_numel, dtype=torch.int32, device="cuda:1")
    _rebuild, args = reduce_tensor(storage)
    metadata = {
        "handle": bytes(args[7])[-64:],
        "tensor_offset_bytes": int(args[9]) + int(args[3]) * storage.element_size(),
    }
    ready_event = torch.cuda.Event(interprocess=True)
    ready_event.record()
    torch.cuda.synchronize()
    send.send((storage, flat_numel, metadata, ready_event.ipc_handle()))
    if not go.wait(timeout=60):
        raise TimeoutError("consumer did not import CUDA IPC event")

    stream = torch.cuda.Stream(device=1)
    with torch.cuda.stream(stream):
        # Ensure a consumer that does not truly wait the imported event observes zeros.
        torch.cuda._sleep(100_000_000)
        storage.copy_(torch.arange(flat_numel, dtype=torch.int32, device="cuda:1"))
        ready_event.record(stream)
    # Match production: the CPU sequence is published after event.record() is enqueued,
    # without synchronizing the producer stream.
    published.set()
    release.wait(timeout=60)


def _event_ipc_consumer(
    recv: multiprocessing.connection.Connection,
    result,
    go,
    published,
    release,
) -> None:
    """Wait an event imported on the reader device before dereferencing peer memory."""
    import gc

    from wbridge.backend.router import (
        _close_cuda_ipc_mapping,
        _enable_cuda_peer_access,
        _open_cuda_ipc_mapping,
    )
    from wbridge.utils.data import CopyPlan

    torch.cuda.set_device(0)
    _enable_cuda_peer_access(0, 1)
    storage, flat_numel, metadata, event_handle = recv.recv()
    mapped_base, allocation_base = _open_cuda_ipc_mapping(0, metadata)
    ready_event = torch.cuda.Event.from_ipc_handle(0, event_handle)
    go.set()
    if not published.wait(timeout=60):
        raise TimeoutError("producer did not publish CUDA IPC event")

    source = storage[:flat_numel]
    destination = torch.zeros_like(source, device="cuda:0")
    plan = CopyPlan([(destination, source)], source_ptrs=[mapped_base])
    stream = torch.cuda.Stream(device=0)
    with torch.cuda.stream(stream):
        stream.wait_event(ready_event)
        plan.run()
    stream.synchronize()
    ok = bool(
        torch.equal(destination.cpu(), torch.arange(flat_numel, dtype=torch.int32))
    )
    _close_cuda_ipc_mapping(0, allocation_base)
    del plan, source, destination, storage
    gc.collect()
    result.send(ok)
    release.set()


@pytest.mark.skipif(
    torch.cuda.device_count() < 2, reason="internal consume CUDA-IPC test needs 2 GPUs"
)
def test_internal_consume_reads_peer_ipc_memory_in_one_kernel() -> None:
    """A consumer process directly reads an IPC-exported peer allocation over NVLink."""
    ctx = torch.multiprocessing.get_context("spawn")
    source_recv, source_send = ctx.Pipe(duplex=False)
    result_recv, result_send = ctx.Pipe(duplex=False)
    release = ctx.Event()
    source = ctx.Process(target=_ipc_source, args=(source_send, release))
    consumer = ctx.Process(
        target=_ipc_consumer, args=(source_recv, result_send, release)
    )
    source.start()
    consumer.start()
    try:
        assert result_recv.poll(90), "CUDA-IPC internal consume timed out"
        ok, launches, source_device = result_recv.recv()
        assert ok
        assert launches == 1
        assert source_device == "cuda:1"
    finally:
        release.set()
        source.join(timeout=30)
        consumer.join(timeout=30)
        if source.is_alive():
            source.terminate()
        if consumer.is_alive():
            consumer.terminate()
    assert source.exitcode == 0
    assert consumer.exitcode == 0


@pytest.mark.skipif(
    torch.cuda.device_count() < 2, reason="internal consume CUDA-IPC test needs 2 GPUs"
)
def test_internal_consume_reads_mixed_dtype_peer_ipc_memory() -> None:
    """A BF16/FP32 lane reads peer IPC memory once per dtype without byte corruption."""
    ctx = torch.multiprocessing.get_context("spawn")
    source_recv, source_send = ctx.Pipe(duplex=False)
    result_recv, result_send = ctx.Pipe(duplex=False)
    release = ctx.Event()
    source = ctx.Process(target=_mixed_ipc_source, args=(source_send, release))
    consumer = ctx.Process(
        target=_mixed_ipc_consumer, args=(source_recv, result_send, release)
    )
    source.start()
    consumer.start()
    try:
        assert result_recv.poll(90), "mixed-dtype CUDA-IPC internal consume timed out"
        ok, launches, source_device = result_recv.recv()
        assert ok
        assert launches == 2
        assert source_device == "cuda:1"
    finally:
        release.set()
        source.join(timeout=30)
        consumer.join(timeout=30)
        if source.is_alive():
            source.terminate()
        if consumer.is_alive():
            consumer.terminate()
    assert source.exitcode == 0
    assert consumer.exitcode == 0


@pytest.mark.skipif(
    torch.cuda.device_count() < 2, reason="internal consume CUDA-IPC test needs 2 GPUs"
)
def test_internal_consume_waits_reused_peer_ipc_event() -> None:
    """The reader-device IPC event import orders a delayed producer copy before peer reads."""
    ctx = torch.multiprocessing.get_context("spawn")
    source_recv, source_send = ctx.Pipe(duplex=False)
    result_recv, result_send = ctx.Pipe(duplex=False)
    go = ctx.Event()
    published = ctx.Event()
    release = ctx.Event()
    source = ctx.Process(
        target=_event_ipc_source,
        args=(source_send, go, published, release),
    )
    consumer = ctx.Process(
        target=_event_ipc_consumer,
        args=(source_recv, result_send, go, published, release),
    )
    source.start()
    consumer.start()
    try:
        assert result_recv.poll(90), "CUDA-IPC event ordering test timed out"
        assert result_recv.recv()
    finally:
        release.set()
        source.join(timeout=30)
        consumer.join(timeout=30)
        if source.is_alive():
            source.terminate()
        if consumer.is_alive():
            consumer.terminate()
    assert source.exitcode == 0
    assert consumer.exitcode == 0
