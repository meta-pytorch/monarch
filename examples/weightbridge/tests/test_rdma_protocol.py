# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Unit tests for the RDMA data-plane logic that does not need a GPU or Mooncake.

Covers the two pieces of new pure logic:

* :meth:`~wbridge.utils.data.BoundShardSpec.pack_into` matches the allocating ``__getitem__`` path.
* The flag ping-pong wiring on :class:`~wbridge.backend.router.WBEndpoint` (``_seq`` monotonicity across
  update epochs, exclusive per-message addressing, and direct async publication) using in-process
  loopback engines over real same-process addresses.

Run: ``python tests/test_rdma_protocol.py`` or ``pytest tests/test_rdma_protocol.py``.
"""

import ctypes
import queue
import threading
import time

import pytest

torch = pytest.importorskip("torch")

from wbridge.backend.rdma.base import RdmaEngine  # noqa: E402
from wbridge.backend.router import WBEndpoint  # noqa: E402
from wbridge.backend.sender import _recv_lane_predecessors, WeightSender  # noqa: E402
from wbridge.utils.data import ShardSpec  # noqa: E402


# --------------------------------------------------------------------------- pack_into
def test_pack_into_matches_getitem():
    """pack_into writes the same wire bytes as the allocating ``full[{r: dst}][r]`` path."""
    full = ShardSpec({"a": [[(0, 10, 10)]], "b": [[(0, 6, 6)]]})
    tensors = {
        "a": torch.arange(10, dtype=torch.float32),
        "b": torch.arange(6, dtype=torch.float32) + 100.0,
    }
    dst = ShardSpec({"a": [[(2, 6, 10)]], "b": [[(1, 4, 6)]]})  # sub-regions of each

    ref = full(tensors)[{0: dst}][0]  # allocating reference

    nbytes = dst.nbytes({"a": torch.float32, "b": torch.float32})
    out = torch.zeros(nbytes + 8, dtype=torch.uint8)  # deliberately oversized
    n = full(tensors).pack_into(dst, out)

    assert n == nbytes
    assert torch.equal(out[:n], ref)


# --------------------------------------------------------------------------- loopback engine
def test_single_recv_slot_requires_previous_round_ack_per_destination():
    """Depth one makes each destination's immediately previous contributing round its RECV reuse gate."""
    pred, final = _recv_lane_predecessors(
        [[8, 9], [8], [9], [8, 9]],
        depth=1,
    )
    assert pred == [
        {8: None, 9: None},
        {8: 0},
        {9: 0},
        {8: 1, 9: 2},
    ]
    assert final == {(8, 0): 3, (9, 0): 3}


class LoopbackEngine(RdmaEngine):
    """One-sided ``write`` == ``ctypes.memmove`` between same-process addresses (for tests only)."""

    def __init__(self, name: str) -> None:
        self._name = name

    def init(self, local_host: str, protocol: str, device: str = "") -> None:  # noqa: D401
        pass

    def session_id(self) -> str:
        return self._name

    def register(self, ptr: int, size: int) -> None:
        pass

    def write(self, dst_session, src_ptrs, dst_ptrs, sizes) -> None:
        for s, d, n in zip(src_ptrs, dst_ptrs, sizes):
            ctypes.memmove(int(d), int(s), int(n))

    def close(self) -> None:
        pass


class DeferredFlagEngine(LoopbackEngine):
    """Async loopback whose source bytes are read only by ``wait``.

    Deferring the memcpy makes source-slot reuse observable: the control worker must wait handle N before
    writing sequence N+1 into the same scratch slot, or both remote writes would incorrectly carry N+1.
    """

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.landed = []
        self.sync_writes = 0

    def write(self, dst_session, src_ptrs, dst_ptrs, sizes) -> None:
        self.sync_writes += 1
        super().write(dst_session, src_ptrs, dst_ptrs, sizes)

    def write_async(self, dst_session, src_ptrs, dst_ptrs, sizes):
        return tuple(zip(map(int, src_ptrs), map(int, dst_ptrs), map(int, sizes)))

    def wait(self, handles) -> None:
        for handle in handles:
            if handle is None:
                continue
            for src, dst, size in handle:
                value = ctypes.c_int64.from_address(src).value
                ctypes.memmove(dst, src, size)
                self.landed.append(value)


class IndependentCompletionEngine(LoopbackEngine):
    """Opaque handles are events, allowing a test to complete destinations out of order."""

    def wait(self, handles) -> None:
        for handle in handles:
            if handle is not None and not handle.wait(timeout=2.0):
                raise TimeoutError("test completion was never released")


def _make_endpoint(
    rank: int, peer: int, num_rounds: int, engine: RdmaEngine
) -> WBEndpoint:
    ep = WBEndpoint()
    ep.engine = engine
    ep._rank = rank
    ep.num_rounds = num_rounds
    ep._epoch = 0
    ep.peers = [peer]
    ep.flag_slot_of = {peer: 0}
    ep._flag_buf = torch.zeros(num_rounds, dtype=WBEndpoint._FLAG_DTYPE)
    ep._flag_src = torch.zeros(num_rounds, dtype=WBEndpoint._FLAG_DTYPE)
    ep.peer_session = {peer: f"sess-{peer}"}
    ep._ctlp = False
    return ep


def test_seq_monotonic_across_epochs():
    ep = WBEndpoint()
    ep.num_rounds = 3
    ep._epoch = 0
    assert [ep._seq(ri) for ri in range(3)] == [1, 2, 3]
    ep._epoch = 1
    assert [ep._seq(ri) for ri in range(3)] == [4, 5, 6]
    # Strictly increasing across the epoch boundary → a stale prior-epoch flag never satisfies a later poll.
    seqs = []
    for e in range(3):
        ep._epoch = e
        seqs.extend(ep._seq(ri) for ri in range(3))
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)


def test_flag_pingpong_and_data_ordering():
    """Drive the sender/receiver flag protocol sequentially over a loopback engine, 2 rounds x 2 epochs.

    Because the loopback ``write`` is synchronous, this deterministically exercises the flag addressing,
    the data-before-flag ordering, and ``_poll_flag``'s ``>=`` comparison across the epoch boundary.
    """
    num_rounds = 2
    eng = LoopbackEngine("shared")
    sender = _make_endpoint(rank=0, peer=1, num_rounds=num_rounds, engine=eng)
    receiver = _make_endpoint(rank=1, peer=0, num_rounds=num_rounds, engine=eng)

    # Cross-wire the flag destinations (each writes into the other's incoming slot 0).
    sender._flag_dst = {1: receiver._flag_buf.data_ptr()}
    receiver._flag_dst = {0: sender._flag_buf.data_ptr()}

    # A small data buffer per side: sender source, receiver destination (like a 1-peer round).
    payload_bytes = 16
    sender._data_buf = {1: torch.zeros(payload_bytes, dtype=torch.uint8)}
    receiver._data_buf = {0: torch.zeros(payload_bytes, dtype=torch.uint8)}
    sender._data_dst = {1: receiver._data_buf[0].data_ptr()}

    for epoch in range(2):
        for ri in range(num_rounds):
            seq = sender._seq(ri)
            assert seq == receiver._seq(ri)

            # ---- sender: write data, THEN done flag (ordering guaranteed by sync write) ----
            pattern = torch.arange(payload_bytes, dtype=torch.uint8) + (
                epoch * num_rounds + ri
            )
            sender._data_buf[1].copy_(pattern)
            eng.write(
                "sess-1",
                [sender._data_buf[1].data_ptr()],
                [sender._data_dst[1]],
                [payload_bytes],
            )
            sender._write_flag(1, seq)

            # ---- receiver: poll done, consume (data already landed), ack ----
            receiver._poll_flag(0, seq, timeout_s=5.0)
            assert torch.equal(receiver._data_buf[0], pattern), (
                f"data not landed before flag @e{epoch}r{ri}"
            )
            receiver._write_flag(0, seq)

            # ---- sender: poll consumed ----
            sender._poll_flag(1, seq, timeout_s=5.0)
        sender._epoch += 1
        receiver._epoch += 1

    assert sender._flag_buf.tolist() == [3, 4]
    assert receiver._flag_buf.tolist() == [3, 4]
    sender._flag_reaper_stop()
    receiver._flag_reaper_stop()


def test_async_flags_use_exclusive_round_slots():
    """Two messages use immutable source/destination words and no shared writer queue."""
    eng = DeferredFlagEngine("deferred")
    sender = _make_endpoint(rank=0, peer=1, num_rounds=2, engine=eng)
    receiver = _make_endpoint(rank=1, peer=0, num_rounds=2, engine=eng)
    sender._flag_dst = {1: receiver._flag_buf.data_ptr()}
    try:
        sender._flag_emit(0, 1, 1)
        sender._flag_emit(0, 1, 2)
        deadline = time.time() + 2.0
        while receiver._flag_buf.tolist() != [1, 2] and time.time() < deadline:
            time.sleep(0.001)
        assert eng.landed == [1, 2]
        assert eng.sync_writes == 0
        assert sender._flag_src.tolist() == [1, 2]
        assert receiver._flag_buf.tolist() == [1, 2]
        assert not hasattr(sender, "_ctl_thread")
    finally:
        sender._flag_reaper_stop()


def test_async_flag_reaper_delivers_without_a_later_write_or_epoch_flush():
    """A peer may block on the first flag; the off-path reaper must retire it independently."""
    eng = DeferredFlagEngine("first-flag")
    sender = _make_endpoint(rank=0, peer=1, num_rounds=1, engine=eng)
    receiver = _make_endpoint(rank=1, peer=0, num_rounds=1, engine=eng)
    sender._flag_dst = {1: receiver._flag_buf.data_ptr()}
    try:
        sender._flag_emit(0, 1, 1)
        deadline = time.time() + 2.0
        while int(receiver._flag_buf[0].item()) < 1 and time.time() < deadline:
            time.sleep(0.001)
        assert int(receiver._flag_buf[0].item()) == 1
        assert eng.landed == [1]
        assert eng.sync_writes == 0
    finally:
        sender._flag_reaper_stop()


def test_sender_peer_waiters_publish_data_ready_independently():
    """A blocked peer completion must not delay another peer's DATA-ready publication."""
    sender = WeightSender.__new__(WeightSender)
    sender._rank = 0
    sender.engine = IndependentCompletionEngine("independent")
    sender._wire_wait_queues = {}
    sender._wire_wait_threads = {}
    sender._wire_wait_lock = threading.Lock()
    sender._trace_state = lambda *args, **kwargs: None
    published = []
    publish_cv = threading.Condition()

    def publish(kind, peer, seq):
        with publish_cv:
            published.append((peer, seq, time.perf_counter()))
            publish_cv.notify_all()

    sender._flag_emit = publish
    result_q = queue.Queue()
    slow = threading.Event()
    fast = threading.Event()
    sender._ensure_wire_waiters({8, 9})
    now = time.time()
    try:
        sender._wire_wait_queues[8].put((0, 2, 3, slow, now, now, False, result_q))
        sender._wire_wait_queues[9].put((0, 2, 3, fast, now, now, False, result_q))
        released_at = time.perf_counter()
        fast.set()
        with publish_cv:
            publish_cv.wait_for(
                lambda: any(peer == 9 for peer, _seq, _t in published), timeout=1.0
            )
        assert any(peer == 9 for peer, _seq, _t in published)
        assert not any(peer == 8 for peer, _seq, _t in published)
        fast_published_at = next(t for peer, _seq, t in published if peer == 9)
        assert fast_published_at - released_at < 0.1

        slow.set()
        with publish_cv:
            publish_cv.wait_for(
                lambda: any(peer == 8 for peer, _seq, _t in published), timeout=1.0
            )
        assert {result_q.get(timeout=1.0)[0] for _ in range(2)} == {8, 9}
    finally:
        slow.set()
        fast.set()
        sender._stop_wire_waiters()


def test_direct_async_ready_flag_addressing():
    """Kind-1 writes must use the replica-ready source/destination rather than the sender ACK channel."""
    eng = DeferredFlagEngine("ready")
    ep = _make_endpoint(rank=1, peer=2, num_rounds=1, engine=eng)
    remote_ready = torch.zeros(1, dtype=WBEndpoint._FLAG_DTYPE)
    ep._repl_flag_slot_of = {2: 0}
    ep._repl_flag_src = torch.zeros(ep.num_rounds, dtype=WBEndpoint._FLAG_DTYPE)
    ep._repl_peer_session = {2: "sess-2"}
    ep._repl_flag_dst = {2: remote_ready.data_ptr()}
    try:
        ep._flag_emit(1, 2, 7)
        deadline = time.time() + 2.0
        while int(remote_ready[0].item()) != 7 and time.time() < deadline:
            time.sleep(0.001)
        assert eng.landed == [7]
        assert eng.sync_writes == 0
        assert int(remote_ready[0].item()) == 7
    finally:
        ep._flag_reaper_stop()


def test_copyplan_reuse():
    """A cached CopyPlan re-reads current source data on each run() (incl. a transposed segment)."""
    from wbridge.utils.data import CopyPlan

    d = "cuda"
    src = torch.randn(64, 32, device=d)
    dst = torch.zeros(64, 32, device=d)
    src_t = torch.randn(20, 10, device=d)  # transposed segment: src_t.t() -> dst_t
    dst_t = torch.zeros(10, 20, device=d)
    plan = CopyPlan([(dst, src), (dst_t, src_t.t())])
    plan.run()
    torch.cuda.synchronize()
    assert torch.equal(dst, src) and torch.equal(dst_t, src_t.t().contiguous()), (
        "first run wrong"
    )
    # mutate sources IN PLACE, replay the SAME plan: dst must reflect the new values (reuse invariant)
    src.copy_(torch.randn(64, 32, device=d))
    src_t.copy_(torch.randn(20, 10, device=d))
    plan.run()
    torch.cuda.synchronize()
    assert torch.equal(dst, src) and torch.equal(dst_t, src_t.t().contiguous()), (
        "reuse run wrong"
    )


def test_copyplan_single_kernel_mixes_flat_and_transposed_segments():
    """Internal consume must preserve one launch per peer even when its model mappings mix layouts."""
    from wbridge.utils.data import CopyPlan

    d = "cuda"
    src = torch.randn(64, 32, device=d)
    dst = torch.zeros_like(src)
    src_t = torch.randn(20, 10, device=d)
    dst_t = torch.zeros(10, 20, device=d)
    plan = CopyPlan([(dst, src), (dst_t, src_t.t())], single_kernel=True)
    assert plan.launch_count == 1
    plan.run()
    torch.cuda.synchronize()
    assert torch.equal(dst, src)
    assert torch.equal(dst_t, src_t.t().contiguous())


def test_copyplan_unifies_mixed_layouts_once_per_dtype():
    """A consume lane with BF16 weights and FP32 norms needs exactly two unified launches."""
    from wbridge.utils.data import CopyPlan

    src_bf16 = torch.randn(64, 32, device="cuda", dtype=torch.bfloat16)
    dst_bf16 = torch.zeros_like(src_bf16)
    src_f32 = torch.randn(20, 10, device="cuda", dtype=torch.float32)
    dst_f32 = torch.zeros(10, 20, device="cuda", dtype=torch.float32)
    plan = CopyPlan(
        [(dst_bf16, src_bf16), (dst_f32, src_f32.t())],
        unified_dtype_kernels=True,
    )

    assert plan.launch_count == 2
    plan.run()
    torch.cuda.synchronize()
    assert torch.equal(dst_bf16, src_bf16)
    assert torch.equal(dst_f32, src_f32.t().contiguous())


def test_copyplan_locally_converts_dtype():
    """The two-stage sender fallback casts BF16 model values into an FP32 wire view."""
    from wbridge.utils.data import CopyPlan

    src_bf16 = torch.randn(128, device="cuda", dtype=torch.bfloat16)
    dst_f32 = torch.zeros(128, device="cuda", dtype=torch.float32)
    src_f32 = torch.randn(128, device="cuda", dtype=torch.float32)
    dst_bf16 = torch.zeros(128, device="cuda", dtype=torch.bfloat16)
    plan = CopyPlan([(dst_f32, src_bf16), (dst_bf16, src_f32)])

    assert plan.launch_count == 2
    plan.run()
    torch.cuda.synchronize()
    assert torch.equal(dst_f32, src_bf16.float())
    assert torch.equal(dst_bf16, src_f32.bfloat16())


def test_copyplan_rebinds_epoch_scratch_source():
    """A cached A+R descriptor plan may follow a newly allocated non-RDMA scratch base each epoch."""
    from wbridge.utils.data import CopyPlan

    template = torch.zeros(1024, dtype=torch.uint8, device="cuda")
    actual = (
        torch.arange(1024, dtype=torch.int32, device="cuda")
        .remainder(251)
        .to(torch.uint8)
    )
    dst = torch.zeros_like(actual)
    plan = CopyPlan(
        [(dst, template)],
        source_region=(template.data_ptr(), template.numel()),
    )
    plan.rebase_sources(actual.data_ptr())
    plan.run()
    torch.cuda.synchronize()
    assert torch.equal(dst, actual)


if __name__ == "__main__":
    test_pack_into_matches_getitem()
    print("PASS test_pack_into_matches_getitem")
    test_seq_monotonic_across_epochs()
    print("PASS test_seq_monotonic_across_epochs")
    test_flag_pingpong_and_data_ordering()
    print("PASS test_flag_pingpong_and_data_ordering")
    test_async_flags_use_exclusive_round_slots()
    print("PASS test_async_flags_use_exclusive_round_slots")
    test_direct_async_ready_flag_addressing()
    print("PASS test_direct_async_ready_flag_addressing")
    if torch.cuda.is_available():
        test_copyplan_reuse()
        print("PASS test_copyplan_reuse")
    else:
        print("SKIP test_copyplan_reuse (no CUDA)")
    print("ALL RDMA PROTOCOL UNIT TESTS PASSED")
