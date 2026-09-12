# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Smoke test for :class:`~wbridge.backend.rdma.mooncake.MooncakeEngine` (register + one-sided write).

Two processes on the same node exchange (session_id, dst_ptr, size) over a spawn ``Queue``; the sender
RDMA-writes a known byte pattern into the receiver's registered buffer over Mooncake ``tcp``; the receiver
verifies the bytes. Runs both a **host-pinned** and a **CUDA (VRAM)** buffer variant — the CUDA variant
settles whether the ``tcp`` transport can register+write GPU memory (the data-plane design depends on it).

Needs a GPU + ``mooncake`` (skipped otherwise), so run in-container:

    python tests/test_mooncake_engine.py            # both variants
    python tests/test_mooncake_engine.py host        # host-pinned only
    python tests/test_mooncake_engine.py cuda        # CUDA/VRAM only
"""

import multiprocessing as mp
import sys

import pytest

N = 256 * 1024  # bytes to transfer


def _pattern(torch):
    """Deterministic uint8 pattern of length N (0,1,2,...,255,0,...)."""
    return (torch.arange(N) % 256).to(torch.uint8)


def _alloc(torch, mode):
    if mode == "cuda":
        return torch.zeros(N, dtype=torch.uint8, device="cuda")
    return torch.zeros(N, dtype=torch.uint8).pin_memory()


def _receiver(mode, q_info, q_done, out_q) -> None:
    import torch
    from wbridge.backend.rdma import MooncakeEngine
    from wbridge.utils.distributed import get_local_ip

    eng = MooncakeEngine()
    eng.init(get_local_ip(), "tcp", "")
    buf = _alloc(torch, mode)  # zero-initialised destination
    eng.register(buf.data_ptr(), N)
    q_info.put((eng.session_id(), buf.data_ptr(), N))

    q_done.get()  # block until the sender reports the write is done
    got = buf.detach().to("cpu")
    ok = bool(torch.equal(got, _pattern(torch)))
    out_q.put(("receiver", mode, ok, got[:4].tolist(), got[-4:].tolist()))
    eng.close()


def _sender(mode, q_info, q_done) -> None:
    import torch
    from wbridge.backend.rdma import MooncakeEngine
    from wbridge.utils.distributed import get_local_ip

    eng = MooncakeEngine()
    eng.init(get_local_ip(), "tcp", "")
    session, dst_ptr, size = q_info.get()

    src = _pattern(torch)
    src = src.cuda() if mode == "cuda" else src.pin_memory()
    eng.register(src.data_ptr(), N)
    eng.write(session, [src.data_ptr()], [dst_ptr], [size])  # blocking one-sided write
    q_done.put("done")
    eng.close()


def run_once(mode: str) -> None:
    ctx = mp.get_context("spawn")  # CUDA-safe; avoids fork+CUDA issues
    q_info, q_done, out_q = ctx.Queue(), ctx.Queue(), ctx.Queue()
    recv = ctx.Process(target=_receiver, args=(mode, q_info, q_done, out_q))
    send = ctx.Process(target=_sender, args=(mode, q_info, q_done))
    recv.start()
    send.start()
    try:
        result = out_q.get(timeout=120)
    finally:
        send.join(timeout=15)
        recv.join(timeout=15)
        for p in (send, recv):
            if p.is_alive():
                p.terminate()
    print(f"[mooncake-smoke] {result}", flush=True)
    assert result[2], f"{mode}: received bytes != sent pattern ({result})"


@pytest.mark.parametrize("mode", ["host", "cuda"])
def test_mooncake_write(mode):
    pytest.importorskip("mooncake")
    torch = pytest.importorskip("torch")
    if mode == "cuda" and not torch.cuda.is_available():
        pytest.skip("no CUDA")
    run_once(mode)


if __name__ == "__main__":
    modes = sys.argv[1:] or ["host", "cuda"]
    for m in modes:
        print(f"=== mooncake smoke: {m} ===", flush=True)
        run_once(m)
        print(f"PASS {m}", flush=True)
    print("ALL MOONCAKE SMOKE TESTS PASSED", flush=True)
