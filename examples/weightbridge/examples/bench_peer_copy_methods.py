# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Compare three ways to copy many tensors over a same-node GPU peer link.

The payload and tensor addresses are static, matching WeightBridge's replay use
case.  The three steady-state paths are:

* a Python loop of ``Tensor.copy_`` peer copies;
* an explicitly constructed CUDA Graph containing the same chained memcpy
  nodes; and
* one descriptor-driven Triton kernel which reads peer UVA addresses and
  writes the local destination tensors.

Run this on a node with two mutually accessible GPUs.  Graph construction,
descriptor construction, and Triton compilation/warmup are intentionally
excluded from the steady-state measurements and reported separately.
"""

from __future__ import annotations

import argparse
import json
import socket
import statistics
import time
from pathlib import Path
from typing import Callable

import torch
import triton
from cuda.bindings import runtime as cudart
from wbridge.utils.data import CopyPlan


def _cuda_call(name: str, *args):
    result = getattr(cudart, name)(*args)
    if not isinstance(result, tuple):
        result = (result,)
    error, *values = result
    if error != cudart.cudaError_t.cudaSuccess:
        raise RuntimeError(f"{name} failed: {error}")
    if not values:
        return None
    return values[0] if len(values) == 1 else tuple(values)


def _enable_peer_access(dst_device: int, src_device: int) -> None:
    """Enable SM loads from ``src_device`` while executing on ``dst_device``."""
    torch.cuda.set_device(dst_device)
    result = cudart.cudaDeviceEnablePeerAccess(src_device, 0)
    error = result[0]
    allowed = {
        cudart.cudaError_t.cudaSuccess,
        cudart.cudaError_t.cudaErrorPeerAccessAlreadyEnabled,
    }
    if error not in allowed:
        raise RuntimeError(
            f"cudaDeviceEnablePeerAccess({src_device}) on device {dst_device} failed: {error}"
        )


class PeerMemcpyGraph:
    """A CUDA Graph containing a stream-ordered chain of peer memcpy nodes."""

    def __init__(self, pairs: list[tuple[torch.Tensor, torch.Tensor]]) -> None:
        self.graph = _cuda_call("cudaGraphCreate", 0)
        previous = None
        self.nodes = []
        for dst, src in pairs:
            dependencies = None if previous is None else [previous]
            node = _cuda_call(
                "cudaGraphAddMemcpyNode1D",
                self.graph,
                dependencies,
                0 if previous is None else 1,
                dst.data_ptr(),
                src.data_ptr(),
                dst.numel() * dst.element_size(),
                cudart.cudaMemcpyKind.cudaMemcpyDefault,
            )
            self.nodes.append(node)
            previous = node
        self.executable = _cuda_call("cudaGraphInstantiate", self.graph, 0)

    def launch(self, stream: torch.cuda.Stream) -> None:
        _cuda_call("cudaGraphLaunch", self.executable, stream.cuda_stream)

    def close(self) -> None:
        if self.executable is not None:
            _cuda_call("cudaGraphExecDestroy", self.executable)
            self.executable = None
        if self.graph is not None:
            _cuda_call("cudaGraphDestroy", self.graph)
            self.graph = None


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    position = fraction * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _measure(
    launch: Callable[[], None],
    stream: torch.cuda.Stream,
    total_bytes: int,
    *,
    warmup: int,
    repeats: int,
) -> dict[str, float]:
    for _ in range(warmup):
        launch()
    stream.synchronize()

    start = torch.cuda.Event(enable_timing=True)
    end = torch.cuda.Event(enable_timing=True)
    gpu_ms: list[float] = []
    enqueue_us: list[float] = []
    end_to_end_ms: list[float] = []

    for _ in range(repeats):
        start.record(stream)
        before = time.perf_counter()
        launch()
        submitted = time.perf_counter()
        end.record(stream)
        end.synchronize()
        finished = time.perf_counter()
        gpu_ms.append(start.elapsed_time(end))
        enqueue_us.append((submitted - before) * 1e6)
        end_to_end_ms.append((finished - before) * 1e3)

    median_gpu_ms = statistics.median(gpu_ms)
    return {
        "gpu_ms_median": median_gpu_ms,
        "gpu_ms_p10": _percentile(gpu_ms, 0.10),
        "gpu_ms_p90": _percentile(gpu_ms, 0.90),
        "enqueue_us_median": statistics.median(enqueue_us),
        "end_to_end_ms_median": statistics.median(end_to_end_ms),
        "payload_gbps": total_bytes / (median_gpu_ms * 1e-3) / 1e9,
    }


def _sample_indices(total_elements: int, tensor_count: int) -> list[int]:
    elements_per_tensor = total_elements // tensor_count
    result: list[int] = []
    for tensor_index in range(tensor_count):
        base = tensor_index * elements_per_tensor
        result.extend(
            (base, base + elements_per_tensor // 2, base + elements_per_tensor - 1)
        )
    return sorted(set(result))


def _verify(
    launch: Callable[[], None],
    stream: torch.cuda.Stream,
    src_storage: torch.Tensor,
    dst_storage: torch.Tensor,
    tensor_count: int,
) -> None:
    with torch.cuda.device(dst_storage.device), torch.cuda.stream(stream):
        dst_storage.zero_()
    stream.synchronize()
    launch()
    stream.synchronize()
    indices = _sample_indices(src_storage.numel(), tensor_count)
    src_index = torch.tensor(indices, dtype=torch.int64, device=src_storage.device)
    dst_index = src_index.to(dst_storage.device)
    expected = src_storage.index_select(0, src_index).to(dst_storage.device)
    actual = dst_storage.index_select(0, dst_index)
    if not torch.equal(actual, expected):
        raise AssertionError(f"copy verification failed for {tensor_count} tensors")


def _run_count(
    src_storage: torch.Tensor,
    dst_storage: torch.Tensor,
    tensor_count: int,
    stream: torch.cuda.Stream,
    *,
    warmup: int,
    repeats: int,
) -> dict:
    if src_storage.numel() % tensor_count:
        raise ValueError(
            f"payload elements must be divisible by tensor count {tensor_count}"
        )
    src_tensors = list(src_storage.view(tensor_count, -1).unbind(0))
    dst_tensors = list(dst_storage.view(tensor_count, -1).unbind(0))
    pairs = list(zip(dst_tensors, src_tensors))
    dst_device = dst_storage.device.index

    def sequential() -> None:
        with torch.cuda.device(dst_device), torch.cuda.stream(stream):
            for dst, src in pairs:
                dst.copy_(src, non_blocking=True)

    graph_build_start = time.perf_counter()
    graph = PeerMemcpyGraph(pairs)
    graph_build_ms = (time.perf_counter() - graph_build_start) * 1e3

    def graph_replay() -> None:
        graph.launch(stream)

    plan_build_start = time.perf_counter()
    plan = CopyPlan(pairs)
    plan_build_ms = (time.perf_counter() - plan_build_start) * 1e3
    if len(plan._flat_groups) != 1 or plan._groups or plan._fallback:
        raise RuntimeError(
            "the single-kernel benchmark unexpectedly produced "
            f"flat={len(plan._flat_groups)} strided={len(plan._groups)} "
            f"fallback={len(plan._fallback)}"
        )

    def single_kernel() -> None:
        with torch.cuda.device(dst_device), torch.cuda.stream(stream):
            plan.run()

    methods = {
        "sequential_copy": sequential,
        "cuda_graph": graph_replay,
        "single_kernel": single_kernel,
    }
    measurements = {}
    try:
        for name, launch in methods.items():
            _verify(launch, stream, src_storage, dst_storage, tensor_count)
            measurements[name] = _measure(
                launch,
                stream,
                src_storage.numel() * src_storage.element_size(),
                warmup=warmup,
                repeats=repeats,
            )
    finally:
        graph.close()

    return {
        "tensor_count": tensor_count,
        "bytes_per_tensor": src_storage.numel()
        * src_storage.element_size()
        // tensor_count,
        "graph_build_ms": graph_build_ms,
        "kernel_plan_build_ms": plan_build_ms,
        "methods": measurements,
    }


def _format_table(results: list[dict]) -> str:
    header = (
        f"{'tensors':>8} {'KiB/tensor':>12} {'method':>18} "
        f"{'GPU ms':>10} {'E2E ms':>10} {'enqueue us':>12} {'GB/s':>10}"
    )
    lines = [header, "-" * len(header)]
    for result in results:
        for method, values in result["methods"].items():
            lines.append(
                f"{result['tensor_count']:8d} {result['bytes_per_tensor'] / 1024:12.1f} "
                f"{method:>18} {values['gpu_ms_median']:10.3f} "
                f"{values['end_to_end_ms_median']:10.3f} "
                f"{values['enqueue_us_median']:12.1f} {values['payload_gbps']:10.1f}"
            )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src-device", type=int, default=1)
    parser.add_argument("--dst-device", type=int, default=0)
    parser.add_argument("--total-mib", type=int, default=512)
    parser.add_argument(
        "--counts", type=int, nargs="+", default=[1, 8, 32, 128, 512, 2048]
    )
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--repeats", type=int, default=30)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    if torch.cuda.device_count() <= max(args.src_device, args.dst_device):
        raise RuntimeError(
            f"need devices {args.dst_device} and {args.src_device}; "
            f"only {torch.cuda.device_count()} CUDA devices are visible"
        )
    if not torch.cuda.can_device_access_peer(args.dst_device, args.src_device):
        raise RuntimeError(
            f"device {args.dst_device} cannot access peer {args.src_device}"
        )

    dtype = torch.bfloat16
    total_bytes = args.total_mib * 1024 * 1024
    element_size = torch.empty((), dtype=dtype).element_size()
    if total_bytes % element_size:
        raise ValueError("payload is not aligned to dtype")
    total_elements = total_bytes // element_size
    for count in args.counts:
        if count <= 0 or total_elements % count:
            raise ValueError(
                f"invalid tensor count {count} for {total_elements} elements"
            )

    _enable_peer_access(args.dst_device, args.src_device)
    with torch.cuda.device(args.src_device):
        src_storage = torch.empty(total_elements, dtype=dtype, device=args.src_device)
        src_storage.uniform_(-1.0, 1.0)
    with torch.cuda.device(args.dst_device):
        dst_storage = torch.empty(total_elements, dtype=dtype, device=args.dst_device)
        stream = torch.cuda.Stream(device=args.dst_device)
    torch.cuda.synchronize(args.src_device)
    torch.cuda.synchronize(args.dst_device)

    metadata = {
        "hostname": socket.gethostname(),
        "torch": torch.__version__,
        "cuda": torch.version.cuda,
        "triton": triton.__version__,
        "src_device": args.src_device,
        "src_gpu": torch.cuda.get_device_name(args.src_device),
        "dst_device": args.dst_device,
        "dst_gpu": torch.cuda.get_device_name(args.dst_device),
        "peer_access": True,
        "dtype": str(dtype),
        "total_bytes": total_bytes,
        "warmup": args.warmup,
        "repeats": args.repeats,
        "graph_shape": "stream-ordered chain of explicit cudaGraphAddMemcpyNode1D nodes",
        "single_kernel": "WeightBridge CopyPlan flat Triton descriptor kernel",
    }
    results = []
    for count in args.counts:
        print(f"benchmarking {count} tensors ...", flush=True)
        results.append(
            _run_count(
                src_storage,
                dst_storage,
                count,
                stream,
                warmup=args.warmup,
                repeats=args.repeats,
            )
        )

    document = {"metadata": metadata, "results": results}
    print()
    print(_format_table(results))
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(document, indent=2) + "\n")
        print(f"\nwrote {args.output}")


if __name__ == "__main__":
    main()
