# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import argparse
import asyncio
import os
import random
import statistics
import time


# parse up front to extract env variables.
args = None
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="RDMA Test with configurable parameters"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=100,
        help="Number of test iterations (default: 100)",
    )
    parser.add_argument(
        "--device",
        type=str,
        nargs=2,
        default=["cpu", "cpu"],
        help="Two devices for actor0 and actor1: cpu or cuda:X where X is 0-7 (default: ['cpu', 'cpu'])",
    )
    parser.add_argument(
        "--operation",
        choices=["write", "read", "ping-pong"],
        default="write",
        help="RDMA operation type: write, read, or ping-pong (default: write)",
    )
    parser.add_argument(
        "--size",
        type=int,
        default=64,
        help="Data size per operation in MB (default: 64, must be multiple of 4)",
    )
    parser.add_argument(
        "--expandable-segments",
        type=str,
        choices=["true", "false"],
        default="true",
        help="Enable/disable PyTorch CUDA expandable segments (default: true)",
    )
    parser.add_argument(
        "--warmup-iterations",
        type=int,
        default=10,
        help="Number of warmup iterations (default: 5)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Number of concurrent RDMA operations (default: 1)",
    )

    args = parser.parse_args()

# Set expandable segments environment variable based on CLI argument
if args and args.expandable_segments == "false":
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:False"
else:
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"


# pyre-ignore
import torch
from monarch.actor import Actor, endpoint, this_host
from monarch.rdma import RDMABuffer


class RDMATest(Actor):
    def __init__(
        self, device: str = "cpu", operation: str = "write", size_mb: int = 64
    ) -> None:
        self.other_actor = None
        self.i = 0
        self.device = device
        self.operation = operation
        self.size_mb = size_mb

        # Timing data storage
        self.timing_data = []
        self.size_data = []
        self.batch_timing_data = []
        self.batch_size_data = []

    @endpoint
    async def set_other_actor(self, other_actor):
        self.other_actor = other_actor

    @endpoint
    async def send(self, is_warmup=False, concurrency: int = 1) -> None:
        # Track wall-clock time for the entire concurrent batch
        batch_start = time.time()

        tasks = []
        for _ in range(concurrency):
            tasks.append(self._send_single(is_warmup))
        await asyncio.gather(*tasks)

        batch_end = time.time()
        batch_elapsed = batch_end - batch_start

        if not is_warmup:
            batch_size = (
                sum(self.size_data[-concurrency:])
                if len(self.size_data) >= concurrency
                else 0
            )
            self.batch_timing_data.append(batch_elapsed)
            self.batch_size_data.append(batch_size)

        self.i += 1

    async def _send_single(self, is_warmup=False) -> None:
        shape = int(
            1024 * 1024 * self.size_mb / 4 * (0.5 * random.randint(1, 3))
        )  # Random size with +/- 50% variation based on user size

        # Use the device string directly
        tensor = torch.rand(shape, dtype=torch.float32, device=self.device)
        size_elem = tensor.numel() * tensor.element_size()
        tensor_addr = tensor.data_ptr()

        # Critical validation - this should catch the null pointer issue
        assert tensor_addr != 0, (
            f"CRITICAL: Tensor has null pointer! Device: {self.device}, Shape: {shape}"
        )
        assert size_elem > 0, f"CRITICAL: Tensor has zero size! Size: {size_elem}"

        byte_view = tensor.view(torch.uint8).flatten()
        # Validate byte_view too
        byte_view_addr = byte_view.data_ptr()
        assert byte_view_addr != 0, (
            f"CRITICAL: Byte view has null pointer! Original addr: 0x{tensor_addr:x}"
        )
        assert byte_view_addr == tensor_addr, (
            f"CRITICAL: Address mismatch! Tensor: 0x{tensor_addr:x}, ByteView: 0x{byte_view_addr:x}"
        )

        execution_start = time.time()
        buffer = RDMABuffer(byte_view)
        execution_end = time.time()
        elapsed = execution_end - execution_start

        # Store timing and size data in this actor
        size_elem = torch.numel(tensor) * tensor.element_size()
        if not is_warmup:
            self.timing_data.append(elapsed)
            self.size_data.append(size_elem)
        buffer_size = buffer.size()
        assert buffer_size == size_elem, f"{buffer_size=} != {size_elem=}"

        # Call recv - timing happens there
        await self.other_actor.recv.call(buffer, tensor.shape, tensor.dtype, is_warmup)

        # cleanup
        await buffer.drop()

    @endpoint
    async def recv(self, rdma_buffer, shape, dtype, is_warmup):
        # Create receiving tensor on the same device
        tensor = torch.rand(shape, dtype=dtype, device=self.device)
        byte_view = tensor.view(torch.uint8).flatten()

        execution_start = time.time()

        if self.operation == "write":
            await rdma_buffer.write_from(byte_view, timeout=5)
        elif self.operation == "read":
            await rdma_buffer.read_into(byte_view, timeout=5)
        elif self.operation == "ping-pong":
            if self.i % 2 == 0:
                await rdma_buffer.write_from(byte_view, timeout=5)
            else:
                await rdma_buffer.read_into(byte_view, timeout=5)

        execution_end = time.time()
        elapsed = execution_end - execution_start

        # Store timing and size data in this actor
        size_elem = torch.numel(tensor) * tensor.element_size()
        if not is_warmup:
            self.timing_data.append(elapsed)
            self.size_data.append(size_elem)

    @endpoint
    async def print_statistics(self, calc_bwd: bool = False):
        """Calculate and print timing statistics for individual operations"""
        if not self.timing_data:
            print("No timing data collected!")
            return

        timings = self.timing_data
        sizes = self.size_data

        # Calculate statistics for individual operations
        avg_time = statistics.mean(timings)
        min_time = min(timings)
        max_time = max(timings)
        std_time = statistics.stdev(timings) if len(timings) > 1 else 0.0

        avg_size = statistics.mean(sizes)
        total_data = sum(sizes)

        device_type = self.device.upper() if self.device != "cpu" else "CPU"
        print("\n" + "=" * 60)
        print(f"RDMA {self.operation.upper()} LOAD TEST RESULTS ({device_type})")
        print("=" * 60)

        print("INDIVIDUAL OPERATION TIMING:")
        print(f"  Average time per operation: {avg_time * 1000:.3f} ms")
        print(f"  Minimum time per operation: {min_time * 1000:.3f} ms")
        print(f"  Maximum time per operation: {max_time * 1000:.3f} ms")
        print(f"  Standard deviation: {std_time * 1000:.3f} ms")

        if calc_bwd:
            # Calculate throughput (GB/s where GB = 1000^3 bytes)
            def calc_throughput_gbs(size_bytes: int, time_seconds: float) -> float:
                if time_seconds == 0:
                    return 0.0
                return size_bytes / (time_seconds * 1e9)

            avg_throughput = calc_throughput_gbs(avg_size, avg_time)
            max_throughput = calc_throughput_gbs(avg_size, min_time)
            min_throughput = calc_throughput_gbs(avg_size, max_time)

            # Print results
            print(f"Total iterations completed: {len(timings)}")
            print(f"Average data per operation: {avg_size / (1024 * 1024):.1f} MB")
            print(f"Total data transferred: {total_data / (1024 * 1024):.1f} MB")
            print()

            print("INDIVIDUAL OPERATION THROUGHPUT (GB = 1000^3 bytes):")
            print(f"  Average throughput: {avg_throughput:.2f} GB/s")
            print(f"  Maximum throughput: {max_throughput:.2f} GB/s")
            print(f"  Minimum throughput: {min_throughput:.2f} GB/s")
            print("=" * 60)

    @endpoint
    async def print_batch_statistics(
        self, concurrency: int = 1, total_elapsed_time: float = 0.0
    ):
        """Calculate and print batch-level statistics for concurrent operations"""
        if not self.batch_timing_data:
            print("No batch timing data collected!")
            return

        batch_timings = self.batch_timing_data
        batch_sizes = self.batch_size_data
        total_data = sum(self.size_data)

        avg_batch_time = statistics.mean(batch_timings)
        min_batch_time = min(batch_timings)
        max_batch_time = max(batch_timings)
        std_batch_time = (
            statistics.stdev(batch_timings) if len(batch_timings) > 1 else 0.0
        )
        avg_batch_size = statistics.mean(batch_sizes)

        print("\nCONCURRENT BATCH TIMING (wall-clock for all concurrent ops):")
        print(f"  Average batch time: {avg_batch_time * 1000:.3f} ms")
        print(f"  Minimum batch time: {min_batch_time * 1000:.3f} ms")
        print(f"  Maximum batch time: {max_batch_time * 1000:.3f} ms")
        print(f"  Standard deviation: {std_batch_time * 1000:.3f} ms")
        print(f"  Average data per batch: {avg_batch_size / (1024 * 1024):.1f} MB")

        # Calculate throughput (GB/s where GB = 1000^3 bytes)
        def calc_throughput_gbs(size_bytes: int, time_seconds: float) -> float:
            if time_seconds == 0:
                return 0.0
            return size_bytes / (time_seconds * 1e9)

        avg_aggregate_tp = calc_throughput_gbs(avg_batch_size, avg_batch_time)
        max_aggregate_tp = calc_throughput_gbs(avg_batch_size, min_batch_time)
        min_aggregate_tp = calc_throughput_gbs(avg_batch_size, max_batch_time)

        print(f"\nAGGREGATE THROUGHPUT (concurrency={concurrency}, GB = 1000^3 bytes):")
        print(f"  Average aggregate throughput: {avg_aggregate_tp:.2f} GB/s")
        print(f"  Maximum aggregate throughput: {max_aggregate_tp:.2f} GB/s")
        print(f"  Minimum aggregate throughput: {min_aggregate_tp:.2f} GB/s")

        total_throughput = calc_throughput_gbs(total_data, total_elapsed_time)
        print("\nTOTAL SUSTAINED THROUGHPUT (GB = 1000^3 bytes):")
        print(f"  Total wall-clock time: {total_elapsed_time:.3f} s")
        print(f"  Total data transferred: {total_data / (1024 * 1024):.1f} MB")
        print(f"  Sustained throughput: {total_throughput:.2f} GB/s")
        if concurrency > 1:
            print(f"  (Accounts for {concurrency}x concurrent overlapping operations)")


async def main(
    devices: list[str],
    iterations: int = 100,
    operation: str = "write",
    size_mb: int = 64,
    warmup_iterations: int = 10,
    concurrency: int = 1,
):
    # Adjust GPU allocation based on the device types
    device_0, device_1 = devices[0], devices[1]
    use_cuda_0 = device_0.startswith("cuda:")
    use_cuda_1 = device_1.startswith("cuda:")

    gpu_config_0 = {"gpus": 1} if use_cuda_0 else {"cpus": 1}
    gpu_config_1 = {"gpus": 1} if use_cuda_1 else {"cpus": 1}

    mesh_0 = this_host().spawn_procs(per_host=gpu_config_0)
    actor_0 = mesh_0.spawn("rdma_test", RDMATest, device_0, operation, size_mb)

    mesh_1 = this_host().spawn_procs(per_host=gpu_config_1)
    actor_1 = mesh_1.spawn("rdma_test", RDMATest, device_1, operation, size_mb)

    await actor_0.set_other_actor.call(actor_1)

    for _ in range(warmup_iterations):
        await actor_0.send.call(is_warmup=True, concurrency=concurrency)

    total_start_time = time.time()
    for _ in range(iterations):
        await actor_0.send.call(concurrency=concurrency)
    total_end_time = time.time()
    total_elapsed_time = total_end_time - total_start_time

    # Actor 0: Print batch statistics (concurrency orchestration)
    await actor_0.print_batch_statistics.call(
        concurrency=concurrency, total_elapsed_time=total_elapsed_time
    )

    # Actor 1: Print individual RDMA transfer statistics
    await actor_1.print_statistics.call(calc_bwd=True)

    await mesh_0.stop()
    await mesh_1.stop()


if __name__ == "__main__":
    assert args

    # Validate size is multiple of 4
    if args.size % 4 != 0:
        print(f"Error: --size must be a multiple of 4. Got: {args.size}")
        exit(1)

    # Parse and validate device list
    devices = [device.lower() for device in args.device]
    validated_devices = []

    for i, device in enumerate(devices):
        if device == "cpu":
            validated_devices.append(device)  # CPU is always valid
        elif device.startswith("cuda:"):
            # Validate CUDA device format
            try:
                device_id = int(device.split(":")[1])
                if device_id < 0 or device_id > 7:
                    print(f"Error: CUDA device ID must be 0-7. Got: {device_id}")
                    exit(1)
            except (ValueError, IndexError):
                print(
                    f"Error: Invalid device format. Use 'cpu' or 'cuda:X' where X is 0-7. Got: {args.device[i]}"
                )
                exit(1)

            # Check if CUDA is available
            if not torch.cuda.is_available():
                print("Warning: CUDA requested but not available. Falling back to CPU.")
                validated_devices.append("cpu")
            elif device_id >= torch.cuda.device_count():
                print(
                    f"Warning: CUDA device {device_id} not available. Available devices: 0-{torch.cuda.device_count() - 1}. Falling back to CPU."
                )
                validated_devices.append("cpu")
            else:
                validated_devices.append(device)
        else:
            print(
                f"Error: Invalid device format. Use 'cpu' or 'cuda:X' where X is 0-7. Got: {args.device[i]}"
            )
            exit(1)

    asyncio.run(
        main(
            validated_devices,
            args.iterations,
            args.operation,
            args.size,
            args.warmup_iterations,
            args.concurrency,
        )
    )
