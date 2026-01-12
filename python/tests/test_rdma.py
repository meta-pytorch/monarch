# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import os

# required to enable RDMA support
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

import pytest
import torch
from monarch.actor import Actor, current_rank, endpoint, this_host
from monarch.rdma import is_rdma_available, RDMAAction, RDMABuffer


needs_cuda = pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="CUDA not available",
)
needs_rdma = pytest.mark.skipif(
    not is_rdma_available(),
    reason="RDMA not available",
)


class ParameterServer(Actor):
    def __init__(self):
        self.params = torch.rand(10, 10)
        self.grad_buffer = torch.rand(10, 10)

    @endpoint
    async def grad_handle(self) -> RDMABuffer:
        byte_tensor = self.grad_buffer.view(torch.uint8).flatten()
        buffer = RDMABuffer(byte_tensor)
        return buffer

    @endpoint
    async def update(self):
        self.params += 0.01 * self.grad_buffer

    @endpoint
    async def get_grad_buffer(self) -> torch.Tensor:
        # just used for testing
        return self.grad_buffer


class ParameterClient(Actor):
    def __init__(self, server, buffer):
        self.server = server
        byte_tensor = buffer.view(torch.uint8).flatten()
        self.buffer = byte_tensor

    @endpoint
    async def upload(self, tensor):
        gh = await self.server.grad_handle.call_one()
        await gh.write_from(tensor)

    @endpoint
    async def download(self):
        gh = await self.server.grad_handle.call_one()
        await gh.read_into(self.buffer)

    @endpoint
    async def get_buffer(self):
        return self.buffer


@needs_rdma
@needs_cuda
async def test_proc_mesh_rdma():
    proc = this_host().spawn_procs(per_host={"gpus": 1})
    server = proc.spawn("server", ParameterServer)

    # --- CPU TESTS ---
    client_cpu = proc.spawn("client_cpu", ParameterClient, server, torch.ones(10, 10))
    x = await client_cpu.get_buffer.call_one()
    assert torch.sum(x.view(torch.float32).view(10, 10)) == 100
    zeros = torch.zeros(10, 10)
    await client_cpu.upload.call_one(zeros.view(torch.uint8).flatten())
    await client_cpu.download.call_one()
    x = await client_cpu.get_buffer.call_one()
    assert torch.sum(x.view(torch.float32).view(10, 10)) == 0

    # --- Modify server's backing buffer directly ---
    await server.update.call_one()

    # Should reflect updated values
    await client_cpu.download.call_one()

    buffer = await client_cpu.get_buffer.call_one()
    remote_grad = await server.get_grad_buffer.call_one()
    assert torch.allclose(buffer.view(torch.float32).view(10, 10), remote_grad)

    # --- GPU TESTS ---
    client_gpu = proc.spawn(
        "client_gpu", ParameterClient, server, torch.ones(10, 10, device="cuda")
    )
    x = await client_gpu.get_buffer.call_one()
    buffer = x.view(torch.float32).view(10, 10)
    assert torch.sum(buffer) == 100
    zeros = torch.zeros(10, 10, device="cuda")
    await client_gpu.upload.call_one(zeros.view(torch.uint8).flatten())
    await client_gpu.download.call_one()
    x = await client_gpu.get_buffer.call_one()
    buffer_gpu = x.view(torch.float32).view(10, 10)
    assert torch.sum(buffer_gpu) == 0

    # Modify server state again
    await server.update.call_one()
    await client_gpu.download.call_one()
    x = await client_gpu.get_buffer.call_one()
    buffer_gpu = x.view(torch.float32).view(10, 10)
    remote_grad = await server.get_grad_buffer.call_one()
    assert torch.allclose(buffer_gpu.cpu(), remote_grad.cpu())


@needs_rdma
async def test_rdma_buffer_drop():
    """Test the new drop() and owner methods on RDMABuffer with two actors"""
    prod_proc = this_host().spawn_procs(per_host={"processes": 1})
    cons_proc = this_host().spawn_procs(per_host={"processes": 1})

    class ProducerActor(Actor):
        def __init__(self):
            self.data = torch.ones(10, 10, dtype=torch.float32)  # 400 bytes
            self.buffer = None

        @endpoint
        async def create_buffer(self) -> RDMABuffer:
            """Create an RDMABuffer and return it"""
            byte_tensor = self.data.view(torch.uint8).flatten()
            self.buffer = RDMABuffer(byte_tensor)
            return self.buffer

        @endpoint
        async def drop_buffer(self) -> None:
            """Drop an RDMABuffer"""
            await self.buffer.drop()

    class ConsumerActor(Actor):
        def __init__(self):
            self.received_data = torch.zeros(10, 10, dtype=torch.float32)

        @endpoint
        async def receive_data(self, buffer: RDMABuffer):
            """Receive data from the buffer into local storage"""
            byte_tensor = self.received_data.view(torch.uint8).flatten()
            await buffer.read_into(byte_tensor)  # Read FROM buffer INTO local tensor
            return torch.sum(self.received_data).item()  # Should be 100 (10*10*1)

        @endpoint
        async def test_buffer_after_drop(self, buffer: RDMABuffer):
            """Try to use buffer after it's been dropped - should fail"""
            byte_tensor = self.received_data.view(torch.uint8).flatten()
            try:
                await buffer.read_into(byte_tensor)  # Try to read from dropped buffer
                return "SUCCESS"  # This should not happen
            except Exception as e:
                return f"EXPECTED_ERROR: {e}"

    # Create both actors
    producer = prod_proc.spawn("producer", ProducerActor)
    consumer = cons_proc.spawn("consumer", ConsumerActor)

    # Create an RDMA buffer in the producer
    buffer = await producer.create_buffer.call_one()

    # Pass buffer to consumer and test write operation
    result = await consumer.receive_data.call_one(buffer)
    assert result == 100.0, f"Expected 100.0, got {result}"

    # Now drop the buffer
    await producer.drop_buffer.call_one()

    # Try to use the buffer after dropping - this should fail
    error_result = await consumer.test_buffer_after_drop.call_one(buffer)
    assert error_result.startswith("EXPECTED_ERROR:"), (
        f"Expected an error after drop, but got: {error_result}"
    )

    print(f"✓ Buffer operations failed after drop as expected: {error_result}")

    # Clean up proc mesh
    await prod_proc.stop()
    await cons_proc.stop()


class TrainerActor(Actor):
    def __init__(self):
        super().__init__()
        self.trainer = torch.nn.Linear(10, 10).to("cuda")
        self.trainer.weight.data.zero_()

    @endpoint
    async def init(self, gen):
        ranks = current_rank()
        self.gen = gen.slice(**ranks)

    @endpoint
    async def exchange_metadata(self):
        byte_tensor = self.trainer.weight.data.view(torch.uint8).flatten()
        self.handle = RDMABuffer(byte_tensor)
        await self.gen.attach_weight_buffer.call(self.handle)

    @endpoint
    async def weights_ready(self):
        self.trainer.weight.data.add_(1.0)


class GeneratorActor(Actor):
    def __init__(self):
        super().__init__()
        self.generator = torch.nn.Linear(10, 10).to("cuda")
        self.step = 0

    @endpoint
    async def init(self, trainer):
        ranks = current_rank()
        self.trainer = trainer.slice(**ranks)

    @endpoint
    async def attach_weight_buffer(self, handle):
        self.handle = handle

    @endpoint
    async def update_weights(self):
        self.step += 1
        byte_tensor = self.generator.weight.data.view(torch.uint8).flatten()
        await self.handle.read_into(byte_tensor)
        assert torch.sum(self.generator.weight.data) == self.step * 100, (
            f"{torch.sum(self.generator.weight.data)=}, {self.step=}"
        )


@needs_rdma
@needs_cuda
async def test_gpu_trainer_generator():
    trainer_proc = this_host().spawn_procs(per_host={"gpus": 2})
    gen_proc = this_host().spawn_procs(per_host={"gpus": 2})
    trainer = trainer_proc.spawn("trainer", TrainerActor)
    generator = gen_proc.spawn("gen", GeneratorActor)

    await generator.init.call(trainer)
    await trainer.init.call(generator)
    await trainer.exchange_metadata.call()

    for _ in range(3):
        await trainer.weights_ready.call()
        await generator.update_weights.call()


@needs_rdma
@needs_cuda
def test_gpu_trainer_generator_sync() -> None:
    trainer_proc = this_host().spawn_procs(per_host={"gpus": 1})
    gen_proc = this_host().spawn_procs(per_host={"gpus": 1})
    trainer = trainer_proc.spawn("trainer", TrainerActor)
    generator = gen_proc.spawn("gen", GeneratorActor)

    generator.init.call(trainer).get()
    trainer.init.call(generator).get()
    trainer.exchange_metadata.call().get()

    for _ in range(1):
        trainer.weights_ready.call().get()
        generator.update_weights.call().get()


@needs_rdma
async def test_rdma_concurrent_2gb_writes_in_order():
    """Test concurrent 2GB RDMA buffer writes with reverse-order awaiting"""
    owner_proc = this_host().spawn_procs(per_host={"processes": 1})
    writer_proc = this_host().spawn_procs(per_host={"processes": 1})
    num_elem = 500_000_000  # 500M elements

    class BufferOwnerActor(Actor):
        def __init__(self):
            # Create a 2GB buffer (500M float32 elements * 4 bytes = 2GB)
            self.data = torch.zeros(num_elem, dtype=torch.float32)
            self.rdma_buffer = None

        @endpoint
        async def create_buffer(self) -> RDMABuffer:
            """Create a 2GB RDMABuffer"""
            byte_tensor = self.data.view(torch.uint8).flatten()
            self.rdma_buffer = RDMABuffer(byte_tensor)
            return self.rdma_buffer

        @endpoint
        async def drop_buffer(self) -> None:
            """Drop an RDMABuffer"""
            await self.rdma_buffer.drop()

        @endpoint
        async def get_buffer_data(self) -> torch.Tensor:
            """Return the current buffer data for verification"""
            return self.data

    class WriterActor(Actor):
        def __init__(self):
            # Create a 2GB buffer (500M float32 elements * 4 bytes = 2GB)
            self.tensor_a = torch.ones(
                num_elem, dtype=torch.float32
            )  # Will receive data
            self.tensor_b = torch.full(
                (num_elem,), 2.0, dtype=torch.float32
            )  # Will send data

        @endpoint
        async def perform_concurrent_writes(self, buffer: RDMABuffer):
            """Perform concurrent read/write operations and await in reverse order"""
            # Convert tensors to byte views for RDMA
            byte_tensor_a = self.tensor_a.view(torch.uint8).flatten()
            byte_tensor_b = self.tensor_b.view(torch.uint8).flatten()

            # Start both operations concurrently
            future_a = buffer.read_into(
                byte_tensor_a, timeout=10
            )  # Read FROM buffer INTO tensor_a
            future_b = buffer.write_from(
                byte_tensor_b, timeout=10
            )  # Write FROM tensor_b INTO buffer

            # Await in reverse order - sets actual execution order
            await future_b  # Await write operation first
            await future_a  # Await read operation second

            return "SUCCESS"

        @endpoint
        async def get_tensors(self) -> tuple[torch.Tensor, torch.Tensor]:
            """Return both tensors for verification"""
            return (self.tensor_a, self.tensor_b)

    # Create actors
    buffer_owner = owner_proc.spawn("buffer_owner", BufferOwnerActor)
    writer = writer_proc.spawn("writer", WriterActor)

    # Create the 2GB RDMA buffer
    buffer = await buffer_owner.create_buffer.call_one()
    print(f"✓ Created 2GB RDMA buffer (size: {buffer.size() / (1024**3):.2f} GB)")

    # Perform concurrent writes with reverse-order awaiting
    result = await writer.perform_concurrent_writes.call_one(buffer)
    assert result == "SUCCESS", f"Concurrent writes failed: {result}"

    # Verify the data flow worked correctly using torch.allclose
    tensor_a_actual, tensor_b_actual = await writer.get_tensors.call_one()
    buffer_data_actual = await buffer_owner.get_buffer_data.call_one()

    expected_result = torch.full((num_elem,), 2.0, dtype=torch.float32)

    # Verify using torch.allclose
    assert torch.allclose(tensor_a_actual, expected_result), (
        "tensor_a does not match expected 2.0s"
    )
    assert torch.allclose(tensor_b_actual, expected_result), (
        "tensor_b does not match expected 2.0s"
    )

    assert torch.allclose(buffer_data_actual, expected_result), (
        "RDMABuffer does not contain expected 2.0s"
    )

    print("✓ Concurrent 2GB operations completed successfully")

    # Drop the buffer
    await buffer_owner.drop_buffer.call_one()
    print("✓ Buffer dropped successfully")

    # Clean up proc mesh
    await owner_proc.stop()
    await writer_proc.stop()


class DataServerActor(Actor):
    def __init__(self, name: str, size: int = 100):
        super().__init__()
        self.name = name
        self.data = torch.full(
            (size,), float(ord(name)), dtype=torch.float32
        )  # Fill with ASCII value of name
        self.buffer = None

    @endpoint
    async def create_buffer(self) -> RDMABuffer:
        """Create an RDMABuffer from our data"""
        byte_tensor = self.data.view(torch.uint8).flatten()
        self.buffer = RDMABuffer(byte_tensor)
        return self.buffer

    @endpoint
    async def update_data(self, value: float):
        """Update our data with a new value"""
        self.data.fill_(value)

    @endpoint
    async def get_sum(self) -> float:
        """Get the sum of our data for verification"""
        return torch.sum(self.data).item()


class ClientActor(Actor):
    def __init__(self, size: int = 100):
        super().__init__()
        self.data_a = torch.zeros(size, dtype=torch.float32)
        self.data_b = torch.zeros(size, dtype=torch.float32)
        self.data_c = torch.zeros(size, dtype=torch.float32)
        self.action = None
        self.size = size

    @endpoint
    async def perform_batch_operations(
        self, buffer_a: RDMABuffer, buffer_b: RDMABuffer, buffer_c: RDMABuffer
    ) -> None:
        """Perform batched operations using RDMAAction across multiple remote actors"""

        # Create RDMAAction instance
        action = RDMAAction()

        # Chain multiple operations across different remote actors
        # Read from buffer A into local data_a
        action.read_into(buffer_a, self.data_a.view(torch.uint8).flatten())

        # # Write data_a to buffer B (modified data goes from A -> B)
        action.write_from(buffer_b, self.data_b.view(torch.uint8).flatten())

        # Read from buffer C into local data_c
        action.read_into(buffer_c, self.data_c.view(torch.uint8).flatten())

        # Let's read from buffer A into local data_a again; this covers edge case
        # but will result in duplicate work for now; which is fine for now.
        action.read_into(buffer_a, self.data_a.view(torch.uint8).flatten())
        self.action = action

    @endpoint
    async def run_action(self) -> None:
        """Run the RDMAAction instance"""
        assert self.action is not None, "action is not initialized"
        await self.action.submit()

    @endpoint
    async def perform_data_race(
        self, buffer_a: RDMABuffer, buffer_b: RDMABuffer, buffer_c: RDMABuffer
    ) -> None:
        """Perform batched operations using RDMAAction across multiple remote actors"""

        # Create RDMAAction instance
        action = RDMAAction()

        # Chain multiple operations across different remote actors
        # Read from buffer A into local data_a
        action.read_into(buffer_a, self.data_a.view(torch.uint8).flatten())

        # # Write data_a to buffer B (modified data goes from A -> B) - Data race!
        action.write_from(buffer_b, self.data_a.view(torch.uint8).flatten())

    @endpoint
    async def perform_data_race_w_slices(
        self, buffer_a: RDMABuffer, buffer_b: RDMABuffer, buffer_c: RDMABuffer
    ) -> None:
        """Perform batched operations using RDMAAction across multiple remote actors"""
        assert self.size == 250, "Size must be 100 for this test"
        # Create RDMAAction instance
        action = RDMAAction()

        # Chain multiple operations across different remote actors
        # Read from buffer A into local data_a
        action.read_into(buffer_a, self.data_a.view(torch.uint8)[0 : 100 * 4].flatten())
        action.write_from(buffer_a, self.data_a.view(torch.uint8)[150 * 4 :].flatten())
        action.read_into(
            buffer_a, self.data_a.view(torch.uint8)[75 * 4 : 175 * 4].flatten()
        )

    @endpoint
    async def get_local_data_sums(self) -> dict:
        """Get sums of local data for verification"""
        return {
            "data_a_sum": torch.sum(self.data_a).item(),
            "data_b_sum": torch.sum(self.data_b).item(),
            "data_c_sum": torch.sum(self.data_c).item(),
        }

    @endpoint
    async def perform_ok_w_slices(
        self, buffer_a: RDMABuffer, buffer_b: RDMABuffer, buffer_c: RDMABuffer
    ) -> None:
        """Perform batched operations using RDMAAction across multiple remote actors"""
        assert self.size == 250, "Size must be 100 for this test"
        # Create RDMAAction instance
        action = RDMAAction()

        # Chain multiple operations across different remote actors
        # Read from buffer A into local data_a
        action.read_into(buffer_a, self.data_a.view(torch.uint8)[0 : 100 * 4].flatten())
        action.read_into(buffer_a, self.data_a.view(torch.uint8)[150 * 4 :].flatten())
        action.read_into(
            buffer_a, self.data_a.view(torch.uint8)[50 * 4 : 150 * 4].flatten()
        )
        self.action = action

    @endpoint
    async def get_local_data_sums(self) -> dict:
        """Get sums of local data for verification"""
        return {
            "data_a_sum": torch.sum(self.data_a).item(),
            "data_b_sum": torch.sum(self.data_b).item(),
            "data_c_sum": torch.sum(self.data_c).item(),
        }


@needs_rdma
async def test_rdma_action_concurrent_execution():
    """Test RDMAAction with concurrent execution across multiple remote actors (2 remote + 1 local)"""

    # Setup: Create 3 separate processes (2 remote + 1 local)
    server_proc_1 = this_host().spawn_procs(per_host={"processes": 1})
    server_proc_2 = this_host().spawn_procs(per_host={"processes": 1})
    client_proc = this_host().spawn_procs(per_host={"processes": 1})

    # Create actors on different processes
    server_a = server_proc_1.spawn(
        "server_a", DataServerActor, "A"
    )  # Data filled with 65.0 (ASCII 'A')
    server_b = server_proc_2.spawn(
        "server_b", DataServerActor, "B"
    )  # Data filled with 66.0 (ASCII 'B')
    server_c = server_proc_1.spawn(
        "server_c", DataServerActor, "C"
    )  # Data filled with 67.0 (ASCII 'C')
    client = client_proc.spawn("client", ClientActor)

    # Create RDMA buffers on each server
    buffer_a = await server_a.create_buffer.call_one()
    buffer_b = await server_b.create_buffer.call_one()
    buffer_c = await server_c.create_buffer.call_one()

    # Verify initial server data
    sum_a_initial = await server_a.get_sum.call_one()
    sum_b_initial = await server_b.get_sum.call_one()
    sum_c_initial = await server_c.get_sum.call_one()

    assert sum_a_initial == 6500.0  # 100 * 65.0 (ASCII 'A')
    assert sum_b_initial == 6600.0  # 100 * 66.0 (ASCII 'B')
    assert sum_c_initial == 6700.0  # 100 * 67.0 (ASCII 'C')

    # Execute: Perform batch operations with RDMAAction
    # This will test concurrent execution across multiple remote actors
    await client.perform_batch_operations.call_one(buffer_a, buffer_b, buffer_c)

    await client.run_action.call_one()
    operation_results = await client.get_local_data_sums.call_one()

    # Verify the results
    assert operation_results["data_a_sum"] == sum_a_initial
    assert operation_results["data_c_sum"] == sum_c_initial

    # Verify that server B received the modified data from A
    sum_b_after = await server_b.get_sum.call_one()
    assert sum_b_after == 0.0

    # Verify servers A and C are unchanged
    sum_a_after = await server_a.get_sum.call_one()
    sum_c_after = await server_c.get_sum.call_one()
    assert sum_a_after == 6500.0  # A should be unchanged
    assert sum_c_after == 6700.0  # C should be unchanged


@needs_rdma
async def test_rdma_action_second_call():
    """Test RDMAAction with concurrent execution across multiple remote actors (2 remote + 1 local)"""

    # Setup: Create 3 separate processes (2 remote + 1 local)
    server_proc_1 = this_host().spawn_procs(per_host={"processes": 1})
    server_proc_2 = this_host().spawn_procs(per_host={"processes": 1})
    client_proc = this_host().spawn_procs(per_host={"processes": 1})

    # Create actors on different processes
    server_a = server_proc_1.spawn(
        "server_a", DataServerActor, "A"
    )  # Data filled with 65.0 (ASCII 'A')
    server_b = server_proc_2.spawn(
        "server_b", DataServerActor, "B"
    )  # Data filled with 66.0 (ASCII 'B')
    server_c = server_proc_1.spawn(
        "server_c", DataServerActor, "C"
    )  # Data filled with 67.0 (ASCII 'C')
    client = client_proc.spawn("client", ClientActor)

    # Create RDMA buffers on each server
    buffer_a = await server_a.create_buffer.call_one()
    buffer_b = await server_b.create_buffer.call_one()
    buffer_c = await server_c.create_buffer.call_one()

    # Verify initial server data
    sum_a_initial = await server_a.get_sum.call_one()
    sum_b_initial = await server_b.get_sum.call_one()
    sum_c_initial = await server_c.get_sum.call_one()

    assert sum_a_initial == 6500.0  # 100 * 65.0 (ASCII 'A')
    assert sum_b_initial == 6600.0  # 100 * 66.0 (ASCII 'B')
    assert sum_c_initial == 6700.0  # 100 * 67.0 (ASCII 'C')

    # Execute: Perform batch operations with RDMAAction
    # This will test concurrent execution across multiple remote actors
    await client.perform_batch_operations.call_one(buffer_a, buffer_b, buffer_c)

    await client.run_action.call_one()
    operation_results = await client.get_local_data_sums.call_one()

    # Verify the results
    assert operation_results["data_a_sum"] == sum_a_initial
    assert operation_results["data_c_sum"] == sum_c_initial

    # Verify that server B received the modified data from A
    sum_b_after = await server_b.get_sum.call_one()
    assert sum_b_after == 0.0

    # Verify servers A and C are unchanged
    sum_a_after = await server_a.get_sum.call_one()
    sum_c_after = await server_c.get_sum.call_one()
    assert sum_a_after == 6500.0  # A should be unchanged
    assert sum_c_after == 6700.0  # C should be unchanged

    # update data of server_a
    await server_a.update_data.call_one(1.0)

    await client.run_action.call_one()
    operation_results = await client.get_local_data_sums.call_one()

    # Verify the results
    assert operation_results["data_a_sum"] == 100.0
    assert operation_results["data_c_sum"] == sum_c_initial

    # Verify that server B, C same as before
    assert 0.0 == await server_b.get_sum.call_one()
    assert sum_c_after == await server_c.get_sum.call_one()


@needs_rdma
async def test_rdma_action_data_races():
    """Test RDMAAction with concurrent execution across multiple remote actors (2 remote + 1 local)"""

    # Setup: Create 3 separate processes (2 remote + 1 local)
    server_proc_1 = this_host().spawn_procs(per_host={"processes": 1})
    server_proc_2 = this_host().spawn_procs(per_host={"processes": 1})
    client_proc = this_host().spawn_procs(per_host={"processes": 1})

    # Create actors on different processes
    server_a = server_proc_1.spawn(
        "server_a",
        DataServerActor,
        "A",
    )  # Data filled with 65.0 (ASCII 'A')
    server_b = server_proc_2.spawn(
        "server_b", DataServerActor, "B"
    )  # Data filled with 66.0 (ASCII 'B')
    server_c = server_proc_1.spawn(
        "server_c", DataServerActor, "C"
    )  # Data filled with 67.0 (ASCII 'C')
    client = client_proc.spawn("client", ClientActor, 250)

    # Create RDMA buffers on each server
    buffer_a = await server_a.create_buffer.call_one()
    buffer_b = await server_b.create_buffer.call_one()
    buffer_c = await server_c.create_buffer.call_one()

    with pytest.raises(Exception):
        await client.perform_data_race.call_one(buffer_a, buffer_b, buffer_c)

    with pytest.raises(Exception):
        await client.perform_data_race_w_slices.call_one(buffer_a, buffer_b, buffer_c)


@needs_rdma
async def test_rdma_action_slicing():
    """Test RDMAAction with concurrent execution across multiple remote actors (2 remote + 1 local)"""

    # Setup: Create 3 separate processes (2 remote + 1 local)
    server_proc_1 = this_host().spawn_procs(per_host={"processes": 1})
    server_proc_2 = this_host().spawn_procs(per_host={"processes": 1})
    client_proc = this_host().spawn_procs(per_host={"processes": 1})

    # Create actors on different processes
    server_a = server_proc_1.spawn(
        "server_a",
        DataServerActor,
        "A",
    )  # Data filled with 65.0 (ASCII 'A')
    server_b = server_proc_2.spawn(
        "server_b", DataServerActor, "B"
    )  # Data filled with 66.0 (ASCII 'B')
    server_c = server_proc_1.spawn(
        "server_c", DataServerActor, "C"
    )  # Data filled with 67.0 (ASCII 'C')
    client = client_proc.spawn("client", ClientActor, 250)

    # Create RDMA buffers on each server
    buffer_a = await server_a.create_buffer.call_one()
    buffer_b = await server_b.create_buffer.call_one()
    buffer_c = await server_c.create_buffer.call_one()

    sum_a_initial = await server_a.get_sum.call_one()
    await client.perform_ok_w_slices.call_one(buffer_a, buffer_b, buffer_c)
    await client.run_action.call_one()
    # Verify that server A received local values
    operation_results = await client.get_local_data_sums.call_one()
    assert (
        operation_results["data_a_sum"] == sum_a_initial * 2.5
    )  # multi-filled from 100 to 250
