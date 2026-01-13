# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""
Unit tests for python/monarch/_src/rdma/rdma.py

RDMA Testing Architecture - Dataflow Summary
===========================================
Tests RDMA operations between distributed actors using Controller-Receiver pattern.
Controller owns tensors, Receiver performs remote memory operations via RDMABuffer handles.
┌─────────────────┐                    ┌─────────────────┐
│ Controller Actor│                    │ Receiver Actor  │
│                 │                    │                 │
│ ┌─────────────┐ │                    │ ┌─────────────┐ │
│ │   Tensor    │ │                    │ │Remote Tensor│ │
│ │             │ │                    │ │             │ │
│ └─────────────┘ │                    │ └─────────────┘ │
│        │        │                    │        │        │
│        ▼        │                    │        ▼        │
│ ┌─────────────┐ │  1. Send RDMABuffer│ ┌─────────────┐ │
│ │ RDMABuffer  │ ├───────────────────►│ │RDMABuffer   │ │
│ │ (Handles)   │ │     Objects        │ │(Received)   │ │
│ └─────────────┘ │                    │ └─────────────┘ │
│                 │                    │        │        │
│                 │                    │        ▼        │
│                 │                    │ ┌─────────────┐ │
│                 │  2. RDMA Operations│ │  Test Ops:  │ │
│                 │ ◄──────────────────┤ │• read_into  │ │
│                 │                    │ |(also verify)| │
│                 │                    │ |             | │
│                 │    (Direct Memory  │ │• write_from │ │
│                 │     Access)        │ └─────────────┘ │
│                 │                    │                 │
│ ┌─────────────┐ │                    │                 │
│ │verify       │ │ 3. Check Results   │                 │
│ │write_from   │ │                    │                 │
│ └─────────────┘ │                    │                 │
└─────────────────┘                    └─────────────────┘
Flow:
1. Setup:    Controller wraps tensors → RDMABuffer objects
2. Distribute: RDMABuffer handles sent to Receiver
3. Test:     Receiver uses RDMA ops for direct memory access
4. Verify:   Verify results on controller or receiver depending on where the memory should be modified.

Design Constraints:
- Tests implemented as endpoints of the controller (RDMABuffers require Actor context)
- Each test endpoint returns errors for failed operations

Adding a New Test:
1. Add receiver endpoint to RDMABufferTestReceiver if new RDMA operation needed:
   @endpoint
   async def do_new_operation(self, key, buffer: RDMABuffer, ...):
       # Perform RDMA operation and save results for verification

2. Add test logic endpoint to RDMABufferTestController:
   @endpoint
   async def test_new_rdma_operation(self) -> Exception | None:
       try:
           # Setup tensors and buffers
           # Call receiver endpoints
           # Verify results
       except Exception as e:
           return e
       return None
3. Add test function
   @pytest.mark.asyncio
   async def test_new_operation():
       # Spawn controller and receiver
       # Call controller.test_new_rdma_operation.call_one()
       # Assert result is None (no exception)

"""

import asyncio
import os
import uuid
from functools import partial
from typing import Callable

import numpy as np
import pytest
import torch
from monarch.actor import Actor, endpoint, this_host
from monarch.rdma import is_rdma_available, RDMABuffer


TIMEOUT = 60  # 60 seconds


def _get_temp_root():
    for var in ["BUCK_SCRATCH_PATH", "TMPDIR", "TMP", "TEMP", "TEMPDIR"]:
        path = os.environ.get(var)
        if path:
            return path
    # Fallback if none are set
    tmp_path = os.path.join(os.getcwd(), "tmp")
    os.makedirs(tmp_path, exist_ok=True)
    return str(tmp_path)


TEMP_ROOT = _get_temp_root()

needs_cuda = pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="CUDA not available",
)
needs_rdma = pytest.mark.skipif(
    not is_rdma_available(),
    reason="RDMA not available",
)


def _assert_value_error(func) -> None:
    error_thrown = False
    try:
        func()
    except Exception as e:
        error_thrown = True
        if not isinstance(e, ValueError):
            raise Exception("Exception raised, but is not a ValueError.") from e
    assert error_thrown, "Exception not raised"


class RDMABufferTestReceiver(Actor):
    def __init__(
        self, *, device: str, data_getter: Callable[[], dict[str, torch.Tensor]]
    ):
        self.data = data_getter()
        self.device = device

    @endpoint
    async def do_read_into_and_verify(
        self, key, buffer: RDMABuffer, tensor_shape, tensor_dtype
    ):
        """Read into a tensor from the RDMABuffer and compare with the expected tensor."""
        tensor = torch.zeros(*tensor_shape, dtype=tensor_dtype, device=self.device)

        await buffer.read_into(tensor, timeout=TIMEOUT)
        expected = self.data[key]
        assert torch.allclose(tensor, expected), (
            "dst tensor is not equal to expected tensor after read_into."
        )

    @endpoint
    async def do_write_from_random(
        self, key, buffer: RDMABuffer, tensor_shape, tensor_dtype, *, seed
    ):
        """Write a random tensor generated by the given seed to the RDMA Buffer."""
        tensor = _get_random_tensor(
            *tensor_shape, dtype=tensor_dtype, device=self.device, seed=seed
        )
        tensor_copy = tensor.clone()
        await buffer.write_from(tensor, timeout=TIMEOUT)
        assert torch.equal(tensor_copy, tensor), (
            "Source tensor was modified during write_from()."
        )


class RDMABufferTestController(Actor):
    """This actor does the actual testing.
    Different tests are implemented as endpoints and returns the error if any.
    Tests have to be implemented this way because RDMABuffers are only available in an Actor.
    RDMABuffers point to tensors owned by the controller actor and the receiver(remote) actor either
    read_into() its own tensor or write_from() a random tensor and thus modifies the local tensor owned by the controller.
    """

    def __init__(
        self,
        *,
        device: str,
        data_getter: Callable[[], dict[str, torch.Tensor]],
        receiver_actor: RDMABufferTestReceiver,
    ):
        self.device = device
        self.data = data_getter()
        self.receiver_actor = receiver_actor

    @endpoint
    async def test_rdma_buffer_init_with_memoryview(self) -> Exception | None:
        """Test RDMABuffer init with memoryview."""
        try:
            tensors = self.data
            np_arrays = {
                key: tensor.detach().numpy().view(np.uint8)
                for key, tensor in tensors.items()
            }
            buffers = {
                key: RDMABuffer(memoryview(np_array))
                for key, np_array in np_arrays.items()
            }
            for key, tensor in tensors.items():
                await self.receiver_actor.do_read_into_and_verify.call_one(
                    key, buffers[key], tensor.shape, tensor.dtype
                )
        except Exception as e:
            return e

    @endpoint
    async def test_rdma_buffer_write_from(self) -> Exception | None:
        """Test RDMABuffer write from a tensor. The RDMABuffer points to the local tensor and is modified by the remote (receiver) actor."""
        try:
            tensors = self.data
            for key, tensor in tensors.items():
                buffer = RDMABuffer(tensor)
                await self.receiver_actor.do_write_from_random.call_one(
                    key, buffer, tensor.shape, tensor.dtype, seed=256
                )
            buffers = {key: RDMABuffer(tensor) for key, tensor in tensors.items()}
            for key, tensor in tensors.items():
                await self.receiver_actor.do_write_from_random.call_one(
                    key, buffers[key], tensor.shape, tensor.dtype, seed=256
                )
            expected = {
                key: _get_random_tensor(
                    *tensor.shape, dtype=tensor.dtype, device=self.device, seed=256
                )
                for key, tensor in tensors.items()
            }
            for key, tensor in tensors.items():
                assert torch.equal(tensor, expected[key]), (
                    "Remote tensor is not equal to local tensor (RDMABuffer) after write_from() for key {key}"
                )

        except Exception as e:
            return e
        return None

    @endpoint
    async def test_rdma_buffer_write_from_concurrent(self) -> Exception | None:
        """Test RDMABuffer write from a tensor. The RDMABuffer points to the local tensor and is modified by the remote (receiver) actor."""
        try:
            tensors = self.data
            buffers = {key: RDMABuffer(tensor) for key, tensor in tensors.items()}
            await asyncio.gather(
                *[
                    self.receiver_actor.do_write_from_random.call_one(
                        key, buffers[key], tensor.shape, tensor.dtype, seed=256
                    )
                    for key, tensor in tensors.items()
                ]
            )
            expected = {
                key: _get_random_tensor(
                    *tensor.shape, dtype=tensor.dtype, device=self.device, seed=256
                )
                for key, tensor in tensors.items()
            }
            for key, tensor in tensors.items():
                assert torch.equal(tensor, expected[key]), (
                    "Remote tensor is not equal to local tensor (RDMABuffer) after write_from() for key {key}"
                )

        except Exception as e:
            return e
        return None

    @endpoint
    async def test_rdma_buffer_read_into(self) -> Exception | None:
        """Test RDMABuffer read into a tensor. The RDMABuffer points to the local tensor and the remote tensor is modified."""
        try:
            tensors = self.data
            buffers = {key: RDMABuffer(tensor) for key, tensor in tensors.items()}
            for key, tensor in tensors.items():
                await self.receiver_actor.do_read_into_and_verify.call_one(
                    key, buffers[key], tensor.shape, tensor.dtype
                )
        except Exception as e:
            return e

    @endpoint
    async def test_rdma_buffer_read_into_concurrent(self) -> Exception | None:
        """Test RDMABuffer read into a tensor. The RDMABuffer points to the local tensor and the remote tensor is modified."""
        try:
            tensors = self.data
            buffers = {key: RDMABuffer(tensor) for key, tensor in tensors.items()}
            await asyncio.gather(
                *[
                    self.receiver_actor.do_read_into_and_verify.call_one(
                        key, buffers[key], tensor.shape, tensor.dtype
                    )
                    for key, tensor in tensors.items()
                ]
            )
        except Exception as e:
            return e

    # Start test for value errors
    # --------------------------------------------------------------------------------------------
    @endpoint
    async def test_rdma_buffer_init_with_zero_size_raises_error(
        self,
    ) -> Exception | None:
        try:
            t = torch.empty(0, device=self.device).view(-1)
            _assert_value_error(lambda: RDMABuffer(t))

            t = torch.zeros(5, 5, device=self.device)[5:, 5:].view(-1)
            _assert_value_error(lambda: RDMABuffer(t))

            t = torch.zeros(5, 5, 5, device=self.device)[5:, 5:, 5:].view(-1)
            _assert_value_error(lambda: RDMABuffer(t))

        except Exception as e:
            return e

    @endpoint
    async def test_rdma_buffer_read_into_non_contiguous_tensor_raises_error(
        self,
    ) -> Exception | None:
        try:
            t0 = torch.zeros(5, 5, device=self.device).view(-1)
            buffer = RDMABuffer(t0)

            t = torch.zeros(25, device=self.device)[::2]
            _assert_value_error(lambda: buffer.read_into(t))

        except Exception as e:
            return e
        return None

    @endpoint
    async def test_rdma_buffer_write_from_non_contiguous_tensor_raises_error(
        self,
    ) -> Exception | None:
        try:
            t0 = torch.zeros(5, 5, device=self.device).view(-1)
            buffer = RDMABuffer(t0)

            t = torch.zeros(25, device=self.device)[::2]
            _assert_value_error(lambda: buffer.write_from(t))

        except Exception as e:
            return e
        return None

    @endpoint
    async def test_rdma_buffer_read_into_smaller_size_raises_error(
        self,
    ) -> Exception | None:
        try:
            t0 = torch.zeros(5, 5, device=self.device).view(-1)
            buffer = RDMABuffer(t0)

            t = torch.zeros(25 - 1, device=self.device).view(-1)
            _assert_value_error(lambda: buffer.read_into(t))

        except Exception as e:
            return e

    @endpoint
    async def test_rdma_buffer_write_from_larger_size_raises_error(
        self,
    ) -> Exception | None:
        try:
            t0 = torch.zeros(5, 5, device=self.device).view(-1)
            buffer = RDMABuffer(t0)

            t = torch.zeros(25 - 1, device=self.device).view(-1)
            _assert_value_error(lambda: buffer.read_into(t))

        except Exception as e:
            return e
        return None

    # --------------------------------------------------------------------------------------------
    # End test for value erros


def _get_random_tensor(
    *size, dtype: torch.dtype, device: str, pin_memory=False, seed=42
):
    torch.manual_seed(seed=seed)
    torch.use_deterministic_algorithms(mode=True)
    if device != "cpu":
        pin_memory = False
    if dtype in [
        torch.uint8,
        torch.int8,
        torch.uint16,
        torch.uint16,
        torch.uint32,
        torch.int32,
        torch.uint16,
        torch.int64,
    ]:
        return torch.randint(
            0, 64, size, dtype=dtype, device=device, pin_memory=pin_memory
        )
    return torch.rand(*size, dtype=dtype, device=device, pin_memory=pin_memory)


async def _spawn_controller_and_receiver(
    *,
    controller_device: str,
    receiver_device: str,
    data_getter: Callable[[], dict[str, torch.Tensor]],
    receiver_actor: RDMABufferTestReceiver,
) -> tuple[RDMABufferTestController, RDMABufferTestReceiver]:
    # Assign a unique ID to the actors to avoid collisions when tests are run in parallel.
    world_id = uuid.uuid4().hex
    receiver_actor = (
        this_host()
        .spawn_procs(per_host={"gpus": 1})
        .spawn(
            f"rdma_receiver_{world_id}",
            RDMABufferTestReceiver,
            device=receiver_device,
            data_getter=data_getter,
        )
    )
    controller_actor = (
        this_host()
        .spawn_procs(per_host={"gpus": 1})
        .spawn(
            f"rdma_test_controller_{world_id}",
            RDMABufferTestController,
            device=controller_device,
            data_getter=data_getter,
            receiver_actor=receiver_actor,
        )
    )
    return controller_actor, receiver_actor


# Start of functions to generate tensors for testing
# ------------------------------------------------------------------------------


@torch.no_grad()
def _whole_tensors_small(device, dtype: torch.dtype) -> dict[str, torch.Tensor]:
    sizes = [(i * 2**exp,) for i in range(1, 8) for exp in range(0, 11, 2)]  # 2^10 = 1K
    return {
        f"tensor_{i}_size_{size}": _get_random_tensor(*size, dtype=dtype, device=device)
        for i, size in enumerate(sizes)
    }


@torch.no_grad()
def _whole_tensors_large(device, dtype: torch.dtype) -> dict[str, torch.Tensor]:
    sizes = [
        (i * 2**exp,)
        for i in range(1, 8)
        for exp in range(11, 20, 2)  # 2K to 2^19 = 512K
    ]
    return {
        f"tensor_{i}_size_{size}": _get_random_tensor(*size, dtype=dtype, device=device)
        for i, size in enumerate(sizes)
    }


# @torch.no_grad()
# def _whole_tensors_very_large(device, dtype: torch.dtype) -> dict[str, torch.Tensor]:
#     sizes = [
#         (i * 2**exp)
#         for i in range(1, 8)
#         for exp in range(21, 26, 2)  # 2^21 2M to 2^24 16M
#     ]
#     return {
#         f"tensor_{i}_size_{size}": _get_random_tensor(*size, dtype=dtype, device=device)
#         for i, size in enumerate(sizes)
#     }


@torch.no_grad()
def _make_chunk_views(tensors: dict[str, torch.Tensor]) -> dict[str, torch.Tensor]:
    res = {}
    for key, tensor in tensors.items():
        n_splits = min(5, tensor.numel())
        chunks = torch.split(tensor, n_splits)
        for i, chunk in enumerate(chunks):
            res[f"{key}_chunk_view_{i}"] = chunk
    return res


@torch.no_grad()
def _tensor_views_small(device, dtype: torch.dtype) -> dict[str, torch.Tensor]:
    return _make_chunk_views(_whole_tensors_small(device, dtype))


@torch.no_grad()
def _tensor_views_large(device, dtype: torch.dtype) -> dict[str, torch.Tensor]:
    return _make_chunk_views(_whole_tensors_large(device, dtype))


# @torch.no_grad()
# def _tensor_views_very_large(device, dtype: torch.dtype) -> dict[str, torch.Tensor]:
#     return _make_chunk_views(_whole_tensors_very_large(device, dtype))


# So that we can reuse the same boilerplate.
def _nothing(device, dtype: torch.dtype) -> dict[str, torch.Tensor]:
    return {}


# End of functions to generate tensors for testing
# ------------------------------------------------------------------------------


TEST_DTYPES = [
    torch.uint8,
    torch.float16,
    torch.float64,
]

ALL_DATA_GETTERS = [
    _tensor_views_small,
    _whole_tensors_small,
    _tensor_views_large,
    _whole_tensors_large,
    # TODO: enable the following tests when we fix flakiness of large tensors
    # _whole_tensors_very_large,
    # _tensor_views_very_large,
]

CONTROLLER_DEVICES = ["cpu"]
RECEIVER_DEVICES = ["cpu"]


async def _do_test(func, dtype, data_getter, controller_device, receiver_device):
    controller, receiver = await _spawn_controller_and_receiver(
        controller_device=controller_device,
        receiver_device=receiver_device,
        data_getter=partial(data_getter, device=controller_device, dtype=dtype),
        receiver_actor=None,
    )
    error = await func(controller)
    if error is not None:
        raise error


def _test_with_all_data(func):
    @needs_cuda
    @needs_rdma
    @pytest.mark.parametrize("dtype", TEST_DTYPES)
    @pytest.mark.parametrize("data_getter", [_nothing])
    @pytest.mark.parametrize("controller_device", CONTROLLER_DEVICES)
    @pytest.mark.parametrize("receiver_device", RECEIVER_DEVICES)
    @pytest.mark.asyncio
    async def marked(dtype, data_getter, controller_device, receiver_device):
        return await func(dtype, data_getter, controller_device, receiver_device)

    return marked


def _test_with_no_data(func):
    @needs_cuda
    @needs_rdma
    @pytest.mark.parametrize("dtype", TEST_DTYPES)
    @pytest.mark.parametrize("data_getter", ALL_DATA_GETTERS)
    @pytest.mark.parametrize("controller_device", CONTROLLER_DEVICES)
    @pytest.mark.parametrize("receiver_device", RECEIVER_DEVICES)
    @pytest.mark.asyncio
    async def marked(dtype, data_getter, controller_device, receiver_device):
        return await func(dtype, data_getter, controller_device, receiver_device)

    return marked


@_test_with_no_data
async def test_rdma_buffer_init_with_zero_size_raises_error(
    dtype, data_getter, controller_device, receiver_device
):
    _do_test(
        lambda controller: controller.test_rdma_buffer_init_with_zero_size_raises_error.call_one(),
        dtype,
        data_getter,
        controller_device,
        receiver_device,
    )


@_test_with_all_data
async def test_rdma_buffer_init_with_memoryview(
    dtype, data_getter, controller_device, receiver_device
):
    """Test that RDMABuffer initialization with memoryview works correctly."""
    await _do_test(
        lambda controller: controller.test_rdma_buffer_init_with_memoryview.call_one(),
        dtype,
        data_getter,
        controller_device,
        receiver_device,
    )


@_test_with_no_data
async def test_rdma_buffer_read_into_non_contiguous_tensor_raises_error(
    dtype, data_getter, controller_device, receiver_device
):
    await _do_test(
        lambda controller: controller.test_rdma_buffer_read_into_non_contiguous_tensor_raises_error.call_one(),
        dtype,
        data_getter,
        controller_device,
        receiver_device,
    )


@_test_with_no_data
async def test_rdma_buffer_write_from_non_contiguous_tensor_raises_error(
    dtype, data_getter, controller_device, receiver_device
):
    await _do_test(
        lambda controller: controller.test_rdma_buffer_write_from_non_contiguous_tensor_raises_error.call_one(),
        dtype,
        data_getter,
        controller_device,
        receiver_device,
    )


@_test_with_no_data
async def test_rdma_buffer_read_into_smaller_size_raises_error(
    dtype, data_getter, controller_device, receiver_device
):
    await _do_test(
        lambda controller: controller.test_rdma_buffer_read_into_smaller_size_raises_error.call_one(),
        dtype,
        data_getter,
        controller_device,
        receiver_device,
    )


@_test_with_no_data
async def test_rdma_buffer_write_from_larger_size_raises_error(
    dtype, data_getter, controller_device, receiver_device
):
    await _do_test(
        lambda controller: controller.test_rdma_buffer_write_from_larger_size_raises_error.call_one(),
        dtype,
        data_getter,
        controller_device,
        receiver_device,
    )


@_test_with_all_data
async def test_rdma_buffer_write_from(
    dtype, data_getter, controller_device, receiver_device
):
    await _do_test(
        lambda controller: controller.test_rdma_buffer_write_from.call_one(),
        dtype,
        data_getter,
        controller_device,
        receiver_device,
    )


@_test_with_all_data
async def test_rdma_buffer_read_into(
    dtype, data_getter, controller_device, receiver_device
):
    await _do_test(
        lambda controller: controller.test_rdma_buffer_read_into.call_one(),
        dtype,
        data_getter,
        controller_device,
        receiver_device,
    )


# TODO: enable the following tests when we fix concurrency issues
# @_test_with_all_data
# async def test_rdma_buffer_read_into_concurrent(
#     dtype, data_getter, controller_device, receiver_device
# ):
#     await _do_test(
#         lambda controller: controller.test_rdma_buffer_read_into_concurrent.call_one(),
#         dtype,
#         data_getter,
#         controller_device,
#         receiver_device,
#     )

# TODO: enable the following tests when we fix concurrency issues
# @_test_with_all_data
# async def test_rdma_buffer_write_from_concurrent(
#     dtype, data_getter, controller_device, receiver_device
# ):
#     await _do_test(
#         lambda controller: controller.test_rdma_buffer_write_from_concurrent.call_one(),
#         dtype,
#         data_getter,
#         controller_device,
#         receiver_device,
#     )
