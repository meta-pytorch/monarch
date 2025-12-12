# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import copy
import heapq
import logging
import traceback
from collections import defaultdict
from itertools import count
from typing import Dict, List, NamedTuple, Optional, Sequence, Set, Tuple, Union

import torch
from monarch.common.fake import fake_call
from monarch.common.tensor_factory import TensorFactory
from monarch.simulator.task import Task, WorkerTaskManager

logger = logging.getLogger(__name__)


class DTensorRef:
    """
    A reference to a `controller.tensor.Tensor` object.

    This class is used to keep track of DTensor objects that have been created
    and by the controller and to provide the mechanism to serialize DTensor
    objects (torch.save/torch.load).
    """

    created: Dict[int, "DTensorRef"] = {}

    def __init__(self, tensor):
        self.ref = tensor.ref
        self.factory = TensorFactory.from_tensor(tensor)
        self._fake: Optional[torch._subclasses.FakeTensor] = getattr(
            tensor, "_fake", None
        )
        # Capture mesh reference from tensor if available
        self._mesh_ref: Optional[int] = None
        if hasattr(tensor, "mesh") and tensor.mesh is not None:
            self._mesh_ref = getattr(tensor.mesh, "ref", None)

        if self._fake is not None:
            self._storage_id: Optional[torch.types._int] = id(
                self._fake.untyped_storage()
            )
            self._size: Optional[int] = self._fake.untyped_storage().size()
        else:
            self._storage_id = None
            self._size = None

    def __repr__(self):
        return f"DTensorRef({self.ref})"

    @classmethod
    def from_ref(cls, tensor) -> "DTensorRef":
        if tensor.ref not in cls.created:
            cls.created[tensor.ref] = cls(tensor)
        return cls.created[tensor.ref]

    def __getstate__(self):
        return {
            "ref": self.ref,
            "factory": self.factory,
            "_fake": None,
        }

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._fake = fake_call(self.factory.zeros)

    def __deepcopy__(self, memo):
        if self._fake is None:
            raise RuntimeError()

        fake = fake_call(self.factory.zeros)
        fake._fake = fake
        fake.ref = self.ref
        return self.__class__(fake)


class FakeTensorTracker:
    """
    Tracks the fake tensors created in the simulator. While each worker and stream
    maintain its own tensors, we don't want to create one FakeTensor per stream/worker.
    Instead, we can just share the fake tensor for the same tensor id.
    This can reduce the simulation time.

    A fake tensor is created when it is first created in any worker and is deleted
    when it is deleted in all workers.
    """

    def __init__(self) -> None:
        self.tensors: Dict[int, torch._subclasses.FakeTensor] = {}
        self._ref: Dict[int, int] = defaultdict(int)
        self._borrowed_tensors: Set[int] = set()

    def add(
        self, tensors: Dict[int, torch._subclasses.FakeTensor], is_borrowed=False
    ) -> None:
        self.tensors.update(tensors)
        if is_borrowed:
            self._borrowed_tensors.update(set(tensors.keys()))

    def is_borrowed(self, tensor: int) -> bool:
        return tensor in self._borrowed_tensors

    def incr_ref(self, tensor_id: int) -> None:
        assert tensor_id in self.tensors, f"Tensor {tensor_id} is not created"
        self._ref[tensor_id] += 1

    def decr_ref(self, tensor_id: int):
        ref = self._ref[tensor_id] - 1
        assert ref >= 0, f"Tensor {tensor_id} has negative ref count {ref}"
        if ref == 0:
            self.tensors.pop(tensor_id)
            self._ref.pop(tensor_id)
        else:
            self._ref[tensor_id] = ref


class StorageEvent(NamedTuple):
    address: int
    delta: int


class WorkerStorageTracker:
    def __init__(self, fake_tensor_tracker) -> None:
        self.storages: Dict[torch.UntypedStorage, Set[int]] = {}
        self.fake_tensor_tracker = fake_tensor_tracker
        self._addr_counter = count(step=128)  # aligning 128-byte cache lines?
        self.storage_addresses: Dict[torch.UntypedStorage, int] = {}

    def incr_ref(self, tensor_id: int) -> Optional[StorageEvent]:
        fake = self.fake_tensor_tracker.tensors[tensor_id]
        storage = fake.untyped_storage()
        if storage not in self.storages:
            self.storages[storage] = {tensor_id}
            addr = next(self._addr_counter)
            self.storage_addresses[storage] = addr
            if self.fake_tensor_tracker.is_borrowed(tensor_id):
                return None  # Q: should self._addr_counter be reversed?
            else:
                return StorageEvent(addr, storage.size())
        else:
            self.storages[storage].add(tensor_id)
            return None

    def decr_ref(self, tensor_id: int) -> Optional[StorageEvent]:
        fake = self.fake_tensor_tracker.tensors[tensor_id]
        storage = fake.untyped_storage()
        if storage not in self.storages:
            raise RuntimeError(
                f"{storage} is being dereferenced but it is not tracked."
            )
        else:
            references = self.storages[storage]
            references.remove(tensor_id)
            if len(references) == 0:
                self.storages.pop(storage)
                addr = self.storage_addresses.pop(storage)
                if self.fake_tensor_tracker.is_borrowed(tensor_id):
                    # The controller creates a new FakeTensor for Borrow.
                    # So we should not count the storage usage of this
                    # FakeTensor as it is not a materialized tensor on
                    # the works.
                    return None
                else:
                    return StorageEvent(addr, storage.size())
            return None

    def clone(self) -> "WorkerStorageTracker":
        ret = WorkerStorageTracker(self.fake_tensor_tracker)
        ret.storages = copy.copy(self.storages)
        return ret


class MemoryEvent(NamedTuple):
    timestamp: int
    address: int
    delta: int
    traceback: Sequence[traceback.FrameSummary]

    def __lt__(self, other):
        if self.timestamp == other.timestamp:
            return self.delta < other.delta
        return self.timestamp < other.timestamp

    def __gt__(self, other):
        if self.timestamp == other.timestamp:
            return self.delta > other.delta
        return self.timestamp > other.timestamp

    def __eq__(self, other):
        return self.timestamp == other.timestamp and self.delta == other.delta


class StreamMemoryTracker:
    """
    Tracks the memory events (timestamp, usage_delta) of a stream. The usage
    may not be added in the correct time order due to the asynchronous
    simulated-execution of worker CPU thread and the stream thread. Thus a
    heap is used to sort the events by timestamp.
    """

    def __init__(self, storage_tracker: WorkerStorageTracker) -> None:
        self.usage = 0
        self.events: List[MemoryEvent] = []
        self.storage_tracker = storage_tracker
        self._tracked_addresses: Dict[int, int] = {}

    def incr_ref(
        self, ts: int, tensor_id, traceback: Optional[Sequence[traceback.FrameSummary]]
    ) -> None:
        storage_event = self.storage_tracker.incr_ref(tensor_id)
        delta = 0 if storage_event is None else storage_event.delta
        logger.debug(
            f"StreamMemoryTracker got {tensor_id} at {ts} and delta is {delta}."
        )
        # Some operators may return zero-size tensors.
        # One example is aten._scaled_dot_product_flash_attention.default
        torch.ops.aten._scaled_dot_product_flash_attention.default
        if storage_event is not None and storage_event.delta != 0:
            assert ts >= 0
            assert traceback is not None
            self._add_usage(ts, storage_event, traceback)

    def decr_ref(
        self, ts: int, tensor_id, traceback: Optional[Sequence[traceback.FrameSummary]]
    ) -> None:
        storage_event = self.storage_tracker.decr_ref(tensor_id)
        if storage_event is not None and storage_event.delta != 0:
            assert ts >= 0
            assert traceback is not None
            self._remove_usage(ts, storage_event, traceback)

    def _remove_usage(self, ts: int, storage_event: StorageEvent, traceback) -> None:
        assert storage_event.delta <= self.usage
        self.usage -= storage_event.delta
        recorded_ts = self._tracked_addresses.pop(storage_event.address, -1)
        if recorded_ts == -1:
            raise RuntimeError(f"Cannot find the address {storage_event.address}")
        if recorded_ts >= ts:
            raise RuntimeError(
                f"The address {storage_event.address} is allocated after being freed"
            )
        heapq.heappush(
            self.events,
            MemoryEvent(ts, storage_event.address, -storage_event.delta, traceback),
        )

    def _add_usage(self, ts: int, storage_event: StorageEvent, traceback) -> None:
        self.usage += storage_event.delta
        self._tracked_addresses[storage_event.address] = ts
        heapq.heappush(
            self.events,
            MemoryEvent(ts, storage_event.address, storage_event.delta, traceback),
        )

    def pop_event(self) -> MemoryEvent:
        return heapq.heappop(self.events)

    def clone(self, storage_tracker: WorkerStorageTracker) -> "StreamMemoryTracker":
        ret = StreamMemoryTracker(storage_tracker)
        ret.usage = self.usage
        ret.events = copy.copy(self.events)
        return ret


class TensorManager:
    """
    Tracks the tensor created in a worker or a stream. It can be CPU tensor,
    which can only be owned by the worker or a gpu tensor which can only be
    owned by a stream.
    """

    def __init__(
        self,
        fake_tensor_tracker: FakeTensorTracker,
        memory: Optional[StreamMemoryTracker],
    ) -> None:
        self.tensors: Dict[int, Set[Union[Task, int]]] = {}
        self.delete_tracebacks: Dict[
            int, Optional[Sequence[traceback.FrameSummary]]
        ] = {}
        self.pending_delete_tensors: Set[int] = set()
        self.memory = memory
        self.fake_tensor_tracker = fake_tensor_tracker

    def add(self, tensor_id: int, refs: Tuple[Union[Task, int], ...], now: int) -> None:
        logger.debug(f"TensorManager got {tensor_id} at {now}.")
        self.tensors[tensor_id] = set(refs)
        self.fake_tensor_tracker.incr_ref(tensor_id)

    def first_use(
        self,
        tensor_id: int,
        now: int,
        traceback: Optional[Sequence[traceback.FrameSummary]],
    ) -> None:
        logging.debug(f"TensorManager: {tensor_id} is first used")
        if self.memory:
            self.memory.incr_ref(now, tensor_id, traceback)

    def incr_ref(self, tensor_id: int, ref: Union[Task, int]) -> None:
        logging.debug(f"TensorManager: {tensor_id} is referenced.")
        self.tensors[tensor_id].add(ref)

    def decr_ref(
        self,
        tensor_id: int,
        ref: Union[Task, int],
        now: int,
        traceback: Optional[Sequence[traceback.FrameSummary]],
    ) -> None:
        logging.debug(f"TensorManager: {tensor_id} decr_ref.")
        self.tensors[tensor_id].remove(ref)
        self._maybe_delete_tensor(tensor_id, now, traceback)

    def delete(
        self,
        tensor_id: int,
        now: int,
        traceback: Optional[Sequence[traceback.FrameSummary]],
    ) -> None:
        self.pending_delete_tensors.add(tensor_id)
        self._maybe_delete_tensor(tensor_id, now, traceback)

    def __contains__(self, key: int) -> bool:
        return key in self.tensors

    def _maybe_delete_tensor(
        self,
        tensor_id: int,
        now: int,
        traceback: Optional[Sequence[traceback.FrameSummary]],
    ) -> None:
        if len(self.tensors[tensor_id]) > 0:
            return

        if tensor_id not in self.pending_delete_tensors:
            # While no one is using this tensor, Controller has not
            # asked us to delete the tensor. Track the traceback of
            # the last task.
            self.delete_tracebacks[tensor_id] = traceback
            return

        traceback = (
            traceback
            if traceback is not None
            else self.delete_tracebacks.pop(tensor_id, None)
        )

        if self.memory:
            self.memory.decr_ref(now, tensor_id, traceback)

        self.tensors.pop(tensor_id)
        self.fake_tensor_tracker.decr_ref(tensor_id)
        self.pending_delete_tensors.remove(tensor_id)

    def clone(
        self, task_manager: WorkerTaskManager, memory: Optional[StreamMemoryTracker]
    ) -> "TensorManager":
        ret = TensorManager(self.fake_tensor_tracker, memory)
        ret.pending_delete_tensors = copy.copy(self.pending_delete_tensors)
        for k, v in self.tensors.items():
            new_v = set()
            for task in v:
                if isinstance(task, Task):
                    assert task.task_id is not None
                    new_v.add(task_manager.tasks[task.task_id])
                else:
                    new_v.add(task)
            ret.tensors[k] = new_v
        return ret
