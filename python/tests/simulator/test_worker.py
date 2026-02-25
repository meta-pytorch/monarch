# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import unittest
from typing import Tuple

import torch
from monarch.common.fake import fake_call
from monarch.simulator.profiling import RuntimeEstimator
from monarch.simulator.task import Task
from monarch.simulator.tensor import FakeTensorTracker
from monarch.simulator.worker import Worker


# pyre-ignore-all-errors[6]
# pyre-ignore-all-errors[16]
def create_test_tasks(fake_tensor_tracker) -> Tuple[Task, ...]:
    def fake():
        for i in range(4):
            tensor = torch.randn(100, 100).cuda()
            tensor.ref = i
            tensor._fake = tensor
            fake_tensor_tracker.add({i: tensor})

        kwargs = {
            "inputs": [],
            "outputs": [0],
            "command_id": 0,
            "start_time": 1,
            "runtime": 1,
            "meta": ["randn"],
        }

        task0 = Task(**kwargs)

        kwargs["outputs"] = [1]
        task1 = Task(**kwargs)

        kwargs["inputs"] = [0, 1]
        kwargs["outputs"] = [2]
        kwargs["meta"] = ["mm"]
        task2 = Task(**kwargs)

        kwargs["inputs"] = [2, 1]
        kwargs["outputs"] = [3]
        kwargs["meta"] = ["mm"]
        task3 = Task(**kwargs)

        return task0, task1, task2, task3

    return fake_call(fake)


class TestWorker(unittest.TestCase):
    def test_stream_clone(self):
        worker = Worker(FakeTensorTracker(), RuntimeEstimator())
        worker.create_stream(0, "main", default=True)

        tasks = create_test_tasks(worker.fake_tensor_tracker)
        for i in range(3):
            worker.add_task(tasks[i], stream=0, now=i * 10 + 1)
        # Execute the first and second task
        for _ in range(2):
            worker.maybe_set_ready()
            worker.maybe_execute()
            worker.maybe_finish()

        main_stream = worker.streams[0]
        cloned_task_manager = worker.task_manager.clone()
        cloned_storage_tracker = worker.storage_tracker.clone()
        cloned_cpu_tensors = worker.cpu_tensors.clone(
            cloned_task_manager, cloned_storage_tracker
        )
        cloned_stream = main_stream.clone(
            cloned_task_manager, cloned_storage_tracker, cloned_cpu_tensors
        )

        self.assertEqual(cloned_stream.last_task.task_id, main_stream.last_task.task_id)
        self.assertEqual(len(cloned_stream.task_queue), len(main_stream.task_queue))

        self.assertEqual(cloned_stream.now, main_stream.now)
        self.assertEqual(cloned_stream.events, main_stream.events)
        self.assertNotEqual(id(cloned_stream.events), id(main_stream.events))
        self.assertEqual(cloned_stream.memory.usage, main_stream.memory.usage)
        self.assertEqual(cloned_stream.memory.events, main_stream.memory.events)
        self.assertNotEqual(
            cloned_stream.memory.storage_tracker, main_stream.memory.storage_tracker
        )
        self.assertNotEqual(id(cloned_stream.memory), id(main_stream.memory))
        self.assertEqual(
            cloned_stream.tensors.pending_delete_tensors,
            main_stream.tensors.pending_delete_tensors,
        )
        self.assertEqual(
            cloned_stream.tensors.tensors.keys(), main_stream.tensors.tensors.keys()
        )
        self.assertNotEqual(cloned_stream.tensors.tensors, main_stream.tensors.tensors)
