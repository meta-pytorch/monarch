# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import unittest

from monarch.simulator.task import Task, TaskState, WorkerTaskManager


class TestTask(unittest.TestCase):
    def test_worker_task_manager(self):
        manager = WorkerTaskManager()
        kwargs = {
            "inputs": [2],
            "outputs": [3],
            "command_id": 1,
            "start_time": 9,
            "runtime": 1,
            "meta": ["a"],
        }
        task = Task(**kwargs)
        task._state = TaskState.EXECUTED

        manager.add(task)
        # This task is executed.
        manager.remove(task)

        task2 = Task(**kwargs)
        task2.dependencies = [task]
        manager.add(task2)

        collectives = []
        collective_task = Task(collectives=collectives, **kwargs)
        collective_task.dependencies = [task2]
        manager.add(collective_task)
        # This is from another worker. Don't add it to the manager.
        other_worker_task = Task(**kwargs)

        collectives.append(other_worker_task)
        wait_task = Task(waits=[task], **kwargs)
        manager.add(wait_task)

        cloned_manager = manager.clone()

        self.assertEqual(len(manager.tasks), 3)
        self.assertEqual(manager.tasks.keys(), cloned_manager.tasks.keys())
        cloned_task2 = cloned_manager.tasks[task2.task_id]
        self.assertNotEqual(task2, cloned_task2)
        for k in kwargs.keys():
            self.assertEqual(getattr(cloned_task2, k), getattr(task2, k))
        self.assertEqual(cloned_task2.dependencies[0].task_id, task.task_id)
        self.assertNotEqual(cloned_task2.dependencies[0], task)
        cloned_wait_task = cloned_manager.tasks[wait_task.task_id]
        self.assertEqual(cloned_wait_task.waits[0].task_id, task.task_id)
        self.assertNotEqual(cloned_wait_task.waits[0], task)

        self.assertEqual(len(collectives), 3)
        cloned_collective_task = cloned_manager.tasks[collective_task.task_id]
        self.assertTrue(collective_task in collectives)
        self.assertTrue(cloned_collective_task in collectives)
        self.assertNotEqual(collective_task, cloned_collective_task)
