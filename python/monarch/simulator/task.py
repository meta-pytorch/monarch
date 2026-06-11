# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import copy
import itertools
import traceback
from dataclasses import dataclass
from enum import auto, Enum
from typing import cast, Dict, List, Optional, Sequence

from monarch.simulator.config import META_VAL


class TaskState(Enum):
    PENDING = auto()
    READY = auto()
    EXECUTING = auto()
    EXECUTED = auto()


class Task:
    """
    A class to represent a task in a stream. A task is ready immediately if all
    its dependencies are executed. A task is executed if it is ready and it is
    the first task in the stream. A task can be marked as executed if it is executing
    and all the collectives, if any, of the task are executing.

    Args:
        inputs (List[int]): A list of input tensor ids.
        outputs (List[int]): A list of output tensor ids.
        command_id (int): The id of the command this task executes.
        runtime (int): The runtime of the task in nanoseconds.
        meta (List[str]): A list of metadata associated with the task.
        collectives (Optional[List]): A list of collectives associated with the task.
            Defaults to None.
    """

    def __init__(
        self,
        inputs: List[int],
        outputs: List[int],
        command_id: int,
        start_time: int,
        runtime: int,
        meta: List[str],
        collectives: Optional[List["Task"]] = None,
        waits: Optional[List["Task"]] = None,
        traceback: Sequence[traceback.FrameSummary] = (),
    ):
        self.inputs = inputs
        self.outputs = outputs
        self.runtime = runtime
        self.meta = meta + META_VAL
        self.dependencies = []
        self.collectives = collectives
        self.waits = waits
        self.command_id = command_id
        self.traceback = traceback
        if self.collectives is not None:
            self.collectives.append(self)

        self._state = TaskState.PENDING
        self.start_time = start_time
        self.end_time = 0

        # Assied by WorkerTaskManager
        self.task_id: Optional[int] = None

    def __repr__(self):
        return " ".join(self.meta)

    @property
    def state(self) -> TaskState:
        return self._state

    def maybe_set_ready(self) -> bool:
        """
        Sets the task state to READY if it is ready. Returns True if the task state
        changes from PENDING to READY.
        """
        if self._state != TaskState.PENDING:
            return False

        if self.dependencies:
            for d in self.dependencies:
                if d._state != TaskState.EXECUTED:
                    return False
                self.start_time = max(self.start_time, d.end_time)
        self._state = TaskState.READY
        return True

    def maybe_execute(self) -> bool:
        """
        Executes the task if it is ready. Returns True if the task state changes
        from READY to EXECUTING.
        """
        if self._state != TaskState.READY:
            return False

        self._state = TaskState.EXECUTING
        return True

    def maybe_finish(self) -> bool:
        """
        Finish the task if it is executing and all the associated collectives,
        if any, are executing or executed. Return True if the task state changes from
        EXECUTING to EXECUTED.
        """
        if not self._state == TaskState.EXECUTING:
            return False

        executed = True
        if self.collectives:
            executed = all(
                c.state in (TaskState.EXECUTING, TaskState.EXECUTED)
                for c in self.collectives
            )
        if self.waits:
            executed = executed and all(
                c.state == TaskState.EXECUTED for c in self.waits
            )
        if not executed:
            return False

        self._state = TaskState.EXECUTED
        if self.collectives:
            straggler_time = max(c.start_time for c in self.collectives)
            self.end_time = straggler_time + self.runtime
        if self.waits:
            last_wait_event_time = max(c.end_time for c in self.waits)
            self.end_time = max(self.end_time, last_wait_event_time)
        if self.meta[0] != "aten.view":
            self.end_time = max(self.end_time, self.start_time + self.runtime)
        else:
            # TODO: this is a workaround to removing `view` from the trace.
            # What we really should do is to have the CPU trace besides GPU trace.
            self.end_time = self.start_time

        return True

    def clone(self) -> "Task":
        return copy.copy(self)


@dataclass
class Borrow:
    ident: int
    tensor_src_id: int
    tensor_dst_id: int
    from_stream: int
    to_stream: int


class EventTask(Task):
    """Represents an event task in a stream."""

    def __init__(
        self,
        recorded_task: Task,
        event_stream: int,
        event_stream_name: str,
        wait_stream: int,
        wait_stream_name: str,
        start_time: int,
        command_id: int,
        runtime: int = 1,
        borrow: Optional[Borrow] = None,
        traceback: Sequence[traceback.FrameSummary] = (),
    ):
        super().__init__(
            inputs=[],
            outputs=[],
            command_id=command_id,
            start_time=start_time,
            runtime=runtime,
            meta=["waiting for", event_stream_name],
            waits=[recorded_task],
            traceback=traceback,
        )
        self.event_stream = event_stream
        self.event_stream_name = event_stream_name
        self.wait_stream = wait_stream
        self.wait_stream_name = wait_stream_name
        self.borrow = borrow

    def clone(self) -> "EventTask":
        return copy.copy(self)


class WorkerTaskManager(Task):
    def __init__(self) -> None:
        self.tasks: Dict[int, Task] = {}
        # pyrefly: ignore [bad-assignment]
        self.task_id = itertools.count()

    def add(self, task: Task) -> int:
        # pyrefly: ignore [bad-argument-type, no-matching-overload]
        task_id = next(self.task_id)
        self.tasks[task_id] = task
        task.task_id = task_id
        return task_id

    def remove(self, task: Task) -> None:
        if (task_id := task.task_id) is not None:
            self.tasks.pop(task_id)
        else:
            raise ValueError("task_id is None")

    def clone(self) -> "WorkerTaskManager":
        cloned_tasks = {}
        for task_id, task in self.tasks.items():
            cloned_task = task.clone()
            # Both dependencies and waits are all tasks on the same worker
            # thread. Thus, they must be in the same WorkerTaskManager or
            # they must be executed.
            cloned_tasks[task_id] = cloned_task
            if task.dependencies:
                cloned_task.dependencies = []
                for dep in task.dependencies:
                    if dep.task_id not in cloned_tasks:
                        # The dependency is executed, so it is not in the
                        # WorkerTaskManager. Just clone it to ensure the
                        # dependency is cloned but not added to the new
                        # WorkerTaskManager.
                        assert dep.state == TaskState.EXECUTED
                        cloned_task.dependencies.append(dep.clone())
                    else:
                        cloned_task.dependencies.append(cloned_tasks[dep.task_id])
            if task.waits is not None:
                cloned_task.waits = []
                for wait in cast(List[Task], task.waits):
                    if wait.task_id not in cloned_tasks:
                        assert wait.state == TaskState.EXECUTED
                        assert cloned_task.waits is not None
                        cloned_task.waits.append(wait.clone())
                    else:
                        assert cloned_task.waits is not None
                        cloned_task.waits.append(cloned_tasks[wait.task_id])

            # TODO: the global list shared by all the tasks with the same collective
            # is a neat idea but can be hard to debug. Consider make it more explicit.
            if cloned_task.collectives:
                cloned_task.collectives.append(cloned_task)

            cloned_tasks[task_id] = cloned_task

        ret = WorkerTaskManager()
        # Waste one to ensure all the cloned WorkerTaskManager has the same task_id.
        # pyrefly: ignore [bad-argument-type, no-matching-overload]
        next_task_id = next(self.task_id)
        # pyrefly: ignore [bad-assignment]
        ret.task_id = itertools.count(next_task_id + 1)
        ret.tasks = cloned_tasks
        return ret
