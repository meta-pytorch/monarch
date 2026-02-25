# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import itertools
import os
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Dict, NamedTuple, Optional, Tuple

import torch
from monarch.common.remote import remote
from monarch.remote_class import ControllerRemoteClass, WorkerRemoteClass


class Schedule(NamedTuple):
    wait: int
    warmup: int
    active: int
    repeat: int = 0
    skip_first: int = 0


class profile:
    """
    The class wraps `torch.profiler.profile()` to allow invoking the profiler remotely.
    There are two main differences:
    1) `on_trace_ready` can only be a string, indicating the folder where the traces
        will be saved.
    2) `schedule` must be of type `monarch.profiler.Schedule`.
    """

    PATH_KEY = "on_trace_ready"
    _counter = itertools.count()

    def __init__(self, *args, **kwargs) -> None:
        assert isinstance(kwargs.get(self.PATH_KEY, None), str), (
            f"{self.PATH_KEY} must be passed and must be a string to represent the "
            "path to save the profiler."
        )
        schedule = kwargs.get("schedule", None)
        assert isinstance(schedule, Schedule) or schedule is None, (
            "schedule can only be monarch.profiler.Schedule or None."
        )
        self.id = next(self._counter)
        _profiler_controller_init(self.id, *args, **kwargs)

    def __enter__(self) -> "profile":
        _profiler_controller_enter(self.id)
        return self

    def __exit__(self, *args, **kwargs) -> None:
        _profiler_controller_exit(self.id)

    def step(self) -> None:
        _profiler_controller_step(self.id)


@dataclass
class _Profiler:
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    profiler: Optional[torch.profiler.profile] = None


_profilers: Dict[int, _Profiler] = {}


def _profiler_init(ident, *args, **kwargs) -> None:
    global _profilers
    assert ident not in _profilers, (
        f"Initializing an already existing profiler, {ident=}"
    )
    _profilers[ident] = _Profiler(args, kwargs)
    # It's unclear why we cannot create the profiler here. Even though
    # the thread is the same, profiler complains thread id mismatch.


def _profiler_enter(ident, *args, **kwargs) -> None:
    def on_trace_ready(prof, dir_path):
        dir_path = Path(dir_path).absolute()
        os.makedirs(dir_path, exist_ok=True)
        # This is not a synchronized call, so it is okay to call without
        # device mesh.
        rank = torch.distributed.get_rank() if torch.distributed.is_initialized() else 0
        prof.export_chrome_trace(f"{dir_path}/trace_{rank}.json")

    profiler = _profilers[ident]
    profiler.kwargs[profile.PATH_KEY] = partial(
        on_trace_ready, dir_path=profiler.kwargs[profile.PATH_KEY]
    )
    schedule = profiler.kwargs.get("schedule", None)
    if schedule is not None:
        profiler.kwargs["schedule"] = torch.profiler.schedule(**schedule._asdict())
    profiler.profiler = torch.profiler.profile(*profiler.args, **profiler.kwargs)

    profiler.profiler.__enter__()


def _profiler_exit(ident, *args, **kwargs) -> None:
    profiler = _profilers[ident].profiler
    assert profiler is not None
    profiler.__exit__(None, None, None)
    _profilers.pop(ident)


def _profiler_step(ident, *args, **kwargs) -> None:
    profiler = _profilers[ident].profiler
    assert profiler is not None
    profiler.step()


_profiler_controller_init = remote(
    "monarch.profiler._profiler_init", propagate="inspect"
)

_profiler_controller_enter = remote(
    "monarch.profiler._profiler_enter", propagate="inspect"
)

_profiler_controller_exit = remote(
    "monarch.profiler._profiler_exit", propagate="inspect"
)

_profiler_controller_step = remote(
    "monarch.profiler._profiler_step", propagate="inspect"
)


class record_function(ControllerRemoteClass):
    """
    The class wraps `torch.profiler.record_function()` to allow invoking the
    record_function remotely.
    """

    def __init__(self, name: str, args: Optional[str] = None) -> None:
        super().__init__("monarch.profiler.WorkerRecordFunction", name, args)

    @ControllerRemoteClass.remote_method
    def __enter__(self) -> "record_function":
        return self

    @ControllerRemoteClass.remote_method
    def __exit__(self, *args, **kwargs) -> None:
        return


class WorkerRecordFunction(WorkerRemoteClass):
    def __init__(self, *args, **kwargs) -> None:
        self._record_function = torch.profiler.record_function(*args, **kwargs)

    def __enter__(self) -> None:
        self._record_function.__enter__()

    def __exit__(self, *args, **kwargs) -> None:
        self._record_function.__exit__(*args, **kwargs)
