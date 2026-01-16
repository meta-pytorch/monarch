# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""A simple timer that utilizes CUDA events to measure time spent in GPU kernels."""

# pyre-strict
import logging
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional, Tuple

import torch


class ExecutionTimer:
    """
    A lightweight timer for measuring CPU or GPU execution time.
    """

    _enable_cuda: bool = torch.cuda.is_available()
    _times: Dict[str, List[float]] = {}
    _lock = threading.Lock()
    _threads: Dict[str, List[threading.Thread]] = {}
    _events: Dict[
        str, List[Tuple[torch.cuda.Event, torch.cuda.Event, torch.cuda.Stream]]
    ] = {}
    _cuda_warning_shown: bool = False
    _cpu_start_times: Dict[str, List[float]] = {}

    @classmethod
    @contextmanager
    # pyre-fixme[3]: Return type must be specified as type that does not contain `Any`.
    def time(
        cls, name: Optional[str] = None, use_cpu: bool = False
    ) -> Generator[None, Any, Any]:
        """
        Context manager for timing an operation.
        Args:
            name (str): Name of the timing section
            use_cpu (bool): Whether to use CPU time instead of CUDA time. Defaults to false.
        Example:
            with ExecutionTimer.time("matrix_multiply"):
                result = torch.matmul(a, b)

            with ExecutionTimer.time("sleep", use_cpu=True):
                time.sleep(1)
        """
        cls.start(name, use_cpu)
        try:
            yield
        finally:
            cls.stop(name, use_cpu)

    @classmethod
    def start(cls, name: Optional[str] = None, use_cpu: bool = False) -> None:
        if not cls._enable_cuda and cls._cuda_warning_shown:
            logging.warning("CUDA not available, falling back to CPU timing")
            cls._cuda_warning_shown = True

        if not name:
            name = "default"
        if name not in cls._times:
            cls._times[name] = []

        if not cls._enable_cuda or use_cpu:
            if name not in cls._cpu_start_times:
                cls._cpu_start_times[name] = []
            cls._cpu_start_times[name].append(time.perf_counter())
        else:
            stream = torch.cuda.current_stream()
            start_event = torch.cuda.Event(enable_timing=True)
            end_event = torch.cuda.Event(enable_timing=True)
            start_event.record(stream)
            if name not in cls._events:
                cls._events[name] = []
            cls._events[name].append((start_event, end_event, stream))

    @classmethod
    def stop(cls, name: Optional[str] = None, use_cpu: bool = False) -> None:
        if not name:
            name = "default"

        if not cls._enable_cuda or use_cpu:
            assert name in cls._cpu_start_times, (
                f"No CPU start time found for {name}, did you run start()?"
            )
            start_time = cls._cpu_start_times[name].pop()
            elapsed_time_ms = (time.perf_counter() - start_time) * 1000
            with cls._lock:
                cls._times[name].append(elapsed_time_ms)

        if name in cls._events and cls._events[name]:
            start_event, end_event, stream = cls._events[name].pop()
            end_event.record(stream)

            # We create a separate thread to poll on the event status
            # to avoid blocking the main thread.
            thread = threading.Thread(
                target=cls._check_event_completion, args=(name, start_event, end_event)
            )
            thread.start()
            if name not in cls._threads:
                cls._threads[name] = []
            cls._threads[name].append(thread)

    @classmethod
    def _check_event_completion(
        cls, name: str, start_event: torch.cuda.Event, end_event: torch.cuda.Event
    ) -> None:
        while True:
            if end_event.query():
                with cls._lock:
                    cuda_time = start_event.elapsed_time(end_event)
                    cls._times[name].append(cuda_time)
                break
            time.sleep(0.01)

    @classmethod
    def reset(cls) -> None:
        """Clear all timing data."""
        with cls._lock:
            cls._times = {}
            cls._threads = {}

    @classmethod
    def summary(cls) -> Dict[str, Dict[str, float]]:
        """
        Get summary of all timing data.
        Returns:
            Dict containing timing statistics for each section
        """
        # Wait for all in-flight measurements to complete
        for _, threads in cls._threads.items():
            for thread in threads:
                thread.join()
        with cls._lock:
            result = {}
            for name, times in cls._times.items():
                if not times:
                    continue
                result[name] = {
                    "count": len(times),
                    "mean_ms": sum(times) / len(times),
                    "total_ms": sum(times),
                    "min_ms": min(times),
                    "max_ms": max(times),
                }
            return result

    @classmethod
    def get_latest_measurement(cls, name: Optional[str] = None) -> float:
        """Get the latest measurement (in ms) for a given section."""
        if not name:
            name = "default"
        if name in cls._threads:
            for thread in cls._threads[name]:
                thread.join()
            cls._threads[name] = []
        with cls._lock:
            if name not in cls._times or not cls._times[name]:
                logging.warning(f"Section {name} not found in timing data.")
                return 0.0
            return cls._times[name][-1]


def execution_timer_start(name: Optional[str] = None, use_cpu: bool = False) -> None:
    """Start the ExecutionTimer."""
    ExecutionTimer.start(name=name, use_cpu=use_cpu)


def execution_timer_stop(name: Optional[str] = None, use_cpu: bool = False) -> None:
    """Stop the ExecutionTimer."""
    ExecutionTimer.stop(name=name, use_cpu=use_cpu)


def get_execution_timer_average_ms(name: str = "default") -> torch.Tensor:
    """Get the ExecutionTimer results."""
    return torch.tensor(ExecutionTimer.summary()[name]["mean_ms"], dtype=torch.float64)


def get_latest_timer_measurement(name: Optional[str] = None) -> torch.Tensor:
    """Get the latest ExecutionTimer results."""
    return torch.tensor(
        ExecutionTimer.get_latest_measurement(name), dtype=torch.float64
    )


def get_execution_timer_summary() -> Dict[str, Dict[str, float]]:
    """Get the ExecutionTimer summary."""
    return ExecutionTimer.summary()


def get_time_perfcounter() -> torch.Tensor:
    """Get the time performance counter. Should be used only for debugging."""
    return torch.tensor(time.perf_counter(), dtype=torch.float64)
