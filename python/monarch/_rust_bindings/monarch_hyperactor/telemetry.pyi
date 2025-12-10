# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging

def forward_to_tracing(record: logging.LogRecord) -> None:
    """
    Log a message with the given metadata using the tracing system.

    This function forwards Python log messages to the Rust tracing system,
    preserving the original source location and log level.

    Args:
    - record (logging.LogRecord): The log record containing message, file, lineno, and level information.
      The function extracts:
        - message: The log message content via record.getMessage()
        - file: The filename via record.filename
        - lineno: The line number via record.lineno
        - level: The log level via record.levelno:
            - 10: DEBUG
            - 20: INFO
            - 30: WARN
            - 40: ERROR
            - other values default to INFO
    """
    ...

def get_current_span_id() -> int:
    """
    Get the current span ID from the active span.

    Returns the span ID of the current active span. If no span is active,
    returns 0 (invalid span ID).

    Returns:
    - int: The span ID as an integer.
    """
    ...

def use_real_clock() -> None:
    """
    Convenience function to switch to real-time clock.
    This switches the telemetry system to use real system time.
    """
    ...

def use_sim_clock() -> None:
    """
    Convenience function to switch to simulated clock.

    This switches the telemetry system to use simulated time, which is useful for
    testing and simulation environments where you want deterministic time control.
    """
    ...

def get_execution_id() -> str:
    """
    Get the current execution ID.

    Returns the execution ID that is set by the telemetry system. This ID is either:
    - Set from the HYPERACTOR_EXECUTION_ID environment variable
    - Set from the MAST_HPC_JOB_NAME environment variable (in MAST environments)
    - Generated as a random string if neither environment variable is available

    Returns:
        str: The current execution ID
    """
    ...

def instant_event(message: str) -> None:
    """
    Emit an instant event. This is a binding for tracing::info!(message)
    """
    ...

class PySpan:
    def __init__(self, name: str, actor_id: str | None = None) -> None:
        """
        Create a new PySpan.

        Args:
        - name (str): The name of the span.
        - actor_id (str | None, optional): The actor ID associated with the span.
          If None, Rust will handle actor identification automatically.
        """
        ...

    def exit(self) -> None:
        """
        Exit the span.
        """
        ...

class PyCounter:
    def __init__(self, name: str) -> None:
        """
        Create a new PyCounter.

        Args:
        - name (str): The name of the counter metric.
        """
        ...

    def add(self, value: int, *, attributes: dict[str, str] | None = None) -> None:
        """
        Add a value to the counter.

        Args:
        - value (int): The value to add to the counter (must be non-negative).
        - attributes (dict[str, str] | None): Optional attributes to associate with the measurement.
        """
        ...

class PyHistogram:
    def __init__(self, name: str) -> None:
        """
        Create a new PyHistogram.

        Args:
        - name (str): The name of the histogram metric.
        """
        ...

    def record(self, value: float, *, attributes: dict[str, str] | None = None) -> None:
        """
        Record a value in the histogram.

        Args:
        - value (float): The value to record in the histogram.
        - attributes (dict[str, str] | None): Optional attributes to associate with the measurement.
        """
        ...

class PyUpDownCounter:
    def __init__(self, name: str) -> None:
        """
        Create a new PyUpDownCounter.

        Args:
        - name (str): The name of the up-down counter metric.
        """
        ...

    def add(self, value: int, *, attributes: dict[str, str] | None = None) -> None:
        """
        Add a value to the up-down counter.

        Args:
        - value (int): The value to add to the counter (can be positive or negative).
        - attributes (dict[str, str] | None): Optional attributes to associate with the measurement.
        """
        ...

class PySqliteTracing:
    def __init__(self, in_memory: bool = False) -> None:
        """
        Create a new PySqliteTracing.

        This creates an RAII guard that sets up SQLite tracing collection.
        When used as a context manager, it will automatically clean up when exiting.

        Args:
        - in_memory (bool): If True, uses an in-memory database. If False, creates a temporary file.
        """
        ...

    def db_path(self) -> str | None:
        """
        Get the path to the database file.

        Returns:
        - str | None: The path to the database file, or None if using in-memory database.

        Raises:
        - RuntimeError: If the guard has been closed.
        """
        ...

    def close(self) -> None:
        """
        Manually close the guard and clean up resources.

        After calling this method, the guard cannot be used anymore.
        """
        ...

    def __enter__(self) -> "PySqliteTracing":
        """
        Enter the context manager.

        Returns:
        - PySqliteTracing: Self for use in the with statement.
        """
        ...

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool:
        """
        Exit the context manager and clean up resources.

        Args:
        - exc_type: Exception type (if any)
        - exc_value: Exception value (if any)
        - traceback: Exception traceback (if any)

        Returns:
        - bool: False (does not suppress exceptions)
        """
        ...
