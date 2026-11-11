# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging

from monarch._rust_bindings.monarch_hyperactor.proc import ActorAddr

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
    def __init__(
        self,
        name: str,
        actor_id: ActorAddr | None = None,
        correlation_id: int | None = None,
    ) -> None:
        """
        Create a new PySpan.

        Args:
        - name (str): The name of the span.
        - actor_id (ActorAddr | None, optional): The actor whose Perfetto track should own the span.
        - correlation_id (int | None, optional): The correlation ID for RPC flow tracing.
        """
        ...

    def __enter__(self) -> PySpan: ...
    def __exit__(
        self,
        exc_type: object | None = None,
        exc_value: object | None = None,
        traceback: object | None = None,
    ) -> bool: ...

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

class PyGauge:
    def __init__(self, name: str, unit: str = "", description: str = "") -> None: ...
    def set(
        self, value: float, *, attributes: dict[str, str] | None = None
    ) -> None: ...

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
