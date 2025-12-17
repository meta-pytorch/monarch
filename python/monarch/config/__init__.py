# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Configuration utilities for Monarch.

This module provides utilities for managing Monarch's runtime
configuration, particularly useful for testing and temporary
configuration overrides.
"""

import contextlib
from typing import Any, Dict, Iterator

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import (
    clear_runtime_config as _clear_runtime_config,
    configure as _configure,
    get_global_config as _get_global_config,
    get_runtime_config as _get_runtime_config,
)


__all__ = [
    "clear_runtime_config",
    "configure",
    "configured",
    "get_global_config",
    "get_runtime_config",
]


def configure(
    *,
    default_transport: ChannelTransport | None = None,
    enable_log_forwarding: bool | None = None,
    enable_file_capture: bool | None = None,
    tail_log_lines: int | None = None,
    codec_max_frame_length: int | None = None,
    message_delivery_timeout: str | None = None,
    host_spawn_ready_timeout: str | None = None,
    mesh_proc_spawn_max_idle: str | None = None,
    **kwargs: object,
) -> None:
    """Configure Hyperactor runtime defaults for this process.

    This updates the **Runtime** configuration layer from Python, setting
    transports, logging behavior, and any other CONFIG-marked keys supplied
    via ``**kwargs``. Duration values should be humantime strings (``"30s"``,
    ``"5m"``, ``"1h 30m"``).

    Args:
        default_transport: Default channel transport for actor communication.
        enable_log_forwarding: Forward child stdout/stderr through the mesh.
        enable_file_capture: Persist child stdout/stderr to per-host files.
        tail_log_lines: Number of log lines to retain in memory.
        codec_max_frame_length: Maximum serialized message size in bytes.
        message_delivery_timeout: Max delivery time (humantime string).
        host_spawn_ready_timeout: Max host bootstrapping time (humantime).
        mesh_proc_spawn_max_idle: Max idle time while spawning procs.
        **kwargs: Additional configuration keys exposed by rust bindings.
    """

    params: Dict[str, Any] = dict(kwargs)
    if default_transport is not None:
        params["default_transport"] = default_transport
    if enable_log_forwarding is not None:
        params["enable_log_forwarding"] = enable_log_forwarding
    if enable_file_capture is not None:
        params["enable_file_capture"] = enable_file_capture
    if tail_log_lines is not None:
        params["tail_log_lines"] = tail_log_lines
    if codec_max_frame_length is not None:
        params["codec_max_frame_length"] = codec_max_frame_length
    if message_delivery_timeout is not None:
        params["message_delivery_timeout"] = message_delivery_timeout
    if host_spawn_ready_timeout is not None:
        params["host_spawn_ready_timeout"] = host_spawn_ready_timeout
    if mesh_proc_spawn_max_idle is not None:
        params["mesh_proc_spawn_max_idle"] = mesh_proc_spawn_max_idle

    _configure(**params)


def get_global_config() -> Dict[str, Any]:
    """Return a merged view of all configuration layers.

    The resulting dict includes defaults, environment overrides, file-based
    settings, and the current Runtime layer. Mutating the returned dict does
    *not* change the active configuration; use :func:`configure` instead.
    """

    return _get_global_config()


def get_runtime_config() -> Dict[str, Any]:
    """Return a snapshot of just the Runtime layer configuration.

    Useful for snapshot/restore flows (see :func:`configured`) or for
    inspecting which keys were last set via Python.
    """

    return _get_runtime_config()


def clear_runtime_config() -> None:
    """Remove every key from the Runtime configuration layer.

    Environment variables, config files, and defaults are untouched. This is
    typically paired with :func:`configure` to reset overrides in long-lived
    processes.
    """

    _clear_runtime_config()


@contextlib.contextmanager
def configured(**overrides) -> Iterator[Dict[str, Any]]:
    """Temporarily apply Python-side config overrides for this
    process.

    This context manager:
      * snapshots the current **Runtime** configuration layer
        (`get_runtime_config()`),
      * applies the given `overrides` via `configure(**overrides)`,
        and
      * yields the **merged** view of config (`get_global_config()`),
        including defaults, env, file, and Runtime.

    On exit it restores the previous Runtime layer by:
      * clearing all Runtime entries, and
      * re-applying the saved snapshot.

    `configured` alters the global configuration; thus other threads
    will be subject to the overriden configuration while the context
    manager is active.

    Thus: this is intended for tests, which run as single threads;
    per-test overrides do not leak into other tests.

    Args:
        **overrides: Configuration key-value pairs to override for the
            duration of the context.

    Yields:
        Dict[str, Any]: The merged global configuration including all
            layers (defaults, environment, file, and runtime).

    Example:
        >>> from monarch.config import configured
        >>> with configured(enable_log_forwarding=True, tail_log_lines=100):
        ...     # Configuration is temporarily overridden
        ...     assert get_global_config()["enable_log_forwarding"] is True
        >>> # Configuration is automatically restored after the context

    """
    # Retrieve runtime
    prev = get_runtime_config()
    try:
        # Merge overrides into runtime
        configure(**overrides)

        # Snapshot of merged config (all layers)
        yield get_global_config()
    finally:
        # Restore previous runtime
        clear_runtime_config()
        configure(**prev)
