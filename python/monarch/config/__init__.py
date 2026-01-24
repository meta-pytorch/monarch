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
from typing import Any, Callable, Dict, Iterator

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import (
    clear_runtime_config as _clear_runtime_config,
    configure as _configure,
    Encoding,
    get_global_config as _get_global_config,
    get_runtime_config as _get_runtime_config,
)


__all__ = [
    "clear_runtime_config",
    "configure",
    "configured",
    "Encoding",
    "get_global_config",
    "get_runtime_config",
    "parametrize_config",
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
    process_exit_timeout: str | None = None,
    message_ack_time_interval: str | None = None,
    message_ack_every_n_messages: int | None = None,
    message_ttl_default: int | None = None,
    split_max_buffer_size: int | None = None,
    split_max_buffer_age: str | None = None,
    stop_actor_timeout: str | None = None,
    cleanup_timeout: str | None = None,
    remote_allocator_heartbeat_interval: str | None = None,
    default_encoding: Encoding | None = None,
    channel_net_rx_buffer_full_check_interval: str | None = None,
    message_latency_sampling_rate: float | None = None,
    enable_dest_actor_reordering_buffer: bool | None = None,
    mesh_bootstrap_enable_pdeathsig: bool | None = None,
    mesh_terminate_concurrency: int | None = None,
    mesh_terminate_timeout: str | None = None,
    shared_asyncio_runtime: bool | None = None,
    small_write_threshold: int | None = None,
    max_cast_dimension_size: int | None = None,
    remote_alloc_bind_to_inaddr_any: bool | None = None,
    remote_alloc_bootstrap_addr: str | None = None,
    remote_alloc_allowed_port_range: slice | None = None,
    read_log_buffer: int | None = None,
    force_file_log: bool | None = None,
    prefix_with_rank: bool | None = None,
    actor_spawn_max_idle: str | None = None,
    get_actor_state_max_idle: str | None = None,
    supervision_watchdog_timeout: str | None = None,
    proc_stop_max_idle: str | None = None,
    get_proc_state_max_idle: str | None = None,
    actor_queue_dispatch: bool | None = None,
    **kwargs: object,
) -> None:
    """Configure Hyperactor runtime defaults for this process.

    This updates the **Runtime** configuration layer from Python, setting
    transports, logging behavior, timeouts, and other runtime parameters.

    All duration parameters accept humantime strings like ``"30s"``, ``"5m"``,
    ``"2h"``, or ``"1h 30m"``.

    Args:
        Transport configuration:
            default_transport: Default channel transport for actor communication.
                Can be a ChannelTransport enum or explicit address string.

        Basic logging behavior:
            enable_log_forwarding: Forward child stdout/stderr through the mesh.
            enable_file_capture: Persist child stdout/stderr to per-host files.
            tail_log_lines: Number of log lines to retain in memory.

        Message encoding and delivery:
            codec_max_frame_length: Maximum serialized message size in bytes.
            message_delivery_timeout: Max delivery time (humantime).

        Core mesh timeouts:
            host_spawn_ready_timeout: Max host bootstrapping time (humantime).
            mesh_proc_spawn_max_idle: Max idle time while spawning procs (humantime).

        Hyperactor timeouts and message handling:
            process_exit_timeout: Timeout for process exit (humantime).
            message_ack_time_interval: Time interval for message acknowledgments (humantime).
            message_ack_every_n_messages: Acknowledge every N messages.
            message_ttl_default: Default message time-to-live.
            split_max_buffer_size: Maximum buffer size for message splitting (bytes).
            split_max_buffer_age: Maximum age for split message buffers (humantime).
            stop_actor_timeout: Timeout for stopping actors (humantime).
            cleanup_timeout: Timeout for cleanup operations (humantime).
            remote_allocator_heartbeat_interval: Heartbeat interval for remote allocator (humantime).
            default_encoding: Default message encoding (Encoding.Bincode, Encoding.Json, or Encoding.Multipart).
            channel_net_rx_buffer_full_check_interval: Network receive buffer check interval (humantime).
            message_latency_sampling_rate: Sampling rate for message latency tracking (0.0 to 1.0).
            enable_dest_actor_reordering_buffer: Enable reordering buffer in dest actor.

        Mesh bootstrap configuration:
            mesh_bootstrap_enable_pdeathsig: Enable parent-death signal for spawned processes.
            mesh_terminate_concurrency: Maximum concurrent terminations during shutdown.
            mesh_terminate_timeout: Timeout per child during graceful termination (humantime).

        Runtime and buffering:
            shared_asyncio_runtime: Share asyncio runtime across actors.
            small_write_threshold: Threshold below which writes are copied (bytes).

        Mesh configuration:
            max_cast_dimension_size: Maximum dimension size for cast operations.

        Remote allocation:
            remote_alloc_bind_to_inaddr_any: Bind remote allocators to INADDR_ANY.
            remote_alloc_bootstrap_addr: Bootstrap address for remote allocators.
            remote_alloc_allowed_port_range: Allowed port range as slice(start, stop).

        Logging configuration:
            read_log_buffer: Buffer size for reading logs (bytes).
            force_file_log: Force file-based logging regardless of environment.
            prefix_with_rank: Prefix log lines with rank information.

        Proc mesh timeouts:
            actor_spawn_max_idle: Maximum idle time while spawning actors (humantime).
            get_actor_state_max_idle: Maximum idle time for actor state queries (humantime).
            supervision_watchdog_timeout: Watchdog timeout for the actor-mesh supervision stream; prolonged
                silence is interpreted as the controller being unreachable (humantime).

        Host mesh timeouts:
            proc_stop_max_idle: Maximum idle time while stopping procs (humantime).
            get_proc_state_max_idle: Maximum idle time for proc state queries (humantime).

        **kwargs: Reserved for future configuration keys exposed by Rust bindings.
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
    if process_exit_timeout is not None:
        params["process_exit_timeout"] = process_exit_timeout
    if message_ack_time_interval is not None:
        params["message_ack_time_interval"] = message_ack_time_interval
    if message_ack_every_n_messages is not None:
        params["message_ack_every_n_messages"] = message_ack_every_n_messages
    if message_ttl_default is not None:
        params["message_ttl_default"] = message_ttl_default
    if split_max_buffer_size is not None:
        params["split_max_buffer_size"] = split_max_buffer_size
    if split_max_buffer_age is not None:
        params["split_max_buffer_age"] = split_max_buffer_age
    if stop_actor_timeout is not None:
        params["stop_actor_timeout"] = stop_actor_timeout
    if cleanup_timeout is not None:
        params["cleanup_timeout"] = cleanup_timeout
    if remote_allocator_heartbeat_interval is not None:
        params["remote_allocator_heartbeat_interval"] = (
            remote_allocator_heartbeat_interval
        )
    if default_encoding is not None:
        params["default_encoding"] = default_encoding
    if channel_net_rx_buffer_full_check_interval is not None:
        params["channel_net_rx_buffer_full_check_interval"] = (
            channel_net_rx_buffer_full_check_interval
        )
    if message_latency_sampling_rate is not None:
        params["message_latency_sampling_rate"] = message_latency_sampling_rate
    if enable_dest_actor_reordering_buffer is not None:
        params["enable_dest_actor_reordering_buffer"] = (
            enable_dest_actor_reordering_buffer
        )
    if mesh_bootstrap_enable_pdeathsig is not None:
        params["mesh_bootstrap_enable_pdeathsig"] = mesh_bootstrap_enable_pdeathsig
    if mesh_terminate_concurrency is not None:
        params["mesh_terminate_concurrency"] = mesh_terminate_concurrency
    if mesh_terminate_timeout is not None:
        params["mesh_terminate_timeout"] = mesh_terminate_timeout
    if shared_asyncio_runtime is not None:
        params["shared_asyncio_runtime"] = shared_asyncio_runtime
    if small_write_threshold is not None:
        params["small_write_threshold"] = small_write_threshold
    if max_cast_dimension_size is not None:
        params["max_cast_dimension_size"] = max_cast_dimension_size
    # Forward new alloc config keys
    if remote_alloc_bind_to_inaddr_any is not None:
        params["remote_alloc_bind_to_inaddr_any"] = remote_alloc_bind_to_inaddr_any
    if remote_alloc_bootstrap_addr is not None:
        params["remote_alloc_bootstrap_addr"] = remote_alloc_bootstrap_addr
    if remote_alloc_allowed_port_range is not None:
        params["remote_alloc_allowed_port_range"] = remote_alloc_allowed_port_range
    if read_log_buffer is not None:
        params["read_log_buffer"] = read_log_buffer
    if force_file_log is not None:
        params["force_file_log"] = force_file_log
    if prefix_with_rank is not None:
        params["prefix_with_rank"] = prefix_with_rank
    if actor_spawn_max_idle is not None:
        params["actor_spawn_max_idle"] = actor_spawn_max_idle
    if get_actor_state_max_idle is not None:
        params["get_actor_state_max_idle"] = get_actor_state_max_idle
    if supervision_watchdog_timeout is not None:
        params["supervision_watchdog_timeout"] = supervision_watchdog_timeout
    if proc_stop_max_idle is not None:
        params["proc_stop_max_idle"] = proc_stop_max_idle
    if get_proc_state_max_idle is not None:
        params["get_proc_state_max_idle"] = get_proc_state_max_idle
    if actor_queue_dispatch is not None:
        params["actor_queue_dispatch"] = actor_queue_dispatch

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
    will be subject to the overridden configuration while the context
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


def parametrize_config(
    **config_options: set[Any],
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Create a pytest parametrize decorator for configuration cross-products.

    This decorator runs the test function under every combination of the
    specified configuration values. Each test invocation wraps the test body
    in a `configured(...)` context manager with the corresponding settings.

    Args:
        **config_options: Configuration keys mapped to sets of values to test.
            Each key should be a valid argument to `configure()`.

    Returns:
        A decorator that parametrizes and wraps the test function.

    Example:
        >>> from monarch.config import parametrize_config
        >>>
        >>> @parametrize_config(
        ...     actor_queue_dispatch={True, False},
        ...     shared_asyncio_runtime={True, False},
        ... )
        ... async def test_actor_feature():
        ...     # Test runs 4 times: all combinations of the two bool options
        ...     pass
    """
    import asyncio
    import functools
    import inspect
    import itertools

    import pytest  # pyre-ignore[21]: pytest is a test-only dependency

    if not config_options:
        raise ValueError("parametrize_config requires at least one config option")

    keys = list(config_options.keys())
    value_lists = [list(config_options[k]) for k in keys]
    combinations = list(itertools.product(*value_lists))

    # Create parameter IDs for clearer test output
    param_ids = [
        "-".join(f"{k}={v}" for k, v in zip(keys, combo)) for combo in combinations
    ]

    # Create the config dicts for each combination
    config_dicts = [dict(zip(keys, combo)) for combo in combinations]

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        # Get original function's signature and add _config_overrides as first param
        orig_sig = inspect.signature(fn)
        new_params = [
            inspect.Parameter(
                "_config_overrides", inspect.Parameter.POSITIONAL_OR_KEYWORD
            )
        ] + list(orig_sig.parameters.values())
        new_sig = orig_sig.replace(parameters=new_params)

        if asyncio.iscoroutinefunction(fn):

            async def async_wrapper(
                _config_overrides: Dict[str, Any], *args: Any, **kwargs: Any
            ) -> Any:
                with configured(**_config_overrides):
                    return await fn(*args, **kwargs)

            functools.update_wrapper(async_wrapper, fn)
            async_wrapper.__signature__ = new_sig  # type: ignore[attr-defined]
            wrapped = async_wrapper
        else:

            def sync_wrapper(
                _config_overrides: Dict[str, Any], *args: Any, **kwargs: Any
            ) -> Any:
                with configured(**_config_overrides):
                    return fn(*args, **kwargs)

            functools.update_wrapper(sync_wrapper, fn)
            sync_wrapper.__signature__ = new_sig  # type: ignore[attr-defined]
            wrapped = sync_wrapper

        return pytest.mark.parametrize(
            "_config_overrides", config_dicts, ids=param_ids
        )(wrapped)

    return decorator
