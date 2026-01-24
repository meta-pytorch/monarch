# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Type hints for the monarch_hyperactor.config Rust bindings.
"""

from enum import Enum
from typing import Any, Dict

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport

class Encoding(Enum):
    """Message encoding format for serialization."""

    Bincode: int
    Json: int
    Multipart: int

def reload_config_from_env() -> None:
    """
    Reload configuration from environment variables.

    This reads all HYPERACTOR_* environment variables and updates
    the global configuration.
    For any configuration setting not present in environment variables,
    this function will not change its value.
    """
    ...

def reset_config_to_defaults() -> None:
    """Reset all configuration to default values, ignoring environment variables.
    Call reload_config_from_env() to reload the environment variables.
    """
    ...

def configure(
    default_transport: ChannelTransport | str = ...,
    enable_log_forwarding: bool = ...,
    enable_file_capture: bool = ...,
    tail_log_lines: int = ...,
    codec_max_frame_length: int = ...,
    message_delivery_timeout: str = ...,
    host_spawn_ready_timeout: str = ...,
    mesh_proc_spawn_max_idle: str = ...,
    process_exit_timeout: str = ...,
    message_ack_time_interval: str = ...,
    message_ack_every_n_messages: int = ...,
    message_ttl_default: int = ...,
    split_max_buffer_size: int = ...,
    split_max_buffer_age: str = ...,
    stop_actor_timeout: str = ...,
    cleanup_timeout: str = ...,
    remote_allocator_heartbeat_interval: str = ...,
    default_encoding: Encoding = ...,
    channel_net_rx_buffer_full_check_interval: str = ...,
    message_latency_sampling_rate: float = ...,
    enable_dest_actor_reordering_buffer: bool = ...,
    mesh_bootstrap_enable_pdeathsig: bool = ...,
    mesh_terminate_concurrency: int = ...,
    mesh_terminate_timeout: str = ...,
    shared_asyncio_runtime: bool = ...,
    small_write_threshold: int = ...,
    max_cast_dimension_size: int = ...,
    remote_alloc_bind_to_inaddr_any: bool = ...,
    remote_alloc_bootstrap_addr: str = ...,
    remote_alloc_allowed_port_range: slice = ...,
    read_log_buffer: int = ...,
    force_file_log: bool = ...,
    prefix_with_rank: bool = ...,
    actor_spawn_max_idle: str = ...,
    get_actor_state_max_idle: str = ...,
    supervision_watchdog_timeout: str = ...,
    proc_stop_max_idle: str = ...,
    get_proc_state_max_idle: str = ...,
    actor_queue_dispatch: bool = ...,
    **kwargs: object,
) -> None:
    """Configure Hyperactor runtime defaults for this process.

    This updates the **Runtime** configuration layer from Python,
    setting transports, logging behavior, timeouts, and other runtime
    parameters.

    All duration parameters accept humantime strings like "30s", "5m",
    "2h", or "1h 30m".

    For complete parameter documentation, see the Python wrapper
    `monarch.config.configure()` which provides the same interface
    with detailed descriptions of all 37 configuration parameters
    organized into logical categories (transport, logging, message
    handling, mesh bootstrap, allocation, proc/host mesh timeouts,
    etc.).

    Args:
        default_transport: Default channel transport. Can be
            ChannelTransport enum (e.g., ChannelTransport.Unix) or
            explicit address string in ZMQ-style URL format (e.g.,
            "tcp://127.0.0.1:8080")
        enable_log_forwarding: Forward logs from actors
        enable_file_capture: Capture file output
        tail_log_lines: Number of log lines to tail
        codec_max_frame_length: Maximum frame length for codec (bytes)
        message_delivery_timeout: Timeout for message delivery
            (humantime)
        host_spawn_ready_timeout: Timeout for host spawn readiness
            (humantime)
        mesh_proc_spawn_max_idle: Maximum idle time for spawning procs
            (humantime)
        process_exit_timeout: Timeout for process exit (humantime)
        message_ack_time_interval: Time interval for message
            acknowledgments (humantime)
        message_ack_every_n_messages: Acknowledge every N messages
        message_ttl_default: Default message time-to-live
        split_max_buffer_size: Maximum buffer size for message splitting
            (bytes)
        split_max_buffer_age: Maximum age for split message buffers
            (humantime)
        stop_actor_timeout: Timeout for stopping actors (humantime)
        cleanup_timeout: Timeout for cleanup operations (humantime)
        remote_allocator_heartbeat_interval: Heartbeat interval for
            remote allocator (humantime)
        default_encoding: Default message encoding (Encoding.Bincode,
            Encoding.Json, or Encoding.Multipart)
        channel_net_rx_buffer_full_check_interval: Network receive buffer
            check interval (humantime)
        message_latency_sampling_rate: Sampling rate for message latency
            (0.0 to 1.0)
        enable_dest_actor_reordering_buffer: Enable client-side sequence
            assignment
        mesh_bootstrap_enable_pdeathsig: Enable parent-death signal for
            spawned processes
        mesh_terminate_concurrency: Maximum concurrent terminations
            during shutdown
        mesh_terminate_timeout: Timeout per child during graceful
            termination (humantime)
        shared_asyncio_runtime: Share asyncio runtime across actors
        small_write_threshold: Threshold below which writes are copied
            (bytes)
        max_cast_dimension_size: Maximum dimension size for cast
            operations
        remote_alloc_bind_to_inaddr_any: Bind remote allocators to
            INADDR_ANY
        remote_alloc_bootstrap_addr: Bootstrap address for remote
            allocators
        remote_alloc_allowed_port_range: Allowed port range as
            slice(start, stop)
        read_log_buffer: Buffer size for reading logs (bytes)
        force_file_log: Force file-based logging regardless of
            environment
        prefix_with_rank: Prefix log lines with rank information
        actor_spawn_max_idle: Maximum idle time while spawning actors
            (humantime)
        get_actor_state_max_idle: Maximum idle time for actor state
            queries (humantime)
        supervision_watchdog_timeout: Liveness timeout for the
            actor-mesh supervision stream; prolonged silence is
            interpreted as the controller being unreachable
            (humantime)
        proc_stop_max_idle: Maximum idle time while stopping procs
            (humantime)
        get_proc_state_max_idle: Maximum idle time for proc state queries
            (humantime)
        **kwargs: Reserved for future configuration keys

    For historical reasons, this API is named ``configure(...)``;
    conceptually it acts as "set runtime config for this process".

    """
    ...

def get_global_config() -> Dict[str, Any]:
    """Return a snapshot of the current Hyperactor configuration.

    The result is a plain dictionary view of the merged configuration
    (defaults plus any overrides from environment or Python), useful
    for debugging and tests.
    """
    ...

def get_runtime_config() -> Dict[str, Any]:
    """Return a snapshot of the Runtime layer configuration.

    The Runtime layer contains only configuration values set from
    Python via configure(). This returns only those Python-exposed
    keys currently in the Runtime layer (not merged across all layers
    like `get_global_config()`).

    This can be used to snapshot/restore Runtime state.
    """
    ...

def clear_runtime_config() -> None:
    """Clear all Runtime layer configuration overrides.

    Safely removes all entries from the Runtime config layer. Since
    the Runtime layer is exclusively populated via Python's
    `configure()`, this will not affect configuration from environment
    variables, config files, or built-in defaults.
    """

    ...
