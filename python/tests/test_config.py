# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import contextlib

import monarch
import pytest
from monarch._rust_bindings.monarch_hyperactor.channel import BindSpec, ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionError
from monarch.actor import Actor, endpoint, this_host
from monarch.config import configured, get_global_config


@contextlib.contextmanager
def override_fault_hook(callback=None):
    original_hook = monarch.actor.unhandled_fault_hook
    try:
        monarch.actor.unhandled_fault_hook = callback or (lambda failure: None)
        yield
    finally:
        monarch.actor.unhandled_fault_hook = original_hook


def test_get_set_transport() -> None:
    for transport in (
        ChannelTransport.Unix,
        ChannelTransport.TcpWithLocalhost,
        ChannelTransport.TcpWithHostname,
        ChannelTransport.MetaTlsWithHostname,
    ):
        with configured(default_transport=transport) as config:
            assert config["default_transport"] == BindSpec(transport)
    with configured(default_transport="tcp") as config:
        assert config["default_transport"] == BindSpec(ChannelTransport.TcpWithHostname)
    # Succeed even if we don't specify the transport, but does not change the
    # previous value.
    with configured() as config:
        assert config["default_transport"] == BindSpec(ChannelTransport.Unix)
    with pytest.raises(TypeError):
        with configured(default_transport=42):  # type: ignore
            pass
    with pytest.raises(TypeError):
        with configured(default_transport={}):  # type: ignore
            pass


def test_get_set_explicit_transport() -> None:
    # Test explicit transport with a TCP address
    with configured(default_transport="tcp://127.0.0.1:8080") as config:
        assert config["default_transport"] == BindSpec("tcp://127.0.0.1:8080")

    # Test that invalid explicit transport strings raise an error
    with pytest.raises(ValueError):
        with configured(default_transport="invalid://scheme"):
            pass

    # Test that random strings (not ZMQ URL format) raise an error
    with pytest.raises(ValueError):
        with configured(default_transport="random_string"):
            pass


def test_nonexistent_config_key() -> None:
    with pytest.raises(ValueError):
        with configured(does_not_exist=42):  # type: ignore
            pass


def test_get_set_multiple() -> None:
    with configured(default_transport=ChannelTransport.TcpWithLocalhost):
        with configured(
            enable_log_forwarding=True, enable_file_capture=True, tail_log_lines=100
        ) as config:
            assert config["enable_log_forwarding"]
            assert config["enable_file_capture"]
            assert config["tail_log_lines"] == 100
            assert config["default_transport"] == BindSpec(
                ChannelTransport.TcpWithLocalhost
            )
    # Make sure the previous values are restored.
    config = get_global_config()
    assert not config["enable_log_forwarding"]
    assert not config["enable_file_capture"]
    assert config["tail_log_lines"] == 0
    assert config["default_transport"] == BindSpec(ChannelTransport.Unix)


# This test tries to allocate too much memory for the GitHub actions
# environment.
@pytest.mark.oss_skip
def test_codec_max_frame_length_exceeds_default() -> None:
    """Test that sending 10 chunks of 1GiB fails with default 10 GiB
    limit."""

    class Chunker(Actor):
        def __init__(self):
            self.chunks = []

        @endpoint
        def process_chunks(self, chunks):
            self.chunks = chunks
            return len(chunks)

    oneGiB = 1024 * 1024 * 1024
    tenGiB = 10 * oneGiB

    # Verify default is 10 GiB
    config = get_global_config()
    assert config["codec_max_frame_length"] == tenGiB

    with override_fault_hook():
        # Try to send 10 chunks of 1GiB each with default 10 GiB limit
        # This should fail due to serialization overhead.
        # Spawn in separate proc so messages are serialized via Unix
        # sockets
        proc = this_host().spawn_procs()

        # Create 10 chunks, 1GiB each (total 10GiB)
        chunks = [bytes(oneGiB) for _ in range(10)]

        # Spawn actor and send chunks - should fail with SupervisionError
        chunker = proc.spawn("chunker", Chunker)
        with pytest.raises(SupervisionError):
            chunker.process_chunks.call_one(chunks).get()


# This test tries to allocate too much memory for the GitHub actions
# environment.
@pytest.mark.oss_skip
def test_codec_max_frame_length_with_increased_limit() -> None:
    """Test that we can successfully send 10 chunks of 1GiB each with
    100 GiB limit."""

    class Chunker(Actor):
        def __init__(self):
            self.chunks = []

        @endpoint
        def process_chunks(self, chunks):
            self.chunks = chunks
            return len(chunks)

    oneGiB = 1024 * 1024 * 1024
    tenGiB = 10 * oneGiB
    oneHundredGiB = 10 * tenGiB

    # Verify default is 10 GiB
    config = get_global_config()
    assert config["codec_max_frame_length"] == tenGiB

    # Set the frame limit to confidently handle 10GiB
    with configured(codec_max_frame_length=oneHundredGiB):
        # Spawn in separate proc so messages are serialized via Unix
        # sockets
        proc = this_host().spawn_procs()

        # Create 10 chunks, 1GiB each (total 10GiB)
        chunks = [bytes(oneGiB) for _ in range(10)]

        # Spawn actor and send chunks - should succeed
        chunker = proc.spawn("chunker", Chunker)
        result = chunker.process_chunks.call_one(chunks).get()

        assert result == 10


def test_duration_config_basic() -> None:
    """Test setting and getting Duration configuration values."""
    # Test with seconds format
    with configured(
        host_spawn_ready_timeout="300s",
        message_delivery_timeout="60s",
        mesh_proc_spawn_max_idle="120s",
    ) as config:
        assert config["host_spawn_ready_timeout"] == "5m"
        assert config["message_delivery_timeout"] == "1m"
        assert config["mesh_proc_spawn_max_idle"] == "2m"

    # Verify values are restored to defaults after context exits
    config = get_global_config()
    assert config["host_spawn_ready_timeout"] == "30s"
    assert config["message_delivery_timeout"] == "30s"
    assert config["mesh_proc_spawn_max_idle"] == "30s"


def test_duration_config_formats() -> None:
    """Test Duration configuration with different humantime formats."""
    test_cases = [
        ("30s", "30s"),  # seconds
        ("5m", "5m"),  # minutes
        ("2h", "2h"),  # hours
        ("90s", "1m 30s"),  # overflow seconds to minutes
        ("1h 30m", "1h 30m"),  # compound duration with space
        ("1h30m", "1h 30m"),  # compound duration without space
    ]

    for input_val, expected_val in test_cases:
        with configured(host_spawn_ready_timeout=input_val) as config:
            assert config["host_spawn_ready_timeout"] == expected_val


def test_duration_config_invalid_format() -> None:
    """Test that invalid Duration formats raise errors."""
    with pytest.raises(TypeError, match="Invalid duration format"):
        with configured(host_spawn_ready_timeout="invalid"):
            pass

    with pytest.raises(TypeError, match="Invalid duration format"):
        with configured(message_delivery_timeout="30"):  # missing unit
            pass

    with pytest.raises(TypeError, match="Invalid duration format"):
        with configured(mesh_proc_spawn_max_idle="abc123"):
            pass


def test_duration_config_type_error() -> None:
    """Test that non-string values for Duration config raise TypeError."""
    with pytest.raises(TypeError):
        with configured(host_spawn_ready_timeout=30):  # type: ignore
            pass

    with pytest.raises(TypeError):
        with configured(message_delivery_timeout=30.5):  # type: ignore
            pass


def test_duration_config_multiple() -> None:
    """Test setting multiple Duration configs together with other configs."""
    with configured(
        default_transport=ChannelTransport.TcpWithLocalhost,
        host_spawn_ready_timeout="10m",
        message_delivery_timeout="5m",
        mesh_proc_spawn_max_idle="2m",
        enable_log_forwarding=True,
        tail_log_lines=100,
    ) as config:
        assert config["default_transport"] == BindSpec(
            ChannelTransport.TcpWithLocalhost
        )
        assert config["host_spawn_ready_timeout"] == "10m"
        assert config["message_delivery_timeout"] == "5m"
        assert config["mesh_proc_spawn_max_idle"] == "2m"
        assert config["enable_log_forwarding"]
        assert config["tail_log_lines"] == 100

    # Verify all values are restored
    config = get_global_config()
    assert config["default_transport"] == BindSpec(ChannelTransport.Unix)
    assert config["host_spawn_ready_timeout"] == "30s"
    assert config["message_delivery_timeout"] == "30s"
    assert config["mesh_proc_spawn_max_idle"] == "30s"
    assert not config["enable_log_forwarding"]
    assert config["tail_log_lines"] == 0


@pytest.mark.parametrize(
    "param_name,test_value,expected_value,default_value",
    [
        # Hyperactor timeouts and message handling
        ("process_exit_timeout", "20s", "20s", "10s"),
        ("message_ack_time_interval", "2s", "2s", "500ms"),
        ("split_max_buffer_age", "100ms", "100ms", "50ms"),
        ("stop_actor_timeout", "15s", "15s", "10s"),
        ("cleanup_timeout", "25s", "25s", "3s"),
        ("remote_allocator_heartbeat_interval", "10m", "10m", "5m"),
        ("channel_net_rx_buffer_full_check_interval", "200ms", "200ms", "5s"),
        # Mesh bootstrap config
        ("mesh_terminate_timeout", "20s", "20s", "10s"),
        # Proc mesh timeouts
        ("actor_spawn_max_idle", "45s", "45s", "30s"),
        ("get_actor_state_max_idle", "90s", "1m 30s", "30s"),
        ("supervision_watchdog_timeout", "90s", "1m 30s", "2m"),
        # Host mesh timeouts
        ("proc_stop_max_idle", "45s", "45s", "30s"),
        ("get_proc_state_max_idle", "90s", "1m 30s", "1m"),
    ],
)
def test_duration_params(param_name, test_value, expected_value, default_value):
    """Test all new duration configuration parameters."""
    # Verify default value
    config = get_global_config()
    assert config[param_name] == default_value

    # Set new value and verify
    with configured(**{param_name: test_value}) as config:
        assert config[param_name] == expected_value

    # Verify restoration to default
    config = get_global_config()
    assert config[param_name] == default_value


@pytest.mark.parametrize(
    "param_name,test_value,default_value",
    [
        # Hyperactor message handling
        ("message_ack_every_n_messages", 500, 1000),
        ("message_ttl_default", 20, 64),
        ("split_max_buffer_size", 2048, 5),
        # Mesh bootstrap config
        ("mesh_terminate_concurrency", 32, 16),
        # Runtime and buffering
        ("small_write_threshold", 512, 256),
        # Mesh config (usize::MAX doesn't have a fixed value, skip default check)
        # Logging config
        ("read_log_buffer", 16384, 100),
    ],
)
def test_integer_params(param_name, test_value, default_value):
    """Test all new integer configuration parameters."""
    # Verify default value
    config = get_global_config()
    assert config[param_name] == default_value

    # Set new value and verify
    with configured(**{param_name: test_value}) as config:
        assert config[param_name] == test_value

    # Verify restoration to default
    config = get_global_config()
    assert config[param_name] == default_value


@pytest.mark.parametrize(
    "param_name,default_value",
    [
        # Hyperactor message handling
        ("enable_dest_actor_reordering_buffer", False),
        # Mesh bootstrap config
        ("mesh_bootstrap_enable_pdeathsig", True),
        # Runtime and buffering
        ("shared_asyncio_runtime", False),
        # Logging config
        ("force_file_log", False),
        ("prefix_with_rank", True),
        # Actor queue dispatch
        ("actor_queue_dispatch", False),
    ],
)
def test_boolean_params(param_name, default_value):
    """Test all new boolean configuration parameters."""
    # Verify default value
    config = get_global_config()
    assert config[param_name] == default_value

    # Set to opposite value and verify
    with configured(**{param_name: not default_value}) as config:
        assert config[param_name] == (not default_value)

    # Verify restoration to default
    config = get_global_config()
    assert config[param_name] == default_value


def test_float_param_message_latency_sampling_rate():
    """Test message_latency_sampling_rate float parameter."""
    # Verify default value (0.01, using approx for f32 precision)
    config = get_global_config()
    assert config["message_latency_sampling_rate"] == pytest.approx(0.01, rel=1e-5)

    # Test various valid sampling rates
    test_values = [0.0, 0.1, 0.5, 0.99, 1.0]
    for rate in test_values:
        with configured(message_latency_sampling_rate=rate) as config:
            assert config["message_latency_sampling_rate"] == pytest.approx(
                rate, rel=1e-5
            )

    # Verify restoration
    config = get_global_config()
    assert config["message_latency_sampling_rate"] == pytest.approx(0.01, rel=1e-5)


def test_encoding_param():
    """Test default_encoding enum parameter with valid encodings."""
    from monarch._rust_bindings.monarch_hyperactor.config import Encoding

    # Verify default value
    config = get_global_config()
    assert config["default_encoding"] == Encoding.Multipart

    # Test all valid encodings
    valid_encodings = [Encoding.Bincode, Encoding.Json, Encoding.Multipart]
    for encoding in valid_encodings:
        with configured(default_encoding=encoding) as config:
            assert config["default_encoding"] == encoding

    # Verify restoration
    config = get_global_config()
    assert config["default_encoding"] == Encoding.Multipart


def test_encoding_param_invalid():
    """Test that invalid encoding values raise errors."""
    # Strings aren't expected
    with pytest.raises(TypeError):
        with configured(default_encoding="bincode"):
            pass

    # Neither are numbers
    with pytest.raises(TypeError):
        with configured(default_encoding=123):
            pass


def test_all_params_together():
    """Test setting all 29 config parameters simultaneously."""
    from monarch._rust_bindings.monarch_hyperactor.config import Encoding

    with configured(
        # Hyperactor timeouts and message handling
        process_exit_timeout="20s",
        message_ack_time_interval="2s",
        message_ack_every_n_messages=500,
        message_ttl_default=20,
        split_max_buffer_size=2048,
        split_max_buffer_age="100ms",
        stop_actor_timeout="15s",
        cleanup_timeout="25s",
        remote_allocator_heartbeat_interval="10s",
        default_encoding=Encoding.Json,
        channel_net_rx_buffer_full_check_interval="200ms",
        message_latency_sampling_rate=0.5,
        enable_dest_actor_reordering_buffer=True,
        # Mesh bootstrap config
        mesh_bootstrap_enable_pdeathsig=False,
        mesh_terminate_concurrency=16,
        mesh_terminate_timeout="20s",
        # Runtime and buffering
        shared_asyncio_runtime=True,
        small_write_threshold=512,
        # Mesh config
        max_cast_dimension_size=2048,
        # Logging config
        read_log_buffer=16384,
        force_file_log=True,
        prefix_with_rank=True,
        # Proc mesh timeouts
        actor_spawn_max_idle="45s",
        get_actor_state_max_idle="90s",
        supervision_watchdog_timeout="90s",
        # Host mesh timeouts
        proc_stop_max_idle="45s",
        get_proc_state_max_idle="90s",
        # Actor queue dispatch
        actor_queue_dispatch=True,
    ) as config:
        # Verify all values are set correctly
        assert config["process_exit_timeout"] == "20s"
        assert config["message_ack_time_interval"] == "2s"
        assert config["message_ack_every_n_messages"] == 500
        assert config["message_ttl_default"] == 20
        assert config["split_max_buffer_size"] == 2048
        assert config["split_max_buffer_age"] == "100ms"
        assert config["stop_actor_timeout"] == "15s"
        assert config["cleanup_timeout"] == "25s"
        assert config["remote_allocator_heartbeat_interval"] == "10s"
        assert config["default_encoding"] == Encoding.Json
        assert config["channel_net_rx_buffer_full_check_interval"] == "200ms"
        assert config["message_latency_sampling_rate"] == pytest.approx(0.5, rel=1e-5)
        assert config["enable_dest_actor_reordering_buffer"] is True
        assert config["mesh_bootstrap_enable_pdeathsig"] is False
        assert config["mesh_terminate_concurrency"] == 16
        assert config["mesh_terminate_timeout"] == "20s"
        assert config["shared_asyncio_runtime"] is True
        assert config["small_write_threshold"] == 512
        assert config["max_cast_dimension_size"] == 2048
        assert config["read_log_buffer"] == 16384
        assert config["force_file_log"] is True
        assert config["prefix_with_rank"] is True
        assert config["actor_spawn_max_idle"] == "45s"
        assert config["get_actor_state_max_idle"] == "1m 30s"
        assert config["supervision_watchdog_timeout"] == "1m 30s"
        assert config["proc_stop_max_idle"] == "45s"
        assert config["get_proc_state_max_idle"] == "1m 30s"
        assert config["actor_queue_dispatch"] is True

    # Verify all values are restored to defaults
    config = get_global_config()
    assert config["process_exit_timeout"] == "10s"
    assert config["message_ack_time_interval"] == "500ms"
    assert config["message_ack_every_n_messages"] == 1000
    assert config["message_ttl_default"] == 64
    assert config["split_max_buffer_size"] == 5
    assert config["split_max_buffer_age"] == "50ms"
    assert config["stop_actor_timeout"] == "10s"
    assert config["cleanup_timeout"] == "3s"
    assert config["remote_allocator_heartbeat_interval"] == "5m"
    assert config["default_encoding"] == Encoding.Multipart
    assert config["channel_net_rx_buffer_full_check_interval"] == "5s"
    assert config["message_latency_sampling_rate"] == pytest.approx(0.01, rel=1e-5)
    assert config["enable_dest_actor_reordering_buffer"] is False
    assert config["mesh_bootstrap_enable_pdeathsig"] is True
    assert config["mesh_terminate_concurrency"] == 16
    assert config["mesh_terminate_timeout"] == "10s"
    assert config["shared_asyncio_runtime"] is False
    assert config["small_write_threshold"] == 256
    # max_cast_dimension_size is usize::MAX, skip checking it
    assert config["read_log_buffer"] == 100
    assert config["force_file_log"] is False
    assert config["prefix_with_rank"] is True
    assert config["actor_spawn_max_idle"] == "30s"
    assert config["get_actor_state_max_idle"] == "30s"
    assert config["supervision_watchdog_timeout"] == "2m"
    assert config["proc_stop_max_idle"] == "30s"
    assert config["get_proc_state_max_idle"] == "1m"
    assert config["actor_queue_dispatch"] is False


def test_params_type_errors():
    """Test that type errors are raised for incorrect parameter types."""
    # Duration param with wrong type
    with pytest.raises(TypeError):
        with configured(process_exit_timeout=30):  # type: ignore
            pass

    # Integer param with wrong type
    with pytest.raises(TypeError):
        with configured(message_ack_every_n_messages="100"):  # type: ignore
            pass

    # Boolean param with wrong type
    with pytest.raises(TypeError):
        with configured(enable_dest_actor_reordering_buffer="true"):  # type: ignore
            pass

    # Float param with wrong type
    with pytest.raises(TypeError):
        with configured(message_latency_sampling_rate="0.5"):  # type: ignore
            pass
