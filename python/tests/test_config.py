# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import contextlib

import monarch
import pytest
from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionError
from monarch.actor import Actor, endpoint, this_proc
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
            assert config["default_transport"] == transport
    # Succeed even if we don't specify the transport, but does not change the
    # previous value.
    with configured() as config:
        assert config["default_transport"] == ChannelTransport.Unix
    with pytest.raises(TypeError):
        with configured(default_transport="unix"):  # type: ignore
            pass
    with pytest.raises(TypeError):
        with configured(default_transport=42):  # type: ignore
            pass
    with pytest.raises(TypeError):
        with configured(default_transport={}):  # type: ignore
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
            assert config["default_transport"] == ChannelTransport.TcpWithLocalhost
    # Make sure the previous values are restored.
    config = get_global_config()
    assert not config["enable_log_forwarding"]
    assert not config["enable_file_capture"]
    assert config["tail_log_lines"] == 0
    assert config["default_transport"] == ChannelTransport.Unix


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
        # This should fail due to serialization overhead
        proc = this_proc()

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
        proc = this_proc()

        # Create 10 chunks, 1GiB each (total 10GiB)
        chunks = [bytes(oneGiB) for _ in range(10)]

        # Spawn actor and send chunks - should succeed
        chunker = proc.spawn("chunker", Chunker)
        result = chunker.process_chunks.call_one(chunks).get()

        assert result == 10
