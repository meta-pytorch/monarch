# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from contextlib import contextmanager

import pytest
from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import (
    configure,
    get_configuration,
    reload_config_from_env,
    reset_config_to_defaults,
)


@contextmanager
def configure_temporary(*args, **kwargs):
    """Call configure, and then reset the configuration to the default values after
    exiting. Always use this when testing so that other tests are not affected by any
    changes made."""

    try:
        configure(*args, **kwargs)
        yield
    finally:
        reset_config_to_defaults()
        # Re-apply any environment variables that were set for this test.
        reload_config_from_env()


def test_get_set_transport() -> None:
    for transport in (
        ChannelTransport.Unix,
        ChannelTransport.TcpWithLocalhost,
        ChannelTransport.TcpWithHostname,
        ChannelTransport.MetaTlsWithHostname,
    ):
        with configure_temporary(default_transport=transport):
            assert get_configuration()["default_transport"] == transport
    # Succeed even if we don't specify the transport, but does not change the
    # previous value.
    with configure_temporary():
        assert get_configuration()["default_transport"] == ChannelTransport.Unix
    with pytest.raises(TypeError):
        with configure_temporary(default_transport="unix"):  # type: ignore
            pass
    with pytest.raises(TypeError):
        with configure_temporary(default_transport=42):  # type: ignore
            pass
    with pytest.raises(TypeError):
        with configure_temporary(default_transport={}):  # type: ignore
            pass


def test_nonexistent_config_key() -> None:
    with pytest.raises(ValueError):
        with configure_temporary(does_not_exist=42):  # type: ignore
            pass


def test_get_set_multiple() -> None:
    with configure_temporary(default_transport=ChannelTransport.TcpWithLocalhost):
        with configure_temporary(
            enable_log_forwarding=True, enable_file_capture=True, tail_log_lines=100
        ):
            config = get_configuration()
            assert config["enable_log_forwarding"]
            assert config["enable_file_capture"]
            assert config["tail_log_lines"] == 100
            assert config["default_transport"] == ChannelTransport.TcpWithLocalhost
    # Make sure the previous values are restored.
    config = get_configuration()
    assert not config["enable_log_forwarding"]
    assert not config["enable_file_capture"]
    assert config["tail_log_lines"] == 0
    assert config["default_transport"] == ChannelTransport.Unix
