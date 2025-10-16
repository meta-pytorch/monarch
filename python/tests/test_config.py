# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import pytest
from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import (
    configure,
    get_configuration,
)


def test_get_set_transport() -> None:
    for transport in (
        ChannelTransport.Unix,
        ChannelTransport.TcpWithLocalhost,
        ChannelTransport.TcpWithHostname,
        ChannelTransport.MetaTlsWithHostname,
    ):
        configure(default_transport=transport)
        assert get_configuration()["default_transport"] == transport
    # Succeed even if we don't specify the transport
    configure()
    assert (
        get_configuration()["default_transport"] == ChannelTransport.MetaTlsWithHostname
    )
    with pytest.raises(TypeError):
        configure(default_transport="unix")  # type: ignore
    with pytest.raises(TypeError):
        configure(default_transport=42)  # type: ignore
    with pytest.raises(TypeError):
        configure(default_transport={})  # type: ignore


def test_nonexistent_config_key() -> None:
    with pytest.raises(ValueError):
        configure(does_not_exist=42)  # type: ignore
