# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import contextlib
from typing import Any, Dict, Iterator

import pytest
from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import (
    clear_runtime_config,
    configure,
    get_global_config,
    get_runtime_config,
)


@contextlib.contextmanager
def configured(**overrides) -> Iterator[Dict[str, Any]]:
    """Temporarily apply Python-side config overrides for this
    process.

    This context manager:
      * snapshots the current **Runtime** configuration layer
        (`get_runtime_configuration()`),
      * applies the given `overrides` via `configure(**overrides)`,
        and
      * yields the **merged** view of config (`get_configuration()`),
        including defaults, env, file, and Runtime.

    On exit it restores the previous Runtime layer by:
      * clearing all Runtime entries, and
      * re-applying the saved snapshot.

    This is intended for tests, so per-test overrides do not leak into
    other tests.

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
