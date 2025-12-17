# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum

class ChannelTransport(Enum):
    """
    Enum representing basic transport types for channels.
    For explicit address binding, use BindSpec instead.
    """

    TcpWithLocalhost = "tcp(localhost)"
    TcpWithHostname = "tcp(hostname)"
    MetaTlsWithHostname = "metatls(hostname)"
    MetaTlsWithIpV6 = "metatls(ipv6)"
    Local = "local"
    Unix = "unix"
    # Sim  # TODO add support

class BindSpec:
    """
    Specify how to bind a channel server.

    Can be created from either a ChannelTransport enum for "any" binding,
    or a ZMQ-style URL string for explicit address.

    Note: This class is for internal use only. Users should pass ChannelTransport
    enum directly or use the ZMQ-style URL string for explicit address.
    """

    def __init__(self, spec: ChannelTransport | str) -> None: ...
    """
        Basic transport types supported by ChannelTransport should be used directly as enum values.
        For explicit address binding, use a ZMQ-style URL string. e.g.:
        - "tcp://127.0.0.1:8080"
    """
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...

class ChannelAddr:
    @staticmethod
    def any(transport: ChannelTransport) -> str:
        """Returns an "any" address for the given transport type.

        Primarily used to bind servers. The returned string can be
        converted into `hyperactor::channel::ChannelAddr` (in Rust) by
        calling `hyperactor::channel::ChannelAddr::from_str()`.
        """
        ...
