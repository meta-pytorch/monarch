# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum

class ChannelTransport(Enum):
    Tcp = "tcp"
    MetaTlsWithHostname = "metatls(hostname)"
    MetaTlsWithIpV6 = "metatls(ipv6)"
    Local = "local"
    Unix = "unix"
    # Sim  # TODO add support

class ChannelAddr:
    @staticmethod
    def any(transport: ChannelTransport) -> str:
        """Returns an "any" address for the given transport type.

        Primarily used to bind servers. The returned string can be
        converted into `hyperactor::channel::ChannelAddr` (in Rust) by
        calling `hyperactor::channel::ChannelAddr::from_str()`.
        """
        ...
