# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import logging
import socket
from enum import auto, Enum
from typing import Optional

logger: logging.Logger = logging.getLogger(__name__)


def get_sockaddr(hostname: str, port: int) -> str:
    """Returns either an IPv6 or IPv4 socket address (that supports TCP) of the given hostname and port.
    The socket address is of the form:
      1. `{ipv4.address}:{port}` (e.g. `127.0.0.1:8080`)
      2. `[{ipv6:address}]:{port}` (e.g. `[::1]:8080`)

    The hostname is resolved to an IPv6 (or IPv4 if IPv6 is not available on the host) address that
    supports `SOCK_STREAM` (TCP).

    Raises a `RuntimeError` if neither ipv6 or ipv4 ip can be resolved from hostname.
    """

    def resolve_sockaddr(family: socket.AddressFamily) -> Optional[str]:
        if ipaddr := _resolve_ipaddr(hostname, port, family):
            if family == socket.AF_INET6:
                socket_address = f"[{ipaddr}]:{port}"
            else:  # socket.AF_INET
                socket_address = f"{ipaddr}:{port}"

            logger.info(
                "resolved %s address `%s` for `%s:%d`",
                family.name,
                socket_address,
                hostname,
                port,
            )
            return socket_address

        return None

    for family in [socket.AF_INET6, socket.AF_INET]:
        if sockaddr := resolve_sockaddr(family):
            return sockaddr

    raise RuntimeError(
        f"Unable to resolve `{hostname}` to ipv6 or ipv4 address that can bind TCP socket."
        " Check the network configuration on the host."
    )


class AddrType(Enum):
    # Default to IPv6, and fallback to IPv4 if IPv6 is not available on the host.
    Default = auto()
    IPv4 = auto()
    IPv6 = auto()


def get_ipaddr(hostname: str, port: int, addr_type: AddrType = AddrType.Default) -> str:
    """Similar to `get_sockaddr` but returns only the ip address instead of the socket address.
    The return IP address is of the form:
      1. `{ipv4.address}` (e.g. `127.0.0.1`)
      2. `[{ipv6:address}]` (e.g. `[::1]`)
    """
    match addr_type:
        case AddrType.IPv4:
            families = [socket.AF_INET]
        case AddrType.IPv6:
            families = [socket.AF_INET6]
        case AddrType.Default:
            families = [socket.AF_INET6, socket.AF_INET]
        case _:
            raise ValueError(f"Unknown AddrType: {AddrType}")

    for family in families:
        if ipaddr := _resolve_ipaddr(hostname, port, family):
            logger.info(
                "resolved %s address `%s` for `%s:%d`",
                family.name,
                ipaddr,
                hostname,
                port,
            )
            return ipaddr

    raise RuntimeError(
        f"Unable to resolve `{hostname}` to ipv6 or ipv4 address that can bind TCP socket."
        " Check the network configuration on the host."
    )


def _resolve_ipaddr(
    hostname: str, port: int, family: socket.AddressFamily
) -> Optional[str]:
    try:
        # patternlint-disable-next-line python-dns-deps (only used for oss)
        addrs = socket.getaddrinfo(hostname, port, family, type=socket.SOCK_STREAM)
        if addrs:
            family, _, _, _, sockaddr = addrs[0]  # use the first address

            # sockaddr is a tuple (ipv4) or a 4-tuple (ipv6)
            # in both cases the first element is the ip addr
            return str(sockaddr[0])
    except socket.gaierror as e:
        logger.info(
            "no %s address that can bind TCP sockets for `%s:%d` (error: %s)",
            family.name,
            hostname,
            port,
            e,
        )
    return None
