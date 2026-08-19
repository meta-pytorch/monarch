# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import ipaddress
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


def get_consistent_sockaddr(hostname: str, port: int) -> str:
    """Returns the socket address of `hostname:port` that *every* host resolves alike.

    Same return shape as `get_sockaddr`: `{ipv4}:{port}` or `[{ipv6}]:{port}`.

    Use this rather than `get_sockaddr` when the address is a shared *identity*:
    one host advertises where it can be reached and another dials it, and the
    two strings must be equal, not merely both reachable. `ChannelAddr::Tcp`
    holds a resolved socket address and a worker's host identity is derived from
    the address it advertises, so a peer that resolves the same name differently
    connects and then fails delivery with "ttl expired".

    Resolvers do differ: a node's own name goes through its `/etc/hosts`, which
    can publish families that DNS does not -- on CoreWeave GB300 each node's
    `/etc/hosts` adds an IPv6 GUA for its own name while cluster DNS serves only
    an A record. So drop loopback and link-local (the routability rule
    `ChannelAddr::any` uses), prefer IPv4, and break ties by address. IPv4-first
    is what makes the side that sees the extra IPv6 agree with the side that
    does not; `AddrType.Default` is IPv6-first and would not, and pinning
    `AddrType.IPv4` would agree but break IPv6-only clusters.

    Raises a `RuntimeError` if `hostname` resolves to no routable address.
    """
    candidates: list[tuple[int, ipaddress.IPv4Address | ipaddress.IPv6Address]] = []
    # patternlint-disable-next-line python-dns-deps (only used for oss)
    for family, _, _, _, sockaddr in socket.getaddrinfo(
        hostname, port, type=socket.SOCK_STREAM
    ):
        # Strip any scope id ("fe80::1%eth0"), which ip_address rejects.
        addr = str(sockaddr[0]).partition("%")[0]
        ipaddr = ipaddress.ip_address(addr)
        # Loopback is dropped as well as link-local (unlike `_resolve_ipaddr`):
        # every host resolves it alike, but no peer can reach it.
        if ipaddr.is_loopback or ipaddr.is_link_local:
            logger.info(
                "skipping unroutable address `%s` for `%s:%d`"
                " (loopback and link-local addresses cannot be dialed by a peer)",
                addr,
                hostname,
                port,
            )
            continue
        candidates.append((family, ipaddr))

    if not candidates:
        raise RuntimeError(
            f"Unable to resolve `{hostname}` to a routable (non-loopback,"
            " non-link-local) address that can bind TCP socket."
            " Check the network configuration on the host."
        )

    # Least address within the preferred family, not the first one the resolver
    # happened to list: that ordering is host-specific (RFC 6724, gai.conf) and
    # a name with several A records would otherwise resolve differently per host.
    family, ipaddr = min(
        candidates, key=lambda c: (c[0] != socket.AF_INET, c[1].packed)
    )
    socket_address = (
        f"[{ipaddr}]:{port}" if family == socket.AF_INET6 else f"{ipaddr}:{port}"
    )
    logger.info(
        "resolved consistent address `%s` for `%s:%d`",
        socket_address,
        hostname,
        port,
    )
    return socket_address


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
            addr = str(sockaddr[0])

            # Link-local addresses (fe80::/10 for IPv6, 169.254.0.0/16 for IPv4)
            # are not routable beyond the local network segment and are unusable
            # for inter-process TCP communication in most environments including
            # containers.
            if ipaddress.ip_address(addr).is_link_local:
                logger.info(
                    "skipping link-local address `%s` for `%s:%d`"
                    " (link-local addresses are not usable for inter-process communication)",
                    addr,
                    hostname,
                    port,
                )
                return None

            return addr
    except socket.gaierror as e:
        logger.info(
            "no %s address that can bind TCP sockets for `%s:%d` (error: %s)",
            family.name,
            hostname,
            port,
            e,
        )
    return None
