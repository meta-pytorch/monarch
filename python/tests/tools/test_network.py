# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import socket
import unittest
from typing import Any, List
from unittest import mock

from monarch.tools import network


class TestNetwork(unittest.TestCase):
    def test_network_ipv4_fallback(self) -> None:
        with mock.patch(
            "socket.getaddrinfo",
            side_effect=[
                socket.gaierror,
                [
                    (
                        socket.AF_INET,
                        socket.SOCK_STREAM,
                        socket.IPPROTO_TCP,
                        "",
                        ("123.45.67.89", 8080),
                    )
                ],
            ]
            * 2,
        ):
            self.assertEqual(
                "123.45.67.89:8080", network.get_sockaddr("foo.bar.facebook.com", 8080)
            )
            self.assertEqual(
                "123.45.67.89", network.get_ipaddr("foo.bar.facebook.com", 8080)
            )

    def test_network_ipv4(self) -> None:
        with mock.patch(
            "socket.getaddrinfo",
            return_value=(
                [
                    (
                        socket.AF_INET,
                        socket.SOCK_STREAM,
                        socket.IPPROTO_TCP,
                        "",
                        ("123.45.67.89", 8080),
                    )
                ]
            ),
        ):
            self.assertEqual(
                "123.45.67.89",
                network.get_ipaddr("foo.bar.facebook.com", 8080, network.AddrType.IPv4),
            )

    def test_network_ipv6(self) -> None:
        with mock.patch(
            "socket.getaddrinfo",
            return_value=(
                [
                    (
                        socket.AF_INET6,
                        socket.SOCK_STREAM,
                        socket.IPPROTO_TCP,
                        "",
                        ("1234:ab00:567c:89d:abcd:0:328:0", 0, 0, 0),
                    )
                ]
            ),
        ):
            self.assertEqual(
                "[1234:ab00:567c:89d:abcd:0:328:0]:8080",
                network.get_sockaddr("foo.bar.facebook.com", 8080),
            )
            self.assertEqual(
                "1234:ab00:567c:89d:abcd:0:328:0",
                network.get_ipaddr("foo.bar.facebook.com", 8080),
            )
            self.assertEqual(
                "1234:ab00:567c:89d:abcd:0:328:0",
                network.get_ipaddr("foo.bar.facebook.com", 8080, network.AddrType.IPv6),
            )

    def test_ipv6_link_local_skipped_falls_back_to_ipv4(self) -> None:
        """Link-local IPv6 addresses (fe80::) are unusable for inter-process
        communication (they require a scope ID). Verify that _resolve_ipaddr
        skips them so that get_ipaddr/get_sockaddr fall back to IPv4."""
        link_local_ipv6: str = "fe80::222:48ff:fe49:ba90"
        ipv4_fallback: str = "10.0.0.1"

        # patternlint-disable-next-line python-dns-deps (only used for oss)
        def fake_getaddrinfo(
            host: str, port: int, family: socket.AddressFamily, type: int
        ) -> List[Any]:
            if family == socket.AF_INET6:
                return [
                    (
                        socket.AF_INET6,
                        socket.SOCK_STREAM,
                        socket.IPPROTO_TCP,
                        "",
                        (link_local_ipv6, port, 0, 0),
                    )
                ]
            elif family == socket.AF_INET:
                return [
                    (
                        socket.AF_INET,
                        socket.SOCK_STREAM,
                        socket.IPPROTO_TCP,
                        "",
                        (ipv4_fallback, port),
                    )
                ]
            return []

        with mock.patch("socket.getaddrinfo", side_effect=fake_getaddrinfo):
            # get_ipaddr with Default should skip fe80:: and return IPv4
            self.assertEqual(
                ipv4_fallback,
                network.get_ipaddr("host", 8080),
            )
            # get_sockaddr should also skip fe80:: and return IPv4 format
            self.assertEqual(
                f"{ipv4_fallback}:8080",
                network.get_sockaddr("host", 8080),
            )

    def test_ipv6_link_local_skipped_raises_when_no_ipv4(self) -> None:
        """When only a link-local IPv6 is available and no IPv4, raise RuntimeError."""
        link_local_ipv6: str = "fe80::1"

        # patternlint-disable-next-line python-dns-deps (only used for oss)
        def fake_getaddrinfo(
            host: str, port: int, family: socket.AddressFamily, type: int
        ) -> List[Any]:
            if family == socket.AF_INET6:
                return [
                    (
                        socket.AF_INET6,
                        socket.SOCK_STREAM,
                        socket.IPPROTO_TCP,
                        "",
                        (link_local_ipv6, port, 0, 0),
                    )
                ]
            elif family == socket.AF_INET:
                raise socket.gaierror("No IPv4 address")
            return []

        with mock.patch("socket.getaddrinfo", side_effect=fake_getaddrinfo):
            with self.assertRaises(RuntimeError):
                network.get_ipaddr("host", 8080)

    def test_ipv4_link_local_skipped(self) -> None:
        """IPv4 link-local addresses (169.254.x.x) are auto-assigned when DHCP
        fails and are not routable. Verify they are skipped."""
        link_local_ipv4 = "169.254.1.1"

        with mock.patch(
            "socket.getaddrinfo",
            return_value=[
                (
                    socket.AF_INET,
                    socket.SOCK_STREAM,
                    socket.IPPROTO_TCP,
                    "",
                    (link_local_ipv4, 8080),
                )
            ],
        ):
            with self.assertRaises(RuntimeError):
                network.get_ipaddr("host", 8080, network.AddrType.IPv4)

    def test_network(self) -> None:
        # since we patched `socket.getaddrinfo` above
        # don't patch and just make sure things don't error out
        self.assertIsNotNone(network.get_sockaddr(socket.getfqdn(), 8080))


def _addrinfo(*addrs: tuple[int, str], port: int = 8080) -> List[Any]:
    """A `socket.getaddrinfo` result for the given `(family, ip)` pairs."""
    entries: List[Any] = []
    for family, ip in addrs:
        sockaddr = (ip, port) if family == socket.AF_INET else (ip, port, 0, 0)
        entries.append((family, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", sockaddr))
    return entries


class TestConsistentSockaddr(unittest.TestCase):
    """`get_consistent_sockaddr` promises one thing: any two hosts resolving the
    same name produce the same string. Each test pins part of that promise."""

    IPV6: tuple[int, str] = (socket.AF_INET6, "2a03:83e4:4027:846b::cba7")
    IPV4: tuple[int, str] = (socket.AF_INET, "10.130.53.206")

    def _resolve(self, *addrs: tuple[int, str]) -> str:
        with mock.patch("socket.getaddrinfo", return_value=_addrinfo(*addrs)):
            return network.get_consistent_sockaddr("node-0", 8080)

    def test_agrees_when_only_one_side_sees_an_extra_ipv6(self) -> None:
        """The bug: a node's own `/etc/hosts` adds an IPv6 GUA that the A record
        its peers get from DNS does not have. Both sides must still pick the
        same address."""
        advertised = self._resolve(self.IPV6, self.IPV4)  # the node's own view
        dialed = self._resolve(self.IPV4)  # what DNS gives its peers
        self.assertEqual(advertised, dialed)
        self.assertEqual("10.130.53.206:8080", advertised)

    def test_choice_is_independent_of_resolver_order(self) -> None:
        """getaddrinfo ordering is host-configurable (RFC 6724, gai.conf), so it
        must not decide the answer -- across families or within one."""
        other_ipv4 = (socket.AF_INET, "10.130.53.7")
        for view in ((self.IPV6, self.IPV4), (self.IPV4, self.IPV6)):
            self.assertEqual("10.130.53.206:8080", self._resolve(*view))
        for view in ((self.IPV4, other_ipv4), (other_ipv4, self.IPV4)):
            self.assertEqual("10.130.53.7:8080", self._resolve(*view))

    def test_ipv6_only_host_gets_a_bracketed_ipv6(self) -> None:
        """No family is hardcoded: unlike `AddrType.IPv4`, an IPv6-only cluster
        still resolves rather than failing."""
        self.assertEqual("[2a03:83e4:4027:846b::cba7]:8080", self._resolve(self.IPV6))

    def test_unroutable_addresses_dropped(self) -> None:
        """Loopback is dropped as well as link-local: both sides would agree on
        it, but no peer can reach it."""
        for unroutable in [
            (socket.AF_INET, "127.0.1.1"),
            (socket.AF_INET6, "::1"),
            (socket.AF_INET6, "fe80::222:48ff:fe49:ba90"),
            (socket.AF_INET, "169.254.1.1"),
            # a scope id ("fe80::1%eth0") must be stripped before the routability
            # check, not crash `ipaddress.ip_address` on the way to dropping it
            (socket.AF_INET6, "fe80::222:48ff:fe49:ba90%eth0"),
        ]:
            with self.subTest(unroutable=unroutable[1]):
                self.assertEqual(
                    "10.130.53.206:8080", self._resolve(unroutable, self.IPV4)
                )

    def test_raises_when_nothing_routable_resolves(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "routable"):
            self._resolve((socket.AF_INET, "127.0.0.1"))

    def test_real_hostname_resolves(self) -> None:
        # unpatched, like `TestNetwork.test_network`: just make sure the rule
        # runs against a real resolver on this host.
        self.assertIsNotNone(
            network.get_consistent_sockaddr(socket.gethostname(), 8080)
        )
