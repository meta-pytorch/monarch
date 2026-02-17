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
