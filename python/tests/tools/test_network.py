# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import socket
import unittest
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

    def test_network(self) -> None:
        # since we patched `socket.getaddrinfo` above
        # don't patch and just make sure things don't error out
        self.assertIsNotNone(network.get_sockaddr(socket.getfqdn(), 8080))
