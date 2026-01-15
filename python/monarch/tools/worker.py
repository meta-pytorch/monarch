# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
"""
Worker startup script

"""

import argparse
import socket

from monarch.actor import run_worker_loop_forever


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start a monarch worker listening on the given address"
    )
    addr_default = f"tcp://{socket.gethostname()}:26600"
    parser.add_argument(
        "--addr",
        type=str,
        default=None,
        help=f"address to listen on for connections. defaults to tcp://{{socket.gethostname()}}:26600 ({addr_default})",
    )
    group = parser.add_argument_group(
        "address builder",
        description="Options to build an address instead of specifying",
    )
    group.add_argument(
        "--scheme",
        type=str,
        default="tcp",
        choices=("tcp", "metatls"),
        help="scheme to use if addr is not set",
    )
    group.add_argument(
        "--host",
        type=str,
        default="hostname",
        help="how to resolve the hostname if addr is not set. Can be 'hostname' to use socket.gethostname(), "
        "'fqdn' to use socket.getfqdn(), or any other string will be the raw hostname",
    )
    group.add_argument(
        "--port",
        type=int,
        default=26600,
        help="port to use. Used if addr is not set.",
    )

    parser.add_argument(
        "--ca",
        type=str,
        default="trust_all_connections",
        help="certificate authority to use for TLS connections",
    )
    args = parser.parse_args()
    if not args.addr:
        if args.host == "hostname":
            hostname = socket.gethostname()
        elif args.host == "fqdn":
            hostname = socket.getfqdn()
        else:
            hostname = args.host
        args.addr = f"{args.scheme}://{hostname}:{args.port}"
    return args


def main() -> None:
    args = parse_args()
    print(
        f"Starting monarch worker on address {args.addr} with ca={args.ca}. Ctrl-C to stop"
    )
    run_worker_loop_forever(address=args.addr, ca=args.ca)


if __name__ == "__main__":
    main()
