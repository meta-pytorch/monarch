# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Helper run as a subprocess by test_smoke.py's multi-gateway QUIC tests.

Usage: ``python gateway_worker.py <root-url> <b-url> <a-tag>``. The QUIC TLS
material is read from the ``MM_QUIC_CERT`` / ``MM_QUIC_KEY`` / ``MM_QUIC_CA``
environment variables, which the parent test process sets and the worker
inherits.

This worker is gateway ``gwB`` (specifier = the authority of ``<b-url>``): it
joins the shared root, serves its own address so a sibling gateway can
side-channel to it, hosts an inproc child ``b1@<b-tag>``, and — on receiving the
cross-gateway ``ping`` — replies ``pong`` directly to ``a1@<a-tag>`` (a direct
gateway-to-gateway reply). It then idles so the reply flushes while it stays
alive; the parent test kills it when finished.
"""

import asyncio
import sys

import minimonarch
from minimonarch import Actor

ba = minimonarch.bytearray


async def run_gwb(root_url: str, b_url: str, a_tag: str) -> None:
    b_tag = b_url.split("://", 1)[1]
    gwb = Actor(f"gwB@{b_tag}".encode(), gateway=True)
    gwb.serve(b_url, "parent")  # listener so siblings can side-channel to us
    gwb.join(root_url, "child")  # join the shared root
    b1 = Actor(f"b1@{b_tag}".encode())
    gwb.serve("inproc://b-b1", "parent")
    b1.join("inproc://b-b1", "child")

    # Drain the establishment hellos for gwB and b1.
    await gwb.next()
    await b1.next()

    # Await the cross-gateway ping, then reply pong straight back to a1@<a-tag>
    # (gwB opens its own direct side-channel to gateway A).
    await b1.next()
    b1.send(f"a1@{a_tag}".encode(), [ba(b"pong")])

    # Idle so the reply side-channel connects and flushes while we stay alive; the
    # parent test kills us once it has observed the reply.
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    root_url, b_url, a_tag = sys.argv[1], sys.argv[2], sys.argv[3]
    asyncio.run(run_gwb(root_url, b_url, a_tag))
