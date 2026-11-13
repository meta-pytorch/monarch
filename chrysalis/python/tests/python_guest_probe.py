# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Reports a Python program's view from inside a Chrysalis Firecracker guest.

`test_python_guest` runs this in a guest built by `python_guest_rootfs` and
checks what it prints: its Python version, the guest's network interfaces and
which of them are up, whether it can write the root filesystem, and whether
two Chrysalis nodes can exchange a message over the guest's loopback.
"""

import asyncio
import errno
import socket
import sys
from pathlib import Path

from chrysalis._rust_bindings import (
    ChrysalisError,
    generate_test_identities,
    Identity,
    Node,
    Pid,
    Stream,
)

MESSAGE = b"hello from the guest"
REPLY = b"hello back"
# `IFF_UP` in `linux/if.h`.
IFF_UP = 0x1


def report_surface() -> None:
    """Print the guest's interfaces, those that are up, and whether the root
    filesystem can be written."""
    names = [name for _, name in sorted(socket.if_nameindex())]
    print(f"interfaces: {','.join(names)}", flush=True)
    up = [
        name
        for name in names
        if int(Path(f"/sys/class/net/{name}/flags").read_text(), 16) & IFF_UP
    ]
    print(f"up: {','.join(up)}", flush=True)
    probe = Path("/.python-guest-probe")
    try:
        with open(probe, "x"):
            pass
    except OSError as e:
        name = errno.errorcode[e.errno] if e.errno is not None else str(e)
        print(f"root write: {name}", flush=True)
    else:
        probe.unlink()
        print("root write: ok", flush=True)


async def dial_when_visible(node: Node, pid: Pid, within: float = 10.0) -> Stream:
    """Open a stream to `pid` once `node` can see it."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while True:
        try:
            return await node.dial(pid)
        except ChrysalisError:
            if loop.time() > deadline:
                raise
            await asyncio.sleep(0.05)


async def read_to_end(stream: Stream) -> bytes:
    """Read until the other end finishes sending."""
    chunks = []
    while chunk := await stream.read():
        chunks.append(chunk)
    return b"".join(chunks)


async def exchange() -> bool:
    """Run a root node and a child that joins it, both on loopback UDP. The
    child sends `MESSAGE` to the root, which replies with `REPLY`. Return
    whether the root got the message from the child and the child got the
    reply."""
    root_identity, child_identity = (
        Identity.from_pem(bundle) for bundle in generate_test_identities(2)
    )
    root = Node(root_identity)
    child = Node(child_identity, parent=(root_identity.pid, root.addresses[0]))

    async def root_replies() -> bool:
        source, stream = await root.accept()
        received = await read_to_end(stream)
        await stream.write(REPLY)
        await stream.finish()
        return source == child_identity.pid and received == MESSAGE

    async def child_sends() -> bool:
        stream = await dial_when_visible(child, root_identity.pid)
        await stream.write(MESSAGE)
        await stream.finish()
        return await read_to_end(stream) == REPLY

    try:
        results = await asyncio.wait_for(
            asyncio.gather(root_replies(), child_sends()), 30
        )
        return all(results)
    finally:
        await asyncio.gather(root.close(), child.close())


def main() -> None:
    print(f"python {sys.version_info.major}.{sys.version_info.minor}", flush=True)
    report_surface()
    if not asyncio.run(exchange()):
        raise SystemExit(1)
    print("probe exchange: ok", flush=True)


if __name__ == "__main__":
    main()
