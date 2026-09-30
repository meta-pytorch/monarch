# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Cross-platform tests for RemoteMount's client-side broadcast transport."""

import ctypes

from monarch._rust_bindings.monarch_extension.chain_broadcast import (
    connect,
    ctx_close,
    ctx_wait_into,
    forward,
    new_ctx,
    send_block,
    serve,
)


def test_chain_broadcast_local_round_trip() -> None:
    """The transport must be available wherever a RemoteMount client can run."""
    payload = bytes(range(256)) * 17
    destination = ctypes.create_string_buffer(len(payload))
    context = new_ctx(len(payload))
    server = serve("tcp!127.0.0.1:0")

    try:
        forward(server, None, context, 2)
        session = connect(server.addr, 2)
        assert send_block(session, payload, 257, 0) == len(payload)
        ctx_wait_into(
            context,
            ctypes.addressof(destination),
            len(destination),
            len(payload),
            10_000,
        )
        assert destination.raw == payload
    finally:
        ctx_close(context)
