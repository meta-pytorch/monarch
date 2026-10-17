# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Root shutdown must run cleanup on actors co-located with the client."""

import tempfile
from pathlib import Path

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._src.actor.actor_mesh import context
from monarch.actor import Actor, endpoint, shutdown_context, this_proc


class CleanupMarker(Actor):
    def __init__(self, marker: str, child_marker: str | None = None) -> None:
        self.marker = Path(marker)
        self.child_marker = None if child_marker is None else Path(child_marker)
        self.child = (
            None
            if child_marker is None
            else this_proc().spawn("grandchild", CleanupMarker, child_marker)
        )

    @endpoint
    def ready(self) -> bool:
        return self.child is None or self.child.ready.call_one().get(timeout=30)

    def __cleanup__(self, exc: Exception | None) -> None:
        if self.child_marker is not None:
            assert self.child_marker.exists(), "owned child cleanup must finish first"
        self.marker.write_text("cleaned up\n")


@isolate_in_subprocess
def test_root_shutdown_waits_for_colocated_actor_cleanup() -> None:
    with tempfile.TemporaryDirectory() as directory:
        markers = [Path(directory) / str(rank) for rank in range(3)]
        actors = [
            this_proc().spawn("cleanup_0", CleanupMarker, str(markers[0])),
            this_proc().spawn(
                "cleanup_1", CleanupMarker, str(markers[1]), str(markers[2])
            ),
        ]
        for actor in actors:
            assert actor.ready.call_one().get(timeout=30)
        shutdown_context().get(timeout=30)
        assert all(marker.read_text() == "cleaned up\n" for marker in markers)


@isolate_in_subprocess
def test_root_shutdown_settles_pending_port_receives() -> None:
    mailbox = context().actor_instance._mailbox
    # Keep the senders alive: shutdown must wake receivers even if the user
    # still holds handles or the static client instance retains its mailbox.
    _port, receiver = mailbox.open_port()
    _once_port, once_receiver = mailbox.open_once_port()
    pending = [
        receiver.recv(False).spawn_handle(),
        once_receiver.recv(False).spawn_handle(),
    ]
    shutdown_context().get(timeout=30)
    for handle in pending:
        with pytest.raises(EOFError, match="Port closed"):
            handle.get(timeout=2)
