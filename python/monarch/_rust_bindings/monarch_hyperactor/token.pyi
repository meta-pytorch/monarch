# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from typing import final, Tuple

from monarch._rust_bindings.monarch_hyperactor.actor_mesh import PythonActorMesh
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.handle import Handle
from monarch._rust_bindings.monarch_hyperactor.shape import Region

@final
class PublishedToken:
    """A token for sharing an existing mesh, plus connection notifications.

    Share `token` with another process so it can call `connect()`.
    Use `next_peer()` to learn which actors have connected.

    The publishing actor or root client owns the rendezvous actor that
    handles connections. Keeping this object does not keep that actor alive.
    """

    @property
    def token(self) -> str:
        """The string another process passes to `connect()`."""
        ...

    def next_peer(self) -> Handle[str]:
        """Start waiting for the next connecting actor's address.

        Return a `Handle` resolving to that address as a string.
        Each call receives one notification; notifications are not broadcast.
        """
        ...

def create_token(instance: Instance, mesh: PythonActorMesh) -> PublishedToken:
    """Publish `mesh` using the actor represented by `instance`.

    Called by Python's `publish(mesh)`, which supplies the calling actor's
    instance, or the process's root client when called outside an actor.
    Create the rendezvous as that actor's child: stopping the publishing
    actor prevents further connections, without changing mesh ownership.

    Return a `PublishedToken` for sharing the token and receiving notifications.

    This does not wait for mesh creation. A pending mesh raises `ValueError`;
    wait for `initialized` first. A failed mesh propagates its creation error.
    A stopping or ended publishing actor raises `RuntimeError`.
    """
    ...

def connect_token(
    instance: Instance, token: str, timeout_secs: float
) -> Tuple[PythonActorMesh, Region]:
    """Send a join and return a pending mesh and its advertised region at once.

    Called by Python's `connect(token, Class, ...)`. Here `instance` is the
    connecting actor or root client, not the publishing actor. Send its
    address to the rendezvous without waiting for the published mesh reference.
    The token carries the region; `connect()` uses it and `Class` to build
    the Python wrapper. Endpoint calls wait for the join to complete.

    Invalid token text or timeout raises `ValueError` before sending.
    Initialization and reply-bearing calls fail with `ValueError` if the join
    is rejected, `TimeoutError` if no answer arrives within `timeout_secs`,
    or `RuntimeError` on a receive error or a region mismatch.
    """
    ...
