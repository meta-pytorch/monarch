# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Let another process connect to existing actors using a token string.

Call `publish(mesh)` and share the returned object's `token` string.
Another process calls `connect(token, Class)` to obtain a mesh reference
and call those actors' endpoints. Connecting does not create actors or
add them to the mesh.

A small rendezvous actor handles connection requests. It belongs to the
actor that calls `publish()`, or to the process's root client when called
outside an actor. Stopping that publisher prevents new connections.
The publisher need not be one of the actors in the mesh.

The token identifies the mesh, but does not provide its Python actor class
or that class's code. The connecting process must already have the class
available and pass it to `connect(token, Class)`. Python uses this class
to expose the endpoint methods; calls reach the existing actors.
The caller must supply the class used to spawn the mesh; the token does
not check that choice.

Use trusted tokens on a trusted network. Connected callers can invoke
the published actors' endpoints. If an endpoint executes a function
supplied by a caller, that function runs in the serving actor's process
with that process's permissions.
"""

from typing import cast, Type, TypeVar

from monarch._rust_bindings.monarch_hyperactor.actor_mesh import PythonActorMesh
from monarch._rust_bindings.monarch_hyperactor.token import (
    connect_token,
    create_token,
    PublishedToken,
)
from monarch._src.actor.actor_mesh import Actor, ActorMesh, context

TActor = TypeVar("TActor", bound=Actor)


def publish(mesh: Actor) -> PublishedToken:
    """Create a token that lets other processes connect to `mesh`.

    Share the returned object's `token` string with callers of `connect()`.
    Use its `next_peer()` method to receive connecting actors' addresses.

    The calling actor owns the rendezvous actor behind the token. Outside
    an actor, the process's root client owns it instead. Stopping that actor
    or root client prevents new connections. Mesh ownership is unchanged.

    Wait for `mesh.initialized` before publishing. A pending mesh raises
    `ValueError`; a failed mesh propagates its creation error. A stopping
    or ended publisher raises `RuntimeError`. A mesh not backed by the native
    Python actor binding raises `TypeError`.

    A mesh returned by `connect()` can be published after its join completes;
    wait for its `initialized` first.
    """
    inner = cast(ActorMesh, mesh)._inner
    if not isinstance(inner, PythonActorMesh):
        raise TypeError(f"a token can only publish a spawned actor mesh, not {inner!r}")
    return create_token(context().actor_instance._as_rust(), inner)


def connect(
    token: str, Class: Type[TActor], *, name: str = "connected", timeout: float = 10.0
) -> TActor:
    """Connect to the actors published through `token` and return their mesh.

    Pass the actor class used to spawn the mesh as `Class`. It must already
    be available in this process; Python uses it to expose the endpoint methods.
    The returned mesh preserves the published ranks and dimensions. No actors
    are created or added.

    The publisher receives the calling actor's address, or the process's root
    client address when called outside an actor. `name` labels the local mesh
    wrapper; it does not rename the remote actors.

    Return immediately, before the connection completes. Calls on the mesh
    queue until the publisher replies. `timeout` bounds that join, not later
    endpoint calls; it starts without waiting for an observation.

    Invalid tokens or timeouts raise `ValueError` here, before sending.
    A rejected connection raises `ValueError` from `initialized` and
    reply-bearing calls; no reply within the deadline raises `TimeoutError`
    there. A receive error or a token whose region disagrees with the published
    mesh raises `RuntimeError` there. Fire-and-forget broadcasts return no error.
    """
    inner, region = connect_token(context().actor_instance._as_rust(), token, timeout)
    return cast(TActor, ActorMesh(Class, name, inner, region.as_shape(), None))
