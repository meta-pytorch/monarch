# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import final, Optional, Type

from monarch._rust_bindings.monarch_hyperactor.actor import Actor, PythonActorHandle

def init_proc(
    *,
    proc_id: str,
    bootstrap_addr: str,
    timeout: int = 5,
    supervision_update_interval: int = 0,
    listen_addr: Optional[str] = None,
) -> Proc:
    """
    Helper function to bootstrap a new Proc.

    Arguments:
    - `proc_id`: String representation of the ProcId eg. `"world_name[0]"`
    - `bootstrap_addr`: String representation of the channel address of the system
        actor. eg. `"tcp![::1]:2345"`
    - `timeout`: Number of seconds to wait to successfully connect to the system.
    - `supervision_update_interval`: Number of seconds between supervision updates.
    - `listen_addr`: String representation of the channel address of the proc
        actor. eg. `"tcp![::1]:2345"`
    """
    ...
@final
class Serialized:
    """
    An opaque wrapper around a message that hhas been serialized in a hyperactor
    friendly manner.
    """

    ...

@final
class ActorId:
    """
    A python wrapper around hyperactor ActorId. It represents a unique reference
    for an actor.

    Arguments:
    - `world_name`: The world the actor belongs in (same as the Proc containing the Actor)
    - `rank`: The rank of the proc containing the actor.
    - `actor_name`: Name of the actor.
    - `pid`: The pid of the actor.
    """

    def __init__(
        self, *, world_name: str, rank: int, actor_name: str, pid: int = 0
    ) -> None: ...
    def __str__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    @property
    def world_name(self) -> str:
        """The world the actor belongs in (same as the Proc containing the Actor)"""
        ...

    @property
    def rank(self) -> int:
        """The rank of the proc containing the actor."""
        ...

    @property
    def actor_name(self) -> str:
        """Name of the actor."""
        ...

    @property
    def pid(self) -> int:
        """The pid of the actor."""
        ...

    @property
    def proc_id(self) -> str:
        """String representation of the ProcId eg. `"world_name[0]"`"""
        ...

    @staticmethod
    def from_string(actor_id_str: str) -> ActorId:
        """
        Create an ActorId from a string representation.

        Arguments:
        - `actor_id_str`: String representation of the actor id.
        """
        ...

@final
class Proc:
    """
    A python wrapper around hyperactor Proc. This is the root container
    for all actors in the process.
    """

    def __init__(self) -> None:
        """Create a new Proc."""
        ...

    @property
    def world_name(self) -> str:
        """The world the Proc is a part of."""
        ...

    @property
    def rank(self) -> int:
        """Rank of the Proc in that world."""
        ...

    def destroy(self, timeout_in_secs: int) -> list[str]:
        """Destroy the Proc."""
        ...

    async def spawn(self, actor: Type[Actor]) -> PythonActorHandle:
        """
        Spawn a new actor.

        Arguments:
        - `actor_name`: Name of the actor.
        - `actor`: The type of the actor, which
        """
        ...
