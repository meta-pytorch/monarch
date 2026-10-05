# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Minimal actor fault injection for chaos testing.

Enter the injection context before creating the process meshes that will host
the selected actors. Each patched actor periodically rolls for failure on its
event loop. A successful roll aborts the actor and reports a supervision error
to its owner.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import math
import random
from collections.abc import Generator, Mapping
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
from typing import cast

from monarch._src.actor.actor_mesh import Instance
from monarch._src.actor.mock import patch_actor
from monarch.actor import Actor, context

logger: logging.Logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActorFailurePolicy:
    probability: float
    interval_seconds: float = 1.0

    def __post_init__(self) -> None:
        if (
            isinstance(self.probability, bool)
            or not math.isfinite(self.probability)
            or not 0.0 <= self.probability <= 1.0
        ):
            raise ValueError("actor failure probability must be between zero and one")

        if (
            isinstance(self.interval_seconds, bool)
            or not math.isfinite(self.interval_seconds)
            or self.interval_seconds <= 0.0
        ):
            raise ValueError("actor failure interval must be positive and finite")


@dataclass
class _ActorFailureTimer:
    actor_instance: Instance
    actor_name: str
    policy: ActorFailurePolicy

    def arm(self) -> None:
        asyncio.get_running_loop().call_later(
            self.policy.interval_seconds,
            self.maybe_fail,
        )

    def maybe_fail(self) -> None:
        if random.random() < self.policy.probability:
            reason = f"periodic chaos check selected actor={self.actor_name}"
            logger.warning("injecting actor failure: %s", reason)
            self.actor_instance._inject_failure(reason)
            return

        self.arm()


def _fault_injected_actor_class(
    actor_type: type[Actor],
    policy: ActorFailurePolicy,
) -> type[Actor]:
    original_init = actor_type.__init__

    @functools.wraps(original_init)
    def injected_init(actor: Actor, *args: object, **kwargs: object) -> None:
        original_init(actor, *args, **kwargs)

        actor_instance = context().actor_instance
        _ActorFailureTimer(
            actor_instance,
            str(actor_instance.name),
            policy,
        ).arm()

    namespace: dict[str, object] = {
        "__module__": actor_type.__module__,
        "__qualname__": actor_type.__qualname__,
        "__doc__": actor_type.__doc__,
        "__init__": injected_init,
    }
    return cast(type[Actor], type(actor_type.__name__, (actor_type,), namespace))


@contextmanager
def inject_actor_failures(
    policies: Mapping[type[Actor], ActorFailurePolicy],
) -> Generator[None, None, None]:
    """Periodically give each selected actor a chance to crash."""
    with ExitStack() as stack:
        for actor_type, policy in policies.items():
            injected_type = _fault_injected_actor_class(actor_type, policy)
            stack.enter_context(patch_actor(actor_type, injected_type))
        yield
