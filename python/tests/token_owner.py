# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Helper program launched by `test_token.py` to test cross-process connections.

This program starts a `Probe` actor and publishes its mesh through a token.
The test, running in another process, uses that token to connect to `Probe`
and call its endpoints.

The test chooses one of two modes:

- `process DIR`: this helper calls `publish()` outside an actor. Monarch's
  built-in client actor owns the token; exiting this helper ends it.
- `actor DIR`: a `Publisher` actor calls `publish()`. The test can stop that
  actor and end the token while this helper process stays running.

`DIR` is the test's temporary directory. Both modes write `DIR/token`;
process mode also records connecting actors' addresses in `DIR/peers`.

Commands arrive through stdin. In actor mode, `stop` stops `Publisher`
and writes `DIR/stopped` after completion. `exit` or end of input exits
the helper process.
"""

import os
import sys
import threading
from _thread import LockType
from collections.abc import Callable
from typing import TypeVar

from monarch._rust_bindings.monarch_hyperactor.token import PublishedToken
from monarch._src.actor.token import publish
from monarch.actor import Actor, endpoint, this_proc

TResult = TypeVar("TResult")


class State:
    """A value kept in the serving process to test shared remote state.

    Separate connections to `Probe` must see each other's changes to it.
    """

    def __init__(self) -> None:
        """Start with zero and a lock shared by readers and writers."""
        # Readers and writers use the same lock to protect `value`.
        self.lock: LockType = threading.Lock()
        # The value clients inspect and change through the probe's endpoints.
        self.value: int = 0

    def set(self, value: int) -> int:
        """Replace the value and return the value immediately before this update."""
        with self.lock:
            # Read and replace together, before another caller can change it.
            previous, self.value = self.value, value
        return previous


# Each process importing this helper has its own `STATE`. Every `Probe` in
# that process uses this same object, rather than a copy passed during spawn.
STATE: State = State()


class Probe(Actor):
    """The actor reached by clients connecting through the published token.

    Its endpoints let the test read shared state and run a function in the
    serving process, proving that calls reach the existing actor.
    """

    @endpoint
    def read(self) -> int:
        """Return this process's shared value while holding its lock."""
        with STATE.lock:
            return STATE.value

    @endpoint
    def run(self, fn: Callable[[State], TResult]) -> TResult:
        """Run a trusted test function against this process's shared state.

        Return the function's result. Tests use this to change the state
        or report the serving process's PID, not the connecting client's PID.
        """
        return fn(STATE)


class Publisher(Actor):
    """Publish a token owned by an actor the test can stop independently.

    Used in actor mode: stopping this actor ends its token while the helper
    process remains running.
    """

    # Retain the token string and its connection-notification receiver.
    _creator: PublishedToken

    @endpoint
    def publish_probe(self) -> str:
        """Start a probe, publish its mesh, and return the token to the test."""
        probe = this_proc().spawn("probe", Probe)
        # `publish()` requires an existing mesh; wait for creation to finish.
        probe.initialized.get()
        # Called inside this actor, so its rendezvous actor belongs to `self`.
        self._creator = publish(probe)
        return self._creator.token


def write(path: str, text: str) -> None:
    """Write a token or acknowledgement file for the test process to read."""
    with open(path, "w") as f:
        f.write(text)


def report_peers(creator: PublishedToken, path: str) -> None:
    """Record connecting actors' addresses for the test to count.

    Runs in this helper's background thread in process mode. `path` is the
    `peers` file in the test's temporary directory, not a remote actor path.
    """
    while True:
        # Each Handle waits for one notification; `get()` blocks this thread
        # until a connecting actor's address arrives.
        address = creator.next_peer().get()
        with open(path, "a") as f:
            f.write(address + "\n")


def main() -> None:
    mode, directory = sys.argv[1], sys.argv[2]
    publisher: Publisher | None = None
    if mode == "process":
        probe = this_proc().spawn("probe", Probe)
        probe.initialized.get()
        creator = publish(probe)
        token = creator.token
        threading.Thread(
            target=report_peers,
            args=(creator, os.path.join(directory, "peers")),
            daemon=True,
        ).start()
    else:
        publisher = this_proc().spawn("publisher", Publisher)
        token = publisher.publish_probe.call_one().get(timeout=30)
    write(os.path.join(directory, "token"), token)

    for line in sys.stdin:
        command = line.strip()
        if command == "stop" and publisher is not None:
            publisher.stop().get(timeout=30)
            write(os.path.join(directory, "stopped"), "stopped")
        elif command == "exit":
            break
    os._exit(0)


if __name__ == "__main__":
    # A worker cannot import this script's `__main__` actor classes. Load a
    # second copy as `token_owner` and call its `main()`, so the actors pickle
    # by their importable module name rather than shipping their definitions.
    import token_owner

    token_owner.main()
