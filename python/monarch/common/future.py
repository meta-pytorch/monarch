# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import logging
import math
import os
import subprocess
from typing import (
    Any,
    Callable,
    cast,
    Generic,
    Optional,
    Sequence,
    TYPE_CHECKING,
    TypeVar,
)

from monarch_supervisor import TTL

if TYPE_CHECKING:
    from monarch.common.client import Client

from .invocation import RemoteException

logger = logging.getLogger(__name__)

try:
    PYSPY_REPORT_INTERVAL: Optional[float] = float(
        os.environ["CONTROLLER_PYSPY_REPORT_INTERVAL"]
    )
except KeyError:
    PYSPY_REPORT_INTERVAL = None


def _split(elems, cond):
    trues = []
    falses = []
    for elem in elems:
        if cond(elem):
            trues.append(elem)
        else:
            falses.append(elem)
    return trues, falses


def _periodic_TTL(interval: Optional[float]) -> Callable[[], float]:
    if interval is None:
        return lambda: math.inf

    ttl = TTL(interval)

    def _remaining():
        nonlocal ttl
        rem = ttl()
        if rem == 0:
            ttl = TTL(interval)
        return rem

    return _remaining


T = TypeVar("T")


class Future(Generic[T]):
    """A future object representing the result of an asynchronous computation.

    Future provides a way to access the result of a computation that may not
    have completed yet. It allows for non-blocking execution and provides
    methods to wait for completion and retrieve results.

    Args:
        client (Client): The client connection for handling the future
    """

    def __init__(self, client: "Client"):
        self._client = client
        self._status = "incomplete"
        self._callbacks = None
        self._result: T | Exception | None = None

    def _set_result(self, r):
        assert self._status == "incomplete"
        self._result = r
        self._status = "exception" if isinstance(r, RemoteException) else "complete"
        if self._callbacks:
            for cb in self._callbacks:
                try:
                    cb(self)
                except Exception:
                    logger.exception("exception in controller's Future callback")
        self._callbacks = None
        self._client = None

    def _wait(self, timeout: Optional[float]):
        if self._status != "incomplete":
            return True

        assert self._client is not None

        # see if the future is done already
        # and we just haven't processed the messages
        while self._client.handle_next_message(0):
            if self._status != "incomplete":
                return True

        ttl = TTL(timeout)
        ttl_pyspy = _periodic_TTL(PYSPY_REPORT_INTERVAL)
        while self._status == "incomplete" and _wait(self._client, ttl, ttl_pyspy):
            ...

        return self._status != "incomplete"

    def result(self, timeout: Optional[float] = None) -> T:
        if not self._wait(timeout):
            raise TimeoutError()
        if self._status == "exception":
            raise cast(Exception, self._result)
        return cast(T, self._result)

    def done(self) -> bool:
        return self._wait(0)

    def exception(self, timeout: Optional[float] = None):
        if not self._wait(timeout):
            raise TimeoutError()
        return self._result if self._status == "exception" else None

    def add_callback(self, callback):
        if not self._callbacks:
            self._callbacks = [callback]
        else:
            self._callbacks.append(callback)


def _wait(client: "Client", ttl: Callable[[], float], ttl_pyspy: Callable[[], float]):
    remaining = ttl()
    pyspy_remaining = ttl_pyspy()
    if pyspy_remaining == 0:
        try:
            logging.warning(
                f"future has not finished in {PYSPY_REPORT_INTERVAL} seconds (remaining time to live is {remaining}), py-spying process to debug."
            )
            subprocess.run(["py-spy", "dump", "-s", "-p", str(os.getpid())])
        except FileNotFoundError:
            logging.warning("py-spy is not installed.")
    timeout = min(remaining, pyspy_remaining)
    client.handle_next_message(timeout=None if timeout == math.inf else timeout)
    return remaining > 0


def stream(futures: Sequence[Future], timeout: Optional[float] = None):
    """Stream the provided futures as they complete.

    If a timeout is provided, it applies to the completion of the entire set of futures.
    """
    assert len(futures) > 0

    ttl = TTL(timeout)
    pyspy_ttl = _periodic_TTL(PYSPY_REPORT_INTERVAL)

    assert len({f._client for f in futures if f._client is not None}) <= 1, (
        "all futures must be from the same controller"
    )

    todo = futures
    while True:
        done, todo = _split(todo, lambda f: f._status != "incomplete")
        for f in done:
            yield f

        if len(todo) == 0 or not _wait(todo[0]._client, ttl, pyspy_ttl):
            break
