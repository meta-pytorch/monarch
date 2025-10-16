# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import time
from typing import Callable

import pytest
from monarch import Future, RemoteException
from monarch._rust_bindings.monarch_hyperactor.proc import (  # @manual=//monarch/monarch_extension:monarch_extension
    ActorId,
)
from monarch.common import future
from monarch.common.client import Client


class TestFuture:
    def test_future(self, monkeypatch: pytest.MonkeyPatch) -> None:
        the_time: int = 0
        the_messages: list[tuple[int | float, Callable[[], None]]] = []

        class MockClient(Client):
            def __init__(self):
                pass

            def handle_next_message(self, timeout) -> bool:
                nonlocal the_time
                if not the_messages:
                    return False
                time, action = the_messages[0]
                if timeout is None or time <= the_time + timeout:
                    the_time = time
                    action()
                    the_messages.pop(0)
                    return True
                else:
                    the_time += timeout
                    return False

            def _request_status(self):
                pass

        client: Client = MockClient()

        def mock_time() -> int:
            return the_time

        monkeypatch.setattr(time, "time", mock_time)
        f = Future(client)
        the_messages = [(1, lambda: f._set_result(4))]
        assert not f.done()
        with pytest.raises(TimeoutError):
            f.result(timeout=0.5)
        assert 4 == f.result(timeout=1)
        assert f.exception() is None
        assert f.done()
        f = Future(client)
        the_messages = [(1, lambda: None), (2, lambda: f._set_result(3))]
        the_time = 0
        assert 3 == f.result()
        f = Future(client)
        re = RemoteException(
            0, Exception(), None, [], [], ActorId.from_string("unknown[0].unknown[0]")
        )

        the_messages = [(1, lambda: None), (2, lambda: f._set_result(re))]
        the_time = 0
        assert f.exception() is not None

        f = Future(client)
        the_messages = [(0, lambda: None), (0.2, lambda: f._set_result(7))]
        the_time = 0
        assert 7 == f.result(timeout=0.3)

        fs = []

        def setup() -> None:
            nonlocal fs, the_messages
            fs = [Future(client) for _ in range(4)]

            # To avoid closure binding gotcha.
            def set_at_time(f: Future, time: int) -> tuple[int, Callable[[], None]]:
                return (time, lambda: f._set_result(time))

            the_messages = [set_at_time(f, time) for time, f in enumerate(fs)]

        setup()
        assert {f.result() for f in future.stream(fs, timeout=2)} == {0, 1, 2}

        setup()
        assert {f.result() for f in future.stream(fs, timeout=3)} == {0, 1, 2, 3}
