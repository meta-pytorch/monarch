# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from monarch._src.actor.waker import TestWaker


async def test_ping_pong():
    import asyncio
    import threading

    waker1, event1 = TestWaker.create()
    waker2, event2 = TestWaker.create()
    counter = [0]

    N = 100

    def thread2():
        async def run():
            for _ in range(N):
                await event2.wait()
                counter[0] += 1
                event2.clear()
                waker1.wake()

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(run())
        finally:
            loop.close()

    t = threading.Thread(target=thread2)
    t.start()

    for i in range(N):
        event1.clear()
        waker2.wake()
        await event1.wait()
        assert counter[0] == i + 1

    t.join()
    assert counter[0] == N
