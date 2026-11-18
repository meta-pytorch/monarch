# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import asyncio
import unittest
from typing import Any

from monarch._rust_bindings.monarch_hyperactor.handle import Handle
from monarch._rust_bindings.monarch_hyperactor.testing import _make_delayed_handle
from monarch.actor import context
from monarch.benches import handle_microbench as bench


def setUpModule() -> None:
    context()


def _pending() -> Handle[Any]:
    # Publishes None once an observer registers, so as_asyncio() finds it
    # pending.
    return _make_delayed_handle(0.0)


class HandleMicrobenchTest(unittest.TestCase):
    def test_each_pending_await_is_attributed_to_its_loop_path(self) -> None:
        for fallback in (False, True):
            with self.subTest(fallback=fallback):
                latencies = bench.pending_awaits(_pending, None, 20, fallback)
                self.assertEqual(latencies.excluded, 0)
                self.assertEqual(len(latencies.samples), 20)

    def test_a_reused_loop_fails_the_other_paths_witness(self) -> None:
        # The registry caches the loop's first decision, so later awaits on it
        # keep that path whatever its add_reader now does.
        for first in (False, True):
            with self.subTest(first_fallback=first):
                loop = asyncio.new_event_loop()
                try:
                    bench.run_pending_awaits(loop, _pending, None, 5, first)
                    reused = bench.run_pending_awaits(
                        loop, _pending, None, 5, not first
                    )
                finally:
                    loop.close()
                self.assertEqual(reused.add_reader_calls, 0)
                self.assertEqual(reused.latencies.excluded, 5)
