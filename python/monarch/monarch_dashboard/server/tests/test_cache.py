# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Unit tests for the TTL cache module."""

import time
import unittest

from monarch.monarch_dashboard.server import cache


class CacheTest(unittest.TestCase):
    def setUp(self):
        cache._cache.clear()

    def test_cache_miss_calls_fn(self):
        calls = []

        def fn():
            calls.append(1)
            return {"data": 42}

        result = cache.cached("key1", fn)
        self.assertEqual(result, {"data": 42})
        self.assertEqual(len(calls), 1)

    def test_cache_hit_returns_cached_value(self):
        calls = []

        def fn():
            calls.append(1)
            return len(calls)

        first = cache.cached("key1", fn)
        second = cache.cached("key1", fn)
        self.assertEqual(first, 1)
        self.assertEqual(second, 1)
        self.assertEqual(len(calls), 1)

    def test_different_keys_are_independent(self):
        result_a = cache.cached("a", lambda: "alpha")
        result_b = cache.cached("b", lambda: "beta")
        self.assertEqual(result_a, "alpha")
        self.assertEqual(result_b, "beta")

    def test_expired_entry_recomputes(self):
        calls = []

        def fn():
            calls.append(1)
            return len(calls)

        cache.cached("key1", fn, ttl=0.05)
        self.assertEqual(len(calls), 1)

        time.sleep(0.06)

        result = cache.cached("key1", fn, ttl=0.05)
        self.assertEqual(result, 2)
        self.assertEqual(len(calls), 2)

    def test_fn_exception_propagates(self):
        def fn():
            raise ValueError("boom")

        with self.assertRaises(ValueError):
            cache.cached("key1", fn)

        # Failed call should not cache anything
        self.assertNotIn("key1", cache._cache)

    def test_fn_exception_does_not_cache(self):
        """After a failed fn, a subsequent call should retry."""
        calls = []

        def fn():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("first call fails")
            return "success"

        with self.assertRaises(RuntimeError):
            cache.cached("key1", fn)

        result = cache.cached("key1", fn)
        self.assertEqual(result, "success")
        self.assertEqual(len(calls), 2)
