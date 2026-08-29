# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import pytest
from monarch._src.actor.future import Future


async def _return_value(v: object) -> object:
    return v


async def _raise_error(msg: str) -> object:
    raise ValueError(msg)


def test_gather_sync() -> None:
    f1 = Future(coro=_return_value(1))
    f2 = Future(coro=_return_value(2))
    f3 = Future(coro=_return_value(3))
    results = Future.gather(f1, f2, f3).get()
    assert results == [1, 2, 3]


def test_gather_preserves_order() -> None:
    futures = [Future(coro=_return_value(i)) for i in range(10)]
    results = Future.gather(*futures).get()
    assert results == list(range(10))


def test_gather_empty() -> None:
    results = Future.gather().get()
    assert results == []


def test_gather_single() -> None:
    result = Future.gather(Future(coro=_return_value(42))).get()
    assert result == [42]


def test_gather_error_propagation() -> None:
    f1 = Future(coro=_return_value(1))
    f2 = Future(coro=_raise_error("boom"))
    f3 = Future(coro=_return_value(3))
    with pytest.raises(ValueError, match="boom"):
        Future.gather(f1, f2, f3).get()


def test_gather_return_exceptions() -> None:
    f1 = Future(coro=_return_value(1))
    f2 = Future(coro=_raise_error("boom"))
    f3 = Future(coro=_return_value(3))
    results = Future.gather(f1, f2, f3, return_exceptions=True).get()
    assert results[0] == 1
    assert isinstance(results[1], ValueError)
    assert str(results[1]) == "boom"
    assert results[2] == 3


def test_gather_consumed_future() -> None:
    f1 = Future(coro=_return_value(1))
    Future.gather(f1)
    with pytest.raises(ValueError, match="consumed"):
        f1.get()


def test_gather_already_consumed_raises() -> None:
    f1 = Future(coro=_return_value(1))
    f1.get()
    with pytest.raises(ValueError, match="already been consumed"):
        Future.gather(f1)


async def test_gather_async() -> None:
    f1 = Future(coro=_return_value(10))
    f2 = Future(coro=_return_value(20))
    results = await Future.gather(f1, f2)
    assert results == [10, 20]
