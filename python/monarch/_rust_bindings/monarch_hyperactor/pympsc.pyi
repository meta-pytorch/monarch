# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Generic, Protocol, TypeVar

T = TypeVar("T", covariant=True)
U = TypeVar("U")

class Receiver(Protocol[T]):
    def try_recv(self) -> T | None: ...
    async def recv(self) -> T: ...

class TestSender(Generic[U]):
    def send(self, value: U) -> None: ...

def channel_for_test() -> tuple[TestSender[Any], Receiver[Any]]: ...
