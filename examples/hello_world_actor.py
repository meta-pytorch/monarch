import asyncio
import importlib.resources
import logging
import operator
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import unittest.mock
from types import ModuleType
from typing import cast

import pytest

import torch
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask

from monarch._src.actor.actor_mesh import ActorMesh, Channel, Port

from monarch.actor import (
    Accumulator,
    Actor,
    current_actor_name,
    current_rank,
    current_size,
    endpoint,
    local_proc_mesh,
    proc_mesh,
)
from monarch.tools.config import defaults
from typing_extensions import assert_type


class Counter(Actor):
    def __init__(self, v: int):
        self.v = v

    @endpoint
    async def incr(self):
        self.v += 1

    @endpoint
    async def value(self) -> int:
        return self.v

    @endpoint
    def value_sync_endpoint(self) -> int:
        return self.v


async def main():
    proc = await proc_mesh(gpus=2)
    am = await proc.spawn("counter", Counter, 1)
    await am.incr.call()

    # v = await proc.spawn("counter2", Counter, 3)
    # v.incr.broadcast()
    # assert 8 == sum([await x for x in v.value.stream()])

if __name__ == "__main__":
    asyncio.run(main())