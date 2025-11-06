# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import functools
import importlib
import importlib.abc
import linecache

from monarch._src.actor.actor_mesh import _context, Actor
from monarch._src.actor.endpoint import endpoint
from monarch._src.actor.proc_mesh import get_or_spawn_controller
from monarch._src.actor.sync_state import fake_sync_state


class SourceLoaderController(Actor):
    @endpoint
    def get_source(self, filename: str) -> str:
        return "".join(linecache.getlines(filename))


@functools.cache
def source_loader_controller() -> SourceLoaderController:
    with fake_sync_state():
        return get_or_spawn_controller("source_loader", SourceLoaderController).get()


@functools.cache
def load_remote_source(filename: str) -> str:
    with fake_sync_state():
        return source_loader_controller().get_source.call_one(filename).get()


class RemoteImportLoader(importlib.abc.Loader):
    def __init__(self, filename: str) -> None:
        self._filename = filename

    def get_source(self, _module_name: str) -> str:
        if _context.get(None) is not None:
            return load_remote_source(self._filename)
        else:
            raise ImportError(f"could not get source for {self._filename}")
