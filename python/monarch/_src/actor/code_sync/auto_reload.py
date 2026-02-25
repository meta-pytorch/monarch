# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import contextlib
import dataclasses
import importlib
import importlib.abc
import importlib.util
import itertools
import site
import sys
import threading
from pathlib import Path
from types import ModuleType, TracebackType
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from monarch._src.actor.actor_mesh import Actor
from monarch._src.actor.endpoint import endpoint


class SysAuditHookGuard(contextlib.AbstractContextManager["SysAuditHookGuard", None]):
    """
    A guard (and context manager), which will unregister an import hook when
    closed or deleted.
    """

    def __init__(
        self, hooks: Dict[int, Callable[[str, tuple[Any, ...]], None]], idx: int
    ) -> None:
        self._hooks: Dict[int, Callable[[str, tuple[Any, ...]], None]] = hooks
        self._idx: int = idx

    def close(self) -> None:
        self._hooks.pop(self._idx, None)

    def __enter__(self) -> "SysAuditHookGuard":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


class SysAuditHookMultiplexer:
    """
    Multiplexes import hooks to multiple hooks.

    `sys.addaudithook`s can only be added and not removed, so this class provides
    a global singleton that can be used to multiplex multiple hooks which support
    removal.
    """

    def __init__(self) -> None:
        self._idx: itertools.count[int] = itertools.count()
        self._hooks: Dict[int, Callable[[str, tuple[Any, ...]], None]] = {}

    def _callback(self, event: str, args: tuple[Any, ...]) -> None:
        # Guard against being called during interpreter shutdown when
        # self._hooks may have been garbage collected or cleared
        hooks = self._hooks
        if hooks is None:
            return
        # Copy values to avoid iteration issues if hooks dict is modified
        for hook in list(hooks.values()):
            try:
                hook(event, args)
            except Exception:
                # Swallow errors during shutdown to prevent crashes
                pass

    def add(self, hook: Callable[[str, tuple[Any, ...]], None]) -> SysAuditHookGuard:
        idx = next(self._idx)
        self._hooks[idx] = hook
        return SysAuditHookGuard(self._hooks, idx)

    _instance_lock = threading.Lock()
    _instance: Optional["SysAuditHookMultiplexer"] = None

    @classmethod
    def singleton(cls) -> "SysAuditHookMultiplexer":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = SysAuditHookMultiplexer()
                    sys.addaudithook(cls._instance._callback)
        assert cls._instance is not None
        return cls._instance


@dataclasses.dataclass
class ThreadLocalState(threading.local):
    last_import: Optional[str] = None


class SysAuditImportHook:
    """
    An audit hook that processes and coalesces import/exec events and calls a
    user-defined callback with the module name and module object which was
    imported.
    """

    def __init__(self, callback: Callable[[str, ModuleType], None]) -> None:
        self._callback: Callable[[str, ModuleType], None] = callback
        self._state = ThreadLocalState()

    @classmethod
    def install(cls, callback: Callable[[str, ModuleType], None]) -> SysAuditHookGuard:
        return SysAuditHookMultiplexer.singleton().add(SysAuditImportHook(callback))

    def _py_filename(self, filename: Path) -> Path:
        if filename.suffix in (".pyc", ".pyo"):
            return filename.with_suffix(".py")
        return filename

    def __call__(self, event: str, args: tuple[Any, ...]) -> None:
        if event == "import":
            # While `filename` is specific as an argument to the import event, it's
            # almost always `None`, so we need to wait for a subsequent exec event
            # to get the filename.
            module, _, _, _, _ = args
            self._state.last_import = module
        elif event == "exec":
            module_name = self._state.last_import
            if module_name is None:
                return
            # We always expect an exec right after an import, so we can clear the
            # last import module name we store.
            self._state.last_import = None
            module = sys.modules.get(module_name)
            if module is None:
                return
            module_file = getattr(module, "__file__", None)
            if module_file is None:
                return
            (code_obj,) = args
            if code_obj.co_filename is None:
                return
            # code objects store the original source name, not the pyc
            if self._py_filename(Path(module_file)) != Path(code_obj.co_filename):
                return
            self._callback(module_name, module)


@dataclasses.dataclass(frozen=True, kw_only=True)
class Fingerprint:
    mtime: float
    size: int

    @classmethod
    def for_path(cls, path: Path) -> "Fingerprint":
        stat = path.stat()
        return Fingerprint(mtime=stat.st_mtime, size=stat.st_size)


class AutoReloader:
    """
    Track changes to modules and reloads them when they change.
    """

    def __init__(
        self, reload: Callable[[ModuleType], ModuleType] = importlib.reload
    ) -> None:
        self._reload: Callable[[ModuleType], ModuleType] = reload
        self._tracked_modules: Dict[str, Tuple[Path, Fingerprint]] = {}
        self._track_all_imported()

    def _maybe_track_module(self, name: str, module: ModuleType) -> None:
        filename = getattr(module, "__file__", None)
        if filename is None:
            return
        if filename == "static-extension":
            return
        filename = Path(filename)

        # It's rare for modules to have relative path names, but can happen in
        # weird special situations (e.g. `_ops.py` from `torch.ops`).
        if not filename.is_absolute():
            return

        # Ignore builtin modules.
        if filename.is_relative_to(sys.prefix):
            for dirpath in site.getsitepackages():
                if filename.is_relative_to(dirpath):
                    break
            else:
                return

        self._tracked_modules[name] = (
            filename,
            Fingerprint.for_path(filename),
        )

    def _track_all_imported(self) -> None:
        for name, module in sys.modules.items():
            if module is None:
                continue
            self._maybe_track_module(name, module)

    def import_callback(self, name: str, module: ModuleType) -> None:
        """
        Callback for when a module has been imported.
        """

        self._maybe_track_module(name, module)

    def reload_changes(self) -> List[str]:
        """
        Reload all modules that have changed since they were last imported.
        """

        reloaded = []

        for module_name, (filename, stored_fingerprint) in list(
            self._tracked_modules.items()
        ):
            fingerprint = Fingerprint.for_path(filename)
            if fingerprint == stored_fingerprint:
                continue
            reloaded.append(module_name)
            self._reload(sys.modules[module_name])
            self._tracked_modules[module_name] = (filename, fingerprint)

        return reloaded


class AutoReloadActor(Actor):
    def __init__(self) -> None:
        self._reloader = AutoReloader()
        self._hook_guard: SysAuditHookGuard = SysAuditImportHook.install(
            self._reloader.import_callback
        )

    @endpoint
    async def reload(self) -> None:
        changed = self._reloader.reload_changes()
        print(f"reloaded modules: {changed}")
