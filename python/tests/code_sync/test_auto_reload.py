# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import compileall
import contextlib
import importlib
import os
import py_compile
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Generator

import pytest

from monarch.code_sync.auto_reload import AutoReloader, SysAuditImportHook


def write_text(path: Path, content: str):
    with open(path, "w") as f:
        print(content, file=f, end="")
        os.fsync(f.fileno())  # needed for mtimes changes to be reflected immediately


@contextlib.contextmanager
def importable_workspace() -> Generator[Path, Any, Any]:
    """Context manager to add the workspace to sys.path."""
    with tempfile.TemporaryDirectory() as workspace:
        sys.path.insert(0, workspace)
        try:
            yield Path(workspace)
        finally:
            for module in list(sys.modules.values()):
                filename = getattr(module, "__file__", None)
                if filename is not None and filename.startswith(workspace + "/"):
                    del sys.modules[module.__name__]
            sys.path.remove(workspace)


class TestAutoReloader(unittest.TestCase):
    @pytest.mark.oss_skip  # pyre-ignore[56] TODO T229709067
    def test_source_change(self):
        with importable_workspace() as workspace:
            reloader = AutoReloader(workspace)
            with SysAuditImportHook.install(reloader.import_callback):
                filename = workspace / "test_module.py"
                write_text(filename, "foo = 1\n")

                import test_module  # pyre-ignore: Undefined import [21]

                self.assertEqual(Path(test_module.__file__), filename)
                self.assertEqual(test_module.foo, 1)

                write_text(filename, "foo = 2\nbar = 4\n")
                os.remove(importlib.util.cache_from_source(filename))  # force recompile

                self.assertEqual(
                    reloader.reload_changes(),
                    ["test_module"],
                )
                self.assertEqual(test_module.foo, 2)

    def test_pyc_only_change(self):
        with importable_workspace() as workspace:
            reloader = AutoReloader(workspace)
            with SysAuditImportHook.install(reloader.import_callback):
                filename = workspace / "test_module.py"
                pyc = filename.with_suffix(".pyc")

                write_text(filename, "foo = 1\n")
                compileall.compile_dir(
                    workspace,
                    legacy=True,
                    quiet=True,
                    invalidation_mode=py_compile.PycInvalidationMode.CHECKED_HASH,
                )
                filename.unlink()

                import test_module  # pyre-ignore: Undefined import [21]

                self.assertEqual(Path(test_module.__file__), pyc)
                self.assertEqual(test_module.foo, 1)

                write_text(filename, "foo = 2\nbar = 4\n")
                pyc.unlink()  # force recompile
                compileall.compile_dir(
                    workspace,
                    legacy=True,
                    quiet=True,
                    invalidation_mode=py_compile.PycInvalidationMode.CHECKED_HASH,
                )
                filename.unlink()

                self.assertEqual(
                    reloader.reload_changes(),
                    ["test_module"],
                )
                self.assertEqual(test_module.foo, 2)
