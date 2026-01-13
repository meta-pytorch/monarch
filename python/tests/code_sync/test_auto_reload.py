# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

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
from monarch._src.actor.code_sync.auto_reload import AutoReloader, SysAuditImportHook


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
    def test_source_change(self):
        with importable_workspace() as workspace:
            reloader = AutoReloader()
            with SysAuditImportHook.install(reloader.import_callback):
                filename = workspace / "test_module.py"
                write_text(filename, "foo = 1\n")

                import test_module  # pyre-ignore: Undefined import [21]

                self.assertEqual(Path(test_module.__file__), filename)
                self.assertEqual(test_module.foo, 1)

                write_text(filename, "foo = 2\nbar = 4\n")
                try:
                    # force recompile
                    os.remove(importlib.util.cache_from_source(filename))
                except FileNotFoundError:
                    pass  # python may not always implicitly generate bytecode

                self.assertEqual(
                    reloader.reload_changes(),
                    ["test_module"],
                )
                self.assertEqual(test_module.foo, 2)

    def test_builtin_module_no_file_attribute(self):
        """Test that modules without __file__ attribute don't cause AttributeError."""
        reloader = AutoReloader()
        with SysAuditImportHook.install(reloader.import_callback):
            # C extensions don't have a `__file__` attr and won't trigger an "exec"
            # event, so we're verifying that an unrelated "exec" (via `eval`) won't
            # cause issues.
            assert "_tracemalloc" not in sys.modules
            import _tracemalloc  # noqa

            eval("5")  # trigger `exec` event in the reloader

    def test_pyc_only_change(self):
        with importable_workspace() as workspace:
            reloader = AutoReloader()
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
