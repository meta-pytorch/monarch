#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Test that Monarch's native extensions keep the GIL disabled on free-threaded
Python builds.

CPython re-enables the GIL when it loads an extension module that has not
declared ``Py_mod_gil = Py_MOD_GIL_NOT_USED``, which silently turns a
free-threaded interpreter back into a GIL one.
"""

import importlib.util
import io
import os
import subprocess
import sys
import sysconfig
import threading
import unittest
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import monarch._src.actor.pickle as monarch_pickle
from monarch._rust_bindings.monarch_hyperactor.telemetry import (
    PyCounter,
    PyHistogram,
    PyUpDownCounter,
)

NATIVE_MODULES = [
    "monarch._rust_bindings",
    "monarch.common._C",
    "monarch.gradient._gradient_generator",
]

THREADS = 8


def run_concurrently(fn: Callable[[], None], iterations: int) -> None:
    """Run ``fn`` ``iterations`` times on each of ``THREADS`` threads that
    start together, re-raising the first exception."""
    barrier = threading.Barrier(THREADS)

    def worker() -> None:
        barrier.wait()
        for _ in range(iterations):
            fn()

    with ThreadPoolExecutor(THREADS) as pool:
        for future in [pool.submit(worker) for _ in range(THREADS)]:
            future.result()


@unittest.skipUnless(
    sysconfig.get_config_var("Py_GIL_DISABLED"), "requires a free-threaded build"
)
class FreeThreadingTest(unittest.TestCase):
    def test_import_keeps_gil_disabled(self) -> None:
        # `_C` and `_gradient_generator` are absent without the tensor engine.
        modules = [m for m in NATIVE_MODULES if importlib.util.find_spec(m)]
        self.assertIn("monarch._rust_bindings", modules)

        # A fresh interpreter, without PYTHON_GIL, so neither earlier imports
        # in this process nor an override can mask a module that needs the GIL.
        # Stub parent packages without executing their `__init__`: importing
        # the dotted name directly would run them (e.g. `monarch.gradient`
        # imports torch), whose GIL behavior is not Monarch's to assert.
        env = {k: v for k, v in os.environ.items() if k != "PYTHON_GIL"}
        code = (
            "import importlib, importlib.machinery, importlib.util, sys\n"
            f"for name in {modules!r}:\n"
            "    parts = name.split('.')\n"
            "    for i in range(1, len(parts)):\n"
            "        parent = '.'.join(parts[:i])\n"
            "        if parent not in sys.modules:\n"
            "            parent_spec = importlib.util.find_spec(parent)\n"
            "            pkg = importlib.util.module_from_spec(importlib.machinery.ModuleSpec(parent, None, is_package=True))\n"
            "            pkg.__path__ = parent_spec.submodule_search_locations\n"
            "            sys.modules[parent] = pkg\n"
            "    importlib.import_module(name)\n"
            "print(sys._is_gil_enabled())\n"
        )
        result = subprocess.run(
            [sys.executable, "-W", "error::RuntimeWarning", "-c", code],
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "False", result.stderr)

    def test_shared_metrics_accept_concurrent_updates(self) -> None:
        # Process-wide metrics (e.g. MESSAGES_HANDLED) are updated from every
        # actor's dispatch loop. A `&mut self` method would raise
        # "RuntimeError: Already borrowed" when two updates overlap.
        counter = PyCounter("test_free_threading_counter")
        histogram = PyHistogram("test_free_threading_histogram")
        up_down = PyUpDownCounter("test_free_threading_up_down")

        def update() -> None:
            counter.add(1, attributes=None)
            histogram.record(1.0, attributes=None)
            up_down.add(1, attributes=None)

        run_concurrently(update, 10_000)

    def test_torch_pickler_concurrent_first_use(self) -> None:
        import torch

        tensor = torch.zeros(1)

        def dump() -> None:
            buffer = io.BytesIO()
            monarch_pickle.torch_dump(tensor, buffer)
            self.assertGreater(buffer.tell(), 0)

        # Each round resets the lazily built pickler so that all threads race
        # its first use; none may observe it half-initialized.
        for _ in range(20):
            monarch_pickle._TorchPickler = None
            run_concurrently(dump, 1)
