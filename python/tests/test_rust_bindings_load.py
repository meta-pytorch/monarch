# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-ignore-all-errors

import importlib
import os
import pathlib
import re
import shutil
import subprocess
import sys
import sysconfig

import pytest


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux-only")
def test_import_is_hermetic_without_loader_env():
    """
    Import the extension in a clean loader environment so RUNPATH must do the work.
    This catches cases where the wheel works locally only because LD_LIBRARY_PATH is set.
    """
    # Keep PYTHONPATH (Buck's link-tree). Only scrub loader paths.
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH")
    }
    env["PYTHONNOUSERSITE"] = "1"
    # Ensure child's sys.path matches parent explicitly (works in Buck & pip)
    env["PYTHONPATH"] = os.pathsep.join(sys.path)
    subprocess.run(
        [sys.executable, "-c", "import monarch._rust_bindings"], env=env, check=True
    )


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux-only")
def test_runpath_and_needed_dependencies_resolve():
    if not shutil.which("readelf") or not shutil.which("ldd"):
        pytest.skip("readelf/ldd not available")

    # 1) Import (normal env) and locate the .so
    mod = importlib.import_module("monarch._rust_bindings")
    so_file = getattr(mod, "__file__", None)
    if not so_file or so_file == "static-extension" or not os.path.isabs(so_file):
        pytest.skip("extension is statically linked here; no shared object to inspect")

    so_path = pathlib.Path(so_file)

    # 2) Inspect dynamic section
    dyn = subprocess.run(
        ["readelf", "-d", str(so_path)], capture_output=True, text=True, check=True
    ).stdout

    # RUNPATH sanity: we expect our relocation entries to be present
    # ($ORIGIN for sidecars; parent dirs; and jump to the env's lib)
    assert "$ORIGIN" in dyn, f"RUNPATH missing $ORIGIN:\n{dyn}"
    assert "$ORIGIN/.." in dyn, f"RUNPATH missing $ORIGIN/..:\n{dyn}"
    assert "$ORIGIN/../../.." in dyn, f"RUNPATH missing $ORIGIN/../../..:\n{dyn}"

    # 3) Parse NEEDED
    needed = re.findall(r"\(NEEDED\)\s+Shared library: \[(.+?)\]", dyn)

    # 4) ldd resolution check (conditional)
    ldd = subprocess.run(
        ["ldd", str(so_path)], capture_output=True, text=True, check=True
    ).stdout

    # libpython may be absent if built as abi3; only assert when present
    if any("libpython" in n for n in needed):
        py_hits = [ln for ln in ldd.splitlines() if "libpython" in ln]
        assert py_hits, f"ldd did not list libpython:\n{ldd}"
        assert all("not found" not in ln for ln in py_hits), (
            f"libpython unresolved:\n{ldd}"
        )

    # CUDA runtime: if you linked against cudart, ensure it resolves
    if any("cudart" in n for n in needed):
        cu_hits = [ln for ln in ldd.splitlines() if "cudart" in ln]
        assert cu_hits, f"ldd did not list libcudart:\n{ldd}"
        assert all("not found" not in ln for ln in cu_hits), (
            f"libcudart unresolved:\n{ldd}"
        )
