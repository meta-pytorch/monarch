# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-ignore-all-errors

import importlib
import os
import pathlib
import subprocess
import sys
import sysconfig

import pytest


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="ldd check is Linux-only"
)
def test_rust_bindings_loads_and_links_libpython():
    # 1) Import should succeed (exercises RPATH at load time)
    mod = importlib.import_module("monarch._rust_bindings")

    # If the extension is statically linked, there's no .so to ldd.
    so_file = getattr(mod, "__file__", None)
    if not so_file or so_file == "static-extension" or not os.path.isabs(so_file):
        pytest.skip(
            "extension is statically linked in this environment; no shared object to ldd"
        )

    # 2) Verify libpython resolves in the ELF deps
    so_path = pathlib.Path(so_file)
    ldlibrary = sysconfig.get_config_var("LDLIBRARY")  # e.g. "libpython3.10.so"
    needle = ldlibrary or "libpython"

    proc = subprocess.run(
        ["ldd", str(so_path)], capture_output=True, text=True, check=True
    )
    hits = [
        line
        for line in proc.stdout.splitlines()
        if needle in line or "libpython" in line
    ]

    assert hits, f"ldd output did not mention {needle!r}:\n{proc.stdout}"
    assert all(
        "not found" not in line for line in hits
    ), f"libpython unresolved:\n{proc.stdout}"
