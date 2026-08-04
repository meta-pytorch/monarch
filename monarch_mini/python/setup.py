# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#
# Build the minimonarch Python extension. Project metadata lives in
# pyproject.toml; this file only declares the C extension (and builds the Rust
# staticlib it links against) since setuptools needs ext_modules in setup.py.
#
#   uv run pytest        # builds + installs minimonarch, then runs the tests

import os
import subprocess

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext

HERE = os.path.dirname(os.path.abspath(__file__))
CRATE_DIR = os.path.dirname(HERE)  # monarch_mini/
WORKSPACE_DIR = os.path.dirname(CRATE_DIR)  # monarch/
STATIC_LIB = os.path.join(WORKSPACE_DIR, "target", "debug", "libmonarch_mini.a")


class CargoBuildExt(build_ext):
    """Build the Rust staticlib before compiling the C extension."""

    def run(self):
        subprocess.check_call(
            ["cargo", "build", "-p", "monarch_mini"], cwd=WORKSPACE_DIR
        )
        super().run()


ext = Extension(
    name="minimonarch",
    sources=["minimonarch.c"],
    include_dirs=[CRATE_DIR],  # for minimonarch.h
    extra_objects=[STATIC_LIB],
    # System libraries the Rust standard library / tokio pull in.
    libraries=["pthread", "dl", "m"],
)

setup(
    ext_modules=[ext],
    cmdclass={"build_ext": CargoBuildExt},
)
