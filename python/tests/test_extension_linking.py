#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Test to ensure the Python extension is built correctly without linking libpython.

Python extensions should NOT have a DT_NEEDED entry for libpython because they
get their Python symbols from the interpreter that loads them. Linking libpython
can cause runtime issues and is incorrect for Python extensions.

This test uses readelf to verify the extension's dynamic dependencies.
"""

import os
import subprocess
import unittest

import monarch._rust_bindings


class TestExtensionLinking(unittest.TestCase):
    """Test that the Rust Python extension is built correctly."""

    def test_no_libpython_dependency(self):
        """Verify that _rust_bindings.so does not link against libpython."""
        # Get the path to the loaded extension module
        extension_path = monarch._rust_bindings.__file__
        self.assertIsNotNone(extension_path, "Could not find extension module path")
        self.assertTrue(
            os.path.exists(extension_path),
            f"Extension not found at {extension_path}",
        )

        # Run readelf to get dynamic dependencies
        try:
            result = subprocess.run(
                ["readelf", "-d", extension_path],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            self.fail(f"readelf failed: {e.stderr}")
        except FileNotFoundError:
            self.skipTest("readelf not available (not on Linux?)")

        # Parse NEEDED entries
        needed_libs = []
        for line in result.stdout.splitlines():
            if "(NEEDED)" in line and "Shared library:" in line:
                # Extract library name from: 0x... (NEEDED) Shared library: [libfoo.so]
                lib = line.split("[")[1].split("]")[0]
                needed_libs.append(lib)

        # Check that libpython is NOT in the dependencies
        libpython_deps = [lib for lib in needed_libs if lib.startswith("libpython")]
        self.assertEqual(
            libpython_deps,
            [],
            f"Extension should NOT link against libpython, but found: {libpython_deps}\n"
            f"Full dependencies: {needed_libs}\n\n"
            f"Python extensions should get Python symbols from the interpreter that "
            f"loads them, not from a DT_NEEDED entry. If this test fails, the build "
            f"configuration needs to be fixed to remove libpython linking.\n\n"
            f"For OSS builds: Ensure pyo3 has the 'extension-module' feature enabled.\n"
            f"For BUCK builds: Check that autocargo configs specify extension-module.",
        )

        # Verify we do have some expected system libraries
        # (to ensure readelf worked and we're checking real dependencies)
        has_libc = any(lib.startswith("libc.so") for lib in needed_libs)
        self.assertTrue(
            has_libc,
            f"Expected to find libc.so in dependencies, but got: {needed_libs}",
        )

        print("✓ Extension correctly built without libpython dependency")
        print(f"  Extension: {extension_path}")
        print(f"  Dependencies: {needed_libs}")


if __name__ == "__main__":
    unittest.main()
