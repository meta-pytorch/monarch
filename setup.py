# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import importlib.util
import os
import shutil
import subprocess
import sys
import sysconfig

from setuptools import Command, setup
from setuptools.command.build_ext import build_ext
from setuptools.extension import Extension

from setuptools_rust import Binding, RustBin, RustExtension


# Helper functions to find torch without importing it
def find_torch_paths():
    """Find torch installation paths without importing torch"""
    spec = importlib.util.find_spec("torch")
    if not spec or not spec.origin:
        raise RuntimeError("torch not found - please install PyTorch first")

    base = os.path.dirname(spec.origin)
    lib_path = os.path.join(base, "lib")
    include_path = os.path.join(base, "include")

    # Get all include paths (similar to torch.utils.cpp_extension.include_paths())
    include_paths = [include_path]

    # Add torch/csrc includes if available
    torch_csrc_include = os.path.join(include_path, "torch", "csrc", "api", "include")
    if os.path.exists(torch_csrc_include):
        include_paths.append(torch_csrc_include)

    return {"lib_path": lib_path, "include_paths": include_paths}


def find_cuda_home():
    """Find CUDA installation without importing torch"""
    # Check environment variable first
    cuda_home = os.environ.get("CUDA_HOME") or os.environ.get("CUDA_PATH")

    if cuda_home and os.path.exists(cuda_home):
        return cuda_home

    # Try to find nvcc
    try:
        nvcc_path = subprocess.run(
            ["which", "nvcc"], capture_output=True, text=True, timeout=5
        )
        if nvcc_path.returncode == 0:
            # Get directory containing bin/nvcc
            nvcc = nvcc_path.stdout.strip()
            cuda_home = os.path.dirname(os.path.dirname(nvcc))
            return cuda_home
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # Check common locations
    for path in ["/usr/local/cuda", "/usr/cuda"]:
        if os.path.exists(path):
            return path

    return None


def detect_cxx11_abi():
    """Detect if torch uses C++11 ABI by examining library symbols"""
    paths = find_torch_paths()
    lib_path = paths["lib_path"]

    # Try to find a torch library to check
    for lib_name in ["libtorch_cpu.so", "libtorch.so", "libc10.so"]:
        lib_file = os.path.join(lib_path, lib_name)
        if os.path.exists(lib_file):
            try:
                result = subprocess.run(
                    ["nm", "-D", lib_file], capture_output=True, text=True, timeout=10
                )
                if result.returncode == 0:
                    # Check for __cxx11 namespace which indicates new ABI
                    if "__cxx11" in result.stdout:
                        return 1  # New ABI
                    else:
                        return 0  # Old ABI
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass

    # Default to new ABI if we can't determine
    return 1


# Get torch paths and settings
torch_paths = find_torch_paths()
TORCH_LIB_PATH = torch_paths["lib_path"]
torch_include_paths = torch_paths["include_paths"]
CUDA_HOME = find_cuda_home()
cxx11_abi = detect_cxx11_abi()

USE_CUDA = CUDA_HOME is not None
USE_TENSOR_ENGINE = os.environ.get("USE_TENSOR_ENGINE", "1") == "1"


def create_torch_extension(name, sources):
    """Helper to create a C++ extension with torch dependencies"""
    return Extension(
        name,
        sources,
        extra_compile_args=["-g", "-O3"],
        libraries=["dl", "c10", "torch", "torch_cpu", "torch_python"],
        library_dirs=[TORCH_LIB_PATH],
        include_dirs=[
            os.path.dirname(os.path.abspath(__file__)),
            sysconfig.get_config_var("INCLUDEDIR"),
        ]
        + torch_include_paths,
        runtime_library_dirs=[TORCH_LIB_PATH] if sys.platform != "win32" else [],
    )


monarch_cpp_src = ["python/monarch/common/init.cpp"]
if USE_CUDA:
    monarch_cpp_src.append("python/monarch/common/mock_cuda.cpp")

# Create C++ extensions using standard Extension instead of CppExtension
common_C = create_torch_extension("monarch.common._C", monarch_cpp_src)
controller_C = create_torch_extension(
    "monarch.gradient._gradient_generator",
    ["python/monarch/gradient/_gradient_generator.cpp"],
)

ENABLE_MSG_LOGGING = (
    "--cfg=enable_hyperactor_message_logging"
    if os.environ.get("ENABLE_MESSAGE_LOGGING")
    else ""
)

ENABLE_TRACING_UNSTABLE = "--cfg=tracing_unstable"

os.environ.update(
    {
        "CXXFLAGS": f"-D_GLIBCXX_USE_CXX11_ABI={cxx11_abi}",
        "RUSTFLAGS": " ".join(
            ["-Zthreads=16", ENABLE_MSG_LOGGING, ENABLE_TRACING_UNSTABLE]
        ),
        "LIBTORCH_LIB": TORCH_LIB_PATH,
        "LIBTORCH_INCLUDE": ":".join(torch_include_paths),
        "_GLIBCXX_USE_CXX11_ABI": str(cxx11_abi),
        "TORCH_SYS_USE_PYTORCH_APIS": "0",
    }
)
if USE_CUDA:
    os.environ.update(
        {
            "CUDA_HOME": CUDA_HOME,
        }
    )


class Clean(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import glob
        import re

        with open(".gitignore") as f:
            ignores = f.read()
            pat = re.compile(r"^#( BEGIN NOT-CLEAN-FILES )?")
            for wildcard in filter(None, ignores.split("\n")):
                match = pat.match(wildcard)
                if match:
                    if match.group(1):
                        # Marker is found and stop reading .gitignore.
                        break
                    # Ignore lines which begin with '#'.
                else:
                    # Don't remove absolute paths from the system
                    wildcard = wildcard.lstrip("./")

                    for filename in glob.glob(wildcard):
                        try:
                            os.remove(filename)
                        except OSError:
                            shutil.rmtree(filename, ignore_errors=True)

        subprocess.run(["cargo", "clean"])


if sys.platform.startswith("linux"):
    # Always include the active env's lib (Conda-safe)
    conda_lib = os.path.join(sys.prefix, "lib")

    # Only use LIBDIR if it actually contains the current libpython
    ldlib = sysconfig.get_config_var("LDLIBRARY") or ""
    libdir = sysconfig.get_config_var("LIBDIR") or ""
    py_lib = ""
    if libdir and ldlib:
        cand = os.path.join(libdir, ldlib)
        if os.path.exists(cand) and os.path.realpath(libdir) != os.path.realpath(
            conda_lib
        ):
            py_lib = libdir

    # Prefer sidecar .so next to the extension; then the conda env;
    # then (optionally) py_lib
    flags = [
        "-C",
        "link-arg=-Wl,--enable-new-dtags",
        "-C",
        "link-arg=-Wl,-z,origin",
        "-C",
        "link-arg=-Wl,-rpath,$ORIGIN",
        "-C",
        "link-arg=-Wl,-rpath,$ORIGIN/..",
        "-C",
        "link-arg=-Wl,-rpath,$ORIGIN/../../..",
        "-C",
        "link-arg=-Wl,-rpath," + conda_lib,
        # Add the conda lib to the search path for the linker, as libraries like
        # libunwind may be installed there.
        "-L",
        conda_lib,
    ]
    if py_lib:
        flags += ["-C", "link-arg=-Wl,-rpath," + py_lib]

    cur = os.environ.get("RUSTFLAGS", "")
    os.environ["RUSTFLAGS"] = (cur + " " + " ".join(flags)).strip()

# Check if MONARCH_BUILD_MESH_ONLY=1 to skip legacy builds
SKIP_LEGACY_BUILDS = os.environ.get("MONARCH_BUILD_MESH_ONLY", "0") == "1"

rust_extensions = []

# Legacy builds (kept for tests that still depend on them)
if not SKIP_LEGACY_BUILDS:
    rust_extensions.append(
        RustBin(
            target="process_allocator",
            path="monarch_hyperactor/Cargo.toml",
            debug=False,
        )
    )

# Main extension (always built)
rust_extensions.append(
    RustExtension(
        "monarch._rust_bindings",
        binding=Binding.PyO3,
        path="monarch_extension/Cargo.toml",
        debug=False,
        features=["extension-module", "tensor_engine"]
        if USE_TENSOR_ENGINE
        else ["extension-module"],
        args=[] if USE_TENSOR_ENGINE else ["--no-default-features"],
    )
)

package_name = os.environ.get("MONARCH_PACKAGE_NAME", "monarch")
package_version = os.environ.get("MONARCH_VERSION", "0.0.1")

setup(
    name=package_name,
    version=package_version,
    ext_modules=[
        controller_C,
        common_C,
    ],
    rust_extensions=rust_extensions,
    cmdclass={
        "build_ext": build_ext,
        "clean": Clean,
    },
)
