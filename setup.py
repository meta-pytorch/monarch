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
from typing import Dict, List, Optional

from setuptools import Command, setup
from setuptools.command.build_ext import build_ext
from setuptools.extension import Extension

from setuptools_rust import Binding, RustBin, RustExtension


# Helper functions for finding paths on installed packages
def get_torch_config() -> Optional[Dict[str, any]]:
    """
    Detect torch installation and return configuration dict.
    Returns None if torch is not available.

    Returns:
        Dictionary with keys: lib_path, include_paths, cxx11_abi
        or None if torch is not found
    """
    try:
        spec = importlib.util.find_spec("torch")
        if not spec or not spec.origin:
            return None

        base = os.path.dirname(spec.origin)
        lib_path = os.path.join(base, "lib")
        include_path = os.path.join(base, "include")
        include_paths = [include_path]

        # Add torch/csrc includes if available
        torch_csrc_include = os.path.join(
            include_path, "torch", "csrc", "api", "include"
        )
        if os.path.exists(torch_csrc_include):
            include_paths.append(torch_csrc_include)

        # Detect C++11 ABI
        cxx11_abi = 1  # Default to new ABI
        for lib_name in ["libtorch_cpu.so", "libtorch.so", "libc10.so"]:
            lib_file = os.path.join(lib_path, lib_name)
            if os.path.exists(lib_file):
                try:
                    result = subprocess.run(
                        ["nm", "-D", lib_file],
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    if result.returncode == 0:
                        cxx11_abi = 1 if "__cxx11" in result.stdout else 0
                        break
                except (subprocess.TimeoutExpired, FileNotFoundError):
                    pass

        return {
            "lib_path": lib_path,
            "include_paths": include_paths,
            "cxx11_abi": cxx11_abi,
        }
    except Exception:
        return None


def get_cuda_home() -> Optional[str]:
    """
    Find CUDA installation.

    Returns:
        Path to CUDA installation or None if not found
    """
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
            nvcc = nvcc_path.stdout.strip()
            return os.path.dirname(os.path.dirname(nvcc))
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # Check common locations
    for path in ["/usr/local/cuda", "/usr/cuda"]:
        if os.path.exists(path):
            return path

    return None


# Build config detection
torch_config = get_torch_config()
cuda_home = get_cuda_home()
use_tensor_engine = os.environ.get("USE_TENSOR_ENGINE", "1") == "1"

if use_tensor_engine and not torch_config:
    print("=" * 80)
    print("ERROR: tensor_engine build requested but torch is not available!")
    print("")
    print("To fix this:")
    print("  1. Install torch first: pip install torch")
    print("  2. Then rebuild")
    print("")
    print("OR build without tensor_engine:")
    print("  USE_TENSOR_ENGINE=0 pip install -e .")
    print("=" * 80)
    sys.exit(1)

build_tensor_engine = use_tensor_engine and torch_config is not None
build_cuda = build_tensor_engine and cuda_home is not None

print("=" * 80)
if build_tensor_engine:
    print("✓ Building WITH tensor_engine (CUDA/GPU support)")
    print(f"  - PyTorch: {torch_config['lib_path']}")
    print(f"  - CUDA: {cuda_home if build_cuda else 'Not found (CPU-only)'}")
    print(f"  - C++11 ABI: {'enabled' if torch_config['cxx11_abi'] else 'disabled'}")
else:
    print("Building WITHOUT tensor_engine (CPU-only, no CUDA support)")
print("=" * 80)

# Set PYO3_PYTHON for Rust binaries
if "PYO3_PYTHON" not in os.environ:
    os.environ["PYO3_PYTHON"] = sys.executable

# Set Rust and C++ flags
rustflags = ["-Zthreads=16", "--cfg=tracing_unstable"]
if os.environ.get("ENABLE_MESSAGE_LOGGING"):
    rustflags.append("--cfg=enable_hyperactor_message_logging")

env_vars = {"RUSTFLAGS": " ".join(rustflags)}

if build_tensor_engine:
    cxx11_abi = torch_config["cxx11_abi"]
    env_vars.update(
        {
            "CXXFLAGS": f"-D_GLIBCXX_USE_CXX11_ABI={cxx11_abi}",
            "LIBTORCH_LIB": torch_config["lib_path"],
            "LIBTORCH_INCLUDE": ":".join(torch_config["include_paths"]),
            "_GLIBCXX_USE_CXX11_ABI": str(cxx11_abi),
            "TORCH_SYS_USE_PYTORCH_APIS": "0",
        }
    )
else:
    env_vars["CXXFLAGS"] = "-D_GLIBCXX_USE_CXX11_ABI=1"

if build_cuda:
    env_vars["CUDA_HOME"] = cuda_home

os.environ.update(env_vars)

# RPATH configuration for Linux
if sys.platform.startswith("linux"):
    conda_lib = os.path.join(sys.prefix, "lib")
    ldlib = sysconfig.get_config_var("LDLIBRARY") or ""
    libdir = sysconfig.get_config_var("LIBDIR") or ""

    py_lib = ""
    if libdir and ldlib:
        cand = os.path.join(libdir, ldlib)
        if os.path.exists(cand) and os.path.realpath(libdir) != os.path.realpath(
            conda_lib
        ):
            py_lib = libdir

    rpath_flags = [
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
        f"link-arg=-Wl,-rpath,{conda_lib}",
        "-L",
        conda_lib,
    ]
    if py_lib:
        rpath_flags += ["-C", f"link-arg=-Wl,-rpath,{py_lib}"]

    cur_rustflags = os.environ.get("RUSTFLAGS", "")
    os.environ["RUSTFLAGS"] = (cur_rustflags + " " + " ".join(rpath_flags)).strip()


# Extension Creation
def create_cpp_extension(name: str, sources: List[str]) -> Extension:
    """
    Create a C++ extension with torch dependencies.

    Args:
        name: Extension module name (e.g., "monarch.common._C")
        sources: List of source file paths

    Returns:
        Extension object configured for torch
    """
    return Extension(
        name,
        sources,
        extra_compile_args=["-std=c++17", "-g", "-O3"],
        libraries=["dl", "c10", "torch", "torch_cpu", "torch_python"],
        library_dirs=[torch_config["lib_path"]],
        include_dirs=[
            os.path.dirname(os.path.abspath(__file__)),
            sysconfig.get_config_var("INCLUDEDIR"),
        ]
        + torch_config["include_paths"],
        runtime_library_dirs=[torch_config["lib_path"]]
        if sys.platform != "win32"
        else [],
    )


# C++ extensions
ext_modules = []
if build_tensor_engine:
    cpp_sources = ["python/monarch/common/init.cpp"]
    if build_cuda:
        cpp_sources.append("python/monarch/common/mock_cuda.cpp")

    ext_modules = [
        create_cpp_extension("monarch.common._C", cpp_sources),
        create_cpp_extension(
            "monarch.gradient._gradient_generator",
            ["python/monarch/gradient/_gradient_generator.cpp"],
        ),
    ]

# Rust extensions
rust_extensions = []

# Legacy process_allocator binary (optional)
skip_legacy_builds = os.environ.get("MONARCH_BUILD_MESH_ONLY", "1") == "1"
if not skip_legacy_builds:
    rust_extensions.append(
        RustBin(
            target="process_allocator",
            path="monarch_hyperactor/Cargo.toml",
            debug=False,
            args=["--bin", "process_allocator", "--no-default-features"],
        )
    )

# Main Python extension (always built)
rust_features = ["extension-module"]
if build_tensor_engine:
    rust_features.append("tensor_engine")

rust_extensions.append(
    RustExtension(
        "monarch._rust_bindings",
        binding=Binding.PyO3,
        path="monarch_extension/Cargo.toml",
        debug=False,
        features=rust_features,
        args=[] if build_tensor_engine else ["--no-default-features"],
    )
)


# Clean command
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
                        break
                else:
                    wildcard = wildcard.lstrip("./")
                    for filename in glob.glob(wildcard):
                        try:
                            os.remove(filename)
                        except OSError:
                            shutil.rmtree(filename, ignore_errors=True)

        subprocess.run(["cargo", "clean"])


# Actual Setup
package_name = os.environ.get("MONARCH_PACKAGE_NAME", "torchmonarch")
package_version = os.environ.get("MONARCH_VERSION", "0.0.1")

setup(
    name=package_name,
    version=package_version,
    ext_modules=ext_modules,
    rust_extensions=rust_extensions,
    cmdclass={
        "build_ext": build_ext,
        "clean": Clean,
    },
)
