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
from setuptools.command.build_ext import build_ext as _build_ext
from setuptools.command.build_py import build_py
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

    # Check common CUDA locations
    for path in ["/usr/local/cuda", "/usr/cuda"]:
        if os.path.exists(path):
            return path

    return None


def get_rocm_home() -> Optional[str]:
    """
    Find ROCm installation.

    Returns:
        Path to ROCm installation or None if not found
    """
    # Check environment variable first
    rocm_home = os.environ.get("ROCM_PATH") or os.environ.get("ROCM_HOME")
    if rocm_home and os.path.exists(rocm_home):
        return rocm_home

    # Check default ROCm location
    if os.path.exists("/opt/rocm"):
        return "/opt/rocm"

    return None


# Build config detection
torch_config = get_torch_config()
cuda_home = get_cuda_home()
rocm_home = get_rocm_home()
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

# GPU platform selection: use MONARCH_RDMA_GPU_PLATFORM env var or auto-detect
gpu_platform = os.environ.get("MONARCH_RDMA_GPU_PLATFORM", "").lower()
if gpu_platform and gpu_platform not in ("cuda", "rocm"):
    sys.exit(f"Invalid MONARCH_RDMA_GPU_PLATFORM={gpu_platform}. Use 'cuda' or 'rocm'")
if gpu_platform == "rocm" and not rocm_home:
    sys.exit("MONARCH_RDMA_GPU_PLATFORM=rocm but ROCm not found")
if gpu_platform == "cuda" and not cuda_home:
    sys.exit("MONARCH_RDMA_GPU_PLATFORM=cuda but CUDA not found")
if not gpu_platform and build_tensor_engine and cuda_home and rocm_home:
    sys.exit("Both CUDA and ROCm detected. Set MONARCH_RDMA_GPU_PLATFORM=cuda or =rocm")

build_cuda = build_tensor_engine and (
    gpu_platform == "cuda" or (not gpu_platform and cuda_home)
)
build_rocm = build_tensor_engine and (
    gpu_platform == "rocm" or (not gpu_platform and rocm_home)
)

print("=" * 80)
if build_tensor_engine:
    print("✓ Building WITH tensor_engine (GPU support)")
    print(f"  - PyTorch: {torch_config['lib_path']}")
    if build_cuda:
        print(f"  - CUDA: {cuda_home}")
    elif build_rocm:
        print(f"  - ROCm: {rocm_home}")
    else:
        print("  - GPU: Not found (CPU-only)")
    print(f"  - C++11 ABI: {'enabled' if torch_config['cxx11_abi'] else 'disabled'}")
else:
    print("Building WITHOUT tensor_engine (CPU-only, no GPU support)")
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
elif build_rocm:
    env_vars["ROCM_PATH"] = rocm_home

os.environ.update(env_vars)

# RPATH configuration for Linux
# These flags are passed via RustExtension.rustc_flags (applied only to the final
# cdylib link) rather than RUSTFLAGS (which would apply to every crate in the
# workspace). Putting environment-specific paths in RUSTFLAGS invalidates cargo's
# fingerprint cache for all 800+ dependency crates, causing a full rebuild every
# time the build environment path changes (e.g. uv's PEP 517 build isolation
# creates a new temp venv per invocation).
rust_link_flags: List[str] = []
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

    rust_link_flags = [
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
        rust_link_flags += ["-C", f"link-arg=-Wl,-rpath,{py_lib}"]


# Custom build_ext that skips C++ extensions when .so files are already fresh.
# uv's PEP 517 build isolation rebuilds the entire package whenever any cache-key
# file changes (e.g. a .rs file). Cargo handles its own caching, but setuptools
# always recompiles C++ into fresh temp dirs. This skips that when unnecessary.
class build_ext(_build_ext):
    def build_extension(self, ext):
        # Only apply caching to C/C++ extensions (those with .sources)
        if not hasattr(ext, "sources") or not ext.sources:
            return super().build_extension(ext)

        # In PEP 517 builds, get_ext_fullpath points to a temp build dir.
        # Look for the .so in the source tree instead (editable install puts
        # the .so alongside the Python package).
        ext_filename = self.get_ext_filename(ext.name)
        # ext_filename is e.g. "monarch/common/_C.cpython-312-x86_64-linux-gnu.so"
        # source tree location is python/<ext_filename>
        src_root = os.path.dirname(os.path.abspath(__file__))
        so_path = os.path.join(src_root, "python", ext_filename)
        if not os.path.exists(so_path):
            return super().build_extension(ext)

        so_mtime = os.path.getmtime(so_path)

        # Rebuild if any source file is newer than the .so
        for src in ext.sources:
            if os.path.exists(src) and os.path.getmtime(src) > so_mtime:
                return super().build_extension(ext)

        # .so is up to date — copy it to the build dir instead of recompiling
        dest = self.get_ext_fullpath(ext.name)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.copy2(so_path, dest)
        print(f"skipping {ext.name} (up to date, copied existing .so)")


# Extension Creation
def create_cpp_extension(name: str, sources: List[str], define_macros: Optional[List] = None) -> Extension:
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
        define_macros=define_macros or [],
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
    cpp_defines = []
    if build_cuda:
        # mock_cuda.cpp is not compatible with ROCm (relies on CUDA-specific assembly)
        cpp_sources.append("python/monarch/common/mock_cuda.cpp")
        cpp_defines.append(("MONARCH_BUILD_CUDA", "1"))

    ext_modules = [
        create_cpp_extension("monarch.common._C", cpp_sources, define_macros=cpp_defines),
        create_cpp_extension(
            "monarch.gradient._gradient_generator",
            ["python/monarch/gradient/_gradient_generator.cpp"],
        ),
    ]

# Rust extensions
rust_extensions = []

# Main Python extension
rust_features = ["extension-module", "distributed_sql_telemetry"]
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
        rustc_flags=rust_link_flags,
    )
)

# Mesh Admin TUI — standalone Rust binary bundled into the wheel so that
# `pip install torchmonarch` places `monarch-tui` on PATH.
rust_extensions.append(
    RustBin(
        {"hyperactor_mesh_admin_tui": "monarch-tui"},
        path="hyperactor_mesh_admin_tui/Cargo.toml",
        debug=False,
    )
)


# BuildFrontend command
class BuildFrontend(Command):
    """Build the React frontend for monarch_dashboard.

    Uses esbuild. esbuild produces a single JS bundle and a CSS file;
    this command also creates the output directory structure and generates
    index.html, since esbuild does not emit HTML.

    The OSS build (``pip install .``) triggers this via BuildPyWithFrontend.
    The Buck build uses a separate genrule in frontend/BUCK instead.
    """

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        frontend_dir = os.path.join(
            os.path.dirname(__file__),
            "python",
            "monarch",
            "monarch_dashboard",
            "frontend",
        )
        build_dir = os.path.join(frontend_dir, "build")
        build_index = os.path.join(build_dir, "index.html")

        # Skip npm if pre-built assets already exist (e.g. from CI).
        if os.path.isfile(build_index):
            print(">> Pre-built frontend found, skipping npm build")
            return

        if not os.path.exists(frontend_dir):
            print(f"Frontend directory not found: {frontend_dir}")
            return

        # Use real npm, bypassing any system wrappers
        npm_cmd = "/usr/bin/npm" if os.path.exists("/usr/bin/npm") else "npm"

        print("Building dashboard frontend...")
        try:
            subprocess.check_call([npm_cmd, "install"], cwd=frontend_dir)
            os.makedirs(os.path.join(build_dir, "static", "css"), exist_ok=True)
            subprocess.check_call([npm_cmd, "run", "build"], cwd=frontend_dir)
            # esbuild puts CSS next to JS; move it to static/css/
            js_css = os.path.join(build_dir, "static", "js", "main.css")
            if os.path.isfile(js_css):
                shutil.move(
                    js_css,
                    os.path.join(build_dir, "static", "css", "main.css"),
                )
            # Copy the shared index.html template into the build output.
            shutil.copy(
                os.path.join(frontend_dir, "public", "index.html"),
                build_index,
            )
            print("Frontend build completed successfully")
        except FileNotFoundError:
            print("WARNING: npm not found. Skipping frontend build.")
            print(
                "Install Node.js to build the dashboard frontend, "
                "or use pre-built assets."
            )
        except subprocess.CalledProcessError as e:
            print("Frontend build failed with error:", e)


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


class BuildPyWithFrontend(build_py):
    """Build the frontend before collecting package data."""

    def run(self):
        self.run_command("build_frontend")
        build_py.run(self)


# Actual Setup
package_name = os.environ.get("MONARCH_PACKAGE_NAME", "torchmonarch")
package_version = os.environ.get("MONARCH_VERSION", "0.5.0.dev0")

setup(
    name=package_name,
    version=package_version,
    ext_modules=ext_modules,
    rust_extensions=rust_extensions,
    cmdclass={
        "build_py": BuildPyWithFrontend,
        "build_ext": build_ext,
        "clean": Clean,
        "build_frontend": BuildFrontend,
    },
)
