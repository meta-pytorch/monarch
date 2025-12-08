# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os

import shutil
import subprocess
import sys
import sysconfig

import torch

from setuptools import Command, find_packages, setup

from setuptools_rust import Binding, RustBin, RustExtension
from torch.utils.cpp_extension import (
    BuildExtension,
    CppExtension,
    CUDA_HOME,
    include_paths as torch_include_paths,
    TORCH_LIB_PATH,
)

USE_CUDA = CUDA_HOME is not None


# Feature detection for building torchmonarch-* variants.
def get_rust_features():
    """
    Determine which Rust features to build.

    Environment variable:
    - MONARCH_FEATURES: "core", "tensor_engine", "rdma", "full" (comma-separated)
    - Default: "full" (all features)

    Returns:
        list: features to enable
    """
    features_str = os.environ.get("MONARCH_FEATURES", "").strip()

    if features_str:
        return [f.strip() for f in features_str.split(",") if f.strip()]
    else:
        # Use the full build by default.
        return ["full"]


# Get features for this build
RUST_FEATURES = get_rust_features()


def has_feature(feature):
    """
    Check if a feature is enabled.

    Args:
        feature: Feature name to check (e.g., "rdma", "tensor_engine", "core")

    Returns:
        bool: True if the feature is explicitly listed or "full" is enabled
    """
    return feature in RUST_FEATURES or "full" in RUST_FEATURES


# Print build configuration
package_version = os.environ.get("MONARCH_VERSION", "0.0.1")
package_name = os.environ.get("MONARCH_PACKAGE_NAME", "torchmonarch")
print(f"Building {package_name} v{package_version} with features: {RUST_FEATURES}")


def setup_build_environment():
    """
    Configure environment variables for Rust and C++ builds.

    Sets up compiler flags, PyTorch library paths, and feature-specific configuration.
    """
    enable_msg_logging = (
        "--cfg=enable_hyperactor_message_logging"
        if os.environ.get("ENABLE_MESSAGE_LOGGING")
        else ""
    )
    enable_tracing_unstable = "--cfg=tracing_unstable"

    # RDMA requires PyTorch CUDA libraries (torch_cuda, c10_cuda) for GPUDirect support
    # So we only enable TORCH_SYS_USE_PYTORCH_APIS when building with RDMA
    use_pytorch_apis = "1" if has_feature("rdma") else "0"

    env_updates = {
        "CXXFLAGS": f"-D_GLIBCXX_USE_CXX11_ABI={int(torch._C._GLIBCXX_USE_CXX11_ABI)}",
        "RUSTFLAGS": " ".join(
            ["-Zthreads=16", enable_msg_logging, enable_tracing_unstable]
        ),
        "LIBTORCH_LIB": TORCH_LIB_PATH,
        "LIBTORCH_INCLUDE": ":".join(torch_include_paths()),
        "_GLIBCXX_USE_CXX11_ABI": str(int(torch._C._GLIBCXX_USE_CXX11_ABI)),
        "TORCH_SYS_USE_PYTORCH_APIS": use_pytorch_apis,
    }

    if USE_CUDA:
        env_updates["CUDA_HOME"] = CUDA_HOME

    print("Setting environment variables:")
    for k, v in env_updates.items():
        print(f"  {k}={v}")

    os.environ.update(env_updates)


# Setup build environment
setup_build_environment()


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


with open("requirements.txt") as f:
    reqs = f.read()

with open("README.md", encoding="utf8") as f:
    readme = f.read()

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
rust_ext = RustExtension(
    "monarch._rust_bindings",
    binding=Binding.PyO3,
    path="monarch_extension/Cargo.toml",
    debug=False,
    features=RUST_FEATURES,
    args=["--no-default-features"],
)

print(f"   Rust extension features: {RUST_FEATURES}")
print(f"   Rust extension args: {rust_ext.args}")

rust_extensions.append(rust_ext)

# Build C++ extensions conditionally based on features
cpp_ext_modules = []

# common_C is always needed
monarch_cpp_src = ["python/monarch/common/init.cpp"]
if USE_CUDA:
    monarch_cpp_src.append("python/monarch/common/mock_cuda.cpp")

cpp_ext_modules.append(
    CppExtension(
        "monarch.common._C",
        monarch_cpp_src,
        extra_compile_args=["-g", "-O3"],
        libraries=["dl"],
        include_dirs=[
            os.path.dirname(os.path.abspath(__file__)),
            sysconfig.get_config_var("INCLUDEDIR"),
        ],
    )
)
print("   Building common._C C++ extension")

# Only build gradient_generator if tensor_engine is enabled
if has_feature("tensor_engine"):
    cpp_ext_modules.append(
        CppExtension(
            "monarch.gradient._gradient_generator",
            ["python/monarch/gradient/_gradient_generator.cpp"],
            extra_compile_args=["-g", "-O3"],
            include_dirs=[
                os.path.dirname(os.path.abspath(__file__)),
                sysconfig.get_config_var("INCLUDEDIR"),
            ],
        )
    )
    print("   Building gradient_generator C++ extension")
else:
    print("   Skipping gradient_generator C++ extension (tensor_engine not enabled)")

print(f"   C++ extensions: {[ext.name for ext in cpp_ext_modules]}")

setup(
    name=package_name,
    version=package_version,
    packages=find_packages(
        where="python",
        exclude=["python/tests.*", "python/tests"],
    ),
    package_dir={"": "python"},
    python_requires=">= 3.10",
    install_requires=reqs.strip().split("\n"),
    extras_require={
        "examples": [
            "bs4",
            "ipython",
        ],
    },
    license="BSD-3-Clause",
    author="Meta",
    author_email="oncall+monarch@xmail.facebook.com",
    description="Monarch: Single controller library",
    long_description=readme,
    long_description_content_type="text/markdown",
    ext_modules=cpp_ext_modules,
    entry_points={
        "console_scripts": [
            "monarch=monarch.tools.cli:main",
            "monarch_bootstrap=monarch._src.actor.bootstrap_main:invoke_main",
        ],
    },
    rust_extensions=rust_extensions,
    cmdclass={
        "build_ext": BuildExtension.with_options(no_python_abi_suffix=True),
        "clean": Clean,
    },
)
