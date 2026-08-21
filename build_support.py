# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Shared configuration and helpers for Monarch's OSS package build."""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
import sysconfig
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FRONTEND_DIR = ROOT / "python" / "monarch" / "monarch_dashboard" / "frontend"
DEFAULT_VERSION = "0.7.0.dev0"


class BuildConfigurationError(RuntimeError):
    """Raised when the requested build cannot be satisfied by the host."""


@dataclass(frozen=True)
class TorchConfig:
    lib_path: Path
    include_paths: tuple[Path, ...]
    cxx11_abi: int


@dataclass(frozen=True)
class BuildConfig:
    torch: TorchConfig | None
    cuda_home: Path | None
    rocm_home: Path | None
    use_tensor_engine: bool
    gpu_platform: str
    build_tensor_engine: bool
    has_cuda: bool
    has_rocm: bool
    build_cuda: bool
    build_rocm: bool
    build_gpu: bool
    build_rdma: bool
    cargo_features: tuple[str, ...]
    environment: Mapping[str, str]
    rust_link_flags: tuple[str, ...]


def detect_torch_config() -> TorchConfig | None:
    """Locate the installed PyTorch headers/libraries and its C++ ABI."""

    try:
        spec = importlib.util.find_spec("torch")
        if not spec or not spec.origin:
            return None

        base = Path(spec.origin).parent
        lib_path = base / "lib"
        include_path = base / "include"
        include_paths = [include_path]
        torch_csrc_include = include_path / "torch" / "csrc" / "api" / "include"
        if torch_csrc_include.exists():
            include_paths.append(torch_csrc_include)

        # Preserve setup.py's ABI detection behavior. On platforms without the
        # Linux .so names (notably macOS), the historical default is the new ABI.
        cxx11_abi = 1
        for lib_name in ("libtorch_cpu.so", "libtorch.so", "libc10.so"):
            lib_file = lib_path / lib_name
            if not lib_file.exists():
                continue
            try:
                result = subprocess.run(
                    ["nm", "-D", os.fspath(lib_file)],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue
            if result.returncode == 0:
                cxx11_abi = int("__cxx11" in result.stdout)
                break

        return TorchConfig(lib_path, tuple(include_paths), cxx11_abi)
    except Exception:  # noqa: BLE001 - torch discovery must remain best-effort.
        return None


def detect_cuda_home(environ: Mapping[str, str] = os.environ) -> Path | None:
    for variable in ("CUDA_HOME", "CUDA_PATH"):
        value = environ.get(variable)
        if value and Path(value).exists():
            return Path(value)

    try:
        result = subprocess.run(
            ["which", "nvcc"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip():
            return Path(result.stdout.strip()).parent.parent
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    for path in (Path("/usr/local/cuda"), Path("/usr/cuda")):
        if path.exists():
            return path
    return None


def detect_rocm_home(environ: Mapping[str, str] = os.environ) -> Path | None:
    for variable in ("ROCM_PATH", "ROCM_HOME"):
        value = environ.get(variable)
        if value and Path(value).exists():
            return Path(value)
    default = Path("/opt/rocm")
    return default if default.exists() else None


def _linux_rust_link_flags(
    *,
    platform: str,
    prefix: str,
    config_vars: Mapping[str, object],
) -> tuple[str, ...]:
    if not platform.startswith("linux"):
        return ()

    conda_lib = Path(prefix) / "lib"
    ld_library = str(config_vars.get("LDLIBRARY") or "")
    libdir_value = str(config_vars.get("LIBDIR") or "")
    py_lib = ""
    if libdir_value and ld_library:
        libdir = Path(libdir_value)
        candidate = libdir / ld_library
        if candidate.exists() and libdir.resolve() != conda_lib.resolve():
            py_lib = os.fspath(libdir)

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
        f"link-arg=-Wl,-rpath,{conda_lib}",
        "-L",
        os.fspath(conda_lib),
    ]
    if py_lib:
        flags.extend(("-C", f"link-arg=-Wl,-rpath,{py_lib}"))
    return tuple(flags)


def select_build_config(
    *,
    environ: Mapping[str, str] | None = None,
    torch_config: TorchConfig | None = None,
    cuda_home: Path | None = None,
    rocm_home: Path | None = None,
    detect: bool = True,
    platform: str = sys.platform,
    prefix: str = sys.prefix,
    config_vars: Mapping[str, object] | None = None,
    python_executable: str | None = None,
) -> BuildConfig:
    """Resolve all feature, compiler, and linker settings for one build."""

    source_env = os.environ if environ is None else environ
    if detect:
        if torch_config is None:
            torch_config = detect_torch_config()
        if cuda_home is None:
            cuda_home = detect_cuda_home(source_env)
        if rocm_home is None:
            rocm_home = detect_rocm_home(source_env)

    use_tensor_engine = source_env.get("USE_TENSOR_ENGINE", "1") == "1"
    if use_tensor_engine and torch_config is None:
        raise BuildConfigurationError(
            "tensor_engine build requested but torch is not available. "
            "Install torch first, or set USE_TENSOR_ENGINE=0."
        )
    build_tensor_engine = use_tensor_engine and torch_config is not None

    gpu_platform = source_env.get("MONARCH_GPU_PLATFORM", "").lower()
    if gpu_platform not in ("", "cuda", "rocm", "none"):
        raise BuildConfigurationError(
            f"Invalid MONARCH_GPU_PLATFORM={gpu_platform}. "
            "Use 'cuda', 'rocm', or 'none'"
        )
    if gpu_platform == "cuda" and cuda_home is None:
        raise BuildConfigurationError("MONARCH_GPU_PLATFORM=cuda but CUDA not found")
    if gpu_platform == "rocm" and rocm_home is None:
        raise BuildConfigurationError("MONARCH_GPU_PLATFORM=rocm but ROCm not found")
    if not gpu_platform and build_tensor_engine and cuda_home and rocm_home:
        raise BuildConfigurationError(
            "Both CUDA and ROCm detected. Set MONARCH_GPU_PLATFORM=cuda, "
            "=rocm, or =none."
        )

    auto_detect = gpu_platform == ""
    has_cuda = gpu_platform == "cuda" or (auto_detect and cuda_home is not None)
    has_rocm = gpu_platform == "rocm" or (auto_detect and rocm_home is not None)
    build_cuda = build_tensor_engine and has_cuda
    build_rocm = build_tensor_engine and has_rocm
    build_gpu = build_cuda or build_rocm
    build_rdma = has_cuda or has_rocm

    rustflags = ["-Zthreads=16", "--cfg=tracing_unstable"]
    if source_env.get("CI") == "true":
        rustflags.append("--cfg=hyperactor_verify_auto_traits")
    if source_env.get("ENABLE_MESSAGE_LOGGING"):
        rustflags.append("--cfg=enable_hyperactor_message_logging")

    python_include_candidates = (
        sysconfig.get_path("include"),
        str((config_vars or sysconfig.get_config_vars()).get("CONFINCLUDEPY") or ""),
        str((config_vars or sysconfig.get_config_vars()).get("INCLUDEPY") or ""),
        str((config_vars or sysconfig.get_config_vars()).get("INCLUDEDIR") or ""),
    )
    python_include = next(
        (
            path
            for path in python_include_candidates
            if path and (Path(path) / "Python.h").is_file()
        ),
        next((path for path in python_include_candidates if path), ""),
    )

    if python_executable is None:
        python_executable = getattr(sys, "_base_executable", sys.executable)

    build_env: dict[str, str] = {
        "PYO3_PYTHON": source_env.get("PYO3_PYTHON", python_executable),
        "RUSTFLAGS": " ".join(rustflags),
        "MONARCH_PYTHON_INCLUDE": python_include,
    }
    if build_tensor_engine:
        assert torch_config is not None
        build_env.update(
            {
                "CXXFLAGS": f"-D_GLIBCXX_USE_CXX11_ABI={torch_config.cxx11_abi}",
                "LIBTORCH_LIB": os.fspath(torch_config.lib_path),
                "LIBTORCH_INCLUDE": os.pathsep.join(
                    os.fspath(path) for path in torch_config.include_paths
                ),
                "_GLIBCXX_USE_CXX11_ABI": str(torch_config.cxx11_abi),
                "TORCH_SYS_USE_PYTORCH_APIS": "0",
            }
        )
    else:
        build_env["CXXFLAGS"] = "-D_GLIBCXX_USE_CXX11_ABI=1"

    if build_cuda:
        assert cuda_home is not None
        build_env["CUDA_HOME"] = os.fspath(cuda_home)
        build_env["MONARCH_BUILD_CUDA"] = "1"
    elif build_rocm:
        assert rocm_home is not None
        build_env["ROCM_PATH"] = os.fspath(rocm_home)

    features = ["extension-module", "distributed_sql_telemetry", "tui-bin"]
    if build_rdma:
        features.append("rdma")
    if build_tensor_engine:
        features.extend(("tensor_engine", "embedded-cpp"))
    if build_gpu:
        features.append("tensor_engine_gpu")

    return BuildConfig(
        torch=torch_config,
        cuda_home=cuda_home,
        rocm_home=rocm_home,
        use_tensor_engine=use_tensor_engine,
        gpu_platform=gpu_platform,
        build_tensor_engine=build_tensor_engine,
        has_cuda=has_cuda,
        has_rocm=has_rocm,
        build_cuda=build_cuda,
        build_rocm=build_rocm,
        build_gpu=build_gpu,
        build_rdma=build_rdma,
        cargo_features=tuple(features),
        environment=build_env,
        rust_link_flags=_linux_rust_link_flags(
            platform=platform,
            prefix=prefix,
            config_vars=config_vars or sysconfig.get_config_vars(),
        ),
    )


def apply_build_environment(
    config: BuildConfig, environ: MutableMapping[str, str] = os.environ
) -> None:
    environ.update(config.environment)


def print_build_summary(config: BuildConfig) -> None:
    print("=" * 80)
    if config.build_tensor_engine:
        assert config.torch is not None
        if config.build_gpu:
            print("✓ Building WITH tensor_engine + GPU support")
            print(f"  - PyTorch: {config.torch.lib_path}")
            if config.build_cuda:
                print(f"  - CUDA: {config.cuda_home}")
            else:
                print(f"  - ROCm: {config.rocm_home}")
        else:
            print("✓ Building WITH tensor_engine (CPU-only, no GPU/NCCL)")
            print(f"  - PyTorch: {config.torch.lib_path}")
        state = "enabled" if config.torch.cxx11_abi else "disabled"
        print(f"  - C++11 ABI: {state}")
    else:
        print("Building WITHOUT tensor_engine (no torch)")
    if config.build_rdma:
        print("  - RDMA: included")
    else:
        print("  - RDMA: not included (no CUDA/ROCm toolchain found)")
    print("=" * 80)


def build_frontend(
    output_dir: Path,
    *,
    frontend_dir: Path = FRONTEND_DIR,
    require: bool = False,
) -> bool:
    """Build dashboard assets into *output_dir* without staging in the package."""

    output_dir = output_dir.resolve()
    build_index = output_dir / "index.html"
    if build_index.is_file():
        print(">> Pre-built frontend found, skipping npm build")
        return True

    source_build = frontend_dir / "build"
    if output_dir != source_build.resolve() and (source_build / "index.html").is_file():
        shutil.copytree(source_build, output_dir, dirs_exist_ok=True)
        print(">> Copied pre-built frontend assets")
        return True

    if not frontend_dir.is_dir():
        message = f"Frontend directory not found: {frontend_dir}"
        if require:
            raise BuildConfigurationError(message)
        print(message)
        return False

    npm = "/usr/bin/npm" if Path("/usr/bin/npm").exists() else shutil.which("npm")
    if not npm:
        message = (
            "npm not found. Install Node.js to build the dashboard frontend, "
            "or provide pre-built assets."
        )
        if require:
            raise BuildConfigurationError(message)
        print(f"WARNING: {message}")
        return False

    print(f"Building dashboard frontend into {output_dir}...")
    try:
        subprocess.check_call([npm, "ci"], cwd=frontend_dir)
        output_js = output_dir / "static" / "js" / "main.js"
        output_js.parent.mkdir(parents=True, exist_ok=True)
        esbuild = frontend_dir / "node_modules" / ".bin" / "esbuild"
        subprocess.check_call(
            [
                os.fspath(esbuild),
                "src/index.tsx",
                "--bundle",
                f"--outfile={output_js}",
                "--loader:.tsx=tsx",
                "--loader:.ts=ts",
                "--loader:.css=css",
                "--jsx=automatic",
                "--minify",
                "--target=es2020",
                '--define:process.env.NODE_ENV="production"',
            ],
            cwd=frontend_dir,
        )
        js_css = output_dir / "static" / "js" / "main.css"
        if js_css.is_file():
            css_dir = output_dir / "static" / "css"
            css_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(js_css, css_dir / "main.css")
        shutil.copy2(frontend_dir / "public" / "index.html", build_index)
    except FileNotFoundError as error:
        if require:
            raise BuildConfigurationError("frontend build tool not found") from error
        print("WARNING: frontend build tool not found. Skipping frontend build.")
        return False
    except subprocess.CalledProcessError as error:
        raise BuildConfigurationError("frontend build failed") from error

    print("Frontend build completed successfully")
    return True


def merge_maturin_args(
    config_settings: Mapping[str, object] | None,
    required: Sequence[str],
) -> dict[str, object]:
    """Add required maturin PEP 517 arguments without dropping caller options."""

    result = dict(config_settings or {})
    key = "maturin.build-args"
    existing = result.get(key, result.get("build-args", []))
    if isinstance(existing, str):
        import shlex

        args = shlex.split(existing)
    else:
        args = list(existing) if existing else []
    result.pop("build-args", None)
    result[key] = [*required, *args]
    return result
