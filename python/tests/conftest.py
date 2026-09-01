# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import os
import platform

pytest_plugins = ["crash_recovery_plugin"]
import sys
from pathlib import Path

import pytest

_THIS_DIR = Path(__file__).parent

# The multihost RDMA benchmark's modules import as bare names -- `bench_topology`,
# not a dotted path -- so the owning directory needs to be added to `sys.path`.
_BENCH_DIR = _THIS_DIR.parent / "benches" / "multihost_rdma"
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

collect_ignore: list[str] = []

# FUSE and RDMA require Linux; skip these files on other platforms to avoid
# ImportError during collection.
if sys.platform != "linux":
    collect_ignore.extend(
        str(_THIS_DIR / name)
        for name in [
            "test_remotemount.py",
            "test_rdma.py",
            "test_rdma_cpu_no_torch.py",
            "test_rdma_unit.py",
            "rdma_load_test.py",
        ]
    )

# Several test files import monarch.mesh_controller or monarch._testing which
# transitively require the tensor_engine Rust extension.  When the extension is
# not compiled in (USE_TENSOR_ENGINE=0), skip collection to avoid ImportError.
# When running as the crash-recovery worker, inherit the controller's detection
# results via env vars so both processes make identical collect_ignore decisions.
if "_CRASH_RECOVERY_HAS_TENSOR_ENGINE" in os.environ:
    _HAS_TENSOR_ENGINE = os.environ["_CRASH_RECOVERY_HAS_TENSOR_ENGINE"] == "1"
else:
    try:
        from monarch._rust_bindings import has_tensor_engine as _has_te_fn

        _HAS_TENSOR_ENGINE = _has_te_fn()
    except Exception:
        _HAS_TENSOR_ENGINE = False
    os.environ["_CRASH_RECOVERY_HAS_TENSOR_ENGINE"] = "1" if _HAS_TENSOR_ENGINE else "0"

if not _HAS_TENSOR_ENGINE:
    collect_ignore.extend(
        str(_THIS_DIR / name)
        for name in [
            "test_tensor_engine.py",
            "test_remote_functions.py",
            "test_controller.py",
            "test_builtins_log.py",
            "test_builtins_random.py",
            "test_coalescing.py",
            "test_device_mesh.py",
            "test_future.py",
            "test_grad_generator.py",
            "simulator/test_communication_model.py",
            "simulator/test_ir.py",
            "simulator/test_profiling.py",
            "simulator/test_simulator.py",
            "simulator/test_worker.py",
        ]
    )

# The simulator tests and a handful of others exercise paths that unconditionally
# construct CUDA tensors. They are collectable with a CPU-only tensor_engine
# build but cannot run without a CUDA device, so skip them when CUDA is absent.
if "_CRASH_RECOVERY_HAS_CUDA" in os.environ:
    _HAS_CUDA = os.environ["_CRASH_RECOVERY_HAS_CUDA"] == "1"
else:
    try:
        import torch as _torch

        _HAS_CUDA = _torch.cuda.is_available()
    except (ImportError, RuntimeError):
        _HAS_CUDA = False
    os.environ["_CRASH_RECOVERY_HAS_CUDA"] = "1" if _HAS_CUDA else "0"

if _HAS_TENSOR_ENGINE and not _HAS_CUDA:
    collect_ignore.extend(
        str(_THIS_DIR / name)
        for name in [
            "simulator/test_simulator.py",
            "simulator/test_worker.py",
        ]
    )

# Propagate sys.path to PYTHONPATH so that worker subprocesses spawned by
# monarch (e.g. distributed_proc_mesh) see the same import paths as the
# pytest parent process. pytest's default "prepend" import mode modifies
# sys.path at the Python level, but child processes don't inherit that —
# they only see PYTHONPATH.
os.environ["PYTHONPATH"] = os.pathsep.join(sys.path)

# disabled_tests.txt lives at the project root (three levels up from here:
# python/tests/conftest.py -> python/tests -> python -> project root).
_DISABLED_TESTS_FILE = Path(__file__).parent.parent.parent / "disabled_tests.txt"
_IS_MACOS_ARM64 = sys.platform == "darwin" and platform.machine() == "arm64"
_NO_TENSOR_ENGINE_SKIP_PREFIXES = (
    "python/tests/simulator/test_communication_model.py::",
    "python/tests/simulator/test_ir.py::",
)
_MACOS_ARM64_SKIP_NODEIDS = frozenset(
    {
        "python/tests/test_config.py::test_codec_max_frame_length_with_increased_limit",
        "python/tests/test_cuda.py::TestEnvBeforeCuda::test_cleanup_torch_distributed",
        "python/tests/test_cuda.py::TestEnvBeforeCuda::test_lambda_sets_env_vars_before_cuda_init",
        "python/tests/test_cuda.py::TestEnvBeforeCuda::test_proc_mesh_with_dictionary_env",
        "python/tests/test_cuda.py::TestEnvBeforeCuda::test_proc_mesh_with_lambda_env",
        "python/tests/test_debugger.py::test_debug_with_pickle_by_value",
        "python/tests/test_host_mesh.py::test_host_mesh_context_manager",
        "python/tests/test_host_mesh.py::test_spawn_procs_with_taskset_bind",
        "python/tests/test_host_mesh.py::test_stop_and_reconnect",
    }
)


def _load_disabled_tests() -> frozenset[str]:
    if not _DISABLED_TESTS_FILE.exists():
        return frozenset()
    return frozenset(
        line.strip()
        for line in _DISABLED_TESTS_FILE.read_text().splitlines()
        if line.strip()
    )


def _is_disabled_test(node_id: str, disabled: frozenset[str]) -> bool:
    """Return whether a pytest node ID matches a disabled-test entry.

    Entries may identify a complete node ID, a structural prefix such as a
    file or class node ID, or a bare test name. Prefixes only match at pytest
    node-ID boundaries so similarly named tests and files remain enabled.
    """
    test_name = node_id.rsplit("::", 1)[-1]
    for entry in disabled:
        if node_id == entry or test_name == entry:
            return True
        if node_id.startswith((f"{entry}::", f"{entry}[")):
            return True
        if test_name.startswith(f"{entry}["):
            return True
    return False


def pytest_collection_modifyitems(
    items: list[pytest.Item],
    config: pytest.Config,
) -> None:
    """Skip any test whose name or node ID appears in disabled_tests.txt."""
    disabled = _load_disabled_tests()

    for item in items:
        node_id = item.nodeid
        if not _HAS_TENSOR_ENGINE and node_id.startswith(
            _NO_TENSOR_ENGINE_SKIP_PREFIXES
        ):
            item.add_marker(
                pytest.mark.skip(reason="requires tensor_engine Rust extension")
            )

        if _IS_MACOS_ARM64 and node_id in _MACOS_ARM64_SKIP_NODEIDS:
            item.add_marker(
                pytest.mark.skip(reason="unsupported or flaky on macOS arm64 CPU CI")
            )

        if not disabled:
            continue

        test_name = node_id.rsplit("::", 1)[-1]
        if _is_disabled_test(node_id, disabled):
            item.add_marker(
                pytest.mark.skip(
                    reason=f"Disabled via GitHub issue: DISABLED {test_name}"
                )
            )


def _test_uses_rdma(request: pytest.FixtureRequest) -> bool:
    """Whether a test exercises monarch's RDMA path (and so needs torch preloaded).

    Matches both the RDMA test modules (``test_rdma*`` / ``rdma_load_test``) and any
    test parametrized by ``@rdma_backends`` (rdma_test_utils) -- which drives the
    RDMA path from non-rdma-named modules too, e.g. ``test_gil_on_control_plane``.
    Both appear in the node id: the module path, or the ``rdma_disable_ibverbs``
    param id. (``@rdma_backends`` parametrizes via a config helper, so the params
    show up in the id but NOT in ``callspec.params`` -- hence checking the id.)
    """
    return "rdma" in (getattr(request.node, "nodeid", "") or "").lower()


@pytest.fixture(autouse=True)
def _scope_torch_preload_to_rdma(request: pytest.FixtureRequest):
    """Scope MONARCH_PRELOAD_TORCH so only RDMA tests pay the torch-preload cost.

    Only monarch's RDMA path makes the rdmaxcel HIP call that races with torch's
    bundled libamdhip64 on ROCm (the fatal hip.cpp:512 "hipApiName ..." abort).
    The ROCm CI enables MONARCH_PRELOAD_TORCH job-wide so the test worker's own
    (inline) RDMA calls stay safe -- but that also made *every* proc mesh / isolated
    subprocess spawned by the many non-RDMA actor tests import torch (~10s each),
    timing them out on slower ROCm runners. So turn the preload OFF while a
    non-RDMA test runs: the procs it spawns then skip the torch import. RDMA test
    modules keep it on (a test may still override, e.g. test_rdma_cpu_no_torch pins
    it to "0" in its isolated subprocess). No-op when the flag isn't set (CUDA/CPU).
    """
    if _test_uses_rdma(request):
        # RDMA tests keep the preload on. Spawned procs use the lite dlopen preload
        # (proc_mesh sets MONARCH_PRELOAD_TORCH_HIP), which is ~ms rather than the
        # ~10s of a full `import torch`, so multi-proc RDMA spawns no longer overrun
        # the Host::spawn readiness window -- no timeout override needed.
        yield
        return
    previous = os.environ.get("MONARCH_PRELOAD_TORCH")
    os.environ["MONARCH_PRELOAD_TORCH"] = "0"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("MONARCH_PRELOAD_TORCH", None)
        else:
            os.environ["MONARCH_PRELOAD_TORCH"] = previous
