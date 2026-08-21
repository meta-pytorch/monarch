# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

from __future__ import annotations

import csv
import io
import os
import stat
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

import build_backend
from build_support import BuildConfigurationError, select_build_config, TorchConfig

TORCH = TorchConfig(
    lib_path=Path("/opt/torch/lib"),
    include_paths=(Path("/opt/torch/include"),),
    cxx11_abi=0,
)


def config(
    environ: dict[str, str],
    *,
    torch: TorchConfig | None = None,
    cuda: Path | None = None,
    rocm: Path | None = None,
):
    return select_build_config(
        environ=environ,
        torch_config=torch,
        cuda_home=cuda,
        rocm_home=rocm,
        detect=False,
        platform="darwin",
        config_vars={"INCLUDEPY": "/python/include"},
        python_executable="/python/bin/python",
    )


class BuildConfigTest(unittest.TestCase):
    def test_slim_build(self) -> None:
        selected = config({"USE_TENSOR_ENGINE": "0"})
        self.assertFalse(selected.build_tensor_engine)
        self.assertFalse(selected.build_rdma)
        self.assertEqual(
            selected.cargo_features,
            ("extension-module", "distributed_sql_telemetry", "tui-bin"),
        )

    def test_cpu_tensor_engine_embeds_cpp(self) -> None:
        selected = config({"MONARCH_GPU_PLATFORM": "none"}, torch=TORCH)
        self.assertTrue(selected.build_tensor_engine)
        self.assertFalse(selected.build_gpu)
        self.assertIn("embedded-cpp", selected.cargo_features)
        self.assertEqual(selected.environment["_GLIBCXX_USE_CXX11_ABI"], "0")

    def test_cuda_tensor_engine(self) -> None:
        selected = config(
            {"MONARCH_GPU_PLATFORM": "cuda"},
            torch=TORCH,
            cuda=Path("/cuda"),
        )
        self.assertTrue(selected.build_cuda)
        self.assertTrue(selected.build_rdma)
        self.assertIn("tensor_engine_gpu", selected.cargo_features)
        self.assertEqual(selected.environment["MONARCH_BUILD_CUDA"], "1")

    def test_rocm_tensor_engine(self) -> None:
        selected = config(
            {"MONARCH_GPU_PLATFORM": "rocm"},
            torch=TORCH,
            rocm=Path("/rocm"),
        )
        self.assertTrue(selected.build_rocm)
        self.assertNotIn("MONARCH_BUILD_CUDA", selected.environment)
        self.assertEqual(selected.environment["ROCM_PATH"], "/rocm")

    def test_invalid_platform(self) -> None:
        with self.assertRaisesRegex(BuildConfigurationError, "Invalid"):
            config({"MONARCH_GPU_PLATFORM": "metal"}, torch=TORCH)

    def test_forced_toolchain_must_exist(self) -> None:
        with self.assertRaisesRegex(BuildConfigurationError, "CUDA not found"):
            config({"MONARCH_GPU_PLATFORM": "cuda"}, torch=TORCH)

    def test_dual_toolchain_requires_selection_for_tensor_engine(self) -> None:
        with self.assertRaisesRegex(BuildConfigurationError, "Both CUDA and ROCm"):
            config({}, torch=TORCH, cuda=Path("/cuda"), rocm=Path("/rocm"))

    def test_dual_toolchain_preserves_slim_semantics(self) -> None:
        selected = config(
            {"USE_TENSOR_ENGINE": "0"},
            cuda=Path("/cuda"),
            rocm=Path("/rocm"),
        )
        self.assertTrue(selected.build_rdma)
        self.assertFalse(selected.build_gpu)


class ArtifactRewriteTest(unittest.TestCase):
    def test_wheel_rewrite_adds_assets_and_valid_record(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            wheel = root / "torchmonarch-0.0.0-cp312-cp312-macosx_11_0_arm64.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("monarch/__init__.py", b"")
                archive.writestr(
                    "torchmonarch-0.0.0.dist-info/METADATA",
                    b"Metadata-Version: 2.4\nName: torchmonarch\nVersion: 0.0.0\n",
                )
                script_info = zipfile.ZipInfo(
                    "torchmonarch-0.0.0.data/scripts/monarch-tui"
                )
                script_info.create_system = 3
                script_info.external_attr = (stat.S_IFREG | 0o755) << 16
                archive.writestr(script_info, b"native executable")
                archive.writestr(
                    "torchmonarch-0.0.0.dist-info/RECORD",
                    b"torchmonarch-0.0.0.dist-info/RECORD,,\n",
                )
            frontend = root / "frontend"
            frontend.mkdir()
            (frontend / "index.html").write_text("dashboard")

            rewritten = build_backend._rewrite_wheel(
                wheel,
                version="1.2.3rc1",
                frontend=frontend,
            )
            self.assertEqual(
                rewritten.name,
                "torchmonarch-1.2.3rc1-cp312-cp312-macosx_11_0_arm64.whl",
            )
            with zipfile.ZipFile(rewritten) as archive:
                names = set(archive.namelist())
                metadata_path = "torchmonarch-1.2.3rc1.dist-info/METADATA"
                record_path = "torchmonarch-1.2.3rc1.dist-info/RECORD"
                script_path = "torchmonarch-1.2.3rc1.data/scripts/monarch-tui"
                self.assertIn(b"Version: 1.2.3rc1", archive.read(metadata_path))
                self.assertIn(script_path, names)
                self.assertIn(
                    "monarch/monarch_dashboard/frontend/build/index.html", names
                )
                script_mode = archive.getinfo(script_path).external_attr >> 16
                self.assertTrue(script_mode & stat.S_IXUSR)

                records = list(
                    csv.reader(io.StringIO(archive.read(record_path).decode()))
                )
                record_map = {row[0]: row[1:] for row in records}
                self.assertEqual(record_map[record_path], ["", ""])
                for name in names - {record_path}:
                    data = archive.read(name)
                    digest = build_backend._record_hash(data)
                    self.assertEqual(record_map[name], [digest, str(len(data))])

    def test_sdist_rewrite_patches_rebuild_version(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "torchmonarch-0.0.0.tar.gz"
            files = {
                "torchmonarch-0.0.0/PKG-INFO": b"Name: torchmonarch\nVersion: 0.0.0\n",
                "torchmonarch-0.0.0/monarch_extension/Cargo.toml": (
                    b'[package]\nname = "monarch_extension"\nversion = "0.0.0"\n'
                ),
                "torchmonarch-0.0.0/Cargo.lock": (
                    b'[[package]]\nname = "monarch_extension"\nversion = "0.0.0"\n'
                ),
            }
            with tarfile.open(source, "w:gz") as archive:
                for name, data in files.items():
                    info = tarfile.TarInfo(name)
                    info.size = len(data)
                    archive.addfile(info, io.BytesIO(data))

            with mock.patch.object(
                build_backend,
                "_refresh_sdist_lock",
                return_value=build_backend._patch_lock_version(
                    files["torchmonarch-0.0.0/Cargo.lock"], "1.2.3-rc.1"
                ),
            ):
                rewritten = build_backend._rewrite_sdist(source, "1.2.3rc1")
            self.assertEqual(rewritten.name, "torchmonarch-1.2.3rc1.tar.gz")
            with tarfile.open(rewritten, "r:gz") as archive:
                pkg = archive.extractfile("torchmonarch-1.2.3rc1/PKG-INFO").read()
                manifest_info = archive.getmember(
                    "torchmonarch-1.2.3rc1/monarch_extension/Cargo.toml"
                )
                manifest = archive.extractfile(manifest_info).read()
                lock_info = archive.getmember("torchmonarch-1.2.3rc1/Cargo.lock")
                lock = archive.extractfile(lock_info).read()
            self.assertIn(b"Version: 1.2.3rc1", pkg)
            self.assertIn(b'version = "1.2.3-rc.1"', manifest)
            self.assertIn(b'version = "1.2.3-rc.1"', lock)
            self.assertGreater(lock_info.mtime, manifest_info.mtime)

    def test_environment_version_is_normalized(self) -> None:
        with mock.patch.dict(os.environ, {"MONARCH_VERSION": "1.2.3-rc1"}):
            self.assertEqual(build_backend._package_version(), "1.2.3rc1")


if __name__ == "__main__":
    unittest.main()
