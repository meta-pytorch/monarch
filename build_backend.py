# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Project-owned PEP 517 backend which adds Monarch assets to maturin."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path

import maturin
from packaging.version import InvalidVersion, Version

from build_support import (
    build_frontend,
    BuildConfig,
    DEFAULT_VERSION,
    merge_maturin_args,
    print_build_summary,
    ROOT,
    select_build_config,
)

DIST_NAME = "torchmonarch"
EXTENSION_MANIFEST = ROOT / "monarch_extension" / "Cargo.toml"
LOCKFILE = ROOT / "Cargo.lock"
TUI_MANIFEST = ROOT / "hyperactor_mesh_admin_tui" / "Cargo.toml"
TUI_TARGET_NAME = "hyperactor_mesh_admin_tui"


def _metadata_version(data: bytes) -> str | None:
    match = re.search(rb"(?m)^Version:\s*(\S+)\s*$", data)
    return match.group(1).decode() if match else None


def _replace_metadata_version(data: bytes, version: str) -> bytes:
    replacement = f"Version: {version}".encode()
    updated, count = re.subn(rb"(?m)^Version:\s*\S+\s*$", replacement, data, count=1)
    if count != 1:
        raise RuntimeError("generated package metadata has no Version field")
    return updated


def _manifest_version(path: Path = EXTENSION_MANIFEST) -> str | None:
    in_package = False
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("["):
            in_package = stripped == "[package]"
        elif in_package:
            match = re.match(r'version\s*=\s*"([^"]+)"', stripped)
            if match:
                return match.group(1)
    return None


def _package_version() -> str:
    raw = os.environ.get("MONARCH_VERSION")
    if raw is None:
        pkg_info = ROOT / "PKG-INFO"
        if pkg_info.is_file():
            raw = _metadata_version(pkg_info.read_bytes())
    if raw is None:
        manifest_version = _manifest_version()
        raw = (
            manifest_version
            if manifest_version not in (None, "0.0.0")
            else DEFAULT_VERSION
        )
    try:
        return str(Version(raw))
    except InvalidVersion as error:
        raise RuntimeError(f"Invalid MONARCH_VERSION={raw!r}") from error


def _wheel_component(value: str) -> str:
    return re.sub(r"[^\w\d.]+", "_", value, flags=re.ASCII)


def _cargo_version(version: str) -> str:
    """Convert the supported PEP 440 release forms to valid Cargo semver."""

    parsed = Version(version)
    release = ".".join(str(part) for part in (*parsed.release, 0, 0)[:3])
    prerelease: list[str] = []
    if parsed.pre:
        tag, number = parsed.pre
        prerelease.extend(({"a": "alpha", "b": "beta", "rc": "rc"}[tag], str(number)))
    if parsed.dev is not None:
        prerelease.extend(("dev", str(parsed.dev)))

    result = release
    if prerelease:
        result += "-" + ".".join(prerelease)

    metadata: list[str] = []
    if parsed.post is not None:
        metadata.extend(("post", str(parsed.post)))
    if parsed.local:
        metadata.extend(re.split(r"[-_.]+", parsed.local))
    if metadata:
        result += "+" + ".".join(metadata)
    return result


def _required_maturin_args(
    config: BuildConfig, config_settings: Mapping[str, object] | None
) -> dict[str, object]:
    settings = merge_maturin_args(
        config_settings,
        [
            "--locked",
            "--no-default-features",
            "--features",
            ",".join(config.cargo_features),
        ],
    )
    args = list(settings["maturin.build-args"])
    if config.rust_link_flags:
        if "--" in args:
            separator = args.index("--")
            args[separator + 1 : separator + 1] = config.rust_link_flags
        else:
            args.extend(("--", *config.rust_link_flags))
    settings["maturin.build-args"] = args
    return settings


@contextmanager
def _configured_build() -> Iterator[BuildConfig]:
    config = select_build_config()
    print_build_summary(config)
    keys = set(config.environment) | {"MONARCH_BUILD_CUDA"}
    previous = {key: os.environ.get(key) for key in keys}
    try:
        for key in keys:
            if key not in config.environment:
                os.environ.pop(key, None)
        os.environ.update(config.environment)
        yield config
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _cargo_option(args: Sequence[str], name: str) -> str | None:
    for index, arg in enumerate(args):
        if arg == name and index + 1 < len(args):
            return args[index + 1]
        if arg.startswith(f"{name}="):
            return arg.split("=", 1)[1]
    return None


def _tui_target_dir() -> Path:
    base = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    return base / "monarch-tui"


def _build_tui(config_settings: Mapping[str, object] | None) -> Path:
    settings = merge_maturin_args(config_settings, [])
    maturin_args = list(settings["maturin.build-args"])
    command = [
        "cargo",
        "build",
        "--locked",
        "--release",
        "--manifest-path",
        os.fspath(TUI_MANIFEST),
        "--target-dir",
        os.fspath(_tui_target_dir()),
        "--message-format=json-render-diagnostics",
    ]
    target = _cargo_option(maturin_args, "--target") or os.environ.get(
        "CARGO_BUILD_TARGET"
    )
    if target:
        command.extend(("--target", target))

    print(f"Running `{' '.join(command)}`")
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        text=True,
    )
    executable: Path | None = None
    assert process.stdout is not None
    for line in process.stdout:
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            print(line, end="")
            continue
        rendered = message.get("message", {}).get("rendered")
        if rendered:
            print(rendered, end="", file=sys.stderr)
        target_info = message.get("target", {})
        if (
            message.get("reason") == "compiler-artifact"
            and target_info.get("name") == TUI_TARGET_NAME
            and message.get("executable")
        ):
            executable = Path(message["executable"])
    return_code = process.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, command)
    if executable is None or not executable.is_file():
        raise RuntimeError("cargo did not report the monarch-tui executable")
    return executable


def _zip_info(name: str, mode: int) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = mode << 16
    return info


def _record_hash(data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
    return f"sha256={digest.decode()}"


def _rewrite_wheel(
    wheel_path: Path,
    *,
    version: str,
    tui: Path,
    frontend: Path | None,
) -> Path:
    with zipfile.ZipFile(wheel_path) as source:
        original = [(info, source.read(info.filename)) for info in source.infolist()]

    metadata_entries = [
        (info.filename, data)
        for info, data in original
        if info.filename.endswith(".dist-info/METADATA")
    ]
    if len(metadata_entries) != 1:
        raise RuntimeError("wheel must contain exactly one METADATA file")
    metadata_path, metadata = metadata_entries[0]
    old_version = _metadata_version(metadata)
    if old_version is None:
        raise RuntimeError("wheel METADATA has no Version field")
    old_dist_info = metadata_path.rsplit("/", 1)[0]
    new_dist_info = f"{DIST_NAME}-{_wheel_component(version)}.dist-info"
    old_data_dir = f"{DIST_NAME}-{_wheel_component(old_version)}.data"
    new_data_dir = f"{DIST_NAME}-{_wheel_component(version)}.data"
    record_path = f"{new_dist_info}/RECORD"

    entries: dict[str, tuple[zipfile.ZipInfo, bytes]] = {}
    for info, data in original:
        name = info.filename
        if name == f"{old_dist_info}/RECORD":
            continue
        if name.startswith(f"{old_dist_info}/"):
            name = new_dist_info + name[len(old_dist_info) :]
        elif name.startswith(f"{old_data_dir}/"):
            name = new_data_dir + name[len(old_data_dir) :]
        if name == f"{new_dist_info}/METADATA":
            data = _replace_metadata_version(data, version)
        info.filename = name
        entries[name] = (info, data)

    script_name = f"{new_data_dir}/scripts/monarch-tui"
    entries[script_name] = (
        _zip_info(script_name, stat.S_IFREG | 0o755),
        tui.read_bytes(),
    )

    if frontend is not None:
        for path in sorted(frontend.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(frontend).as_posix()
            name = f"monarch/monarch_dashboard/frontend/build/{relative}"
            entries[name] = (
                _zip_info(name, stat.S_IFREG | 0o644),
                path.read_bytes(),
            )

    record_buffer = io.StringIO(newline="")
    writer = csv.writer(record_buffer, lineterminator="\n")
    for name in sorted(entries):
        data = entries[name][1]
        writer.writerow((name, _record_hash(data), str(len(data))))
    writer.writerow((record_path, "", ""))
    record_data = record_buffer.getvalue().encode()
    entries[record_path] = (
        _zip_info(record_path, stat.S_IFREG | 0o644),
        record_data,
    )

    parts = wheel_path.name.removesuffix(".whl").split("-")
    if len(parts) < 5:
        raise RuntimeError(f"invalid wheel filename: {wheel_path.name}")
    new_filename = "-".join((DIST_NAME, _wheel_component(version), *parts[2:])) + ".whl"
    destination = wheel_path.with_name(new_filename)
    temporary = destination.with_suffix(".whl.tmp")
    with zipfile.ZipFile(temporary, "w") as output:
        for name in sorted(entries):
            info, data = entries[name]
            output.writestr(info, data)
    os.replace(temporary, destination)
    if destination != wheel_path:
        wheel_path.unlink()
    return destination


def _patch_manifest_version(data: bytes, version: str) -> bytes:
    text = data.decode()
    lines = text.splitlines(keepends=True)
    in_package = False
    replaced = False
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("["):
            in_package = stripped == "[package]"
        elif in_package and re.match(r"version\s*=", stripped):
            newline = "\n" if line.endswith("\n") else ""
            lines[index] = f'version = "{version}"{newline}'
            replaced = True
            break
    if not replaced:
        raise RuntimeError("monarch_extension/Cargo.toml has no package version")
    return "".join(lines).encode()


def _patch_lock_version(data: bytes, version: str) -> bytes:
    text = data.decode()
    pattern = re.compile(
        r'(?ms)(^\[\[package\]\]\nname = "monarch_extension"\nversion = ")([^"]+)(")'
    )
    updated, count = pattern.subn(rf"\g<1>{version}\g<3>", text, count=1)
    if count != 1:
        raise RuntimeError("Cargo.lock has no monarch_extension package entry")
    return updated.encode()


def _rewrite_sdist(sdist_path: Path, version: str) -> Path:
    cargo_version = _cargo_version(version)
    with tarfile.open(sdist_path, "r:gz") as source:
        members = source.getmembers()
        if not members:
            raise RuntimeError("generated sdist is empty")
        old_root = members[0].name.split("/", 1)[0]
        archived: list[tuple[tarfile.TarInfo, bytes | None]] = []
        for member in members:
            stream = source.extractfile(member) if member.isfile() else None
            archived.append((member, stream.read() if stream else None))

    new_root = f"{DIST_NAME}-{version}"
    destination = sdist_path.with_name(f"{new_root}.tar.gz")
    temporary = destination.with_suffix(".tar.gz.tmp")
    with tarfile.open(temporary, "w:gz", format=tarfile.PAX_FORMAT) as output:
        for member, data in archived:
            suffix = member.name[len(old_root) :]
            member.name = new_root + suffix
            relative = suffix.lstrip("/")
            if data is not None:
                if relative == "PKG-INFO":
                    data = _replace_metadata_version(data, version)
                elif relative == "monarch_extension/Cargo.toml":
                    data = _patch_manifest_version(data, cargo_version)
                elif relative == "Cargo.lock":
                    data = _patch_lock_version(data, cargo_version)
                member.size = len(data)
                output.addfile(member, io.BytesIO(data))
            else:
                output.addfile(member)
    os.replace(temporary, destination)
    if destination != sdist_path:
        sdist_path.unlink()
    return destination


def _rewrite_prepared_metadata(
    metadata_directory: Path, dirname: str, version: str
) -> str:
    source = metadata_directory / dirname
    target_name = f"{DIST_NAME}-{_wheel_component(version)}.dist-info"
    target = metadata_directory / target_name
    metadata = source / "METADATA"
    metadata.write_bytes(_replace_metadata_version(metadata.read_bytes(), version))
    if source != target:
        source.rename(target)
    return target_name


def get_requires_for_build_wheel(
    config_settings: Mapping[str, object] | None = None,
) -> list[str]:
    return maturin.get_requires_for_build_wheel(config_settings)


def get_requires_for_build_editable(
    config_settings: Mapping[str, object] | None = None,
) -> list[str]:
    return maturin.get_requires_for_build_editable(config_settings)


def get_requires_for_build_sdist(
    config_settings: Mapping[str, object] | None = None,
) -> list[str]:
    return maturin.get_requires_for_build_sdist(config_settings)


def prepare_metadata_for_build_wheel(
    metadata_directory: str,
    config_settings: Mapping[str, object] | None = None,
) -> str:
    version = _package_version()
    with _configured_build() as config:
        dirname = maturin.prepare_metadata_for_build_wheel(
            metadata_directory,
            _required_maturin_args(config, config_settings),
        )
    return _rewrite_prepared_metadata(Path(metadata_directory), dirname, version)


def prepare_metadata_for_build_editable(
    metadata_directory: str,
    config_settings: Mapping[str, object] | None = None,
) -> str:
    return prepare_metadata_for_build_wheel(metadata_directory, config_settings)


def _build_wheel(
    wheel_directory: str,
    config_settings: Mapping[str, object] | None,
    metadata_directory: str | None,
    *,
    editable: bool,
) -> str:
    version = _package_version()
    require_frontend = os.environ.get("MONARCH_REQUIRE_FRONTEND") == "1"
    with (
        _configured_build() as config,
        tempfile.TemporaryDirectory(prefix="monarch-frontend-") as temp_dir,
    ):
        if editable:
            frontend = (
                ROOT / "python" / "monarch" / "monarch_dashboard" / "frontend" / "build"
            )
        else:
            frontend = Path(temp_dir) / "build"
        has_frontend = build_frontend(frontend, require=require_frontend)

        settings = _required_maturin_args(config, config_settings)
        # Maturin's prepared metadata still carries the Cargo version. Let it
        # regenerate internally, then normalize the finished wheel once.
        if editable:
            filename = maturin.build_editable(wheel_directory, settings, None)
        else:
            filename = maturin.build_wheel(wheel_directory, settings, None)
        tui = _build_tui(config_settings)
        rewritten = _rewrite_wheel(
            Path(wheel_directory) / filename,
            version=version,
            tui=tui,
            frontend=frontend if has_frontend else None,
        )
    return rewritten.name


def build_wheel(
    wheel_directory: str,
    config_settings: Mapping[str, object] | None = None,
    metadata_directory: str | None = None,
) -> str:
    return _build_wheel(
        wheel_directory,
        config_settings,
        metadata_directory,
        editable=False,
    )


def build_editable(
    wheel_directory: str,
    config_settings: Mapping[str, object] | None = None,
    metadata_directory: str | None = None,
) -> str:
    return _build_wheel(
        wheel_directory,
        config_settings,
        metadata_directory,
        editable=True,
    )


def build_sdist(
    sdist_directory: str,
    config_settings: Mapping[str, object] | None = None,
) -> str:
    version = _package_version()
    filename = maturin.build_sdist(sdist_directory, config_settings)
    return _rewrite_sdist(Path(sdist_directory) / filename, version).name
