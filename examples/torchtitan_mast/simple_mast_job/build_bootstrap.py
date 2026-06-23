#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Build a slim monarch bootstrap fbpkg and launch the h100 MAST job.

H100 x86 only. ``build_bootstrap`` packages the slim wheel that
``setup_env.sh`` cached in ``/tmp/monarch_bootstrap_$USER/wheel`` into a
monarch-only venv and uploads it as an ephemeral fbpkg (cached, keyed by the
wheel). ``launch_mast`` schedules the torchtitan h100 jobspec; the worker boots
as ``run_worker_loop_forever`` from that fbpkg.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from subprocess import check_output
from typing import Optional

_WORKER_PORT = 26600
_DEFAULT_CACHE_DIR = Path(f"/tmp/monarch_bootstrap_{os.environ['USER']}")
# Python used to create the worker venv on the devvm (platform010 is present on
# every host we target; the wheel installed into it is python3.12).
_PYTHON = "/usr/local/fbcode/platform010/bin/python3.12"

DEFAULT_WORKER_FBPKG_NAME = "monarch_additional_packages"

_DEFAULT_NETWORK_AFFINITY = {"preferredScope": 3, "fallbackScope": 3}
_DEFAULT_X86_LOCALITY_CONSTRAINTS = ("region", "pci")

# h100 x86 host shape (whole 8-GPU host) for resourceLimit/machineConstraints.
_H100_RAM_MB = (2048 - 145) * 1024
_H100_CPU = 112
_H100_GPU = 8
_H100_SERVER_SUBTYPE = 200007

# Boot the worker loop. Kept in the ``-X faulthandler -c <code>`` form so a
# hung worker dumps tracebacks on SIGABRT.
_BOOTSTRAP_CODE = (
    "import socket; "
    "from monarch.actor import run_worker_loop_forever; "
    f'run_worker_loop_forever(address=f"metatls://{{socket.getfqdn()}}:{_WORKER_PORT}", '
    'ca="trust_all_connections")'
)


def run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
    print(f"+ {' '.join(str(c) for c in cmd)}")
    return subprocess.run(cmd, check=True, **kwargs)  # pyre-ignore[6]


def find_wheel(wheel_dir: Path) -> Path | None:
    wheels = list(wheel_dir.glob("*.whl"))
    return wheels[0] if wheels else None


def _cache_path(wheel: Path, package_name: str, cache_dir: Path) -> Path:
    """Identifier cache keyed by the wheel's identity so a fresh wheel rebuilds."""
    stat = wheel.stat()
    cache_key = hashlib.sha256(
        json.dumps(
            {
                "wheel": wheel.name,
                "wheel_mtime_ns": stat.st_mtime_ns,
                "package_name": package_name,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    return cache_dir / f"{package_name}_{cache_key}.txt"


def create_fbpkg(name: str, directory: str, expire: str) -> str:
    """Upload directory contents as an ephemeral fbpkg. Returns 'name:version'."""
    config_dir = tempfile.mkdtemp()
    try:
        materialized_dir = os.path.join(config_dir, "materialized_configs")
        os.makedirs(materialized_dir)
        json_path = os.path.join(materialized_dir, f"{name}.fbpkg.materialized_JSON")

        package_json = {
            "paths": sorted(os.listdir(directory)),
            "build_command": "",
        }
        with open(json_path, "w") as f:
            json.dump(package_json, f)

        output = check_output(
            [
                "fbpkg",
                "build",
                "--yes",
                "--ephemeral",
                "--configerator-path",
                config_dir,
                name,
                "--expire",
                expire,
            ],
            cwd=directory,
        ).decode("utf-8")
    finally:
        subprocess.run(["rm", "-rf", config_dir], check=False)

    print(output)
    lines = [line for line in output.splitlines() if line.strip()]
    return lines[-1].strip()


def build_bootstrap(
    package_name: str = DEFAULT_WORKER_FBPKG_NAME,
    cache_dir: Path = _DEFAULT_CACHE_DIR,
    expire: str = "4w",
    rebuild: bool = False,
) -> str:
    """Package the slim wheel ``setup_env.sh`` cached into an ephemeral fbpkg.

    Returns the fbpkg identifier, cached (keyed by the wheel) so repeat applies
    skip the venv build + upload. Set ``MONARCH_MAST_WORKER_FBPKG_ID`` to reuse
    a prebuilt fbpkg, or ``MONARCH_REBUILD=1`` to force a re-upload.
    """
    override_identifier = os.environ.get("MONARCH_MAST_WORKER_FBPKG_ID")
    if override_identifier:
        return override_identifier

    if not rebuild:
        rebuild = os.environ.get("MONARCH_REBUILD", "0") == "1"

    wheel_dir = cache_dir / "wheel"
    venv_dir = cache_dir / "venv"
    wheel_dir.mkdir(parents=True, exist_ok=True)

    if rebuild:
        print("=== MONARCH_REBUILD set -- clearing cached fbpkg identifier ===")
        for f in cache_dir.glob(f"{package_name}_*.txt"):
            f.unlink()

    # The wheel is built + cached by setup_env.sh; we only package it.
    wheel = find_wheel(wheel_dir)
    if wheel is None:
        raise RuntimeError(
            f"no monarch wheel in {wheel_dir}; run setup_env.sh first (it builds "
            "the slim wheel and caches it here)"
        )
    print(f"=== Using wheel: {wheel} ===")

    package_file = _cache_path(wheel, package_name, cache_dir)
    if package_file.exists():
        identifier = package_file.read_text().strip()
        if identifier:
            print(f"=== Using cached fbpkg identifier: {identifier} ===")
            return identifier

    print("\n=== Creating venv ===")
    if venv_dir.exists():
        shutil.rmtree(venv_dir)
    venv_python = venv_dir / "bin" / "python3.12"
    run([_PYTHON, "-m", "venv", str(venv_dir)])

    print("\n=== Installing wheel into venv ===")
    run([str(venv_python), "-m", "pip", "install", str(wheel)])

    print("\n=== Uploading venv as fbpkg ===")
    identifier = create_fbpkg(package_name, str(venv_dir), expire)
    package_file.write_text(identifier + "\n")
    print(f"Done. fbpkg identifier: {identifier}")
    return identifier


def _package_spec(name: str, version: str) -> dict[str, object]:
    package: dict[str, object] = {
        "name": name,
        "fbpkgIdentifier": f"{name}:{version}",
    }
    if re.fullmatch(r"[0-9a-fA-F]+", version):
        package["version"] = {"ephemeralId": version}
    return package


def _parse_locality_constraints(parts: tuple[str, ...]) -> Optional[dict[str, object]]:
    if not parts:
        return None
    kind, *options = parts
    if kind != "region":
        raise ValueError(
            f"Only region locality is supported in this example. Got {kind!r}"
        )
    if not options:
        raise ValueError("Region locality requires at least one option")
    return {"locality": 1, "options": list(options)}


def launch_mast(
    *,
    package_name: str,
    package_version: str,
    hosts: int,
    hpc_identity: str = "hyper_monarch",
    hpc_job_oncall: str = "monarch",
    rm_attribution: str = "msl_infra_pytorch_dev",
    hpc_cluster_uuid: str = "MastGenAICluster",
    env: Optional[dict[str, str]] = None,
    locality_constraints: tuple[str, ...] = (),
    job_name_prefix: str = "monarch_remotemount",
    run: bool = True,
) -> str:
    """Launch the torchtitan h100 MAST job using the bootstrap fbpkg (x86 only)."""
    if not locality_constraints:
        locality_constraints = _DEFAULT_X86_LOCALITY_CONSTRAINTS

    name = f"{job_name_prefix}_{time.time_ns()}"
    package = _package_spec(package_name, package_version)
    package_root = f"/packages/{package_name}"

    jobspec = {
        "name": name,
        "hpcClusterUuid": hpc_cluster_uuid,
        "hpcTaskGroups": [
            {
                "name": "workers",
                "taskCount": hosts,
                "taskCountPerHost": 1,
                "hardwareSpecificTaskGroupOverride": {},
                "spec": {
                    "command": f"{package_root}/bin/python3.12",
                    "arguments": [
                        "-X",
                        "faulthandler",
                        "-c",
                        _BOOTSTRAP_CODE,
                    ],
                    "applicationPackages": [package],
                    "packages": [],
                    "env": {
                        "CONDA_DIR": package_root,
                        # No LD_LIBRARY_PATH: the slim worker has nothing under
                        # /packages/<pkg>/lib that needs it, and setting it is
                        # what historically let fat-worker fbpkgs clobber the
                        # workspace .venv's CUDA.
                        "PATH": (
                            f"{package_root}/bin:"
                            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
                        ),
                        "PYTHONDONTWRITEBYTECODE": "1",
                        "PYTHONNOUSERSITE": "1",
                        **(env or {}),
                    },
                    "resourceLimit": {
                        "ramMB": _H100_RAM_MB,
                        "compute": {"cpu": _H100_CPU, "gpu": _H100_GPU},
                        "enableSwapAndSenpai": False,
                        "limitType": 1,
                        "wholeHost": True,
                        "enableZSwap": True,
                        "migType": 0,
                    },
                    "machineConstraints": {
                        "types": {"serverSubTypes": [_H100_SERVER_SUBTYPE]}
                    },
                    "networkAffinity": dict(_DEFAULT_NETWORK_AFFINITY),
                    "oncallShortname": hpc_job_oncall,
                    "ports": {"mesh": _WORKER_PORT},
                    "bindMounts": [],
                    "runningTimeoutSec": 2592000,
                    "unixUser": "root",
                    "restartPolicy": {
                        "scope": 0,
                        "maxTotalFailures": 0,
                        "failoverOnHostFailures": False,
                        "failJobOnFinalFailure": True,
                    },
                    "ttlsConfig": {"enable": False},
                    "opecTag": 0,
                },
            }
        ],
        "networkAffinity": dict(_DEFAULT_NETWORK_AFFINITY),
        "applicationMetadata": {
            "model_type_name": "gen_ai_default",
            "rm_attribution": rm_attribution,
        },
        "identity": {"name": hpc_identity},
        "owner": {
            "oncallShortname": hpc_job_oncall,
            "unixname": os.environ["USER"],
        },
        "enableGracefulPreemption": False,
        "maxJobFailures": 0,
        "jobType": 0,
        "aiTrainingMetadata": {
            "jobType": 0,
            "modelTypeName": "gen_ai_default",
            "entitlement": rm_attribution,
            "productGroup": "gen_ai",
            "mastJobID": name,
            "model_lifecycle_status": {},
        },
    }

    locality = _parse_locality_constraints(locality_constraints)
    if locality is not None:
        jobspec["localityConstraints"] = locality

    with tempfile.NamedTemporaryFile(
        delete=False, mode="w", suffix=".json"
    ) as spec_file:
        json.dump(jobspec, spec_file)
        spec_path = spec_file.name
        print(f"Job spec written to: {spec_path}")

    try:
        if run:
            subprocess.check_call(["mast", "schedule", spec_path])
    finally:
        subprocess.run(["rm", "-f", spec_path], check=False)

    # Honor MONARCH_MAST_JOB_PRIORITY (e.g. ``high``, ``very_high``) by raising
    # the freshly-scheduled job's priority -- the on-demand mount is sensitive
    # to mid-apply preemption, and a higher priority avoids the reschedule.
    priority = os.environ.get("MONARCH_MAST_JOB_PRIORITY")
    if run and priority:
        subprocess.check_call(
            ["mast", "update-job-priority", name, "--priority", priority.lower()]
        )

    return name


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--expire", default="4w", help="fbpkg expiry (default: 4w)")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        default=os.environ.get("MONARCH_REBUILD", "0") == "1",
        help="Re-upload the fbpkg even if cached (also set by MONARCH_REBUILD=1)",
    )
    parser.add_argument(
        "--launch", action="store_true", help="Launch a MAST job after building"
    )
    parser.add_argument(
        "--hosts", type=int, default=1, help="Number of hosts (default: 1)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print job spec without submitting"
    )
    args = parser.parse_args()

    identifier = build_bootstrap(expire=args.expire, rebuild=args.rebuild)
    print(f"Done. fbpkg identifier: {identifier}")

    if args.launch or args.dry_run:
        package_name, package_version = identifier.split(":", 1)
        job_name = launch_mast(
            package_name=package_name,
            package_version=package_version,
            hosts=args.hosts,
            run=not args.dry_run,
        )
        if args.dry_run:
            print(f"\nDry run -- job name would be: {job_name}")
        else:
            print(f"\nScheduled MAST job: {job_name}")


if __name__ == "__main__":
    main()
