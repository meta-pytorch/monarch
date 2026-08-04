#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Package the minimonarch smoke test for MAST and emit a job spec.

This does NOT need a venv or any extra Python packages: MAST machines already
have a platform010 Python 3.12, and the only non-stdlib import is the compiled
``minimonarch`` extension, which we ship as a single ``.so`` placed directly on
the bootstrap's ``PYTHONPATH``.

Steps:
  1. Build the minimonarch extension via ``uv run`` (which rebuilds on any
     Rust/C change) and copy the resulting cp312 ``.so``.
  2. Stage ``smoke.py``, ``mast_bootstrap.py``, the ``.so`` and ``certs/``
     (cert.pem/key.pem/ca.pem) into one directory.
  3. Upload that directory as an ephemeral fbpkg.
  4. Write a MAST job spec that runs ``mast_bootstrap.py`` on every task.

By default it stops after writing the spec and prints the ``mast schedule``
command for you to run. It never schedules a job itself.

Usage:
    python build_mast.py --hosts 4            # build pkg + write spec
    python build_mast.py --hosts 4 --print-spec
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent  # monarch_mini/mast/
_MONARCH_MINI_DIR = _HERE.parent  # monarch_mini/
# The minimonarch extension's uv/pyproject project lives here; the .so is built by
# running uv in this directory (its cache-keys pull in ../src for the Rust sources).
_PYTHON_DIR = _MONARCH_MINI_DIR / "python"
_TEST_CERTS = _MONARCH_MINI_DIR / "test_certs"
# Interpreter used in the MAST job command; the cp312 extension we ship is
# ABI-compatible with it.
_MAST_PACKAGE_PYTHON = "/usr/local/fbcode/platform010/bin/python3"

# Must be an already-registered fbpkg package name; we reuse the one the
# monarch examples use rather than inventing a new (unregistered) name, which
# fails with NO_SUCH_PACKAGE / METADATA_PACKAGE_NOT_FOUND_ERROR.
_FBPKG_NAME = "monarch_additional_packages"
_DEFAULT_PORT = 26600

# SMC tier of the MAST ResourceLocatorService (maps a cluster -> its scheduler
# frontend's write tier). Used to submit directly via thrift, since `mast schedule`
# routes writes only to the GenAI/MSL frontend.
_RESOURCE_LOCATOR_TIER = "mast.lookup.prod"

# Known entitlement -> tenantPath. monarch_cicd is authorized only on
# MastGenAICluster; monarch_training is also authorized on the classic-MAST
# clusters (MastProdCluster / CPUTrainingWorkloads) with their large T1 pools.
_ENTITLEMENT_TENANT_PATHS = {
    "monarch_cicd": "root/gen_ai/msl/msl_infra/cicd/monarch_cicd",
    "monarch_training": (
        "root/cfp/ai_rnd/ai_systems_rnd/ai_infra_training_rnd_tc/monarch_training"
    ),
}

# Clusters served by the classic-MAST frontend; their applicationMetadata backing
# cluster is MastProdCluster (matching how real jobs on these clusters are shaped).
_MAST_BACKED_CLUSTERS = {"MastProdCluster", "CPUTrainingWorkloads"}


def run(cmd: list[str], **kwargs: object) -> None:
    print(f"+ {' '.join(str(c) for c in cmd)}", flush=True)
    subprocess.run(cmd, check=True, **kwargs)  # pyre-ignore[6]


def build_so(out_dir: Path) -> Path:
    """Build the minimonarch extension and copy the ``.so`` into ``out_dir``.

    We build via ``uv run`` (which respects pyproject's ``cache-keys`` and so
    rebuilds whenever the Rust sources or ``minimonarch.c`` change) and copy the
    freshly built, imported ``.so``.

    We deliberately do NOT use ``uv build``: its isolated build did not pick up
    working-tree changes to the out-of-tree Rust sources (``../src``) and linked a
    stale staticlib, producing a ``.so`` that did not match the source. The ``uv
    run`` path builds in place against the real workspace ``target/`` and the
    current sources. The resulting ``.so`` is a CPython 3.12 (cp312) extension,
    ABI-compatible with the platform010 cp312 runtime on MAST.
    """
    # Build (cache-keys trigger a rebuild on any Rust/C change) and locate it.
    # Run uv in the extension's project dir (python/), not this script's dir.
    run(["uv", "run", "python", "-c", "import minimonarch"], cwd=str(_PYTHON_DIR))
    so_path = Path(
        subprocess.check_output(
            [
                "uv",
                "run",
                "python",
                "-c",
                "import minimonarch, sys; sys.stdout.write(minimonarch.__file__)",
            ],
            cwd=str(_PYTHON_DIR),
        )
        .decode()
        .strip()
    )
    if not so_path.exists():
        print(f"ERROR: built extension not found at {so_path}", file=sys.stderr)
        sys.exit(1)
    dest = out_dir / so_path.name
    shutil.copy2(so_path, dest)
    return dest


def stage_package(staging: Path) -> None:
    """Assemble everything the MAST tasks need into ``staging``."""
    staging.mkdir(parents=True, exist_ok=True)

    so = build_so(staging)
    print(f"staged extension: {so.name}", flush=True)

    for name in ("smoke.py", "mast_bootstrap.py"):
        shutil.copy2(_HERE / name, staging / name)

    certs_out = staging / "certs"
    certs_out.mkdir(exist_ok=True)
    for pem in ("cert.pem", "key.pem", "ca.pem"):
        src = _TEST_CERTS / pem
        if not src.exists():
            print(f"ERROR: missing cert {src}", file=sys.stderr)
            sys.exit(1)
        shutil.copy2(src, certs_out / pem)
    print(f"staged certs: {sorted(p.name for p in certs_out.iterdir())}", flush=True)


def create_fbpkg(name: str, directory: Path, expire: str) -> str:
    """Upload directory contents as an ephemeral fbpkg. Returns 'name:version'."""
    config_dir = tempfile.mkdtemp()
    materialized_dir = os.path.join(config_dir, "materialized_configs")
    os.makedirs(materialized_dir)
    json_path = os.path.join(materialized_dir, f"{name}.fbpkg.materialized_JSON")

    package_json = {"paths": os.listdir(directory), "build_command": ""}
    with open(json_path, "w") as f:
        json.dump(package_json, f)

    output = subprocess.check_output(
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
        cwd=str(directory),
    ).decode("utf-8")
    print(output, flush=True)
    lines = [line for line in output.splitlines() if line.strip()]
    return lines[-1].strip()


def make_jobspec(
    *,
    name: str,
    package_name: str,
    package_version: str,
    hosts: int,
    port: int,
    workers_per_host: int,
    extra_env: dict[str, str] | None = None,
    region: str | None = None,
    server_subtype: int | None = None,
    cluster: str = "MastGenAICluster",
    entitlement: str = "monarch_cicd",
) -> dict:
    """A CPU MAST job that runs the bootstrap on every task.

    By default it constrains to serverType 100 (classic T1: Skylake/CooperLake/
    Milan). Pass ``server_subtype`` (e.g. 10018 = T1_BGM Bergamo) to instead pin a
    specific CPU sub-type — required to land on Bergamo, which the plain T1
    serverType does not cover.

    ``entitlement`` selects the MAST entitlement/tenant. Use ``monarch_training``
    for the classic-MAST clusters (CPUTrainingWorkloads / MastProdCluster), which
    ``monarch_cicd`` is not authorized on.
    """
    tenant_path = _ENTITLEMENT_TENANT_PATHS.get(entitlement)
    if tenant_path is None:
        raise ValueError(
            f"unknown entitlement {entitlement!r}; known: "
            f"{sorted(_ENTITLEMENT_TENANT_PATHS)}"
        )
    # applicationMetadata backing cluster: MastProdCluster for the classic-MAST
    # clusters (matches how real jobs on those clusters are shaped), else itself.
    app_cluster = "MastProdCluster" if cluster in _MAST_BACKED_CLUSTERS else cluster
    command = f"{_MAST_PACKAGE_PYTHON} /packages/{package_name}/mast_bootstrap.py"
    machine_constraints = (
        {"types": {"serverSubTypes": [server_subtype]}}
        if server_subtype is not None
        else {"types": {"serverTypes": [100]}}
    )
    env = {
        "SMOKE_PORT": str(port),
        "SMOKE_WORKERS_PER_HOST": str(workers_per_host),
    }
    if extra_env:
        env.update(extra_env)
    spec = {
        "name": name,
        "hpcClusterUuid": cluster,
        "hpcTaskGroups": [
            {
                "name": "workers",
                "taskCount": hosts,
                "taskCountPerHost": 1,
                "hardwareSpecificTaskGroupOverride": {},
                "spec": {
                    "command": command,
                    "arguments": [],
                    "applicationPackages": [
                        {
                            "name": package_name,
                            "version": {"ephemeralId": package_version},
                            "fbpkgIdentifier": f"{package_name}:{package_version}",
                        }
                    ],
                    "packages": [],
                    "env": env,
                    "resourceLimit": {
                        "ramMB": 54272,
                        "compute": {"cpu": 15, "gpu": 0},
                        "enableSwapAndSenpai": False,
                        "limitType": 0,
                        "wholeHost": True,
                    },
                    "machineConstraints": machine_constraints,
                    "networkAffinity": {"preferredScope": 2, "fallbackScope": 1},
                    "oncallShortname": "monarch",
                    "bindMounts": [],
                    "runningTimeoutSec": 3600,
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
        "networkAffinity": {"preferredScope": 2, "fallbackScope": 1},
        "applicationMetadata": {
            "model_type_name": "gen_ai_default",
            "rm_attribution": entitlement,
            "hpcClusterUuid": app_cluster,
        },
        "identity": {"name": "hyper_monarch"},
        "owner": {"oncallShortname": "monarch", "unixname": os.environ["USER"]},
        "enableGracefulPreemption": False,
        "maxJobFailures": 0,
        "jobType": 0,
        "aiTrainingMetadata": {
            "jobType": 0,
            "modelTypeName": "gen_ai_default",
            "entitlement": entitlement,
            "tenantPath": tenant_path,
            "productGroup": "gen_ai",
            "mastJobID": name,
            "model_lifecycle_status": {},
        },
    }
    if region:
        # Pin every task to a single region (locality=1 = region scope) so the
        # whole fleet lands together where capacity is — required when one region
        # must hold all the hosts.
        spec["localityConstraints"] = {"locality": 1, "options": [region]}
    return spec


def locate_write_tier(cluster: str) -> str:
    """Resolve a cluster's scheduler write tier via MAST's ResourceLocatorService.

    `mast schedule` only ever hits the GenAI/MSL frontend, so it can't create jobs
    on the classic-MAST clusters. We instead ask the locator which frontend serves
    the cluster and submit straight to that write tier (see `submit_via_thrift`).
    """
    out = subprocess.check_output(
        [
            "thriftdbg",
            "sendRequest",
            "locateHpcCluster",
            json.dumps({"request": {"hpcClusterUuid": cluster}}),
            "--tier",
            _RESOURCE_LOCATOR_TIER,
        ],
        text=True,
    )
    line = [ln for ln in out.strip().splitlines() if ln.strip()][-1]
    return json.loads(line)["smcTiers"]["writeTier"]


def submit_via_thrift(spec: dict) -> None:
    """Submit the job by calling HpcSchedulerService.scheduleHpcJob directly.

    Both `mast schedule` and torchx ultimately call this same RPC; we call it via
    `thriftdbg` against the cluster's own write tier, which lets us reach the
    classic-MAST clusters (CPUTrainingWorkloads / MastProdCluster) that the CLI's
    fixed GenAI/MSL write endpoint rejects.
    """
    cluster = spec["hpcClusterUuid"]
    tier = locate_write_tier(cluster)
    print(f"=== submitting to write tier {tier} (cluster {cluster}) ===", flush=True)
    request = {"request": {"hpcJob": spec}}
    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False, prefix="mm_sched_req_"
    ) as f:
        json.dump(request, f)
        req_path = f.name
    proc = subprocess.run(
        [
            "thriftdbg",
            "sendRequest",
            "scheduleHpcJob",
            "",
            "--request_json",
            req_path,
            "--tier",
            tier,
            "--request_timeout_ms",
            "90000",
        ],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        print(proc.stderr.strip() or proc.stdout.strip(), file=sys.stderr)
        sys.exit("scheduleHpcJob failed (see error above)")
    # A successful scheduleHpcJob returns an empty struct "{}".
    print(f"scheduled OK: {proc.stdout.strip() or '{}'}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hosts", type=int, default=2, help="number of MAST tasks")
    parser.add_argument(
        "--workers-per-host",
        type=int,
        default=1,
        help="worker processes per host on sequential ports (default: 1); lets "
        "you scale worker count without more machines",
    )
    parser.add_argument(
        "--port", type=int, default=_DEFAULT_PORT, help="base worker port"
    )
    parser.add_argument("--expire", default="1w", help="fbpkg expiry (default: 1w)")
    parser.add_argument(
        "--name", default=None, help="job name (default: minimonarch_smoke_<user>)"
    )
    parser.add_argument(
        "--print-spec", action="store_true", help="print the job spec JSON"
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="reuse an existing fbpkg id from --package instead of rebuilding",
    )
    parser.add_argument(
        "--package",
        default=None,
        help="existing 'name:version' to reuse with --skip-build",
    )
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="extra environment variable for the job (repeatable), e.g. "
        "--env MM_QUIC_CLIENT_ENDPOINTS=8",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="pin all tasks to a single region (e.g. vcn) so the whole fleet "
        "lands where there is capacity",
    )
    parser.add_argument(
        "--cluster",
        default="MastGenAICluster",
        help="MAST cluster (default: MastGenAICluster; CPUTrainingWorkloads has a "
        "much larger best-effort T1 pool)",
    )
    parser.add_argument(
        "--bergamo",
        action="store_true",
        help="target Bergamo CPU hosts (T1_BGM, serverSubType 10018; ~88 cores, "
        "256 GB) instead of classic T1 — needed to use the large vcn pool",
    )
    parser.add_argument(
        "--server-subtype",
        type=int,
        default=None,
        help="explicit LogicalServerSubType to constrain to (overrides --bergamo)",
    )
    parser.add_argument(
        "--entitlement",
        default=None,
        help="MAST entitlement (default: monarch_training for classic-MAST "
        "clusters, else monarch_cicd)",
    )
    parser.add_argument(
        "--launch",
        action="store_true",
        help="submit the job now via thrift (thriftdbg -> the cluster's write "
        "tier) instead of only printing instructions",
    )
    args = parser.parse_args()

    extra_env: dict[str, str] = {}
    for item in args.env:
        if "=" not in item:
            parser.error(f"--env expects KEY=VALUE, got {item!r}")
        key, value = item.split("=", 1)
        extra_env[key] = value

    if args.skip_build:
        if not args.package:
            parser.error("--skip-build requires --package name:version")
        identifier = args.package
    else:
        staging = Path(tempfile.mkdtemp(prefix="mm_smoke_pkg_"))
        print(f"=== staging package in {staging} ===", flush=True)
        stage_package(staging)
        print("=== uploading ephemeral fbpkg ===", flush=True)
        identifier = create_fbpkg(_FBPKG_NAME, staging, args.expire)

    package_name, package_version = identifier.split(":", 1)
    entitlement = args.entitlement or (
        "monarch_training" if args.cluster in _MAST_BACKED_CLUSTERS else "monarch_cicd"
    )
    # MAST rejects duplicate job names, so make each launch unique unless the
    # caller pinned an exact --name.
    base_name = args.name or f"minimonarch_smoke_{os.environ['USER']}"
    job_name = base_name if args.name else f"{base_name}_{int(time.time())}"

    spec = make_jobspec(
        name=job_name,
        package_name=package_name,
        package_version=package_version,
        hosts=args.hosts,
        port=args.port,
        workers_per_host=args.workers_per_host,
        extra_env=extra_env,
        region=args.region,
        server_subtype=args.server_subtype
        if args.server_subtype is not None
        else (10018 if args.bergamo else None),
        cluster=args.cluster,
        entitlement=entitlement,
    )

    spec_path = Path(tempfile.mkdtemp(prefix="mm_smoke_spec_")) / "jobspec.json"
    spec_path.write_text(json.dumps(spec, indent=2))

    if args.print_spec:
        print(json.dumps(spec, indent=2), flush=True)

    print("\n=== ready ===", flush=True)
    print(f"fbpkg:       {identifier}", flush=True)
    print(f"job name:    {job_name}", flush=True)
    print(f"cluster:     {args.cluster}  entitlement: {entitlement}", flush=True)
    print(f"job spec:    {spec_path}", flush=True)

    if args.launch:
        submit_via_thrift(spec)
        print(f"\nmonitor with:  mast get-status {job_name}", flush=True)
    else:
        print("\nTo launch (rerun with --launch, or submit directly):", flush=True)
        print(
            f"    thriftdbg sendRequest scheduleHpcJob '' --request_json "
            f"<{{'request':{{'hpcJob': <spec>}}}}> --tier "
            f"<writeTier for {args.cluster}>",
            flush=True,
        )


if __name__ == "__main__":
    main()
