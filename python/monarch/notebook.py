# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import argparse
import json
import logging
import os
import socket
import subprocess
import sys
import tempfile
import time
from getpass import getuser
from importlib.abc import SourceLoader
from importlib.machinery import ExtensionFileLoader, SourceFileLoader
from pathlib import Path
from pprint import pprint
from socket import gethostname
from subprocess import check_call, check_output
from tempfile import NamedTemporaryFile
from threading import Thread
from typing import Any, List, Optional

import zmq
from monarch.common.device_mesh import DeviceMesh
from monarch.common.mast import mast_get_jobs, MastJob
from monarch.common.remote import remote
from monarch.world_mesh import world_mesh
from monarch_supervisor import Context, get_message_queue, HostConnected
from monarch_supervisor.host import main as host_main
from monarch_supervisor.logging import initialize_logging

logger = logging.getLogger(__name__)

RESERVE_MAST_TASK_GROUP_NAME = "workers"
TORCHX_MAST_TASK_GROUP_NAME = "script"


class _Importer:
    def __init__(self, ctx: zmq.Context, endpoint):
        self.socket = ctx.socket(zmq.REQ)
        self.socket.setsockopt(zmq.IPV6, True)
        self.socket.connect(endpoint)

    def find_spec(self, fullname, path, target=None):
        self.socket.send_pyobj((fullname, path, target))
        r = self.socket.recv_pyobj()
        return r


class _SourceLoader(SourceLoader):
    def __init__(self, name, path, data):
        self.name = name
        self.path = path
        self.data = data

    def get_filename(self, fullname):
        return self.path

    def get_data(self, path):
        return self.data


class _ExtensionLoader:
    def __init__(self, name, path, data):
        self.name = name
        self.path = path
        self.data = data

    def create_module(self, spec):
        with NamedTemporaryFile("wb", delete=False) as f:
            f.write(self.data)
        self._loader = ExtensionFileLoader(self.name, f.name)
        return self._loader.create_module(spec)

    def exec_module(self, module):
        return self._loader.exec_module(module)


class ControllerImporterServer:
    def __init__(self, context: zmq.Context):
        self.socket: zmq.Socket = context.socket(zmq.REP)
        self.socket.setsockopt(zmq.IPV6, True)
        self.hostname = socket.gethostname()
        self.port = self.socket.bind_to_random_port("tcp://*")
        self.endpoint = f"tcp://{self.hostname}:{self.port}"

    def run(self):
        while True:
            fullname, path, target = self.socket.recv_pyobj()
            s = None
            for m in sys.meta_path:
                s = m.find_spec(fullname, path, target)
                if s is not None:
                    # print("SERVER FOUND", s.loader, s.loader.__dict__)
                    if isinstance(s.loader, SourceFileLoader):
                        s.loader = _SourceLoader(
                            s.loader.name,
                            s.loader.path,
                            s.loader.get_data(s.loader.path),
                        )
                    elif isinstance(s.loader, ExtensionFileLoader):
                        with open(s.loader.path, "rb") as f:
                            s.loader = _ExtensionLoader(
                                s.loader.name, s.loader.path, f.read()
                            )
                    else:
                        s = None
                    break
            self.socket.send_pyobj(s)


def _start_importer_server(context: zmq.Context):
    server = ControllerImporterServer(
        context,
    )
    thread = Thread(target=server.run, daemon=True)
    thread.start()
    return thread, server.endpoint


def _create_fbpkg(name, directory):
    def create_json(data):
        temp_dir = tempfile.mkdtemp()
        d = os.path.join(temp_dir, "materialized_configs")
        os.makedirs(d)
        json_file_path = os.path.join(d, f"{name}.fbpkg.materialized_JSON")
        with open(json_file_path, "w") as json_file:
            json.dump(data, json_file)
        return temp_dir

    package_json = {
        "paths": os.listdir(directory),
        "build_command": "",
    }
    path = create_json(package_json)
    package_version = (
        check_output(
            [
                "fbpkg",
                "build",
                "--yes",
                "--ephemeral",
                "--configerator-path",
                path,
                name,
                "--expire",
                "4w",
            ],
            cwd=directory,
        )
        .decode("utf-8")
        .split("\n")[-2]
    )
    return package_version.split(":")


def _nfs_start(
    endpoint: str,
    datacenter: str,
    twmount="/packages/nfs.twmount/twmount",
    dest="/mnt/gen_ai_input_data_nfs",
    options="vers=4.2,port=20492,proto=tcp6,nosharecache",
):
    addr_map = {"eag": "[2401:db00:3ff:c0c3::2a]:/ai"}
    addr = addr_map[datacenter]
    cmds = [
        f"{twmount} mount -t nfs4 -s {addr} -d {dest} -o {options}",
        "mkdir -p /mnt/aidev/$MAST_JOB_OWNER_UNIXNAME",
        f"mount --bind {dest}/aidev/$MAST_JOB_OWNER_UNIXNAME /mnt/aidev/$MAST_JOB_OWNER_UNIXNAME",
        f"{sys.executable} -m monarch.notebook worker --endpoint {endpoint}",
    ]
    return " && ".join(cmds)


def _ephemeral_package(package, version):
    return {
        "fbpkgIdentifier": f"{package}:{version}",
        "name": package,
        "version": {"ephemeralId": version},
    }


def _package(package: str, version: str):
    return {
        "fbpkgIdentifier": f"{package}:{version}",
        "name": package,
    }


def launch_mast(
    base_name: str,
    packages: List[Any],
    hosts: int,
    command: str,
    run: bool = True,
    base_image: Optional[str] = None,
    datacenter: Optional[str] = None,
):
    name = f"{base_name}_{time.time_ns()}"
    jobspec = {
        "applicationMetadata": {},
        "enableGracefulPreemption": False,
        "hpcClusterUuid": "MastGenAICluster",
        "hpcTaskGroups": [
            {
                "hardwareSpecificTaskGroupOverride": {},
                "name": RESERVE_MAST_TASK_GROUP_NAME,
                "spec": {
                    "applicationPackages": [
                        {
                            "fbpkgIdentifier": "ttls_so:1125",
                            "name": "ttls_so",
                        },
                        *packages,
                    ],
                    "arguments": [],
                    "bindMounts": [],
                    "command": command,
                    "env": {
                        "LD_PRELOAD": "/packages/ttls_so/TransparentTls3.so",
                        "TTLS_ENABLED": "1",
                    },
                    "machineConstraints": {"types": {"serverTypes": [100]}},
                    "networkAffinity": {"fallbackScope": 1, "preferredScope": 2},
                    "oncallShortname": "pytorch_distributed",
                    "opecTag": 0,
                    "packages": [],
                    "resourceLimit": {
                        "compute": {"cpu": 15, "gpu": 0},
                        "enableSwapAndSenpai": False,
                        "limitType": 0,
                        "ramMB": 54272,
                        "wholeHost": True,
                    },
                    "restartPolicy": {
                        "failoverOnHostFailures": False,
                        "maxTotalFailures": 10,
                        "scope": 0,
                    },
                    "runningTimeoutSec": 2592000,
                    "unixUser": "root",
                    "ttlsConfig": {"enable": True},
                },
                "taskCount": hosts,
                "taskCountPerHost": 1,
            }
        ],
        "identity": {"name": "oncall_pytorch_distributed"},
        "jobType": 0,
        "maxJobFailures": 0,
        "name": name,
        "networkAffinity": {"fallbackScope": 1, "preferredScope": 2},
        "owner": {
            "oncallShortname": "pytorch_distributed",
            "unixname": os.environ["USER"],
        },
        "aiTrainingMetadata": {
            "launcher": 3,
            "trainingFramework": 10,
            "jobType": None,
            "modelTypeName": "gen_ai_conda",
            "jobPurpose": None,
            "entitlement": "pytorch_distributed",
            "tenantPath": None,
            "tenantPriority": None,
            "productGroup": None,
            "rootWorkflowRunID": None,
            "mastJobID": name,
            "mastWorkflowRunID": None,
            "productGroupMetadata": None,
            "modelIDs": None,
            "model_lifecycle_status": {},
        },
    }

    if base_image is not None:
        # pyre-fixme[16]: Item `bool` of `Union[Dict[typing.Any, typing.Any], Dict[st...
        jobspec["hpcTaskGroups"][0]["baseImage"] = {
            "baseImagePackage": {
                "fbpkgIdentifier": base_image,
            }
        }
    if datacenter is not None:
        jobspec["localityConstraints"] = {"locality": 1, "options": [datacenter]}

    pprint(jobspec)
    with tempfile.NamedTemporaryFile(delete=False, mode="w") as job_spec_file:
        json.dump(jobspec, job_spec_file)
        print(job_spec_file.name)
    if run:
        check_call(["mast", "schedule", job_spec_file.name])
    return name


def _endpoint():
    hostname = socket.gethostname()
    with socket.socket() as sock:
        sock.bind(("", 0))
        port = sock.getsockname()[1]
    return hostname, port


def reserve(hosts, nfs=False, force_rebuild=False):
    hostname, port = _endpoint()
    endpoint = f"tcp://{hostname}:{port}"
    name = f"notebook_{port}"
    if nfs:
        executable = Path(sys.executable)
        nfs_path = Path(f"/mnt/aidev/{getuser()}")
        if nfs_path not in executable.parents:
            raise RuntimeError(
                f"conda environment ({executable}) must be installed in nfs path ({nfs_path}) to use nfs workers."
            )
        datacenter = socket.gethostname().split(".")[1][:3]
        jobname = launch_mast(
            base_name=name,
            packages=[_package("nfs.twmount", "stable")],
            hosts=hosts,
            command=_nfs_start(endpoint, datacenter),
            base_image="tupperware.image.sendstream.c9.flare",
            datacenter=datacenter,
        )
    else:
        environment = os.environ["CONDA_PREFIX"]
        cache = f"{os.environ['HOME']}/.controller_notebook_package"
        try:
            with open(cache, "r") as f:
                package, version = f.read().strip().split(":")
        except FileNotFoundError:
            package, version = None, None

        if force_rebuild or package is None:
            package, version = _create_fbpkg("ptd_supervisor_testbed", environment)
            with open(cache, "w") as f:
                f.write(f"{package}:{version}\n")
        jobname = launch_mast(
            base_name=name,
            packages=[_ephemeral_package(package, version)],
            hosts=hosts,
            command=f"/packages/{package}/bin/python -m monarch.notebook worker --endpoint {endpoint}",
        )

    return jobname


def _register_importer(endpoint: str):
    sys.meta_path.append(_Importer(get_message_queue()._ctx, endpoint))


register_importer = remote("monarch.notebook._register_importer", propagate="inspect")

_chdir = remote("os.chdir", propagate="inspect")


def mast_job_is_valid(job):
    args = job.get_arguments()
    if args[0:3] != ["-mmonarch.notebook", "worker", "--endpoint"]:
        return False
    maybe_host_and_port = args[3].removeprefix("tcp://").split(":")
    if len(maybe_host_and_port) != 2:
        return False
    host, port = maybe_host_and_port
    return host == socket.gethostname() and port.isdigit()


def get_mast_notebook_jobs(task_group):
    jobs = []
    for job in mast_get_jobs(task_group):
        if "monarch" in job.name() and mast_job_is_valid(job):
            jobs.append(job)
    return jobs


def connect(jobname=None):
    job = None
    user_jobs = get_mast_notebook_jobs(RESERVE_MAST_TASK_GROUP_NAME)
    if jobname is None:
        for j in sorted(user_jobs, key=lambda x: x.get_create_time(), reverse=True):
            if j.name().startswith("notebook_"):
                jobname = j.name()
                job = j
                break
        if job is None:
            raise RuntimeError(
                "no valid jobs found, use monarch.notebook.reserve_workers to create one."
            )
    else:
        for j in user_jobs:
            if j.name() == jobname:
                job = j
                break
        if job is None:
            names = "\n".join([j.name() for j in user_jobs])
            raise RuntimeError(
                f"{jobname} is not one of your current running jobs. Choose from:\n{names}"
            )

    job.wait_for_running(600 * 10)

    N = job.get_task_count()
    uses_nfs = job.uses_nfs()
    port = int(job.name().split("_")[1])

    ctx = Context(port=port)
    ctx.request_hosts(N)
    connections = ctx.messagefilter(HostConnected)
    hosts = [connections.recv(timeout=30).sender for _ in range(N)]
    mesh = world_mesh(ctx, hosts, 8)
    if uses_nfs:
        nfs_path = Path(f"/mnt/aidev/{getuser()}")
        cwd = Path(os.getcwd())
        if nfs_path in cwd.parents:
            with mesh.activate():
                _chdir(str(cwd))
    else:
        _, importer_endpoint = _start_importer_server(ctx._context)
        with mesh.activate():
            register_importer(importer_endpoint)
    logger.info("connected to mast workers")
    return mesh


_ctx: Optional[Context] = None
_active_mesh: Optional[DeviceMesh] = None
_is_logging_initialized = False


_DEFAULT_TORCHX_WORKSPACE_PATH = (
    f"/data/users/{os.getenv('USER')}/fbsource/fbcode/monarch/examples"
)
_DEFAULT_LOCALITY_CONSTRAINTS = "region;pci"
_DEFAULT_RM_ATTRIBUTION = "gen_ai_rf_nextgen_infra"
_DEFAULT_RUNNING_TIMEOUT_SEC = 3600


def reserve_torchx(
    hosts: int,
    torchx_workspace_path: str = _DEFAULT_TORCHX_WORKSPACE_PATH,
    nfs_workspace_dir: Optional[str] = None,
    oilfs_workspace_dir: Optional[str] = None,
    workspace_dir: Optional[str] = None,
    conda_dir: Optional[str] = None,
    locality_constraints: str = _DEFAULT_LOCALITY_CONSTRAINTS,
    rm_attribution: str = _DEFAULT_RM_ATTRIBUTION,
    running_timeout_sec: int = _DEFAULT_RUNNING_TIMEOUT_SEC,
    additional_scheduler_args: Optional[str] = None,
) -> str:
    global _is_logging_initialized
    # Avoid initializing logging more than once. Otherwise we'll
    # get duplicate logs.
    if not _is_logging_initialized:
        initialize_logging()
        _is_logging_initialized = True

    hostname, port = _endpoint()
    old_cwd = os.getcwd()
    os.chdir(torchx_workspace_path)

    scheduler_args = ",".join(
        [
            f"localityConstraints={locality_constraints}",
            f"rmAttribution={rm_attribution}",
            f"runningTimeoutSec={running_timeout_sec}",
        ]
    )
    if additional_scheduler_args:
        scheduler_args += "," + additional_scheduler_args

    job_base_name = f"monarch_{time.time_ns()}"

    torchx_cmd = [
        "torchx",
        "run",
        "--scheduler_args",
        scheduler_args,
        "mast.py:train",
        "--name",
        job_base_name,
        "--nodes",
        str(hosts),
        "--enable_ttls",
        "True",
    ]

    if nfs_workspace_dir is not None:
        torchx_cmd.extend(["--nfs_workspace_dir", nfs_workspace_dir])

    if oilfs_workspace_dir is not None:
        torchx_cmd.extend(["--oilfs_workspace_dir", oilfs_workspace_dir])

    if workspace_dir is not None:
        torchx_cmd.extend(["--workspace_dir", workspace_dir])

    if conda_dir is not None:
        torchx_cmd.extend(["--conda_dir", conda_dir])

    torchx_cmd.extend(
        [
            "--module",
            "monarch.notebook",
            "--",
            "worker",
            "--endpoint",
            f"tcp://{hostname}:{port}",
        ]
    )

    try:
        subprocess.run(torchx_cmd)

        logger.info(
            f"Started mast workers with supervisor_addr: {f'tcp://{hostname}:{port}'}"
        )

        return [
            job.name()
            for job in get_mast_notebook_jobs(TORCHX_MAST_TASK_GROUP_NAME)
            if job.name().startswith(job_base_name)
        ][0]
    finally:
        # This gets called even if the try block succeeds and returns.
        os.chdir(old_cwd)


log = remote("monarch.worker._testing_function.log", propagate="inspect")


def mast_mesh(
    mast_job_name: str,
    hosts: Optional[int] = None,
    n_gpus_per_host: Optional[int] = None,
    max_retries: Optional[int] = None,
):
    global _ctx, _active_mesh, _is_logging_initialized
    # Avoid initializing logging more than once. Otherwise we'll
    # get duplicate logs.
    if not _is_logging_initialized:
        initialize_logging()
        _is_logging_initialized = True

    mast_job = MastJob(mast_job_name, TORCHX_MAST_TASK_GROUP_NAME)
    try:
        assert mast_job_is_valid(mast_job)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("Failed to get mast job status") from e
    except AssertionError as e:
        raise RuntimeError(
            "Based on job name and args, this does not appear to be a "
            "mast job created by monarch's monarch.notebook module. "
            f"Your valid mast jobs are: {get_mast_notebook_jobs(TORCHX_MAST_TASK_GROUP_NAME)}"
        ) from e

    while True:
        if mast_job.is_running():
            logger.info(f"Found running mast job {mast_job_name}. Connecting...")
            break
        else:
            logger.info(
                f"Waiting for mast job {mast_job_name} and all its workers "
                "to have status RUNNING. Sleeping for 10 seconds..."
            )
            time.sleep(10)

    if hosts is None:
        hosts = mast_job.get_num_hosts()
    else:
        assert hosts <= mast_job.get_num_hosts(), (
            f"Requested {hosts} hosts, but job only has {mast_job.get_num_hosts()} hosts."
        )

    if n_gpus_per_host is None:
        n_gpus_per_host = mast_job.get_gpus_per_host()
    else:
        assert n_gpus_per_host <= mast_job.get_gpus_per_host(), (
            f"Requested {n_gpus_per_host} gpus per host, but job only has "
            f"{mast_job.get_gpus_per_host()} gpus per host."
        )

    port = mast_job.get_port()

    retry = 0
    while max_retries is None or retry < max_retries:
        retry += 1
        try:
            _ctx = Context(port=port)
            ctx: Context = _ctx
            ctx.request_hosts(hosts)
            connections = ctx.messagefilter(HostConnected)
            logger.info(f"connections: {connections}")
            ctx_hosts = [connections.recv(timeout=30).sender for _ in range(hosts)]
            logger.info(f"connections: {ctx_hosts}")
            logger.info(
                f"Connected to mast workers ({hosts} hosts, {n_gpus_per_host} gpus per host)"
            )
            _active_mesh = mesh = world_mesh(ctx, ctx_hosts, n_gpus_per_host)
            mesh.exit = cleanup

            def remote_mount_activate(
                mesh,
                local_mount_home_dir,
                remote_mount_home_dir,
                remote_mount_workspace_dir,
            ):
                """
                This function does two things:
                  1. If the mast workers are running in a remote mounted workspace directory,
                     then add the local equivalent to sys.path so that the notebook can import
                     modules relative to the remote workspace directory.
                  2. If the (local) current working directory is inside a mounted file system
                     (e.g. NFS or OILFS), and the workers are also running inside the same mounted
                     file system, then change the workers' current working directory to the remote equivalent
                     of the local current working directory. Additionally, add the empty string to the workers'
                     sys.path so that they search their current working directory for modules.
                """
                if remote_mount_home_dir is None or remote_mount_workspace_dir is None:
                    return

                local_mount_home_path = Path(local_mount_home_dir)
                remote_mount_home_path = Path(remote_mount_home_dir)
                remote_mount_workspace_path = Path(remote_mount_workspace_dir)
                relative_workspace_path = remote_mount_workspace_path.relative_to(
                    remote_mount_home_path
                )
                local_mount_workspace_path = (
                    local_mount_home_path / relative_workspace_path
                )

                if str(local_mount_workspace_path) not in sys.path:
                    # So that the notebook can call remote functions defined in remote_mount_workspace_dir
                    # via call_remote even if the cwd isn't inside the local equivalent of remote_mount_workspace_dir.
                    sys.path.append(str(local_mount_workspace_path))

                cwd = Path(os.getcwd())
                if local_mount_home_path in cwd.parents or local_mount_home_path == cwd:
                    with mesh.activate():
                        # Append the empty string to sys.path on each of the workers so that they
                        # search their current working directory for modules.
                        remote(
                            lambda: (
                                sys.path.append("") if "" not in sys.path else None
                            )
                        )()
                        relative_cwd = cwd.relative_to(local_mount_home_path)
                        remote(lambda cwd: os.chdir(cwd))(
                            str(remote_mount_home_path / relative_cwd)
                        )

            if mast_job.get_oilfs_workspace_dir() is not None:
                remote_mount_activate(
                    mesh,
                    f"/home/{getuser()}/fuse-aidev",
                    mast_job.get_oilfs_home_dir(),
                    mast_job.get_oilfs_workspace_dir(),
                )
            elif mast_job.get_nfs_workspace_dir() is not None:
                remote_mount_activate(
                    mesh,
                    f"/mnt/aidev/{getuser()}",
                    mast_job.get_nfs_home_dir(),
                    mast_job.get_nfs_workspace_dir(),
                )

            return _active_mesh
        except TimeoutError:
            logger.warning(
                "Timed out waiting to connect to mast workers. "
                f"Tried {retry} out of {max_retries if max_retries is not None else '(inf)'} "
                "times."
            )
            cleanup()
        except Exception as e:
            cleanup()
            raise e


def list_mast_jobs():
    for job in get_mast_notebook_jobs():
        print(job)


def cleanup():
    global _ctx, _active_mesh
    if _active_mesh:
        _active_mesh.client.shutdown()
        _active_mesh = None
    if _ctx:
        _ctx.shutdown()
        _ctx = None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    reserve_parser = subparsers.add_parser("reserve")
    reserve_parser.add_argument("--hosts", type=int)
    reserve_parser.add_argument(
        "--torchx_workspace_path",
        type=str,
        default=_DEFAULT_TORCHX_WORKSPACE_PATH,
    )
    reserve_parser.add_argument(
        "--locality_constraints", type=str, default=_DEFAULT_LOCALITY_CONSTRAINTS
    )
    reserve_parser.add_argument("--nfs_workspace_dir", type=str, required=False)
    reserve_parser.add_argument("--oilfs_workspace_dir", type=str, required=False)
    reserve_parser.add_argument("--workspace_dir", type=str, required=False)
    reserve_parser.add_argument("--conda_dir", type=str, required=False)
    reserve_parser.add_argument(
        "--rm_attribution", type=str, required=False, default=_DEFAULT_RM_ATTRIBUTION
    )
    reserve_parser.add_argument(
        "--running_timeout_sec",
        type=int,
        required=False,
        default=_DEFAULT_RUNNING_TIMEOUT_SEC,
    )
    reserve_parser.add_argument("--additional_scheduler_args", type=str, required=False)

    worker_parser = subparsers.add_parser("worker")
    worker_parser.add_argument("--endpoint", type=str)

    args = parser.parse_args(sys.argv[1:])
    if args.command == "reserve":
        reserve_torchx(
            args.hosts,
            args.torchx_workspace_path,
            args.nfs_workspace_dir,
            args.oilfs_workspace_dir,
            args.workspace_dir,
            args.conda_dir,
            args.locality_constraints,
            args.rm_attribution,
            args.running_timeout_sec,
            args.additional_scheduler_args,
        )
        sys.exit(0)
    else:
        initialize_logging(f"{gethostname()} pid {os.getpid()} host-manager")
        endpoint = args.endpoint
        logger.info(f"Connecting to {endpoint}")
        while True:
            pid = os.fork()
            if pid == 0:
                try:
                    host_main(endpoint)
                except ConnectionAbortedError:
                    logger.warning("host manager aborted, restarting new host manager")
                sys.exit(0)
            else:
                exitpid, status = os.wait()
                if status != 0:
                    logger.warning("Abnormal exit, stopping")
                    break
