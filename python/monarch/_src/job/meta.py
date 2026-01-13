# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import tempfile
import uuid
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Literal, NamedTuple, Optional, Sequence, Tuple, Union

from monarch._rust_bindings.monarch_hyperactor.alloc import (
    Alloc,
    AllocConstraints,
    AllocSpec,
)
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask, Shared
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._src.actor.allocator import AllocateMixin
from monarch._src.actor.host_mesh import host_mesh_from_alloc
from monarch._src.actor.meta.allocator import (
    MastAllocatorConfig,
    MastHostAllocator,
    MastHostAllocatorBase,
)
from monarch._src.job.job import BatchJob, JobState, JobTrait
from monarch._src.tools.commands import create, info, kill
from monarch.tools.components.meta import hyperactor
from monarch.tools.config import (  # @manual=//monarch/python/monarch/tools/config/meta:defaults
    Config,
    Workspace,
)
from torchx.specs import AppState
from torchx.specs.fb.component_helpers import Packages


class _MASTSpec(NamedTuple):
    hpcIdentity: str
    hpcJobOncall: str
    rmAttribution: str
    hpcClusterUuid: str
    packages: Sequence[str]
    timeout_sec: int
    meshes: List[Tuple[str, int, str]]
    workspace: Dict[Union[Path, str], str]
    env: Dict[str, str]
    # Additional scheduler args for TorchX Mast Scheduler
    runningTimeoutSec: int | None
    jobOwnerUnixname: str | None
    useStrictName: bool | None
    mounts: list[str] | None
    localityConstraints: list[str] | None
    useDataConstraintsForJobPlacement: bool | None
    enableGracefulPreemption: bool | None
    enableLegacy: bool | None
    flexPoolId: str | None
    perpetualRun: bool | None
    jobPriority: str | None
    jobType: str | None
    modelTypeName: str | None
    redundantNodes: int | None
    hpcBaseImagePackage: str | None
    monitoringConfig: str | None
    tags: list[str] | None
    checkpointAgentPkg: str | None
    replicateRoles: list[str] | None
    modelEntityId: str | None
    flowTier: str | None
    gracefulPreemptionTimeoutSecs: int | None
    maxJobFailures: int | None
    stalenessHours: int | None
    disableTcLsstStickiness: bool | None
    hpcJobPriorityBand: str | None
    opecTag: str | None


MONARCH_PORT: int = 26600


class _MASTAllocator(AllocateMixin):
    def __init__(self, config: MastAllocatorConfig, job_start: Shared[None]):
        self._mast = MastHostAllocatorBase(config)
        self._job_start = job_start

    def allocate_nonblocking(self, spec: AllocSpec) -> "PythonTask[Alloc]":
        async def work():
            await self._job_start
            return await self._mast.allocate_nonblocking(spec)

        return PythonTask.from_coroutine(work())

    def _stream_logs(self) -> bool:
        return True


class MASTJob(JobTrait):
    def __init__(
        self,
        *,
        hpcIdentity: str,
        hpcJobOncall: str,
        rmAttribution: str,
        hpcClusterUuid: str = "MastProdCluster",
        packages: Sequence[str] = (),
        timeout_sec=3600,
        env: Dict[str, str] | None = None,
        # Additional optional scheduler args
        runningTimeoutSec: int | None = None,
        jobOwnerUnixname: str | None = None,
        useStrictName: bool | None = None,
        mounts: list[str] | None = None,
        localityConstraints: list[str] | None = None,
        useDataConstraintsForJobPlacement: bool | None = None,
        enableGracefulPreemption: bool | None = None,
        enableLegacy: bool | None = None,
        flexPoolId: str | None = None,
        perpetualRun: bool | None = None,
        jobPriority: str | None = None,
        jobType: str | None = None,
        modelTypeName: str | None = None,
        redundantNodes: int | None = None,
        hpcBaseImagePackage: str | None = None,
        monitoringConfig: str | None = None,
        tags: list[str] | None = None,
        checkpointAgentPkg: str | None = None,
        replicateRoles: list[str] | None = None,
        modelEntityId: str | None = None,
        flowTier: str | None = None,
        gracefulPreemptionTimeoutSecs: int | None = None,
        maxJobFailures: int | None = None,
        stalenessHours: int | None = None,
        disableTcLsstStickiness: bool | None = None,
        hpcJobPriorityBand: str | None = None,
        opecTag: str | None = None,
    ):
        super().__init__()
        self._name: Optional[str] = None
        self._spec = _MASTSpec(
            hpcIdentity,
            hpcJobOncall,
            rmAttribution,
            hpcClusterUuid,
            list(packages),
            timeout_sec,
            [],
            {},
            env or {},
            runningTimeoutSec,
            jobOwnerUnixname,
            useStrictName,
            mounts,
            localityConstraints,
            useDataConstraintsForJobPlacement,
            enableGracefulPreemption,
            enableLegacy,
            flexPoolId,
            perpetualRun,
            jobPriority,
            jobType,
            modelTypeName,
            redundantNodes,
            hpcBaseImagePackage,
            monitoringConfig,
            tags,
            checkpointAgentPkg,
            replicateRoles,
            modelEntityId,
            flowTier,
            gracefulPreemptionTimeoutSecs,
            maxJobFailures,
            stalenessHours,
            disableTcLsstStickiness,
            hpcJobPriorityBand,
            opecTag,
        )
        self._handle: Optional[str] = None

    def add_mesh(self, name: str, num_hosts: int, host_type: str = "gtt_any"):
        self._spec.meshes.append((name, num_hosts, host_type))

    def add_directory(self, path: str, local_path: Optional[str] = None):
        self._spec.workspace[path] = path if local_path is None else local_path

    def _create(self, client_script: Optional[str] = None):
        if not self._spec.meshes:
            raise ValueError("There needs to be at least one mesh in the job")
        packages = Packages()
        for p in self._spec.packages:
            packages.add_package(p)

        appdef = hyperactor.host_mesh_conda(
            meshes=[
                f"{name}:{num_hosts}:{host_type}"
                for name, num_hosts, host_type in self._spec.meshes
            ],
            additional_packages=packages,
            timeout_sec=self._spec.timeout_sec,
            env={
                **hyperactor.DEFAULT_NVRT_ENVS,
                **hyperactor.DEFAULT_NCCL_ENVS,
                **hyperactor.DEFAULT_TORCH_ENVS,
                **self._spec.env,
            },
        )
        with ExitStack() as stack:
            workspace = Workspace(self._spec.workspace)
            name = self._name = f"monarch-{uuid.uuid4().hex}"
            if client_script is not None:
                appdef.roles[0].env["MONARCH_BATCH_JOB"] = client_script
                temp_dir = Path(stack.enter_context(tempfile.TemporaryDirectory()))
                path = temp_dir / "job_state.pkl"
                BatchJob(self).dump(str(path))
                workspace.dirs[temp_dir] = ".monarch"

            # Build scheduler_args from _spec, excluding internal monarch fields
            exclude_fields = {"packages", "timeout_sec", "meshes", "workspace", "env"}
            config = Config(
                scheduler="mast_conda",
                scheduler_args={
                    k: v
                    for k, v in self._spec._asdict().items()
                    if k not in exclude_fields and v is not None
                },
                appdef=appdef,
                workspace=workspace,
            )
            print(f"Creating monarch mast job {name} ...")
            create(config, name)
            print("DONE")

    def can_run(self, spec: "JobTrait"):
        """
        Local jobs are the same regardless of what was saved, so just
        use the spec, which has the correct 'hosts' sequence.
        """
        if not isinstance(spec, MASTJob):
            return False
        r = (
            self._spec == spec._spec
            and self._name is not None
            and status(self._name) != "expired"
        )
        return r

    def _state(self) -> JobState:
        assert self._name is not None
        job_started = PythonTask.from_coroutine(_server_ready(self._name)).spawn()

        host_meshes = {}
        for name, num_host, _ in self._spec.meshes:
            allocator = _MASTAllocator(
                MastAllocatorConfig(
                    job_name=self._name,
                    remote_allocator_port=MONARCH_PORT,
                ),
                job_started,
            )
            constraints = AllocConstraints(
                {MastHostAllocator.ALLOC_LABEL_TASK_GROUP: name}
            )
            host_meshes[name] = host_mesh_from_alloc(
                name, Extent(["hosts"], [num_host]), allocator, constraints
            )

        return JobState(host_meshes)

    def _kill(self):
        if self._name is not None:
            kill(f"mast_conda:///{self._name}")


_CHECK_INTERVAL = timedelta(seconds=5)


class NotReady(NamedTuple):
    reason: str


Status = Literal["ready", "expired"] | NotReady


def status(job_name: str) -> Status:
    server_handle = f"mast_conda:///{job_name}"

    server_spec = info(server_handle)
    if server_spec is None:
        return "expired"
    if server_spec.state <= AppState.PENDING:  # UNSUBMITTED or SUBMITTED or PENDING
        return NotReady(
            f"Waiting for {server_handle} to be {AppState.RUNNING} (current: {server_spec.state}); "
        )

    if server_spec.state == AppState.RUNNING:
        for mesh_spec in server_spec.meshes:
            if mesh_spec.state <= AppState.PENDING:
                return NotReady(
                    f"Job {server_handle} is running but waiting for mesh {mesh_spec.name} "
                    f"to be {AppState.RUNNING} (current: {mesh_spec.state}); "
                )
            if mesh_spec.state > AppState.RUNNING:
                return "expired"
        return "ready"
    return "expired"


async def _server_ready(
    job_name: str,
    check_interval: timedelta = _CHECK_INTERVAL,
):
    check_interval_seconds = check_interval.total_seconds()
    start = datetime.now()

    while True:
        current_status = status(job_name)
        match current_status:
            case "ready":
                return
            case NotReady(reason=reason):
                print(
                    f"{reason} will check again in {check_interval_seconds} seconds. Total wait time: {(datetime.now() - start)}",
                    end="\r",
                )
                await PythonTask.sleep(check_interval_seconds)
            case "expired":
                raise ValueError("job expired")
