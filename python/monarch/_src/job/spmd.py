# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import time
import warnings
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import configure
from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.job.job import JobState, JobTrait
from monarch._src.spmd.actor import SPMDActor
from monarch._src.tools.commands import torchx_runner
from torchx.runner import Runner
from torchx.specs import AppDef, AppState


def _parse_torchrun(
    original_roles: List[Dict[str, Any]],
) -> Tuple[List[str], int]:
    """
    Parse torchrun args to extract script/module args and nproc-per-node.

    The original role structure looks like:
        {
            'entrypoint': 'workspace/entrypoint.sh',
            'args': ['torchrun', '--nnodes=1', '--nproc-per-node=8', '-m', 'train', '--lr', '0.001']
        }

    Supports these patterns:
        - ['torchrun', '--nproc-per-node=8', '-m', 'train', ...]
        - ['python', '-m', 'torch.distributed.run', '--nproc-per-node=8', '-m', 'train', ...]
        - ['python', '-m', 'torchrun', '--nproc-per-node=8', '-m', 'train', ...]
        - ['python', 'train.py', ...]  (single proc)

    Returns:
        (script_args, nproc_per_node) tuple
        e.g., (['-m', 'train', '--lr', '0.001'], 8)

    Raises:
        ValueError: If args format is not recognized
    """
    if not original_roles:
        raise ValueError("No roles provided")

    role = original_roles[0]
    full_args = role.get("args", [])

    if not full_args:
        raise ValueError("Role has no args")

    nproc_per_node = 1
    script_args: List[str] = []

    # Determine start index for torchrun options
    # Handle: torchrun ..., python -m torch.distributed.run ..., python -m torchrun ...
    torchrun_modules = ("torch.distributed.run", "torchrun")

    if full_args[0] in ("torchrun", "torch.distributed.run"):
        # Direct torchrun invocation
        start_idx = 1
    elif full_args[0] in ("python", "python3"):
        # Check for python -m torch.distributed.run or python -m torchrun
        if (
            len(full_args) >= 3
            and full_args[1] == "-m"
            and full_args[2] in torchrun_modules
        ):
            start_idx = 3  # Skip: python -m torch.distributed.run
        else:
            # Plain python script - no torchrun options to parse
            start_idx = 1
    else:
        raise ValueError(
            f"Expected args to start with torchrun, torch.distributed.run, "
            f"python, or python3, got: {full_args[0]}"
        )

    # Parse nproc-per-node from args (after start_idx)
    for arg in full_args[start_idx:]:
        if arg.startswith("--nproc-per-node=") or arg.startswith("--nproc_per_node="):
            try:
                nproc_per_node = int(arg.split("=", 1)[1])
            except ValueError:
                pass
            break

    # Extract script args (skip torchrun options until we hit -m or script path)
    i = start_idx
    while i < len(full_args):
        arg = full_args[i]
        if arg == "-m":
            script_args = list(full_args[i:])
            break
        elif arg.startswith("--"):
            i += 1
        elif arg.startswith("-") and arg != "-m":
            i += 1
        else:
            script_args = list(full_args[i:])
            break

    return (script_args, nproc_per_node)


def serve(
    appdef: AppDef,
    scheduler: str = "mast_conda",
    scheduler_cfg: Optional[Dict[str, Any]] = None,
) -> "SPMDJob":
    """
    Launch SPMD job using custom AppDef.

    This function launches monarch workers using the appdef's entrypoint, then
    allows running SPMD training via run_spmd().

    Assumptions:
        - The appdef's role.entrypoint is a script (e.g., "workspace/entrypoint.sh")
          that sets up the environment (activates conda, sets WORKSPACE_DIR, etc.)
          and runs its arguments (e.g., via "$@" or exec "$@").
        - The appdef's role.args contains a torchrun command with the training script,
          e.g., ["torchrun", "--nnodes=1", "-m", "train", "--lr", "0.001"].
          The script/module args are parsed and passed to SPMDActor.main().
        - The appdef's role.workspace defines which files to upload to workers.

    Args:
        appdef: AppDef instance. The role.workspace defines which files to upload.
        scheduler: Scheduler name (e.g., 'mast_conda')
        scheduler_cfg: Scheduler configuration dict

    Returns:
        SPMDJob instance

    Example:
        from monarch.examples.meta.spmd.launch import appdef
        job = serve(
            appdef=appdef("--lr", "0.001", h="gtt_any", nnodes=2),
            scheduler="mast_conda",
            scheduler_cfg={"hpcClusterUuid": "MastProdCluster"}
        )
    """
    # Clean up stale job state file
    job_state_path = os.path.join(os.getcwd(), ".monarch", "job_state.pkl")
    if os.path.exists(job_state_path):
        warnings.warn(
            f"Removing stale job state file: {job_state_path}",
            stacklevel=2,
        )
        os.remove(job_state_path)

    # Extract workspace from appdef's first role
    workspace = None
    if appdef.roles:
        role_workspace = appdef.roles[0].workspace
        if role_workspace is not None and role_workspace.projects:
            # Get the first project directory as the workspace
            workspace = next(iter(role_workspace.projects.keys()), None)

    # Cache original entrypoints before modifying
    original_roles = []
    for role in appdef.roles:
        original_roles.append(
            {
                "entrypoint": role.entrypoint,
                "args": role.args,
            }
        )

        role.args = [
            "python",
            "-X",
            "faulthandler",
            "-c",
            # Use socket.getfqdn() to get FQDN - wildcard (*) causes routing failures
            'import socket; from monarch.actor import run_worker_loop_forever; run_worker_loop_forever(ca="trust_all_connections", address=f"metatls://{socket.getfqdn()}:26600")',
        ]

    # Fall back to cwd if no workspace defined in appdef
    if workspace is None:
        workspace = os.getcwd()
    scheduler_cfg = scheduler_cfg or {}

    runner = torchx_runner()

    # Dryrun + schedule
    dryrun_info = runner.dryrun(
        app=appdef,
        scheduler=scheduler,
        cfg=scheduler_cfg,
        workspace=workspace,
    )

    handle = runner.schedule(dryrun_info)
    status = runner.status(handle)
    print(f"Launched: {status.ui_url if status else handle}")

    job = SPMDJob(
        handle=handle,
        scheduler=scheduler,
        workspace=workspace,
        original_roles=original_roles,
    )

    return job


class SPMDJob(JobTrait):
    """
    SPMD (Single Program Multiple Data) job that uses torchx directly.

    This job type wraps a torchx Runner and job handle, providing monarch job tracking.
    """

    def __init__(
        self,
        handle: str,
        scheduler: str,
        workspace: Optional[str] = None,
        original_roles: Optional[List[Dict[str, Any]]] = None,
    ):
        super().__init__()
        self._app_handle = handle
        self._scheduler = scheduler
        self._workspace = workspace
        self._original_roles = original_roles or []
        self._hostnames: Optional[List[str]] = None

    def _get_runner(self) -> Runner:
        """Lazily create runner when needed (not pickle-friendly)."""
        return torchx_runner()

    def _create(self, client_script: Optional[str] = None):
        """Job is already created in serve(), this is a no-op."""
        pass

    def can_run(self, spec: "JobTrait") -> bool:
        if not isinstance(spec, SPMDJob):
            return False
        if self._app_handle is None:
            return False

        # Check if job is still running
        status = self._get_runner().status(self._app_handle)
        return status is not None and not status.is_terminal()

    def _check_job_ready(self) -> bool | str:
        """
        Check if job is ready (running with replicas).
        Returns True if ready, error message string if not ready, raises ValueError if failed.
        """
        status = self._get_runner().status(self._app_handle)
        if status is None:
            raise ValueError("Job not found")
        if status.state in [AppState.FAILED, AppState.CANCELLED]:
            raise ValueError(f"Job failed with state: {status.state}")

        if status.state < AppState.RUNNING:
            return f"Waiting for job to be RUNNING (current: {status.state})"

        if not status.roles or not status.roles[0].replicas:
            return "Job is RUNNING but waiting for replicas to be available"

        return True

    def _wait_for_job_ready(self, check_interval_seconds: float = 5.0) -> None:
        """Wait for job to be ready, polling at check_interval_seconds."""
        start = datetime.now()

        while True:
            ready_status = self._check_job_ready()
            if ready_status is True:
                print(f"\nJob is ready. Total wait time: {datetime.now() - start}")
                break
            else:
                print(
                    f"{ready_status}; will check again in {check_interval_seconds} seconds. Total wait time: {datetime.now() - start}",
                    end="\r",
                )
                time.sleep(check_interval_seconds)

    def _state(self) -> JobState:
        assert self._app_handle is not None

        self._wait_for_job_ready()

        status = self._get_runner().status(self._app_handle)
        assert status is not None and status.roles and status.roles[0].replicas

        # Extract hostnames from status
        hostnames = [
            replica.hostname
            for replica in sorted(status.roles[0].replicas, key=lambda r: r.id)
        ]
        self._hostnames = hostnames

        configure(default_transport=ChannelTransport.MetaTlsWithHostname)
        # TODO generalize away from just mast
        workers = attach_to_workers(
            ca="trust_all_connections",
            workers=[
                f"metatls://{hostname}.facebook.com:26600" for hostname in hostnames
            ],
        )

        return JobState({"workers": workers})

    def _kill(self):
        if self._app_handle is not None:
            self._get_runner().cancel(self._app_handle)

    def run_spmd(self):
        state = self._state()
        workers = state.workers

        script_args, nproc_per_node = _parse_torchrun(self._original_roles)

        procs = workers.spawn_procs(per_host={"gpus": nproc_per_node})
        am = procs.spawn("_SPMDActor", SPMDActor)

        # Get master addr/port from first actor
        first_values = dict.fromkeys(procs._labels, 0)
        master_addr, master_port = (
            am.slice(**first_values).get_host_port.call_one(None).get()
        )

        am.main.call(master_addr, master_port, script_args).get()
