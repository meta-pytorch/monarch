# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import argparse
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


def _get_torchrun_parser() -> argparse.ArgumentParser:
    """
    Build argparse parser for torchrun torch/distributed/run.py arguments.
    """
    parser = argparse.ArgumentParser(add_help=False)

    parser.add_argument("--nnodes", type=str, default="1:1")
    parser.add_argument("--nproc-per-node", "--nproc_per_node", type=str, default="1")

    parser.add_argument("--rdzv-backend", "--rdzv_backend", type=str)
    parser.add_argument("--rdzv-endpoint", "--rdzv_endpoint", type=str)
    parser.add_argument("--rdzv-id", "--rdzv_id", type=str)
    parser.add_argument("--rdzv-conf", "--rdzv_conf", type=str)
    parser.add_argument("--standalone", action="store_true")

    parser.add_argument("--max-restarts", "--max_restarts", type=int)
    parser.add_argument("--monitor-interval", "--monitor_interval", type=float)
    parser.add_argument("--start-method", "--start_method", type=str)
    parser.add_argument("--role", type=str)
    parser.add_argument("-m", "--module", action="store_true")
    parser.add_argument("--no-python", "--no_python", action="store_true")
    parser.add_argument("--run-path", "--run_path", action="store_true")

    parser.add_argument("--log-dir", "--log_dir", type=str)
    parser.add_argument("-r", "--redirects", type=str)
    parser.add_argument("-t", "--tee", type=str)
    parser.add_argument("--local-ranks-filter", "--local_ranks_filter", type=str)

    parser.add_argument("--node-rank", "--node_rank", type=int)
    parser.add_argument("--master-addr", "--master_addr", type=str)
    parser.add_argument("--master-port", "--master_port", type=int)
    parser.add_argument("--local-addr", "--local_addr", type=str)

    parser.add_argument("training_script", nargs="?")
    parser.add_argument("training_script_args", nargs=argparse.REMAINDER)

    return parser


def _parse_torchrun(
    original_roles: List[Dict[str, Any]],
) -> Tuple[List[str], int]:
    """
    Parse torchrun args using argparse to match real torchrun torch/distributed/run.py behavior.

    The original role structure looks like:
    {
        'entrypoint': 'workspace/entrypoint.sh',
        'args': ['torchrun', '--nnodes=1', '--nproc-per-node=8', '-m', 'train', '--lr', '0.001']
    }

    Supports:
        - ['torchrun', '--nproc-per-node=8', '-m', 'train', ...]
        - ['python', '-m', 'torch.distributed.run', '--nproc-per-node=8', '-m', 'train', ...]
        - ['python', '-m', 'torchrun', '--nproc-per-node=8', '-m', 'train', ...]
        - ['python', 'train.py', ...] (single proc)

    Returns:
        (script_args, nproc_per_node) tuple
        e.g., (['-m', 'train', '--lr', '0.001'], 8)

    Raises:
        ValueError: If args format is not recognized
    """
    if not original_roles:
        raise ValueError("No roles provided")
    if len(original_roles) > 1:
        raise ValueError(
            "Multiple roles provided. monarch.spmd supports single-role SPMD jobs"
        )

    role = original_roles[0]
    full_args = role.get("args", [])

    if not full_args:
        raise ValueError("Role has no args")

    # Determine where torchrun args start
    torchrun_modules = ("torch.distributed.run", "torchrun")

    if full_args[0] in ("torchrun", "torch.distributed.run"):
        args_to_parse = full_args[1:]
    elif full_args[0] in ("python", "python3"):
        if (
            len(full_args) >= 3
            and full_args[1] == "-m"
            and full_args[2] in torchrun_modules
        ):
            args_to_parse = full_args[3:]
        else:
            # Plain python script - return script and args, nproc=1
            return (list(full_args[1:]), 1)
    else:
        raise ValueError(
            f"Expected args to start with torchrun, torch.distributed.run, "
            f"python, or python3, got: {full_args[0]}"
        )

    # Parse using argparse
    parser = _get_torchrun_parser()
    args, _ = parser.parse_known_args(args_to_parse)

    # Extract nproc_per_node
    nproc_per_node = 1
    nproc_str = getattr(args, "nproc_per_node", "1")
    try:
        nproc_per_node = int(nproc_str)
    except ValueError:
        warnings.warn(
            f"--nproc-per-node={nproc_str} is not an integer, defaulting to 1. "
            f"Use an explicit integer value instead of '{nproc_str}'.",
            stacklevel=2,
        )

    # Build script_args
    script_args: List[str] = []
    if args.module:
        script_args.append("-m")
    if args.training_script:
        script_args.append(args.training_script)
    script_args.extend(args.training_script_args or [])

    return (script_args, nproc_per_node)


def _get_worker_addr(scheduler: str, hostname: str) -> str:
    """Build worker address for the given scheduler and hostname."""
    if scheduler.startswith("mast"):
        if not hostname.endswith(".facebook.com"):
            hostname = hostname + ".facebook.com"
        return f"metatls://{hostname}:26600"
    else:
        return f"tcp://{hostname}:26600"


def _get_channel_transport(scheduler: str) -> ChannelTransport:
    """Get channel transport for the given scheduler."""
    if scheduler.startswith("mast"):
        return ChannelTransport.MetaTlsWithHostname
    else:
        return ChannelTransport.TcpWithHostname


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
    scheme = "metatls" if scheduler.startswith("mast") else "tcp"
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
            f'import socket; from monarch.actor import run_worker_loop_forever; run_worker_loop_forever(ca="trust_all_connections", address=f"{scheme}://{{socket.getfqdn()}}:26600")',
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

        # Check that all replicas are running (use min to find least progressive)
        replica_state = min(r.state for r in status.roles[0].replicas)
        if replica_state < AppState.RUNNING:
            return f"Waiting for replicas to be RUNNING (current: {replica_state})"
        if replica_state > AppState.RUNNING:
            raise ValueError(f"Replica in terminal state: {replica_state}")

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

        configure(default_transport=_get_channel_transport(self._scheduler))
        workers = attach_to_workers(
            ca="trust_all_connections",
            workers=[_get_worker_addr(self._scheduler, h) for h in hostnames],
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
