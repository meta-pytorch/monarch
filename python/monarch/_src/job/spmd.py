# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Internal implementation of SPMD job primitives.

Provides the :func:`serve` function and :class:`SPMDJob` class for launching
torchrun-style SPMD training jobs. Parses torchrun arguments and creates a Monarch
mesh to run the training script, replicating torchrun behavior.
"""

import argparse
import os
import time
import warnings
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import configure
from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.actor.host_mesh import this_host
from monarch._src.job.job import JobState, JobTrait
from monarch._src.spmd.actor import SPMDActor
from monarch._src.tools.commands import torchx_runner
from torchx.runner import Runner
from torchx.specs import AppDef, AppState, Role


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
    full_args = list(role.get("args", []))
    entrypoint = role.get("entrypoint", "")

    # Prepend entrypoint if it's a recognized command
    recognized_commands = ("torchrun", "torch.distributed.run", "python", "python3")
    if entrypoint in recognized_commands:
        full_args = [entrypoint] + full_args

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


def _validate_single_node_command(command: List[str]) -> None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--nnodes", type=str, default=None)
    args, _ = parser.parse_known_args(command)

    if args.nnodes is None:
        return

    nnodes_str = args.nnodes.strip()
    if nnodes_str not in ("1", "1:1"):
        raise ValueError(
            f"Multi-node torchrun commands are not supported with serve(). "
            f"Got --nnodes={nnodes_str}. When passing a command list, only "
            f"single-node (--nnodes=1 or --standalone) is valid. For multi-node "
            f"training, use an AppDef with a scheduler that manages node allocation."
        )


def serve(
    appdef: AppDef | List[str],
    scheduler: str = "mast_conda",
    scheduler_cfg: Optional[Dict[str, Any]] = None,
) -> "SPMDJob":
    """
    Launch SPMD job using an AppDef or a single-node torchrun command.

    This function launches monarch workers, then allows running SPMD training
    via run_spmd().

    Assumptions:
        - When using an AppDef, the role's entrypoint is a script (e.g.,
          "workspace/entrypoint.sh") that sets up the environment (activates
          conda, sets WORKSPACE_DIR, etc.) and runs its arguments.
        - The role's args contains a torchrun command with the training script,
          e.g., ["torchrun", "--nnodes=1", "-m", "train", "--lr", "0.001"].
        - The role's workspace defines which files to upload to workers.
        - When using a command list, it should be a torchrun command, e.g.,
          ["torchrun", "--nproc-per-node=4", "--standalone", "train.py"].

    Note:
        When passing a command list, only single-node torchrun is supported
        (``--standalone`` or ``--nnodes=1``). For multi-node training, use an
        ``AppDef`` with a scheduler that manages node allocation.

    Args:
        appdef: Either a torchx ``AppDef`` instance, or a torchrun command as
            a list of strings (e.g., ``["torchrun", "--nproc-per-node=4",
            "train.py"]``). When a list is provided, the first element is the
            entrypoint and the rest are arguments.
        scheduler: Scheduler name (e.g., 'mast_conda', 'local_cwd')
        scheduler_cfg: Scheduler configuration dict

    Returns:
        SPMDJob instance

    Raises:
        ValueError: If command list specifies multi-node (--nnodes > 1).

    Example:
        Using a torchrun command list (single-node only)::

            from monarch.job.spmd import serve

            job = serve(
                ["torchrun", "--nproc-per-node=4", "--standalone", "train.py"],
                scheduler="local_cwd",
            )
            job.run_spmd()

        Using an AppDef (supports multi-node)::

            from monarch.job.spmd import serve
            from torchx import specs

            app = specs.AppDef(
                name="my-training",
                roles=[
                    specs.Role(
                        name="trainer",
                        image="my_workspace:latest",
                        entrypoint="workspace/entrypoint.sh",
                        args=["torchrun", "--nnodes=2", "--nproc-per-node=8",
                              "-m", "train"],
                        num_replicas=2,
                        resource=specs.resource(h="gtt_any"),
                    ),
                ],
            )
            job = serve(
                app,
                scheduler="mast_conda",
                scheduler_cfg={
                    "hpcClusterUuid": "MastGenAICluster",
                    "hpcIdentity": "my_identity",
                    "localityConstraints": ["region", "pci"],
                },
            )
            job.run_spmd()
    """
    if isinstance(appdef, list):
        command = appdef
        if not command:
            raise ValueError("command cannot be empty")
        _validate_single_node_command(command)

        entrypoint = command[0]
        args = list(command[1:]) if len(command) > 1 else []

        appdef = AppDef(
            name="spmd-job",
            roles=[
                Role(
                    name="trainer",
                    image="",
                    entrypoint=entrypoint,
                    args=args,
                    num_replicas=1,
                ),
            ],
        )

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

        if self._scheduler.startswith("local"):
            return JobState({"workers": this_host()})

        # Remote scheduler - poll for job readiness via torchx
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
