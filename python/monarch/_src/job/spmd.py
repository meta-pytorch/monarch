# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import re
import sys
from typing import Any, Dict, List, Optional

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._rust_bindings.monarch_hyperactor.config import configure
from monarch._src.job.job import JobState, JobTrait
from monarch._src.spmd import SPMDActor
from torchx.runner import get_runner
from torchx.specs import AppDef
from torchx.specs.finder import get_component


def _extract_nproc_per_node_from_appdef(appdef: AppDef) -> int:
    """
    Extract nproc_per_node from the torchrun command in AppDef.

    NOTE: This is for Phase B implementation where we parse torchrun
    and spawn GPU workers as monarch actors. Currently unused in Phase A.

    The AppDef typically has entrypoint="bash" with args=["-c", "torchrun ... --nproc_per_node N ..."].
    This function parses the torchrun command to extract N.
    """
    if not appdef.roles or len(appdef.roles) == 0:
        raise ValueError("AppDef has no roles")

    role = appdef.roles[0]

    # For bash entrypoint, the command is in args[1] (after "-c")
    if role.entrypoint == "bash" and len(role.args) >= 2 and role.args[0] == "-c":
        command = role.args[1]
    # For python entrypoint, the command is in the args list
    elif role.entrypoint == "python":
        command = " ".join(role.args)
    else:
        # Fallback: join all args
        command = " ".join(role.args) if role.args else ""

    # Parse --nproc_per_node or --nproc-per-node
    match = re.search(r"--nproc[_-]per[_-]node[=\s]+(\d+)", command)
    if match:
        return int(match.group(1))

    raise ValueError(f"Could not extract nproc_per_node from AppDef command: {command}")


def _parse_cli_args_to_kwargs(cli_args: List[str]) -> tuple[List[str], List[str]]:
    """
    Convert CLI-style arguments to key=value format expected by component_args_from_cli.

    Args:
        cli_args: Arguments in CLI format, e.g. ['-j', '1x8', '--script', 'train.py', '--', '--lr', '0.001']

    Returns:
        Tuple of (kwargs_format, script_args) where:
        - kwargs_format: ['j=1x8', 'script=train.py']
        - script_args: ['--lr', '0.001'] (arguments after -- delimiter)
    """
    kwargs_format = []
    i = 0
    script_args = []
    found_delimiter = False

    while i < len(cli_args):
        arg = cli_args[i]

        # Check for -- delimiter
        if arg == "--":
            found_delimiter = True
            # Everything after -- becomes script_args
            script_args = cli_args[i + 1 :]
            break

        # Parse flag arguments
        if arg.startswith("--"):
            key = arg[2:]  # Remove --
            if i + 1 < len(cli_args) and not cli_args[i + 1].startswith("-"):
                value = cli_args[i + 1]
                kwargs_format.append(f"{key}={value}")
                i += 2
            else:
                # Boolean flag
                kwargs_format.append(f"{key}=true")
                i += 1
        elif arg.startswith("-"):
            key = arg[1:]  # Remove -
            if i + 1 < len(cli_args) and not cli_args[i + 1].startswith("-"):
                value = cli_args[i + 1]
                kwargs_format.append(f"{key}={value}")
                i += 2
            else:
                # Boolean flag
                kwargs_format.append(f"{key}=true")
                i += 1
        else:
            i += 1

    return kwargs_format, script_args


def create_job_for_scheduler(
    scheduler: str,
    scheduler_cfg: Dict[str, Any],
    num_hosts: int,
    host_type: str,
    workspace: Optional[str] = None,
) -> JobTrait:
    """
    Create appropriate job based on scheduler type.

    Args:
        scheduler: Scheduler name (e.g., "mast", "mast_conda", "slurm")
        scheduler_cfg: Scheduler configuration dict with keys like hpcIdentity, etc.
        num_hosts: Number of hosts to allocate
        host_type: Host type (e.g., "gtt_any")
        workspace: Optional local workspace directory to pack

    Returns:
        JobTrait instance configured for the scheduler

    Raises:
        NotImplementedError: If scheduler is not yet supported
        ValueError: If scheduler is unsupported
    """
    match scheduler:
        case "mast_conda":
            from monarch._src.job.meta import MASTJob

            job = MASTJob(
                hpcIdentity=scheduler_cfg["hpcIdentity"],
                hpcJobOncall=scheduler_cfg["hpcJobOncall"],
                rmAttribution=scheduler_cfg["rmAttribution"],
                hpcClusterUuid=scheduler_cfg.get("hpcClusterUuid", "MastProdCluster"),
            )
            job.add_mesh("workers", num_hosts, host_type)

            # Add workspace if provided (pack to root of WORKSPACE_DIR)
            if workspace:
                job.add_directory(workspace, "")

            return job

        case "slurm":
            raise NotImplementedError(f"Scheduler {scheduler} not yet supported")

        case _:
            raise ValueError(f"Unsupported scheduler: {scheduler}")


class SPMDJob(JobTrait):
    """
    SPMD (Single Program Multiple Data) job that wraps any JobTrait.

    This job type is created via `monarch serve torchx ...` CLI and stores
    both the underlying job (e.g., MASTJob) and the AppDef from the torchx component.
    """

    def __init__(
        self,
        job: JobTrait,
        scheduler: str,
        appdef: AppDef,
        workspace: Optional[str] = None,
        scheduler_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__()
        self._job = job
        self._scheduler = scheduler
        self._appdef = appdef
        self._workspace = workspace
        self._scheduler_args = scheduler_args or {}

    @classmethod
    def serve_from_command(cls, command: List[str]) -> "SPMDJob":
        """
        Create an SPMDJob from a torchx command.

        Args:
            command: List of command arguments starting with 'torchx run'
                    Example: ['torchx', 'run', '-s', 'mast_conda', '-cfg', 'key=val',
                             'dist.ddp', '-j', '1x8', '--script', 'train.py', '--', '--lr', '0.001']

        Returns:
            SPMDJob instance ready to be launched

        Raises:
            ValueError: If command format is invalid or required args are missing
        """
        # Validate input
        if len(command) < 2 or command[0] != "torchx" or command[1] != "run":
            raise ValueError(
                "Command must start with 'torchx run'. "
                f"Got: {' '.join(command[:2]) if len(command) >= 2 else command}"
            )

        # Remove 'torchx run' from beginning
        torchx_args = command[2:]

        # Manually parse scheduler args and component args
        scheduler = None
        scheduler_args_str = ""
        workspace = None
        component_start_idx = None

        i = 0
        while i < len(torchx_args):
            if torchx_args[i] in ["-s", "--scheduler"]:
                if i + 1 < len(torchx_args):
                    scheduler = torchx_args[i + 1]
                    i += 2
                else:
                    raise ValueError("-s/--scheduler requires a value")
            elif torchx_args[i] in ["-cfg", "--scheduler_args"]:
                if i + 1 < len(torchx_args):
                    scheduler_args_str = torchx_args[i + 1]
                    i += 2
                else:
                    raise ValueError("-cfg/--scheduler_args requires a value")
            elif torchx_args[i] == "--workspace":
                if i + 1 < len(torchx_args):
                    workspace = torchx_args[i + 1]
                    i += 2
                else:
                    raise ValueError("--workspace requires a value")
            else:
                # This is the start of component name and args
                component_start_idx = i
                break

        if not scheduler:
            raise ValueError("-s/--scheduler is required")

        if component_start_idx is None or component_start_idx >= len(torchx_args):
            raise ValueError("Component name is required")

        # Get component name and remaining args
        component_name = torchx_args[component_start_idx]
        component_args = torchx_args[component_start_idx + 1 :]

        # Parse scheduler args
        runner = get_runner()
        scheduler_opts = runner.scheduler_run_opts(scheduler)
        scheduler_cfg = scheduler_opts.cfg_from_str(scheduler_args_str)

        # Get component function and call it
        component_fn = get_component(component_name).fn

        # Convert CLI-style args to key=value format and extract script_args
        component_args_kwformat, script_args = _parse_cli_args_to_kwargs(component_args)
        print(f"DEBUG: Original component_args: {component_args}")
        print(f"DEBUG: Converted to kwformat: {component_args_kwformat}")
        print(f"DEBUG: Script args: {script_args}")

        # Parse kwargs manually (bypass component_args_from_cli to avoid *args limitation)
        component_kwargs = {}
        for arg in component_args_kwformat:
            if "=" in arg:
                key, value = arg.split("=", 1)
                component_kwargs[key] = value

        print(f"DEBUG: Component kwargs: {component_kwargs}")

        # Call component function to get AppDef
        try:
            # Pass script_args as positional arguments (*script_args)
            appdef: AppDef = component_fn(*script_args, **component_kwargs)
        except Exception as e:
            raise ValueError(
                f"Failed to call component function '{component_name}': {e}"
            )

        # Extract num_hosts from AppDef
        if not appdef.roles or len(appdef.roles) == 0:
            raise ValueError("AppDef has no roles")

        num_hosts = appdef.roles[0].num_replicas

        # Extract host_type from component_args (if provided)
        host_type = "gtt_any"
        for i, arg in enumerate(component_args):
            if arg in ["-h", "--host_type"] and i + 1 < len(component_args):
                host_type = component_args[i + 1]
                break

        # Workspace: CLI overrides AppDef
        if workspace is None and appdef.roles[0].env:
            workspace = appdef.roles[0].env.get("WORKSPACE_DIR")

        # Auto-detect workspace from script path if not specified
        if workspace is None and "script" in component_kwargs:
            script_path = component_kwargs["script"]
            # If script is a relative path, use current directory as workspace
            if script_path and not script_path.startswith("/"):
                import os

                workspace = os.getcwd()
                print(f"Auto-detected workspace from relative script path: {workspace}")

        # Create underlying job based on scheduler type
        underlying_job = create_job_for_scheduler(
            scheduler=scheduler,
            scheduler_cfg=scheduler_cfg,
            num_hosts=num_hosts,
            host_type=host_type,
            workspace=workspace,
        )

        # Return SPMDJob with AppDef
        return cls(
            job=underlying_job,
            scheduler=scheduler,
            appdef=appdef,
            workspace=workspace,
            scheduler_args=scheduler_cfg,
        )

    def _create(self, client_script: Optional[str] = None):
        self._job._create(client_script)

    def can_run(self, spec: "JobTrait") -> bool:
        if not isinstance(spec, SPMDJob):
            return False
        return (
            self._scheduler == spec._scheduler
            and self._appdef == spec._appdef
            and self._workspace == spec._workspace
            and self._scheduler_args == spec._scheduler_args
            and self._job.can_run(spec._job)
        )

    def _state(self) -> JobState:
        return self._job._state()

    def _kill(self):
        self._job._kill()

    def run_spmd(self):
        """
        Phase A: Execute torchrun command once per host.
        torchrun will spawn the GPU worker processes as child processes.

        Phase B (TODO): Parse torchrun command and spawn GPU workers as monarch actors.
        """
        configure(default_transport=ChannelTransport.MetaTlsWithHostname)
        job_state = self._state()
        workers = job_state.workers

        # Phase A: Spawn 1 actor per host (torchrun will handle GPU processes)
        pm = workers.spawn_procs()
        am = pm.spawn("_SPMDActor", SPMDActor)

        # Extract execution components from AppDef
        role = self._appdef.roles[0]
        entrypoint = role.entrypoint
        args = role.args or []
        env = role.env or {}

        print("Phase A: Running torchrun command on each host")
        print(f"  entrypoint: {entrypoint}")
        print(f"  args: {args[:3] if len(args) > 3 else args}...")

        # Run command on all hosts - torchrun handles coordination
        am.run_command.call(entrypoint, args, env).get()
