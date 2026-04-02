# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import argparse
import asyncio
import inspect
import logging
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Union

from monarch.tools.colors import CYAN, ENDC
from monarch.tools.components.hyperactor import DEFAULT_NAME
from monarch.tools.config import (  # @manual=//monarch/python/monarch/tools/config/meta:defaults
    Config,
    defaults,
)
from monarch.tools.mesh_spec import mesh_spec_from_metadata, ServerSpec
from monarch.tools.utils import MONARCH_HOME
from torchx.runner import Runner  # @manual=//torchx/runner:lib_core
from torchx.specs import AppDef, AppDryRunInfo, AppState, CfgVal, parse_app_handle
from torchx.specs.api import is_terminal
from torchx.specs.builders import parse_args
from torchx.util.types import decode, decode_optional

logger: logging.Logger = logging.getLogger(__name__)

TIMEOUT_AFTER_KILL = 300  # 5 minutes


def torchx_runner() -> Runner:
    # namespace is currently unused so make it empty str
    # so that server handle is short (e.g. slurm:///job-id)
    _EMPTY_NS = ""
    return Runner(_EMPTY_NS, defaults.scheduler_factories())


def component_args_from_cli(
    component_fn: Callable[..., AppDef], component_args: list[str]
) -> dict[str, Any]:
    """Parses component function's arguments from 'argname=argvalue' strings.

    Returns: component arguments kwarg-ified.
    """

    cli_fied_component_args = []
    for arg in component_args:
        argname = arg.split("=")[0]
        # torchx auto-generates an argparse parser for component function based
        # type-hints and docstring as if the component was a CLI itself so we have to
        # CLI arg-ify the component arguments by adding a "-" for
        # single-char argnames (short arg) and "--" for multi-char (long arg)
        cli_fied_component_args.append(f"-{arg}" if len(argname) == 1 else f"--{arg}")

    parsed_args: argparse.Namespace = parse_args(component_fn, cli_fied_component_args)

    # TODO kiuk@ logic below needs to move into torchx.specs.builders.parse_args()
    #  which is copied from torchx.specs.builders.materialize_appdef()
    #  parse_args() returns all the component parameters parsed from cli inputs
    #  as a string. Additional parameter type matching needs to be done (as below)
    #  to turn the CLI inputs to component function arguments.
    component_kwargs = {}

    parameters = inspect.signature(component_fn).parameters
    for param_name, parameter in parameters.items():
        arg_value = getattr(parsed_args, param_name)
        parameter_type = parameter.annotation
        parameter_type = decode_optional(parameter_type)
        arg_value = decode(arg_value, parameter_type)
        if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            raise TypeError(
                f"component fn param `{param_name}` is a '*arg' which is not supported; consider changing the type to a list"
            )
        elif parameter.kind == inspect.Parameter.VAR_KEYWORD:
            raise TypeError(
                f"component fn param `{param_name}` is a '**kwargs' which is not supported; consider changing the type to a dict or explicitly declare the params"
            )
        else:
            component_kwargs[param_name] = arg_value

    return component_kwargs


def create(
    config: Config,
    name: str = DEFAULT_NAME,
) -> Union[str, AppDryRunInfo]:
    """Creates a monarch server by submitting it as a job to the target scheduler.

    Usage:

    .. doc-test::

        from monarch.tools.config import defaults

        config = defaults.config(scheduler="slurm")
        config.appdef = defaults.component_fn(scheduler=config.scheduler)()

        config.scheduler_args.update(
            {
                "partition": "prod",
                "mail-user": "foo@bar.com",
                "mail-type": "FAIL",
            }
        )
        config.dryrun = True

        create(config)


    Args:
        scheduler: where to submit a job that runs the server
        scheduler_args: scheduler configs
        component_fn: a function that returns the AppDef (job def).
            If not provided, defaults to the configured default for the scheduler
            (in most cases ``monarch.tools.components.hyperactor.proc_mesh``)
        name: the name of the job. If none, a default job name will be created.
    """
    scheduler: str = config.scheduler
    cfg: Mapping[str, CfgVal] = config.scheduler_args

    # for logging call-site context in application metadata
    os.environ["TORCHX_CONTEXT_NAME"] = os.getenv("TORCHX_CONTEXT_NAME", "monarch")

    with torchx_runner() as runner:
        appdef: AppDef = AppDef(name, config.appdef.roles, config.appdef.metadata)
        if not config.workspace.dirs and not config.workspace.env:
            info = runner.dryrun(appdef, scheduler, cfg, workspace=None)
        else:
            with tempfile.TemporaryDirectory(dir=MONARCH_HOME("out")) as tmpdir:
                # multi-directory workspace is not supported natively in torchx; so merge into a single one
                # TODO (kiuk@) may be able to delete bootstrap workspace copy (as the job is created)
                #   since proc_mesh.sync_workspace() can do this without having to merge the workspace
                workspace_out = Path(tmpdir) / "workspace"
                config.workspace.merge(workspace_out)
                config.workspace.set_env_vars(appdef)

                info = runner.dryrun(appdef, scheduler, cfg, str(workspace_out))

        info_json_fmt = AppDryRunInfo(
            info.request,
            fmt=defaults.dryrun_info_formatter(info),
        )
        info_json_fmt._app = info._app
        info_json_fmt._cfg = info._cfg
        info_json_fmt._scheduler = info._scheduler

        if config.dryrun:
            return info_json_fmt
        else:
            server_handle = runner.schedule(info)
            return server_handle


def info(server_handle: str) -> Optional[ServerSpec]:
    """Calls the ``describe`` API on the scheduler hosting the server to get
    information about it.

    Returns ``None`` if the server's job is not found in the scheduler's
    control-plane. This can happen if the job does not exist
    (e.g. typo in the server_handle) or the job already exited a long time ago.

    NOTE: This function can return non-empty info for jobs that have
    exited recently.
    """
    with torchx_runner() as runner:
        status = runner.status(server_handle)
        if status is None:
            return None

        appdef = runner.describe(server_handle)
        if appdef is None:
            return None

    # host status grouped by mesh (role) names
    replica_status = {r.role: r.replicas for r in status.roles}

    mesh_specs = []
    for role in appdef.roles:
        spec = mesh_spec_from_metadata(appdef, role.name)
        assert spec is not None, "cannot be 'None' since we iterate over appdef's roles"

        # null-guard since some schedulers do not fill replica_status
        if host_status := replica_status.get(role.name):
            # make sure the hostnames are sorted by their respective node indexes
            # this makes ServerSpec.host0 return hostname of node 0
            spec.hostnames = [
                h.hostname for h in sorted(host_status, key=lambda h: h.id)
            ]
            # the mesh status is based on the "least progressive" replica status
            spec.state = min(h.state for h in host_status)

        mesh_specs.append(spec)

    scheduler, namespace, _ = parse_app_handle(server_handle)

    return ServerSpec(
        name=appdef.name,
        state=status.state,
        meshes=mesh_specs,
        scheduler=scheduler,
        namespace=namespace,
        ui_url=status.ui_url,
        metadata=appdef.metadata,
    )


_5_SECONDS = timedelta(seconds=5)


async def server_ready(
    server_handle: str,
    check_interval: timedelta = _5_SECONDS,
) -> Optional[ServerSpec]:
    """Waits until the server's job is in RUNNING state to returns the server spec.
    Returns `None` if the server does not exist.

    NOTE: Certain fields such as `hostnames` is only filled (and valid) when the server is RUNNING.

    Usage:

    .. code-block:: python

        server_info = await server_ready("slurm:///123")
        if not server_info:
            print(f"Job does not exist")
        else:
            if server_info.is_running:
                for mesh in server_info.meshes:
                    connect_to(mesh.hostnames)
            else:
                print(f"Job in {server_info.state} state. Hostnames are not available")

    """

    check_interval_seconds = check_interval.total_seconds()
    start = datetime.now()
    while True:
        server_spec = info(server_handle)

        if not server_spec:  # server not found
            return None

        if server_spec.state <= AppState.PENDING:  # UNSUBMITTED or SUBMITTED or PENDING
            # NOTE: TorchX currently does not have async APIs so need to loop-on-interval
            # TODO maybe inverse exponential backoff instead of constant interval?
            print(
                f"Waiting for {server_handle} to be {AppState.RUNNING} (current: {server_spec.state}); "
                f"will check again in {check_interval_seconds} seconds. "
                f"Total wait time: {datetime.now() - start}",
                end="\r",
            )
            await asyncio.sleep(check_interval_seconds)
            continue

        # check if hosts are allocated for all the meshes
        if server_spec.state == AppState.RUNNING:
            running = True
            for mesh_spec in server_spec.meshes:
                if mesh_spec.state <= AppState.PENDING:
                    print(
                        f"Job {server_handle} is running but waiting for mesh {mesh_spec.name} "
                        f"to be {AppState.RUNNING} (current: {mesh_spec.state}); "
                        f"will check again in {check_interval_seconds} seconds. "
                        f"Total wait time: {datetime.now() - start}",
                        end="\r",
                    )
                    running = False
                    break
            if not running:
                await asyncio.sleep(check_interval_seconds)
                continue

            # All meshes are ready, return the server spec
            return server_spec

        # If we reach here, the server is in a terminal state
        # Return terminal states so the caller can see what happened
        if is_terminal(server_spec.state):
            return server_spec

        # If we reach here with a non-terminal, non-RUNNING state (e.g., UNKNOWN),
        # it's likely a transient state during a transition - wait and retry
        print(
            f"Server {server_handle} in unexpected state {server_spec.state}; "
            f"will check again in {check_interval_seconds} seconds. "
            f"Total wait time: {datetime.now() - start}",
            end="\r",
        )
        await asyncio.sleep(check_interval_seconds)


# TODO: this API is overloaded. Ideally, we do not need config to get or an handle to create.
async def get_or_create(
    name: str,
    config: Config,
    check_interval: timedelta = _5_SECONDS,
    force_restart: bool = False,
) -> ServerSpec:
    """Waits for the server based on identity `name` in the scheduler specified in the `config`
    to be ready (e.g. RUNNING). If the server is not found then this function creates one
    per the `config` spec, and waits for the server to be ready before returning.

    Usage:

    .. code-block:: python

        from monarch.tools.config import defaults

        config = defaults.config(scheduler)
        config.appdef = defaults.component_fn(config.scheduler)()

        server_handle = get_or_create(name="my_job_name", config)
        server_info = info(server_handle)

    Args:
        name: the name of the server (job) to get or create
        config: configs used to create the job if one does not exist
        check_interval: how often to poll the status of the job when waiting for it to be ready
        force_restart: if True kills and re-creates the job even if one exists

    Returns: A `ServerSpec` containing information about either the existing or the newly
        created server.

    """
    assert not config.dryrun, (
        "dryrun is not supported for get_or_create(), for dryrun use the create() API instead"
    )

    server_handle = f"{config.scheduler}:///{name}"
    server_info = await server_ready(server_handle, check_interval)
    if not server_info or not server_info.is_running:  # then create one
        logger.info(
            "no existing RUNNING server `%s` creating new one...", server_handle
        )

        # no dryrun (see assertion above) support so will always be a handle (str)
        new_server_handle = str(create(config, name))

        logger.info(f"created new `{new_server_handle}` waiting for it to be ready...")

        server_info = await server_ready(new_server_handle, check_interval)

        if not server_info:
            raise RuntimeError(
                f"the new server `{new_server_handle}` went missing (should never happen)"
            )

        if not server_info.is_running:
            raise RuntimeError(
                f"the new server `{new_server_handle}` has {server_info.state}"
            )

        print(f"{CYAN}New job `{new_server_handle}` is ready to serve.{ENDC}")
    else:
        print(f"{CYAN}Found existing job `{server_handle}` ready to serve.{ENDC}")

        if force_restart:
            print(f"{CYAN}force_restart=True, restarting `{server_handle}`.{ENDC}")
            kill(server_handle)
            server_info = await get_or_create(name, config, check_interval)

    if server_info.ui_url:  # not all schedulers have a UI URL
        print(f"{CYAN}Job URL: {server_info.ui_url}{ENDC}")

    return server_info


def kill(server_handle: str) -> None:
    with torchx_runner() as runner:
        runner.cancel(server_handle)


def kill_and_confirm(
    server_handle: str, timeout_after_kill: int = TIMEOUT_AFTER_KILL
) -> None:
    """Kill the server and wait for it to be killed.
    This is needed because torchx cancel is asynchronous. We confirm the server
    is actually terminated before returning to avoid the job still being around
    after cancel() completes.
    """
    with torchx_runner() as runner:
        runner.cancel(server_handle)
        start_time = time.time()
        while time.time() - start_time < timeout_after_kill:
            server_info = runner.status(server_handle)
            if server_info and server_info.state in [
                AppState.SUCCEEDED,
                AppState.FAILED,
            ]:
                logger.info(
                    f"Server {server_handle} reached {server_info.state} state!"
                )
                return
            elif server_info:
                logger.info(
                    f"Server {server_handle} is in {server_info.state} state. Lets wait for it to be killed. Waiting ..."
                )
            else:
                logger.info(
                    f"Something went wrong. Unable to get server {server_handle} info. Waiting..."
                )
            time.sleep(5)
    raise Exception(
        f"Server {server_handle} did not reach a terminal state within {timeout_after_kill} seconds after kill",
    )


def bounce(server_handle: str) -> None:
    """(re)starts the server's processes without tearing down the server's job."""
    raise NotImplementedError("`bounce` is not yet implemented")


def stop(server_handle: str) -> None:
    """Stops the server's unix processes without tearing down the server's job."""
    raise NotImplementedError("`stop` is not yet implemented")


def debug(host: str, port: int) -> None:
    """Connect to the debug server running on the provided host and port."""
    for cmd in ["ncat", "nc", "netcat"]:
        try:
            subprocess.run([cmd, f"{host}", f"{port}"], check=True)
            return
        except FileNotFoundError:
            pass

    logging.error(
        "Could not find a suitable netcat binary. Please install one and try again."
    )


# ---------------------------------------------------------------------------
# Job registry / context
# ---------------------------------------------------------------------------

MONARCH_DIR: str = ".monarch"
DEFAULT_JOB_PATH: str = f"{MONARCH_DIR}/job_state.pkl"
_CONTEXT_STATE_FILE: str = "state.pkl"


def _context_dir(name: str) -> Path:
    return Path(MONARCH_DIR) / name


def _context_state(name: str) -> Path:
    return _context_dir(name) / _CONTEXT_STATE_FILE


def _current_context() -> Optional[str]:
    """Return the name of the currently active context, or None."""
    link = Path(DEFAULT_JOB_PATH)
    if not link.is_symlink():
        return None
    target = Path(os.readlink(str(link)))
    # Symlink is relative like "default/state.pkl"
    parts = target.parts
    if len(parts) >= 2 and parts[-1] == _CONTEXT_STATE_FILE:
        return parts[0]
    return None


def _ensure_symlink_setup() -> None:
    """If job_state.pkl is a plain file, migrate it to the 'default' context."""
    link = Path(DEFAULT_JOB_PATH)
    if link.exists() and not link.is_symlink():
        default_dir = _context_dir("default")
        default_dir.mkdir(parents=True, exist_ok=True)
        target = default_dir / _CONTEXT_STATE_FILE
        link.rename(target)
        # Symlink is relative so it stays valid if .monarch/ is moved
        link.symlink_to(Path("default") / _CONTEXT_STATE_FILE)


def context_create(name: str) -> None:
    """Create a new context directory under .monarch/."""
    _context_dir(name).mkdir(parents=True, exist_ok=True)
    print(f"Created context '{name}'")


def context_use(name: str) -> None:
    """Switch .monarch/job_state.pkl to point at <name>/state.pkl."""
    _ensure_symlink_setup()
    _context_dir(name).mkdir(parents=True, exist_ok=True)
    link = Path(DEFAULT_JOB_PATH)
    if link.is_symlink():
        link.unlink()
    link.symlink_to(Path(name) / _CONTEXT_STATE_FILE)
    print(f"Switched to context '{name}'")


def context_rm(name: str) -> None:
    """Remove a context, killing its job if still running."""
    import shutil

    state_file = _context_state(name)
    if state_file.exists():
        try:
            from monarch._src.job.job import job_load  # pyre-ignore[21]

            job_load(str(state_file)).kill()  # pyre-ignore[16]
        except Exception:
            pass
    shutil.rmtree(str(_context_dir(name)), ignore_errors=True)
    # If the symlink pointed at this context, restore it to default/state.pkl
    link = Path(DEFAULT_JOB_PATH)
    if link.is_symlink():
        target = Path(os.readlink(str(link)))
        if target.parts and target.parts[0] == name:
            link.unlink()
            link.symlink_to(Path("default") / _CONTEXT_STATE_FILE)
    print(f"Removed context '{name}'")


def context_ls() -> None:
    """List all contexts, marking the active one."""
    monarch_dir = Path(MONARCH_DIR)
    if not monarch_dir.exists():
        print("No contexts.")
        return
    current = _current_context()
    contexts = sorted(d.name for d in monarch_dir.iterdir() if d.is_dir())
    if not contexts:
        print("No contexts.")
        return
    for name in contexts:
        marker = "* " if name == current else "  "
        print(f"{marker}{name}")


# ---------------------------------------------------------------------------
# apply_job / exec_on_job
# ---------------------------------------------------------------------------


def apply_job(module_path: str) -> None:
    """Apply a job and wait for workers to be ready.

    Imports *module_path* as a dotted Python module path (e.g. ``myjob.job``),
    reads its named attribute (a :class:`~monarch.job.JobTrait`), then calls
    ``job.apply()``.

    Readiness is confirmed by spawning BashActors on all ranks and waiting
    for them to return.
    """
    import importlib

    from monarch._src.job.job import BashActor, JobTrait  # pyre-ignore[21]

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    # Split on the last '.' to get (module, attr).
    # "job_a.job"           → module="job_a",       attr="job"
    # "path.to.module.cfg"  → module="path.to.module", attr="cfg"
    if "." not in module_path:
        raise ValueError(f"module_path must be 'module.attr', got {module_path!r}")
    mod_name, attr_name = module_path.rsplit(".", 1)
    mod = importlib.import_module(mod_name)
    job = getattr(mod, attr_name, None)
    if job is None:
        raise AttributeError(f"Module '{mod_name}' has no '{attr_name}' attribute")
    if not isinstance(job, JobTrait):  # pyre-ignore[16]
        raise TypeError(
            f"'{mod_name}.{attr_name}' must be a JobTrait, got {type(job).__name__}"
        )

    t0 = time.time()
    state = job.state()
    # When reusing existing workers with a different spec (e.g. different
    # mounts), update the cached running job's config and dump it so future
    # exec calls use the current spec.  We dump the *running* job (which has
    # live worker PIDs), not the fresh module-loaded job (empty _host_to_pid),
    # to avoid creating broken CachedRunning nesting.
    running = job._running
    if running is not None and running is not job:
        running._mounts = job._mounts
        running._default_python_exe = job._default_python_exe
        running._python_executables = dict(job._python_executables)
        running.dump(".monarch/job_state.pkl")
    mesh = next(iter(state._hosts.values()))
    procs = mesh.spawn_procs()
    procs.spawn("_ready_check", BashActor).run.call("true").get()  # pyre-ignore[16]
    print(f"Job is ready ({time.time() - t0:.0f}s)")


def _parse_env(env: Optional[list[str]]) -> dict[str, str]:
    """Parse KEY=VALUE env var strings into a dict."""
    result: dict[str, str] = {}
    if env:
        for item in env:
            if "=" not in item:
                raise ValueError(f"Invalid env var (expected KEY=VALUE): {item!r}")
            k, v = item.split("=", 1)
            result[k] = v
    return result


def _read_script(script: Optional[str]) -> Optional[str]:
    """Read a script from file or stdin. Returns None if no script."""
    if script is None:
        return None
    if script == "-":
        return sys.stdin.read()
    with open(script) as f:
        return f.read()


def _parse_point(s: str) -> dict[str, int]:
    """Parse ``"dim=N,dim=N"`` into a coordinate dict."""
    result: dict[str, int] = {}
    for pair in s.split(","):
        k, v = pair.split("=", 1)
        result[k.strip()] = int(v.strip())
    return result


def _output_dir_for_job(job: "Any") -> tuple[str, str]:
    """Return ``(output_dir_on_workers, human_report)`` for a multi-rank exec.

    If the job has a gather mount with a string remote_mount_point, the output
    directory is placed inside the gathered path so files are accessible on
    the client via the FUSE mount.  Otherwise falls back to a temp dir in
    ``/tmp`` on the workers.
    """
    import uuid as _uuid

    run_id = _uuid.uuid4().hex[:8]
    entries = job._mounts._gather_entries
    if entries:
        entry = entries[0]
        remote = entry.remote_mount_point
        if isinstance(remote, str):
            output_dir = os.path.join(remote, "exec_outputs", run_id)
            local = entry.local_mount_point
            report = (
                f"Output → {local}/<rank>/exec_outputs/{run_id}/\n"
                f"  e.g. {local}/hosts_0/exec_outputs/{run_id}/stdout.txt"
            )
            return output_dir, report
    output_dir = f"/tmp/monarch_exec_{run_id}"
    return output_dir, f"Output → {output_dir}/ on each worker"


def exec_on_job(
    cmd: list[str],
    run_all: bool = False,
    mesh_name: Optional[str] = None,
    point_str: Optional[str] = None,
    env: Optional[list[str]] = None,
    workdir: Optional[str] = None,
    kill: bool = False,
    script: Optional[str] = None,
    per_host: Optional[dict[str, int]] = None,
) -> int:
    """Load the current job and execute a command on its workers.

    Targeting (mutually exclusive; default is ``--one``):
    - ``run_all``: all meshes, all ranks → output redirected to files
    - ``mesh_name``: named mesh, all ranks → output redirected to files
    - ``point_str``: ``"dim=N,dim=N"`` coordinate on first mesh → streamed
    - none of the above (``--one``): rank 0 of first mesh → streamed

    Returns the process exit code (max across targeted ranks).
    """
    from monarch._src.job.job import exec_command, load_job  # pyre-ignore[21]

    job = load_job()  # pyre-ignore[16]

    script_text = _read_script(script)
    if script_text is not None:
        cmd = ["bash", "-c", script_text]

    env_dict = _parse_env(env)

    state = job.state()
    if not state._hosts:
        raise RuntimeError("Job has no host meshes")

    # ── Targeting ──────────────────────────────────────────────────────────
    is_streaming = not run_all and mesh_name is None
    # (--point and --one are always single-rank → stream)

    if run_all:
        target_meshes = list(state._hosts.items())
        rank = None
        point = None
    elif mesh_name is not None:
        if mesh_name not in state._hosts:
            raise ValueError(
                f"Mesh {mesh_name!r} not found. Available: {list(state._hosts)}"
            )
        target_meshes = [(mesh_name, state._hosts[mesh_name])]
        rank = None
        point = None
    elif point_str is not None:
        point = _parse_point(point_str)
        first = next(iter(state._hosts))
        target_meshes = [(first, state._hosts[first])]
        rank = None
    else:  # --one (default)
        first = next(iter(state._hosts))
        target_meshes = [(first, state._hosts[first])]
        rank = 0
        point = None

    # ── Output dir (redirect) or stream ────────────────────────────────────
    output_dir: Optional[str]
    if is_streaming:
        output_dir = None
    else:
        output_dir, report = _output_dir_for_job(job)
        print(report)

    # ── Execute ────────────────────────────────────────────────────────────
    max_rc = 0
    last_mesh = None
    for name, host_mesh in target_meshes:
        last_mesh = host_mesh
        # If a python_exe was set for this mesh's remote mount, prepend its
        # directory to PATH so commands like "python" resolve to the right one.
        mesh_env = dict(env_dict)
        exe = job._python_executables.get(name, job._default_python_exe)
        if exe is not None:
            bin_dir = os.path.dirname(exe)
            existing_path = mesh_env.get("PATH", os.environ.get("PATH", ""))
            mesh_env["PATH"] = f"{bin_dir}:{existing_path}"
        rc = exec_command(  # pyre-ignore[16]
            host_mesh,
            cmd,
            env=mesh_env,
            workdir=workdir,
            output_dir=output_dir,
            rank=rank,
            point=point,
            per_host=per_host,
        )
        max_rc = max(max_rc, rc)

    if kill and last_mesh is not None:
        from monarch.actor import shutdown_context  # pyre-ignore[21]

        last_mesh.shutdown().get()
        job.kill()
        shutdown_context().get()  # pyre-ignore[16]

    return max_rc
