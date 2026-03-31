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
import shlex
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


# ---------------------------------------------------------------------------
# Job registry
# ---------------------------------------------------------------------------

DEFAULT_JOB_PATH: str = ".monarch/job_state.pkl"
DEFAULT_JOB_DIR: str = ".monarch/jobs"
CURRENT_FILE: str = ".monarch/current"


def _job_path(name: str) -> str:
    return os.path.join(DEFAULT_JOB_DIR, f"{name}.pkl")


def _active_job_name() -> Optional[str]:
    try:
        text = Path(CURRENT_FILE).read_text().strip()
        return text or None
    except FileNotFoundError:
        return None


def _unique_job_name(base: str) -> str:
    jobs_dir = Path(DEFAULT_JOB_DIR)
    if not (jobs_dir / f"{base}.pkl").exists():
        return base
    i = 1
    while (jobs_dir / f"{base}-{i}.pkl").exists():
        i += 1
    return f"{base}-{i}"


# ---------------------------------------------------------------------------
# serve_module / exec_on_job
# ---------------------------------------------------------------------------


def serve_module(
    module_path: str,
    name: Optional[str] = None,
    job_path: Optional[str] = None,
) -> None:
    """Import a user module, call its ``serve()`` function, and cache the job.

    The module must expose a ``serve()`` function that returns a
    :class:`~monarch.job.JobTrait`.

    Args:
        module_path: Dotted Python module path (e.g. ``jobs.mast``).
        name: Name for this job. Defaults to the last component of
            ``module_path``, made unique within ``.monarch/jobs/``.
        job_path: Override path for the default ``.monarch/job_state.pkl``.
    """
    import importlib
    import shutil

    from monarch._src.job.job import JobTrait  # pyre-ignore[21]

    # Ensure CWD is importable (like python -c / python -m do).
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    mod = importlib.import_module(module_path)
    serve_fn = getattr(mod, "serve", None)
    if serve_fn is None:
        raise AttributeError(f"Module '{module_path}' has no 'serve()' function")

    job = serve_fn()
    if not isinstance(job, JobTrait):  # pyre-ignore[16]
        raise TypeError(f"serve() must return a JobTrait, got {type(job).__name__}")

    if not job.active:
        job.apply()

    # Wait for workers to be connectable so the first exec doesn't
    # spend minutes waiting for allocation.
    _t_wait = time.time()
    _state = job.state(cached_path=job_path or DEFAULT_JOB_PATH)
    # Access a host mesh to verify workers are connectable.
    _mesh_name = next(iter(_state._hosts))
    _ = _state._hosts[_mesh_name]
    _wait_secs = time.time() - _t_wait
    print(f"Job is ready. Total wait time: {_wait_secs:.0f}s")

    # Derive a unique name if not provided.
    if name is None:
        base = module_path.rsplit(".", 1)[-1]
        name = _unique_job_name(base)

    # Write to named registry.
    Path(DEFAULT_JOB_DIR).mkdir(parents=True, exist_ok=True)
    named_path = _job_path(name)
    job.dump(named_path)

    # Update the active job pointer.
    Path(CURRENT_FILE).write_text(name)

    # Keep default path in sync.
    default = job_path or DEFAULT_JOB_PATH
    Path(default).parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(named_path, default)

    print(f"Job '{name}' cached to {named_path} (active)")


def _force_unmount(procs: Any, mount_point: str) -> None:
    """Force-unmount a FUSE mount, killing processes that hold it."""
    from monarch._src.job.job import BashActor  # pyre-ignore[21]

    script = f"""#!/bin/bash
# Kill processes using the mount
fuser -k {shlex.quote(mount_point)} 2>/dev/null || true
sleep 1
# Lazy unmount (detaches immediately)
fusermount3 -uz {shlex.quote(mount_point)} 2>/dev/null || \
    umount -l {shlex.quote(mount_point)} 2>/dev/null || true
echo "force-unmount done"
"""
    actors = procs.spawn("_ForceUnmount", BashActor)  # pyre-ignore[16]
    results = actors.run.call(script).get()
    for _rank, result in results:
        stdout = result.get("stdout", "")
        if stdout.strip():
            print(f"  {stdout.strip()}")


def _resolve_job_path(job_path: Optional[str]) -> str:
    """Resolve job path from explicit arg, active job registry, or default."""
    if job_path is not None:
        return job_path
    active = _active_job_name()
    if active:
        named = _job_path(active)
        if os.path.exists(named):
            return named
    return DEFAULT_JOB_PATH


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


def _build_bash_script(cmd: list[str], env_dict: dict[str, str], workdir: str) -> str:
    """Build a bash script string from command, env vars, and workdir."""
    lines: list[str] = ["#!/bin/bash", "set -e", "export PYTHONUNBUFFERED=1"]
    for k, v in env_dict.items():
        lines.append(f"export {k}={shlex.quote(v)}")
    lines.append(f"cd {shlex.quote(workdir)}")
    lines.append(shlex.join(cmd))
    return "\n".join(lines) + "\n"


def _host_to_ranks(procs: Any, host_indices: list[int]) -> list[int]:
    """Map host indices to flat rank 0 on each host."""
    mesh_sizes = procs.sizes
    dims = list(mesh_sizes.keys())
    if len(dims) == 1:
        return host_indices
    total = 1
    for s in mesh_sizes.values():
        total *= s
    host_dim = dims[0]
    n_hosts = mesh_sizes[host_dim]
    procs_per_host = total // n_hosts
    for h in host_indices:
        if h < 0 or h >= n_hosts:
            raise ValueError(f"Host index {h} out of range (0-{n_hosts - 1})")
    return [h * procs_per_host for h in host_indices]


def _resolve_ranks(
    procs: Any,
    run_all: bool,
    per_host: bool,
    ranks: Optional[list[int]],
    hosts: Optional[list[int]],
) -> Optional[list[int]]:
    """Resolve targeting flags into a list of flat rank indices.

    Returns None when run_all is True (all ranks, no filtering).
    """
    if run_all:
        return None
    if per_host:
        mesh_sizes = procs.sizes
        dims = list(mesh_sizes.keys())
        n_hosts = mesh_sizes[dims[0]]
        return _host_to_ranks(procs, list(range(n_hosts)))
    if hosts is not None:
        return _host_to_ranks(procs, hosts)
    return ranks if ranks is not None else [0]


def _exec_all(bash_actors: Any, bash_script: str, rm: Any) -> int:
    """Run on all ranks, write per-rank log files. Returns max exit code."""
    results = bash_actors.run.call(bash_script).get()
    rm.close()

    if len(results) > 1:
        from datetime import datetime

        job_name = _active_job_name() or "default"
        exec_id = datetime.now().strftime("%H%M%S-%f")
        log_dir = os.path.join(".monarch", "logs", job_name, f"exec-{exec_id}")
        os.makedirs(log_dir, exist_ok=True)
        for i, (_rank, result) in enumerate(results):
            Path(os.path.join(log_dir, f"rank{i}.stdout.log")).write_text(
                result.get("stdout", "")
            )
            Path(os.path.join(log_dir, f"rank{i}.stderr.log")).write_text(
                result.get("stderr", "")
            )
        print(f"Logs written to {log_dir}/")

    max_rc = 0
    for i, (_rank, result) in enumerate(results):
        rc = result.get("returncode", 1)
        if rc != 0:
            print(f"rank {i} exited with code {rc}")
        max_rc = max(max_rc, rc)
    return max_rc


def _exec_streaming(
    bash_actors: Any, bash_script: str, ranks: list[int], rm: Any
) -> int:
    """Run on targeted ranks with streaming output. Returns max exit code."""
    from monarch.actor import Channel  # pyre-ignore[21]

    port, recv = Channel[str].open()  # pyre-ignore[16]
    bash_actors.run_streaming.broadcast(bash_script, port, ranks)

    use_prefix = len(ranks) > 1
    done_ranks: set[int] = set()
    target_set = set(ranks)
    max_rc = 0

    for msg in iter(lambda: recv.recv().get(), None):
        if msg.startswith("skip:"):
            done_ranks.add(int(msg[5:]))
        else:
            parts = msg.split(":", 2)
            if len(parts) < 3:
                print(f"Warning: unexpected message: {msg!r}", file=sys.stderr)
                continue
            rank_str, tag, content = parts
            rank = int(rank_str)
            prefix = f"[rank {rank}] " if use_prefix else ""
            if tag == "rc":
                max_rc = max(max_rc, int(content))
                done_ranks.add(rank)
            elif tag == "out":
                sys.stdout.write(f"{prefix}{content}")
                sys.stdout.flush()
            elif tag == "err":
                sys.stderr.write(f"{prefix}{content}")
                sys.stderr.flush()

        if target_set.issubset(done_ranks):
            break

    rm.close()
    return max_rc


def exec_on_job(
    cmd: list[str],
    run_all: bool = False,
    per_host: bool = False,
    ranks: Optional[list[int]] = None,
    hosts: Optional[list[int]] = None,
    env: Optional[list[str]] = None,
    verbose: bool = False,
    job_path: Optional[str] = None,
    source_dir: Optional[str] = None,
    mount_point: Optional[str] = None,
    kill: bool = False,
    script: Optional[str] = None,
    refresh_mount: bool = False,
) -> int:
    """Load a cached job and execute a command on its workers.

    Returns the process exit code (max across targeted ranks).
    """
    from monarch._src.job.job import BashActor, job_load  # pyre-ignore[21]
    from monarch.remotemount.remotemount import (  # pyre-ignore[21]
        remotemount as _remotemount,
    )

    job_path = _resolve_job_path(job_path)
    job = job_load(job_path)  # pyre-ignore[16]
    state = job.state(cached_path=job_path)

    if not state._hosts:
        raise RuntimeError("Job has no host meshes")
    mesh_name = next(iter(state._hosts))
    host_mesh = state._hosts[mesh_name]

    if verbose:
        logging.getLogger("monarch.remotemount").setLevel(logging.DEBUG)

    if source_dir is None:
        source_dir = os.getcwd()
    source_dir = os.path.abspath(source_dir)

    script_text = _read_script(script)
    if script_text is not None:
        cmd = ["bash", "-c", script_text]

    env_dict = _parse_env(env)
    if "PYTHONPATH" not in env_dict:
        pp = job.python_path(source_dir, mntpoint=mount_point)
        if pp:
            env_dict["PYTHONPATH"] = pp

    # One spawn_procs shared between FUSE mount and BashActor.
    procs = host_mesh.spawn_procs()
    mntpoint = mount_point or source_dir
    rm = _remotemount(host_mesh, source_dir, mntpoint=mntpoint)  # pyre-ignore[16]

    if refresh_mount:
        _force_unmount(procs, mntpoint)

    _t0 = time.time()
    rm.open()
    _mount_secs = time.time() - _t0
    if _mount_secs > 0.1:
        print(f"Remote mount: {_mount_secs:.1f}s")

    resolved_ranks = _resolve_ranks(procs, run_all, per_host, ranks, hosts)
    bash_script = _build_bash_script(cmd, env_dict, mount_point or source_dir)
    bash_actors = procs.spawn("BashActor", BashActor)  # pyre-ignore[16]

    if resolved_ranks is None:
        max_rc = _exec_all(bash_actors, bash_script, rm)
    else:
        max_rc = _exec_streaming(bash_actors, bash_script, resolved_ranks, rm)

    if kill:
        from monarch.actor import shutdown_context  # pyre-ignore[21]

        host_mesh.shutdown().get()
        job.kill()
        shutdown_context().get()  # pyre-ignore[16]

    return max_rc
