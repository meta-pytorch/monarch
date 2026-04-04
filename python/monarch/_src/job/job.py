# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import contextlib
import io
import logging
import os
import pickle
import shlex
import signal
import subprocess
import sys
import tempfile
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, NamedTuple, Optional, Sequence

from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.actor.host_mesh import _spawn_admin
from monarch._src.job.mount_config import Mounts

# note: the jobs api is intended as a library so it should
# only be importing _public_ monarch API functions.
from monarch.actor import (
    Actor,
    current_rank,
    enable_transport,
    endpoint,
    HostMesh,
    Port,
    this_host,
)
from monarch.distributed_telemetry.actor import start_telemetry
from monarch.distributed_telemetry.engine import QueryEngine


class BashActor(Actor):
    """Actor that executes bash scripts on remote workers.

    Two execution modes:

    1. **Blocking** — ``run(script)`` runs the script to completion and
       returns ``{"returncode", "stdout", "stderr"}``.  Output is also
       printed on the worker for log-forwarding visibility.

    2. **Streaming** — ``start(script)`` launches the script in the
       background and returns immediately.  The client then calls
       ``poll_output()`` in a loop to receive incremental stdout/stderr
       until the process exits.
    """

    def _rank_env(self) -> Dict[str, str]:
        from monarch.actor import context

        rank = context().actor_instance.rank
        return {
            **{f"MONARCH_RANK_{k}": str(v) for k, v in dict(rank).items()},
            **{
                f"MONARCH_SIZE_{k}": str(v)
                for k, v in zip(rank.extent.keys(), rank.extent.sizes)
            },
        }

    def _expand_subdir(self, output_dir: str) -> str:
        if "$SUBDIR" not in output_dir:
            return output_dir
        from monarch.actor import context

        rank = context().actor_instance.rank
        subdir = "_".join(f"{k}_{v}" for k, v in dict(rank).items())
        return output_dir.replace("$SUBDIR", subdir)

    @endpoint
    def run(
        self,
        script: str,
        target_ranks: Optional[list] = None,
        output_dir: Optional[str] = None,
    ):
        my_rank = current_rank().rank
        if target_ranks is not None and my_rank not in target_ranks:
            return {"returncode": 0, "stdout": "", "stderr": "", "skipped": True}
        env = {**os.environ, **self._rank_env()}
        if output_dir is not None:
            output_dir = self._expand_subdir(output_dir)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=True) as f:
            f.write(script)
            f.flush()
            if output_dir is not None:
                os.makedirs(output_dir, exist_ok=True)
                with (
                    open(os.path.join(output_dir, "stdout.txt"), "w") as out,
                    open(os.path.join(output_dir, "stderr.txt"), "w") as err,
                ):
                    result = subprocess.run(
                        ["bash", f.name], stdout=out, stderr=err, env=env
                    )
                return {"returncode": result.returncode, "stdout": "", "stderr": ""}
            else:
                result = subprocess.run(
                    ["bash", f.name], capture_output=True, text=True, env=env
                )
                return {
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }

    @endpoint
    def run_python(
        self,
        py_file: Optional[str] = None,
        module: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        workdir: Optional[str] = None,
        client_cwd: Optional[str] = None,
        output_dir: Optional[str] = None,
    ):
        from unittest.mock import patch

        os.environ.update({**self._rank_env(), **(env or {})})

        if output_dir is not None:
            output_dir = self._expand_subdir(output_dir)

        effective_workdir = workdir or (
            client_cwd if client_cwd and os.path.isdir(client_cwd) else None
        )
        with (
            contextlib.chdir(effective_workdir)
            if effective_workdir
            else contextlib.nullcontext()
        ):
            if module is not None:
                import importlib.util

                spec = importlib.util.find_spec(module)
                assert spec is not None and spec.origin is not None
                py_file = spec.origin

            assert py_file is not None
            with open(py_file) as f:
                source = f.read()
            code = compile(source, py_file, "exec")

            if output_dir is not None:
                os.makedirs(output_dir, exist_ok=True)
                out_f: Any = open(os.path.join(output_dir, "stdout.txt"), "w")
                err_f: Any = open(os.path.join(output_dir, "stderr.txt"), "w")
            else:
                out_f = io.StringIO()
                err_f = io.StringIO()

            with (
                out_f,
                err_f,
                patch.object(sys, "argv", [py_file]),
                contextlib.redirect_stdout(out_f),
                contextlib.redirect_stderr(err_f),
            ):
                exec(code, {"__name__": "__main__", "__file__": py_file})
                return {
                    "returncode": 0,
                    "stdout": out_f.getvalue() if output_dir is None else "",
                    "stderr": err_f.getvalue() if output_dir is None else "",
                }

    @endpoint
    def run_streaming(
        self,
        script: str,
        output_port: Port[str],
        target_ranks: list,
    ):
        """Run *script* on targeted ranks, streaming output via *output_port*.

        Only actors whose ``current_rank().rank`` is in *target_ranks*
        execute the script.  Non-targeted actors send ``"skip:<rank>"``
        and return immediately.

        Each message sent through the port is a tagged line:
        ``"out:<line>"`` for stdout, ``"err:<line>"`` for stderr,
        ``"rc:<code>"`` on exit, and ``"skip:<rank>"`` for skipped ranks.

        Args:
            script: Bash script text to execute.
            output_port: A :class:`Port` obtained from
                ``Channel[str].open()``.  Each line of output is pushed
                through this port as it is produced.
            target_ranks: List of flat rank indices that should run the
                script.
        """
        my_rank = current_rank().rank

        if my_rank not in target_ranks:
            output_port.send(f"skip:{my_rank}")
            return

        env = {**os.environ, **self._rank_env()}

        import selectors

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=True) as f:
            f.write(script)
            f.flush()
            proc = subprocess.Popen(
                ["bash", f.name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )
            stdout = proc.stdout
            stderr = proc.stderr
            assert stdout is not None
            assert stderr is not None
            sel = selectors.DefaultSelector()
            sel.register(stdout, selectors.EVENT_READ, "stdout")
            sel.register(stderr, selectors.EVENT_READ, "stderr")
            while sel.get_map():
                for key, _ in sel.select():
                    # pyre-ignore[16]: fileobj is IO[str] here, not int
                    line = key.fileobj.readline()
                    if not line:
                        sel.unregister(key.fileobj)
                        continue
                    tag = "out" if key.data == "stdout" else "err"
                    output_port.send(f"{my_rank}:{tag}:{line}")
            sel.close()
            proc.wait()
        output_port.send(f"{my_rank}:rc:{proc.returncode}")


@dataclass
class TelemetryConfig:
    """Configuration for automatic telemetry startup.

    When passed to a job constructor, telemetry (and optionally a dashboard)
    is started automatically when ``state()`` is called.

    Args:
        batch_size: Number of rows to buffer before flushing to a RecordBatch.
        retention_secs: Retention window in seconds for message tables.
            0 disables retention.
        include_dashboard: Whether to start the monarch dashboard web server.
        dashboard_port: Preferred port for the dashboard.
    """

    batch_size: int = 1000
    retention_secs: int = 600
    include_dashboard: bool = False
    dashboard_port: int = 8265


@dataclass
class MeshAdminConfig:
    """Configuration for automatic mesh admin agent startup.

    When passed to a job constructor, a MeshAdminAgent HTTP server is
    spawned automatically when ``state()`` is called.  The server
    aggregates topology across all host meshes and exposes it via a
    REST API that the admin TUI can attach to.

    Args:
        admin_addr: Bind address for the admin HTTP server.  When
            ``None`` the server picks an available address automatically.
    """

    admin_addr: Optional[str] = None


class JobState:
    """
    Container for the current state of a job.

    Provides access to the HostMesh objects for each mesh requested in the job
    specification. Each mesh is accessible as an attribute.

    Example::

        state = job.state()
        state.trainers    # HostMesh for the "trainers" mesh
        state.dataloaders # HostMesh for the "dataloaders" mesh
    """

    def __init__(
        self,
        hosts: Dict[str, HostMesh],
        query_engine: Optional[QueryEngine] = None,
        telemetry_url: Optional[str] = None,
        admin_url: Optional[str] = None,
    ):
        self._hosts = hosts
        self.query_engine = query_engine
        self.telemetry_url = telemetry_url
        self.admin_url = admin_url

    def __getattr__(self, attr: str) -> HostMesh:
        try:
            return self._hosts[attr]
        except KeyError:
            available = ", ".join(sorted(self._hosts.keys()))
            raise AttributeError(
                f"'{attr}' is not a valid host mesh name. Available names: {available}"
            )


class CachedRunning(NamedTuple):
    job: "JobTrait"


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stderr))
logger.propagate = False


class JobTrait(ABC):
    """
    A job object represents a specification and set of machines that can be
    used to create monarch HostMeshes and run actors on them.

    A job object comprises a declarative specification for the job and
    optionally the job's *state*. The ``apply()`` operation applies the job's
    specification to the scheduler, creating or updating the job as
    required. If the job exists and there are no changes in its
    specification, ``apply()`` is a no-op. Once applied, we can query the
    job's *state*. The state of the job contains the set of hosts currently
    allocated, arranged into the requested host meshes. Conceptually, the
    state can be retrieved directly from the scheduler, but we may also
    cache snapshots of the state locally.

    The state is the interface to the job consumed by Monarch: Monarch
    bootstraps host meshes from the state alone, and is not concerned with
    any other aspect of the job.

    Conceptually, dynamic jobs (e.g., to enable consistently fast restarts,
    elasticity, etc.) can simply poll the state for changes. In practice,
    notification mechanisms would be developed so that polling isn't
    required. The model allows for late resolution of some parts of the
    job's *specification*. For example, a job that does not specify a
    name may instead resolve the name on the first ``apply()``. In this way,
    jobs can also be "templates". But the model also supports having the job
    refer to a *specific instance* by including the resolved job name in the
    specification itself.

    Note:
        Subclasses must NOT set ``_status`` directly. The ``state()`` method
        manages status transitions and pickle caching. If a subclass
        pre-emptively sets ``_status = "running"``, the ``state()`` method
        will skip the cache dump, breaking job persistence. Instead, let
        ``apply()`` set the status after ``_create()`` returns.
    """

    def __init__(self):
        # WARNING: Do NOT add configuration arguments here.
        # JobTrait.__init__ must remain argument-free so subclass constructors
        # stay orthogonal to cross-cutting concerns like telemetry and admin.
        # Use enable_telemetry() / enable_admin() after construction instead.
        super().__init__()
        self._status: Literal["running", "not_running"] | CachedRunning = "not_running"
        self._telemetry: Optional[TelemetryConfig] = None
        self._mesh_admin: Optional[MeshAdminConfig] = None
        self._query_engine: Optional[QueryEngine] = None
        self._telemetry_url: Optional[str] = None
        self._admin_url: Optional[str] = None
        self._apply_id: Optional[str] = None
        self._mounts: Mounts = Mounts()
        # Per-mesh python executable overrides.  None key means "all meshes".
        self._python_executables: Dict[str, str] = {}
        self._default_python_exe: Optional[str] = None

    def _start_telemetry_if_configured(self) -> None:
        """Start telemetry if configured and not already running."""
        if self._telemetry is None or self._query_engine is not None:
            return

        cfg = self._telemetry
        self._query_engine, self._telemetry_url = start_telemetry(
            batch_size=cfg.batch_size,
            retention_secs=cfg.retention_secs,
            include_dashboard=cfg.include_dashboard,
            dashboard_port=cfg.dashboard_port,
        )

    def _start_admin_if_configured(self, host_meshes: List[HostMesh]) -> None:
        """Start the mesh admin agent if configured and not already running."""
        if self._mesh_admin is None or self._admin_url is not None:
            return

        self._admin_url = _spawn_admin(
            host_meshes,
            admin_addr=self._mesh_admin.admin_addr,
            telemetry_url=self._telemetry_url,
        ).get()

    def _wrap_state(self, job_state: JobState) -> JobState:
        """Attach telemetry and admin fields to a JobState."""
        if self._query_engine is not None:
            job_state.query_engine = self._query_engine
            job_state.telemetry_url = self._telemetry_url
        if self._admin_url is not None:
            job_state.admin_url = self._admin_url
        return job_state

    def enable_telemetry(
        self, config: "Optional[TelemetryConfig]" = None, **kwargs
    ) -> "JobTrait":
        """Configure automatic telemetry startup on the next :meth:`state` call.

        Args:
            config: A :class:`TelemetryConfig` instance.  If omitted, one is
                constructed from *kwargs* (forwarded to ``TelemetryConfig``).

        Returns:
            ``self``, for chaining.
        """
        self._telemetry = config if config is not None else TelemetryConfig(**kwargs)
        return self

    def enable_admin(
        self, config: "Optional[MeshAdminConfig]" = None, **kwargs
    ) -> "JobTrait":
        """Configure automatic mesh admin agent startup on the next :meth:`state` call.

        Args:
            config: A :class:`MeshAdminConfig` instance.  If omitted, one is
                constructed from *kwargs*.

        Returns:
            ``self``, for chaining.
        """
        self._mesh_admin = config if config is not None else MeshAdminConfig(**kwargs)
        return self

    @property
    def _running(self) -> "Optional[JobTrait]":
        match self._status:
            case "not_running":
                return None
            case "running":
                return self
            case CachedRunning(job=job):
                return job

    def apply(self, client_script: Optional[str] = None):
        """
        Request the job as specified is brought into existence or modified to the current specification/
        The worker machines launched in the job should call run_worker_forever to join the job.

        Calling apply when the job as specified has already been applied is a no-op.

        If client_script is not None, then creating the job arranges for the job to run train.py as the client.

        Implementation note: To batch launch the job, we will first write .monarch/job_state.pkl with a Job
        that instructs the client to connect to the job that it is running in.
        Then we will schedule the job including that .monarch/job_state.pkl.
        When the client calls `.state()`, it will find the .monarch/job_state.pkl and connect to it.
        """
        if self._running is None:
            self._create(client_script)
            self._apply_id = str(uuid.uuid4())
            self._status = "running"

    @property
    def apply_id(self) -> Optional[str]:
        """A UUID identifying the current allocation of this job.

        Generated fresh each time :meth:`apply` creates a new allocation.
        ``None`` if the job has not been applied yet. When a job is loaded
        from a cached file, the original ``apply_id`` is preserved.
        """
        running = self._running
        return running._apply_id if running is not None else None

    @property
    def active(self) -> bool:
        return self._running is not None

    def _connect(
        self, cached_path: Optional[str] = ".monarch/job_state.pkl"
    ) -> "JobState":
        """
        Get the current state of this job, containing the host mesh objects of its requires that were requested
            host_meshes = self._connect()
            # properties of state hold the requested host meshes:

            host_meshes.trainers
            host_meshes.dataloaders
            This is a dictionary so that meshes can hold different machine types.

            cached_path: if cached_path is not None and the job has yet to be applied,
            we will first check `cached_path` for an existing created job state.
            If it exists  and `saved_job.can_run(self)`, we will connect to the cached job.
            Otherwise, we will apply this job and connect to it, saving the job in `cached_path` if it is not None.


        Raises: JobExpiredException - when the job has finished and this connection cannot be made.
        """
        # this is implemented uniquely for each scheduler, but it will ultimately make
        # calls to attach_to_workers and return the HostMeshes
        running_job = self._running
        if running_job is not None:
            logger.info("Job is running, returning current state")
            job_state = running_job._state()
            self._start_telemetry_if_configured()
            self._start_admin_if_configured(list(job_state._hosts.values()))
            return self._wrap_state(job_state)

        cached = self._load_cached(cached_path)
        if cached is not None:
            self._status = CachedRunning(cached)
            logger.info("Connecting to cached job")
            job_state = cached._state()
            self._start_telemetry_if_configured()
            self._start_admin_if_configured(list(job_state._hosts.values()))
            return self._wrap_state(job_state)
        logger.info("Applying current job")
        self.apply()
        logger.info("Job has started, connecting to current state")
        job_state = self._state()
        self._start_telemetry_if_configured()
        self._start_admin_if_configured(list(job_state._hosts.values()))
        result = self._wrap_state(job_state)
        if cached_path is not None:
            # Create the directory for cached_path if it doesn't exist
            cache_dir = os.path.dirname(cached_path)
            if cache_dir:  # Only create if there's a directory component
                os.makedirs(cache_dir, exist_ok=True)
            logger.info("Saving job to cache at %s", cached_path)
            self.dump(cached_path)
        return result

    def state(
        self, cached_path: Optional[str] = ".monarch/job_state.pkl"
    ) -> "JobState":
        """Connect to the job and return its state with all configured mounts applied.

        See :meth:`_connect` for the connection logic. After connecting, all
        mount configs registered via :meth:`remote_mount` and gather mount
        configs registered via :meth:`gather_mount` are applied before returning.
        """
        raw = self._connect(cached_path)
        apply_id = self.apply_id
        running = self._running
        if apply_id is not None and running is not None:
            self._mounts.ensure_open(running)
        hosts = {}
        for mesh_name, mesh in raw._hosts.items():
            exe = self._python_executables.get(mesh_name, self._default_python_exe)
            if exe is not None:
                mesh = mesh.with_python_executable(exe)
            hosts[mesh_name] = mesh
        return self._wrap_state(JobState(hosts))

    def _load_cached(self, cached_path: Optional[str]) -> "Optional[JobTrait]":
        if cached_path is None:
            logger.info("No cached path provided")
            return None
        try:
            job = job_load(cached_path)
            logger.info("Found cached job at path: %s", cached_path)
        except FileNotFoundError:
            logger.info("No cached job found at path: %s", cached_path)
            return None
        running = job._running
        if running is None:
            logger.info("Cached job is not running")
            return None
        if not running.can_run(self):
            logger.info("Cached job cannot run this spec, removing cache")
            try:
                running._kill()
            except NotImplementedError as e:
                logger.info("Failed to kill cached job: %s", e)
            # Remove the actual state file, not the symlink, so the context
            # (symlink) remains intact for future applies.
            state_file = (
                os.path.realpath(cached_path)
                if os.path.islink(cached_path)
                else cached_path
            )
            try:
                os.remove(state_file)
            except FileNotFoundError:
                pass
            return None
        return job

    def __getstate__(self):
        state = self.__dict__.copy()
        # QueryEngine holds Rust bindings / network connections and is not
        # picklable.  Drop it so deserialized jobs re-initialize telemetry
        # on the next state() call.
        state["_query_engine"] = None
        state["_telemetry_url"] = None
        state["_admin_url"] = None
        return state

    def dump(self, filename: str) -> None:
        """Save job to a file, following any symlink at *filename*.

        If *filename* is a symlink, writes to the symlink target rather than
        replacing the link itself.  Creates the target's parent directory if
        it does not yet exist.
        """
        path = os.path.realpath(filename) if os.path.islink(filename) else filename
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "wb") as f:
            # @lint-ignore PYTHONPICKLEISBAD
            pickle.dump(self, f)

    def dumps(self) -> bytes:
        # @lint-ignore PYTHONPICKLEISBAD
        return pickle.dumps(self)

    def kill(self):
        if self._apply_id is not None:
            self._mounts.ensure_stopped(self._apply_id)
        running = self._running
        if running is not None:
            running._kill()
        self._status = "not_running"

    def remote_mount(
        self,
        source: str,
        mntpoint: Optional[str] = None,
        meshes: Optional[List[str]] = None,
        python_exe: Optional[str] = ".venv/bin/python",
        **kwargs: Any,
    ) -> None:
        """Declare a local directory to be mounted on workers via FUSE.

        This is configuration-only — no mount is established immediately.
        The mount is applied (and re-applied on reconnect) on the next call
        to :meth:`state`.

        Args:
            source: Local directory path to mount.
            mntpoint: Mount point on workers. Defaults to ``source``.
            meshes: Names of meshes to mount on. ``None`` means all meshes
                returned by :meth:`state`.
            python_exe: Path to the Python executable relative to the mount
                point, used to set ``python_executable`` on the returned mesh.
                Set to ``None`` to skip. Defaults to ``".venv/bin/python"``.
            **kwargs: Forwarded to :func:`remotemount`.
        """
        self._mounts.remote_mount(
            source=source, mntpoint=mntpoint, meshes=meshes, **kwargs
        )
        if python_exe is not None:
            abs_source = os.path.abspath(source)
            local_exe = os.path.join(abs_source, python_exe)
            if not os.path.isfile(local_exe):
                raise ValueError(
                    f"python_exe '{python_exe}' not found locally at '{local_exe}'. "
                    f"Ensure the virtual environment exists in '{source}' before calling remote_mount."
                )
            abs_mntpoint = (
                os.path.abspath(mntpoint) if mntpoint is not None else abs_source
            )
            exe_path = os.path.join(abs_mntpoint, python_exe)
            if meshes is None:
                self._default_python_exe = exe_path
            else:
                for mesh_name in meshes:
                    self._python_executables[mesh_name] = exe_path

    def gather_mount(
        self,
        remote_mount_point: str,
        local_mount_point: str,
        meshes: Optional[List[str]] = None,
    ) -> None:
        """Declare a remote directory to be mounted locally via gather mount.

        This is configuration-only — no mount is established immediately.
        The mount is applied (and re-applied on reconnect) on the next call
        to :meth:`state`.

        Args:
            remote_mount_point: Path on workers to expose. The token
                ``$SUBDIR`` is replaced with each host's mesh-coordinate key
                (e.g. ``hosts_0``).
            local_mount_point: Local path where the remote directory will be
                mounted.
            meshes: Names of meshes to gather from. ``None`` means all meshes
                returned by :meth:`state`.
        """
        self._mounts.gather_mount(
            remote_mount_point=remote_mount_point,
            local_mount_point=local_mount_point,
            meshes=meshes,
        )

    @abstractmethod
    def _state(self) -> JobState: ...

    @abstractmethod
    def _create(self, client_script: Optional[str]):
        """Create the job resources.

        Called by `apply()` when the job is not yet running. Implementations
        should schedule the job with the appropriate scheduler but must NOT
        set `_status` directly; `apply()` handles status transitions after
        this method returns.
        """
        ...

    @abstractmethod
    def can_run(self, spec: "JobTrait") -> bool:
        """
        Is this job capable of running the job spec? This is used to check if a
        cached job can be used to run `spec` instead of creating a new reserveration.

        It is also used by the batch run infrastructure to indicate that the batch job can certainly run itself.
        """

        ...

    @abstractmethod
    def _kill(self):
        """
        Stop the job/reservation.
        """
        ...


def job_loads(data: bytes) -> JobTrait:
    """
    Deserialize a job from bytes.

    Args:
        data: Pickled job bytes, typically from :meth:`JobTrait.dumps`.

    Returns:
        The deserialized job object.
    """
    # @lint-ignore PYTHONPICKLEISBAD
    return pickle.loads(data)


DEFAULT_JOB_PATH: str = ".monarch/job_state.pkl"


def job_load(filename: str = DEFAULT_JOB_PATH) -> JobTrait:
    """
    Load a job from a file.

    Args:
        filename: Path to the pickled job file, typically from :meth:`JobTrait.dump`.
            Defaults to ``.monarch/job_state.pkl``.

    Returns:
        The deserialized job object.
    """
    with open(filename, "rb") as file:
        # @lint-ignore PYTHONPICKLEISBAD
        job: "JobTrait" = pickle.load(file)
        return job


_MONARCH_DIR: str = ".monarch"
_CONTEXT_STATE_FILE: str = "state.pkl"
_CONTEXT_SPEC_FILE: str = "spec"


def _current_spec_file() -> Path:
    """Return the spec file path for the current context.

    Reads the symlink at ``.monarch/job_state.pkl`` to determine which context
    is active.  Falls back to ``default/spec`` when the symlink does not exist.
    """
    link = Path(DEFAULT_JOB_PATH)
    if link.is_symlink():
        target = Path(os.readlink(str(link)))
        return Path(_MONARCH_DIR) / target.parent / _CONTEXT_SPEC_FILE
    return Path(_MONARCH_DIR) / "default" / _CONTEXT_SPEC_FILE


def _import_job_from_spec(module_path: str) -> JobTrait:
    """Import and return the ``JobTrait`` at the dotted *module_path*.

    Args:
        module_path: Dotted import path of the form ``module.attr``
            (e.g. ``myjob.job``).

    Raises:
        ValueError: if *module_path* does not contain a ``'.'``.
        AttributeError: if the named attribute does not exist in the module.
        TypeError: if the attribute is not a :class:`JobTrait`.
    """
    import importlib

    if "." not in module_path:
        raise ValueError(f"module_path must be 'module.attr', got {module_path!r}")
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    mod_name, attr_name = module_path.rsplit(".", 1)
    mod = importlib.import_module(mod_name)
    job = getattr(mod, attr_name, None)
    if job is None:
        raise AttributeError(f"Module '{mod_name}' has no '{attr_name}' attribute")
    if not isinstance(job, JobTrait):
        raise TypeError(
            f"'{mod_name}.{attr_name}' must be a JobTrait, got {type(job).__name__}"
        )
    return job


def set_current_job(module_path: str) -> None:
    """Save *module_path* as the spec for the current context.

    Ensures ``.monarch/`` exists and that ``job_state.pkl`` is a symlink
    pointing to the active context's ``state.pkl`` (sets up the ``default``
    context and symlink on first run; migrates a legacy plain-file
    ``job_state.pkl`` to ``default/state.pkl`` for backward compatibility).
    """
    link = Path(DEFAULT_JOB_PATH)
    Path(_MONARCH_DIR).mkdir(parents=True, exist_ok=True)
    if not link.is_symlink():
        default_dir = Path(_MONARCH_DIR) / "default"
        default_dir.mkdir(parents=True, exist_ok=True)
        if link.exists():
            # Migrate legacy plain file into the default context.
            link.rename(default_dir / _CONTEXT_STATE_FILE)
        link.symlink_to(Path("default") / _CONTEXT_STATE_FILE)
    spec_file = _current_spec_file()
    spec_file.parent.mkdir(parents=True, exist_ok=True)
    spec_file.write_text(module_path)


def load_current_job() -> JobTrait:
    """Return a fresh job object for the current context's spec.

    Reads the dotted module path from the current context's ``spec`` file and
    imports it via :func:`_import_job_from_spec`.  The returned object is a
    plain spec — not yet connected to any workers.  Call ``.state()`` on it
    to connect (or apply) the job; that call may load the cached
    ``state.pkl`` if it is still valid.

    Raises:
        FileNotFoundError: if no spec file exists in the current context.
    """
    spec_file = _current_spec_file()
    try:
        module_path = spec_file.read_text().strip()
    except FileNotFoundError:
        raise FileNotFoundError(
            f"No spec found at {spec_file}. Run 'monarch apply <module.path>' first."
        ) from None
    return _import_job_from_spec(module_path)


def exec_command(
    host_mesh: HostMesh,
    cmd: List[str],
    env: Optional[Dict[str, str]] = None,
    workdir: Optional[str] = None,
    output_dir: Optional[str] = None,
    rank: Optional[int] = None,
    point: Optional[Dict[str, int]] = None,
    per_host: Optional[Dict[str, int]] = None,
) -> int:
    """Run a command on *host_mesh* via BashActor.

    Args:
        host_mesh: The HostMesh to execute on.
        cmd: Command and arguments.
        env: Extra environment variables.
        workdir: Working directory on workers.
        output_dir: If set, redirect stdout/stderr to files in this directory
            on each worker (``stdout.txt`` / ``stderr.txt``).  If ``None``,
            stream stdout/stderr to the caller's terminal.
        rank: Flat rank to execute on (applied after ``flatten("rank")``).
            ``None`` executes on all ranks.
        point: Coordinate dict to slice the process mesh (e.g.
            ``{"host": 4, "gpu": 3}``).  ``None`` executes on all ranks.
            Mutually exclusive with *rank*.
        per_host: If set, spawn multiple processes per host with the given
            dimension sizes (e.g. ``{"gpu": 4}``).  Passed as ``per_host``
            to :meth:`~monarch.actor.HostMesh.spawn_procs`.

    Returns:
        Maximum return code across all ranks (0 = success).
    """
    procs = (
        host_mesh.spawn_procs(per_host=per_host)
        if per_host
        else host_mesh.spawn_procs()
    )
    if point is not None:
        procs = procs.slice(**point)
    elif rank is not None:
        procs = procs.flatten("rank").slice(rank=rank)

    bash_actors = procs.spawn("BashActor", BashActor)

    client_cwd = os.getcwd()

    if cmd[0].endswith(".py") or cmd[0] == "-m":
        results = bash_actors.run_python.call(
            py_file=cmd[0] if cmd[0].endswith(".py") else None,
            module=cmd[1] if cmd[0] == "-m" else None,
            env=env,
            workdir=workdir,
            client_cwd=client_cwd,
            output_dir=output_dir,
        ).get()
    else:
        lines: List[str] = ["#!/bin/bash"]
        if env:
            for k, v in env.items():
                lines.append(f"export {k}={shlex.quote(v)}")
        if workdir:
            lines.append(f"cd {shlex.quote(workdir)}")
        elif client_cwd:
            lines.append(
                f"[ -d {shlex.quote(client_cwd)} ] && cd {shlex.quote(client_cwd)}"
            )
        lines.append(shlex.join(cmd))
        script = "\n".join(lines) + "\n"
        results = bash_actors.run.call(script, output_dir=output_dir).get()
    max_rc = 0
    for _rank_key, result in results:
        rc = result.get("returncode", 1)
        max_rc = max(max_rc, rc)
        if output_dir is None:
            stdout = result.get("stdout", "")
            stderr = result.get("stderr", "")
            if stdout:
                print(stdout, end="")
            if stderr:
                print(stderr, end="", file=sys.stderr)
    return max_rc


class LocalJob(JobTrait):
    """
    Job that runs on the local host.

    This job calls ``this_host()`` for each host mesh requested. It serves as a
    stand-in in configuration so a job can be switched between remote and local
    execution by changing the job configuration.
    """

    def __init__(self, hosts: Sequence["str"] = ("hosts",)):
        """
        Args:
            hosts: Names of the host meshes to create.
        """
        self._host_names = hosts
        # if launched with client_script, the proc corresponding to the
        # locally running client, and the log_dir it is writing to.
        self._proc: Optional[subprocess.Popen] = None
        self._log_dir: Optional[str] = None
        super().__init__()

    def _kill(self):
        pass

    def can_run(self, spec: "JobTrait"):
        """
        Local jobs are the same regardless of what was saved, so just
        use the spec, which has the correct 'hosts' sequence.
        """
        return False

    def _state(self) -> JobState:
        return JobState({k: this_host() for k in self._host_names})

    def _create(self, client_script: Optional[str]):
        if client_script is None:
            return  # noop, because LocalJob always 'exists'

        b = BatchJob(self)
        b.dump(".monarch/job_state.pkl")

        log_dir = self._setup_log_directory()
        self._run_client_as_daemon(client_script, log_dir)

        logger.info(
            "Started client script %s with PID: %d", client_script, self.process.pid
        )
        logger.info("Logs available at: %s", log_dir)

    def _setup_log_directory(self) -> str:
        """Create a log directory for the batch job."""
        log_base_dir = ".monarch/logs"
        os.makedirs(log_base_dir, exist_ok=True)
        # Create a unique subdirectory for this job run
        self._log_dir = tempfile.mkdtemp(prefix="job_", dir=log_base_dir)
        return self._log_dir

    def _run_client_as_daemon(self, client_script: str, log_dir: str) -> None:
        """
        Run the client script as a daemon process.

        Args:
            client_script: Path to the client script to run
            log_dir: Directory to store log files

        Returns:
            The process ID of the daemon
        """
        # Prepare log files
        stdout_log = os.path.join(log_dir, "stdout.log")
        stderr_log = os.path.join(log_dir, "stderr.log")

        # Create environment with MONARCH_BATCH_JOB=1
        env = os.environ.copy()
        env["MONARCH_BATCH_JOB"] = "1"

        # Open log files
        with open(stdout_log, "w") as stdout_file, open(stderr_log, "w") as stderr_file:
            # Start the process with Python interpreter
            self._proc = subprocess.Popen(
                [sys.executable, client_script],
                env=env,
                stdout=stdout_file,
                stderr=stderr_file,
                # Detach the process from parent (daemonize)
                start_new_session=True,
            )

    @property
    def process(self):
        if self._proc is None:
            raise ValueError("no local batch job")
        return self._proc


class BatchJob(JobTrait):
    """
    Wrapper that can be put around other job traits to make
    make it always load from the job_state.pkl when MONARCH_BATCH_JOB
    is set.
    """

    def __init__(self, job: JobTrait):
        super().__init__()
        # Copy telemetry/admin config from the wrapped job so the batch
        # client benefits from the same observability setup.
        self._telemetry = job._telemetry
        self._mesh_admin = job._mesh_admin
        self._job = job

    def can_run(self, spec: JobTrait):
        if "MONARCH_BATCH_JOB" in os.environ:
            import atexit

            atexit.register(self._kill)
            return True
        return False

    @property
    def _running(self) -> Optional[JobTrait]:
        return self

    def _state(self):
        return self._job._state()

    def _create(self, client_script: Optional[str] = None):
        return self._job._create(client_script)

    def _kill(self):
        logger.info("Stopping Batch Job")
        return self._job._kill()


class ProcessState(NamedTuple):
    pid: int
    channel: str


class LoginJob(JobTrait):
    """
    Makes a connections directly to hosts via an explicit list.
    """

    def __init__(self):
        super().__init__()
        self._meshes: Dict[str, List[str]] = {}
        self._host_to_pid: Dict[str, ProcessState] = {}

    def add_mesh(self, name: str, hosts: List[str]):
        self._meshes[name] = hosts

    def _state(self) -> JobState:
        if not self._pids_active():
            raise RuntimeError("lost connection")
        hosts = {
            name: attach_to_workers(
                name=name,
                ca="trust_all_connections",
                workers=[self._host_to_pid[v].channel for v in values],
            )
            for name, values in self._meshes.items()
        }
        return JobState(hosts)

    def _create(self, client_script: Optional[str]):
        if client_script is not None:
            raise RuntimeError("LoginJob cannot run batch-mode scripts")

        for hosts in self._meshes.values():
            for host in hosts:
                self._host_to_pid[host] = self._start_host(host)

    @abstractmethod
    def _start_host(self, host: str) -> ProcessState: ...

    def can_run(self, spec: "JobTrait") -> bool:
        """
        Is this job capable of running the job spec? This is used to check if a
        cached job can be used to run `spec` instead of creating a new reserveration.

        It is also used by the batch run infrastructure to indicate that the batch job can certainly run itself.
        """
        return (
            isinstance(spec, LoginJob)
            and spec._meshes == self._meshes
            and self._pids_active()
        )

    def _pids_active(self) -> bool:
        if not self.active:
            return False
        for _, p in self._host_to_pid.items():
            try:
                # Check if process exists by sending signal 0
                os.kill(p.pid, 0)
            except OSError:
                # Process doesn't exist or we don't have permission to signal it
                return False
        return True

    def _kill(self):
        for p in self._host_to_pid.values():
            try:
                os.kill(p.pid, signal.SIGKILL)
            except OSError:
                pass


class SSHJob(LoginJob):
    def __init__(
        self,
        python_exe: str = "python",
        ssh_args: Sequence[str] = (),
        monarch_port: int = 22222,
    ):
        enable_transport("tcp")
        self._python_exe = python_exe
        self._ssh_args = ssh_args
        self._port = monarch_port
        super().__init__()

    def _start_host(self, host: str) -> ProcessState:
        addr = f"tcp://{host}:{self._port}"
        startup = f'from monarch.actor import run_worker_loop_forever; run_worker_loop_forever(address={repr(addr)}, ca="trust_all_connections")'

        command = f"{shlex.quote(self._python_exe)} -c {shlex.quote(startup)}"
        proc = subprocess.Popen(
            ["ssh", *self._ssh_args, host, "-n", command],
            start_new_session=True,
        )
        return ProcessState(proc.pid, addr)

    def can_run(self, spec):
        return (
            isinstance(spec, SSHJob)
            and spec._python_exe == self._python_exe
            and self._port == spec._port
            and self._ssh_args == spec._ssh_args
            and super().can_run(spec)
        )
