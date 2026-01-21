# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import logging
import os
import pickle
import shlex
import signal
import subprocess
import sys
import tempfile
from abc import ABC, abstractmethod
from typing import Dict, List, Literal, NamedTuple, Optional, Sequence

from monarch._src.actor.bootstrap import attach_to_workers

# note: the jobs api is intended as a library so it should
# only be importing _public_ monarch API functions.
from monarch.actor import enable_transport, HostMesh, this_host


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

    def __init__(self, hosts: Dict[str, HostMesh]):
        self._hosts = hosts

    def __getattr__(self, attr: str) -> HostMesh:
        try:
            return self._hosts[attr]
        except KeyError:
            raise AttributeError(attr)


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
        super().__init__()
        self._status: Literal["running", "not_running"] | CachedRunning = "not_running"

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
            self._status = "running"

    @property
    def active(self) -> bool:
        return self._running is not None

    def state(self, cached_path: Optional[str] = ".monarch/job_state.pkl") -> JobState:
        """
        Get the current state of this job, containing the host mesh objects of its requires that were requested
            host_meshes = self.state()
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
            return running_job._state()

        cached = self._load_cached(cached_path)
        if cached is not None:
            self._status = CachedRunning(cached)
            logger.info("Connecting to cached job")
            return cached._state()
        logger.info("Applying current job")
        self.apply()
        logger.info("Job has started, connecting to current state")
        result = self._state()
        if cached_path is not None:
            # Create the directory for cached_path if it doesn't exist
            cache_dir = os.path.dirname(cached_path)
            if cache_dir:  # Only create if there's a directory component
                os.makedirs(cache_dir, exist_ok=True)
            logger.info("Saving job to cache at %s", cached_path)
            self.dump(cached_path)
        return result

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
            os.remove(cached_path)
            return None
        return job

    def dump(self, filename: str):
        """
            Save job to a file. Helper to make it more apparent
        Jobs are serializable across processes.
        """
        with open(filename, "wb") as file:
            # @lint-ignore PYTHONPICKLEISBAD
            pickle.dump(self, file)

    def dumps(self) -> bytes:
        # @lint-ignore PYTHONPICKLEISBAD
        return pickle.dumps(self)

    def kill(self):
        running = self._running
        if running is not None:
            running._kill()
        self._status = "not_running"

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


def job_load(filename: str) -> JobTrait:
    """
    Load a job from a file.

    Args:
        filename: Path to the pickled job file, typically from :meth:`JobTrait.dump`.

    Returns:
        The deserialized job object.
    """
    with open(filename, "rb") as file:
        # @lint-ignore PYTHONPICKLEISBAD
        job: "JobTrait" = pickle.load(file)
        return job


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
