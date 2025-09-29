# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import pickle
import subprocess
import sys
import tempfile
from abc import ABC, abstractmethod
from typing import Dict, Optional, Sequence

# note: the jobs api is intended as a library so it should
# only be importing _public_ monarch API functions.
from monarch._src.actor.host_mesh import HostMesh, this_host


class JobState:
    """
    Job State

    Currently this just has a property for each HostMesh.
    """

    def __init__(self, hosts: Dict[str, HostMesh]):
        self._hosts = hosts

    def __getattr__(self, attr: str) -> HostMesh:
        try:
            return self._hosts[attr]
        except KeyError:
            raise AttributeError(attr)


class JobTrait(ABC):
    def __init__(self):
        super().__init__()
        self._created = False

    """
    A job object represents a specification and set of machines that can be
    used to create monarch HostMeshes and run actors on them.

    A job object comprises a declarative specification for the job and
    optionally the job's *state*. The `apply()` operation applies the job's
    specification to the scheduler, creating or updating the job as
    required. If the job exists and there are no changes in its
    specification, `apply()` is a no-op. Once applied, we can query the
    job's *state*. The state of the job contains the set of hosts currently
    allocated, arranged into the requested host meshes. Conceptually, the
    state can be retrieved directly from the scheduler, but we may also
    cache snapshots of the state locally.

    The state is the interface to the job consumed by Monarch: Monarch
    bootstraps host meshes from the state alone, and is not concerned with
    any other aspect of the job.

    Conceptually, dynamic jobs (e.g., to enable consistently fast restarts,
    elasticity, etc.), Monarch can simply poll the state for changes. In
    practice we'd develop notification mechanisms so that polling isn't
    jobs can also be "templates". But the model also supports having the job
    job's *specification* is. The model allows for late resolution of some
    parts of the specification. For example, a job that does not specify a
    name may instead resolve the name on the first `apply()`. In this way,
    jobs can also be "templates". But the model also supports having the job
    refer to a *specific instance* by including the resolved job name in the
    specification itself.
    """

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
        if not self._created:
            self._create(client_script)
            self._created = True

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
        if self._created:
            job = self
        else:
            job = self._load_cached(cached_path)
            if job is None:
                job = self
                self.apply()
                if cached_path is not None:
                    # Create the directory for cached_path if it doesn't exist
                    cache_dir = os.path.dirname(cached_path)
                    if cache_dir:  # Only create if there's a directory component
                        os.makedirs(cache_dir, exist_ok=True)
                    self.dump(cached_path)

        return job._state()

    def _load_cached(self, cached_path: Optional[str]) -> "Optional[JobTrait]":
        if cached_path is None:
            return None
        try:
            job = job_load(cached_path)
        except FileNotFoundError:
            return None
        if not job.can_run(self):
            return None
        return job

    def dump(self, filename: str):
        """
            Save job to a file. Helper to make it more apparent
        Jobs are serializable across processes.
        """
        # Ensure the directory exists
        directory = os.path.dirname(filename)
        if directory:  # Only create if there's a directory component
            os.makedirs(directory, exist_ok=True)

        with open(filename, "wb") as file:
            # @lint-ignore PYTHONPICKLEISBAD
            pickle.dump(self, file)

    def dumps(self) -> bytes:
        # @lint-ignore PYTHONPICKLEISBAD
        return pickle.dumps(self)

    @abstractmethod
    def _state(self) -> JobState: ...

    @abstractmethod
    def _create(self, client_script: Optional[str]): ...

    @abstractmethod
    def can_run(self, spec: "JobTrait") -> bool:
        """
        Is this job capable of running the job spec? This is used to check if a
        cached job can be used to run `spec` instead of creating a new reserveration.

        It is also used by the batch run infrastructure to indicate that the batch job can certainly run itself.
        """

        ...

    @abstractmethod
    def kill(self):
        """
        Stop the job/reservation.
        """
        ...


def job_loads(data: bytes) -> JobTrait:
    # @lint-ignore PYTHONPICKLEISBAD
    return pickle.loads(data)


def job_load(filename: str) -> JobTrait:
    with open(filename, "rb") as file:
        # @lint-ignore PYTHONPICKLEISBAD
        job: "JobTrait" = pickle.load(file)
        return job


class LocalJob(JobTrait):
    def __init__(self, hosts: Sequence["str"] = ("hosts",)):
        """
        Job that is just running on the local host.
        This job will just call this_host() for each host mesh requested.
        It is used as standin in config so a job can be configured to either use
        a remote or local job just by changing the job configuration.
        """
        self._host_names = hosts
        # if launched with client_script, the proc corresponding to the
        # locally running client, and the log_dir it is writing to.
        self._proc: Optional[subprocess.Popen] = None
        self._log_dir: Optional[str] = None
        super().__init__()

    def kill(self):
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

        b = _BatchLocalJob(self._host_names)
        b.dump(".monarch/job_state.pkl")

        log_dir = self._setup_log_directory()
        self._run_client_as_daemon(client_script, log_dir)

        print(f"Started client script {client_script} with PID: {self.process.pid}")
        print(f"Logs available at: {log_dir}")

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


class _BatchTrait:
    """
    Mixin that can be written to .monarch/job_state.pkl so that a batch job
    always load its connection information from job_state.pkl.
    """

    def can_run(self, spec: JobTrait):
        if os.environ.get("MONARCH_BATCH_JOB", None) == "1":
            return True
        return False


class _BatchLocalJob(_BatchTrait, LocalJob):
    pass
