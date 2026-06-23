#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""SimpleMastJob: a minimal MAST-backed JobTrait built around the ``mast`` CLI.

Vendored here so the torchtitan_mast example is self-contained. ``job.py``
imports ``SimpleMastJob`` from this package. It builds (or reuses) a slim
monarch bootstrap fbpkg via ``build_bootstrap``, schedules ``hosts`` h100
workers via ``launch_mast``, caches the resolved hostnames at apply time
(reused on later ``state()`` calls -- goes stale on MAST preemption, see the
note in ``_state``), and connects via ``attach_to_workers`` over MetaTLS.
"""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
import time
from typing import Optional

import monarch.actor
from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch._src.actor.bootstrap import attach_to_workers
from monarch._src.job.job import JobState, JobTrait
from monarch.config import configure
from simple_mast_job.build_bootstrap import build_bootstrap, launch_mast

_WORKER_PORT = 26600
_PENDING_STATES = {"PENDING", "STARTING", "ALLOCATED", "QUEUED"}
_RUNNING_STATE = "RUNNING"
_POLL_INTERVAL_SECS = 5

# The per-job mount sidecar is spawned as a bare ``python -m ...`` and never
# calls enable_transport(), so it would default to the un-dialable Unix
# transport. Enable metatls-hostname at import so every interpreter that
# imports this module agrees on the transport. (enable_transport with the
# same transport is a no-op.)
monarch.actor.enable_transport("metatls-hostname")


def _task_sort_key(task_id: str) -> tuple[int, str]:
    match = re.search(r"(\d+)$", task_id)
    if match is None:
        return (0, task_id)
    return (int(match.group(1)), task_id)


class SimpleMastJob(JobTrait):
    """A small example-local MAST backend built around ``mast`` CLI calls."""

    def __init__(
        self,
        *,
        hosts: int = 1,
        env: Optional[dict[str, str]] = None,
    ) -> None:
        super().__init__()
        self._num_hosts = hosts
        self._env = dict(env or {})
        self._mast_job_name: Optional[str] = None
        self._package_version: Optional[str] = None
        # Resolved once at apply time and reused (pickled into
        # ``.monarch/job_state.pkl``) so later ``monarch exec`` invocations skip
        # the ~520ms ``mast get-status`` roundtrip. Goes stale on MAST
        # preemption -- see the note in ``_state``.
        self._hostnames: Optional[list[str]] = None

    @property
    def mast_job_name(self) -> Optional[str]:
        return self._mast_job_name

    @staticmethod
    def from_job_name(mast_job_name: str) -> SimpleMastJob:
        raise NotImplementedError("attach-to-running-job is not supported")

    def _get_status(self) -> dict:
        assert self._mast_job_name is not None, "Job has not been created yet"
        cmd = ["mast", "get-status", "--output", "json", self._mast_job_name]
        last_exc: Exception | None = None
        for _ in range(5):
            try:
                output = subprocess.check_output(cmd, text=True)
                return json.loads(output)["data"]
            except (
                subprocess.CalledProcessError,
                json.JSONDecodeError,
                KeyError,
            ) as exc:
                # ``mast get-status`` occasionally exits non-zero or returns a
                # payload missing the ``data`` envelope mid-poll; retry rather
                # than abort -- the next poll almost always returns clean.
                last_exc = exc
                time.sleep(2)
        raise RuntimeError(f"mast get-status failed 5x: {last_exc!r}")

    def _ready_hostnames_from_status(self, status: dict) -> list[str] | None:
        hostnames = []
        latest = status.get("latestAttempt", {})
        tg_attempts = latest.get("taskGroupExecutionAttempts", {})
        if not tg_attempts:
            return None
        for tg_list in tg_attempts.values():
            for tg in tg_list:
                expected = tg.get("numTasks", 0)
                task_map = tg.get("taskExecutionAttempts", {})
                if len(task_map) < expected:
                    return None
                for task_id in sorted(task_map, key=_task_sort_key):
                    task_attempts = task_map[task_id]
                    if not task_attempts:
                        return None
                    attempt = task_attempts[-1]
                    if attempt.get("state") != _RUNNING_STATE:
                        return None
                    hostname = attempt.get("hostname")
                    if not hostname:
                        return None
                    hostnames.append(hostname)
        return hostnames if hostnames else None

    def _fetch_failure_logs(self, status: dict) -> str:
        log_dir = tempfile.mkdtemp(prefix=f"monarch_failure_{self._mast_job_name}_")
        latest = status.get("latestAttempt", {})
        tg_attempts = latest.get("taskGroupExecutionAttempts", {})
        for tg_list in tg_attempts.values():
            for tg in tg_list:
                tw_handle = tg.get("twJobHandle", "")
                if not tw_handle:
                    continue
                num_tasks = tg.get("numTasks", 1)
                for task_id in range(num_tasks):
                    task_handle = f"{tw_handle}/{task_id}"
                    log_path = f"{log_dir}/task_{task_id}_dedicated_log_monarch.log"
                    try:
                        with open(log_path, "w") as log_file:
                            subprocess.run(
                                [
                                    "tw",
                                    "log",
                                    task_handle,
                                    "--file",
                                    "dedicated_log_monarch.log",
                                ],
                                stdout=log_file,
                                stderr=subprocess.STDOUT,
                                timeout=60,
                            )
                    except Exception as exc:
                        with open(log_path, "w") as log_file:
                            log_file.write(f"Failed to fetch logs: {exc}\n")
        return log_dir

    def _create(self, client_script: Optional[str]) -> None:
        if client_script is not None:
            raise RuntimeError("SimpleMastJob cannot run batch-mode scripts")
        if self._mast_job_name is not None:
            return
        identifier = build_bootstrap()
        package_name, self._package_version = identifier.split(":", 1)
        self._mast_job_name = launch_mast(
            package_name=package_name,
            package_version=self._package_version,
            hosts=self._num_hosts,
            env=self._env,
        )
        # Block until every task is RUNNING and capture hostnames once.
        self._hostnames = self._wait_for_hostnames()

    def can_run(self, spec: JobTrait) -> bool:
        if not isinstance(spec, SimpleMastJob):
            return False
        return spec._num_hosts == self._num_hosts and spec._env == self._env

    def _state(self) -> JobState:
        configure(default_transport=ChannelTransport.MetaTlsWithHostname)

        # Hostnames are cached at apply time and pickled into
        # ``.monarch/job_state.pkl``; later ``monarch exec`` invocations reuse
        # them and skip the ~520ms ``mast get-status`` subprocess.
        #
        # TRADEOFF: if MAST preempts and re-places the job's tasks, the cached
        # hostnames go stale and the next endpoint call surfaces a connect / TLS
        # timeout rather than a clean "preempted" error. We accept this because
        # per-call ``mast get-status`` cost ~1s of every exec; an upcoming
        # supervision-channel liveness signal restores clean preempt-detection
        # for free. ``getattr`` lets old pickles re-resolve on first call.
        hostnames = getattr(self, "_hostnames", None)
        if hostnames is None:
            hostnames = self._wait_for_hostnames()
            self._hostnames = hostnames

        workers = [
            f"metatls://{hostname}.facebook.com:{_WORKER_PORT}"
            for hostname in hostnames
        ]
        print(f"Connecting to workers: {workers}")
        return JobState(
            {
                "workers": attach_to_workers(
                    name="workers",
                    ca="trust_all_connections",
                    workers=workers,
                )
            }
        )

    def _wait_for_hostnames(self) -> list[str]:
        """Poll ``mast get-status`` until all tasks are RUNNING, then return
        their current hostnames."""
        while True:
            status = self._get_status()
            state = status.get("state", "UNKNOWN")
            if state not in _PENDING_STATES and state != _RUNNING_STATE:
                log_dir = self._fetch_failure_logs(status)
                print(f"Failure logs saved to: {log_dir}")
                raise RuntimeError(
                    f"MAST job {self._mast_job_name} entered terminal state: {state}. "
                    f"Logs saved to: {log_dir}"
                )
            hostnames = self._ready_hostnames_from_status(status)
            if hostnames is not None:
                return hostnames
            print(
                f"Job {self._mast_job_name} is {state}, "
                "waiting for all tasks to be RUNNING..."
            )
            time.sleep(_POLL_INTERVAL_SECS)

    def _kill(self) -> None:
        if self._mast_job_name is not None:
            subprocess.check_call(
                [
                    "mast",
                    "kill",
                    "--comment",
                    "killed by SimpleMastJob",
                    self._mast_job_name,
                ]
            )
