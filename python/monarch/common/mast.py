# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

# TODO: This can all be replaced using cleanrer MAST python library.
#       See https://www.internalfb.com/wiki/Components_in_AI/MAST/References/MAST_API_Reference/Read_APIs

import json
import logging
import subprocess
import time
from datetime import datetime

logger = logging.getLogger(__name__)


def _job_definition(jobname):
    return json.loads(
        subprocess.check_output(["mast", "get-job-definition", jobname, "--json"])
    )


def _job_status(jobname):
    return json.loads(
        subprocess.check_output(["mast", "get-status", jobname, "--json"])
    )


def _user_jobs(jobname=None):
    lines = []
    command = ["mast", "list-jobs", "--my", "--json"]
    if jobname is not None:
        command.append(["--job-name", jobname])
    for line in subprocess.check_output(command).split(b"\n"):
        if line:
            lines.append(json.loads(line))
    return lines


class MastJob:
    def __init__(self, name, default_task_group=None):
        self._name = name
        self._def = None
        self._status = None
        self._details = None
        self._twjob_specs = None
        self._default_task_group = default_task_group

    def _get_task_group(self, task_group):
        if task_group is None:
            task_group = self._default_task_group
        if task_group is None:
            raise ValueError("No default task group set and none specified")
        return task_group

    def _get_status(self, force_reload=False):
        if self._status is None or force_reload:
            self._status = _job_status(self._name)
        return self._status

    def _get_definitions(self):
        if self._def is None:
            self._def = _job_definition(self._name)
        return self._def

    def _get_definition(self, task_group=None):
        task_group = self._get_task_group(task_group)
        for d in self._get_definitions()["hpcTaskGroups"]:
            if d["name"] == task_group:
                return d
        raise ValueError(f"Task group {task_group} not found in job definition")

    def _get_details(self):
        if self._details is None:
            self._details = _user_jobs(self._name)
        return self._details

    def _get_twjob_specs(self, task_group=None):
        task_group = self._get_task_group(task_group)
        handle = self.get_twjob_handle(task_group)
        if self._twjob_specs is None:
            self._twjob_specs = json.loads(
                subprocess.check_output(["tw", "print", handle, "--json"])
            )[handle]
        return self._twjob_specs

    def name(self):
        return self._name

    def is_running(self):
        status = self._get_status(force_reload=True)
        if status["state"] != "RUNNING":
            return False
        else:
            for task_group in status["latestAttempt"][
                "taskGroupExecutionAttempts"
            ].values():
                if task_group[-1]["state"] != "RUNNING":
                    return False
            return True

    def get_arguments(self, task_group=None):
        return self._get_definition(task_group)["spec"]["arguments"]

    def get_task_count(self, task_group=None):
        return self._get_definition(task_group)["taskCount"]

    def uses_nfs(self, task_group=None):
        return "nfs" in self._get_definition(task_group)["spec"]["command"]

    def wait_for_running(self, timeout, task_group=None):
        start_time = datetime.now()
        while True:
            status = self._get_status(force_reload=True)
            if status["state"] == "RUNNING":
                app_state = self._get_status()["latestAttempt"][
                    "taskGroupExecutionAttempts"
                ][self._get_task_group(task_group)][0]["state"]
                if app_state == "RUNNING":
                    break
                logger.warning(
                    f"waiting for mast job {self.name()} to start, current worker state: {app_state}"
                )
            else:
                logger.warning(
                    f"waiting for mast job {self.name()} to start, current state: {status['state']}"
                )

            if (datetime.now() - start_time).total_seconds() > timeout:
                raise TimeoutError(
                    f"Timed out waiting for {self.name()} to start running."
                )
            time.sleep(10)

    def get_port(self, task_group=None):
        args = self._get_definition(task_group)["spec"]["arguments"]
        try:
            return int(args[3].removeprefix("tcp://").split(":")[1])
        except Exception as e:
            raise RuntimeError(
                f"Failed to parse endpoint from mast job {self._name}. "
                f"Invalid args in job definition: {' '.join(args)}. "
                f"Expected format: -mmonarch.notebook worker --endpoint tcp://<hostname>:<port>"
            ) from e

    def get_create_time(self):
        return self._get_details()["createdTimestamp"]

    def get_start_time(self):
        return self._get_status()["latestAttempt"]["jobStateTransitionTimestampSecs"][
            "PENDING"
        ]

    def get_num_hosts(self, task_group=None):
        return self._get_definition(task_group)["taskCount"]

    def get_gpus_per_host(self, task_group=None):
        return self._get_definition(task_group)["spec"]["resourceLimit"]["compute"][
            "gpu"
        ]

    def get_twjob_handle(self, task_group=None):
        return self._get_status()["latestAttempt"]["taskGroupExecutionAttempts"][
            self._get_task_group(task_group)
        ][0]["twJobHandle"]

    def get_hostnames(self, task_group=None):
        return self._get_twjob_specs(task_group)["envVariables"][
            "MAST_HPC_TASK_GROUP_HOSTNAMES"
        ].split(",")

    def _get_job_spec_env(self, task_group=None):
        return self._get_definition(task_group)["spec"]["env"]

    def get_nfs_home_dir(self, task_group=None):
        return self._get_job_spec_env(task_group).get("MONARCH_NFS_HOME_DIR")

    def get_oilfs_home_dir(self, task_group=None):
        return self._get_job_spec_env(task_group).get("MONARCH_OILFS_HOME_DIR")

    def get_nfs_workspace_dir(self, task_group=None):
        return (
            self._get_job_spec_env(task_group).get("WORKSPACE_DIR")
            if self.get_nfs_home_dir(task_group) is not None
            else None
        )

    def get_oilfs_workspace_dir(self, task_group=None):
        return (
            self._get_job_spec_env(task_group).get("WORKSPACE_DIR")
            if self.get_oilfs_home_dir(task_group) is not None
            else None
        )

    def __repr__(self):
        job = {}
        job["name"] = self._name
        job["latest_attempt_start_time"] = str(
            datetime.fromtimestamp(self.get_start_time())
        )
        job["hosts"] = self.get_num_hosts()
        job["gpus_per_host"] = self.get_gpus_per_host()
        status = self._get_status()
        job["job_state"] = status["state"]
        job["task_states"] = {
            task_group_name: task_group_states[-1]["state"]
            for task_group_name, task_group_states in status["latestAttempt"][
                "taskGroupExecutionAttempts"
            ].items()
        }
        return json.dumps(job, indent=2)


def mast_get_jobs(default_task_group=None):
    jobs = []
    for job in _user_jobs():
        mast_job = MastJob(job["hpcJobName"], default_task_group)
        jobs.append(mast_job)
    return sorted(jobs, key=lambda j: j.get_start_time(), reverse=True)
