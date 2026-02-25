# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import io
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time
import traceback
from typing import Callable, Optional, Sequence, Tuple

from . import Context, Host
from .host import main
from .logging import gethostname, initialize_logging
from .python_executable import PYTHON_EXECUTABLE

# Default port leveraging one from the reserved range for torchelastic
PORT: str = os.environ.get("SUPERVISOR_PORT", "29401")

logger: logging.Logger = logging.getLogger(__name__)


NON_RETRYABLE_FAILURE: int = 100
JOB_RESTART_SCOPE_ESCALATION: int = 101
TW_USER_METADATA_HOSTNAMES_LIST_KEY: str = "TW_USER_METADATA_HOSTNAMES_LIST_KEY"
TW_USER_METADATA_FILE_PATH: str = "TW_USER_METADATA_FILE_PATH"


def _write_reply_file(msg: str, reply_file: Optional[str] = None) -> None:
    if reply_file is None:
        reply_file = os.environ["MAST_HPC_TASK_FAILURE_REPLY_FILE"]
    job_attempt = int(os.environ["MAST_HPC_JOB_ATTEMPT_INDEX"])
    logger.info(
        f"Supervisor writing a reply file with JOB_RESTART_SCOPE_ESCALATION to {reply_file} (attempt {job_attempt})."
    )
    with open(reply_file, "w") as f:
        timestamp_ns = time.time_ns()
        error_data = {
            "message": msg,
            "errorCode": JOB_RESTART_SCOPE_ESCALATION,
            "timestamp": int(timestamp_ns // 1e9),
            "timestamp_us": int(timestamp_ns // 1e3),
        }
        json.dump(error_data, f)


def mast(supervise: Callable[[Context, Sequence[Host]], None]) -> None:
    """
    This function is the entrypoint for starting the supervisor when
    running on MAST. Each host should call `mast(supervise)` where
    `supervise` is the supervisor policy function for the job.
    Supervisor will be called only on the supervisor machine with
    `supervisor(n_hosts_in_task, port)` where `n_hosts_in_task` is
    the number of hosts reserved in the task group, and `port` is the
    port that supervisor should listen on.

    The supervise function can then create a supervisor Context object,
    request up to n_hosts_in_tasks hosts, and then
    """

    hostnames = get_hostnames()
    N = len(hostnames)
    my_host_name = (os.environ.get("HOSTNAME") or socket.getfqdn()).removesuffix(
        ".facebook.com"
    )
    # Get first host in the task group
    is_supervisor = my_host_name == hostnames[0]
    initialize_logging(
        "supervisor" if is_supervisor else f"{gethostname()} host-manager"
    )

    supervisor_addr = f"tcp://{socket.getfqdn(hostnames[0])}:{PORT}"
    logger.info(
        "hostname %s, supervisor host is %s, supervisor=%s",
        my_host_name,
        hostnames[0],
        is_supervisor,
    )

    if is_supervisor:
        _write_reply_file(
            "Supervisor deadman's switch. "
            "This reply file is written when the supervisor starts and deleted right before a successful exit. "
            "It is used to cause the whole job to restart if for some reason the "
            "supervisor host is unscheduled without it throwing an exception itself."
        )
        # local host manager on supervisor machine
        host_process = subprocess.Popen(
            [PYTHON_EXECUTABLE, "-m", "monarch_supervisor.host", supervisor_addr]
        )
        try:
            ctx = Context(port=int(PORT))
            hosts: Tuple[Host, ...] = ctx.request_hosts(n=N)
            supervise(ctx, hosts)
            ctx.shutdown()
            logger.info("Supervisor shutdown complete.")
        except BaseException:
            ty, e, st = sys.exc_info()
            s = io.StringIO()
            traceback.print_tb(st, file=s)
            _write_reply_file(
                f"{ty.__name__ if ty is not None else 'None'}: {str(e)}\n{s.getvalue()}"
            )
            host_process.send_signal(signal.SIGINT)
            raise
        return_code = host_process.wait(timeout=10)
        if return_code != 0:
            # Host manager may have been instructed to write a reply file, so
            # we do not write a reply file here which would clobber it.
            logger.warning(
                f"Host manager on supervisor returned non-zero code: {return_code}."
            )
            sys.exit(return_code)
        else:
            # successful exit, so we remove the deadman's switch reply file we wrote earlier.
            reply_file = os.environ["MAST_HPC_TASK_FAILURE_REPLY_FILE"]
            os.unlink(reply_file)
    else:
        # host manager on non-supervisor machine
        main(supervisor_addr)


def get_hostnames() -> Sequence[str]:
    """
    Get the list of hostnames for the current task group.
    """
    tw_metatdata_file = os.environ.get(TW_USER_METADATA_FILE_PATH)
    hostnames_key = os.environ.get(TW_USER_METADATA_HOSTNAMES_LIST_KEY)
    if tw_metatdata_file and hostnames_key:
        with open(tw_metatdata_file, "r") as f:
            data = json.load(f)
            hostnames_str = data["userAttributes"][hostnames_key]
            return hostnames_str.split(",")

    return os.environ["MAST_HPC_TASK_GROUP_HOSTNAMES"].split(",")
