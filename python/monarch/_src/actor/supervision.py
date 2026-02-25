# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import socket
import sys
from datetime import datetime

from monarch._rust_bindings.monarch_hyperactor.supervision import MeshFailure


def unhandled_fault_hook(failure: MeshFailure) -> None:
    """When a supervision event is unhandled and is propagated back to the client,
    this hook is called.
    The default implementation is to exit the process with error code 1
    after logging the event.
    If this function raises any exception (including BaseException classes such
    as SystemExit), the client process will exit. Any normal return value will
    cause the fault to be dropped. Logs will be written containing the failure
    message in either case.
    To customize this behavior, overwrite this function in your client code like so:
    ```
    import monarch.actor

    def my_unhandled_fault_hook(failure: MeshFailure) -> None:
        # log it, add metrics, etc.
        print(f"Mesh failure was not handled: {failure}")
        # To ignore this error, return any value (including None) without an exception.


    monarch.actor.unhandled_fault_hook = my_unhandled_fault_hook
    ```
    """
    from monarch._rust_bindings.monarch_hyperactor.telemetry import instant_event

    pid = os.getpid()
    hostname = socket.gethostname()
    message = (
        f"Unhandled monarch error on the root actor, hostname={hostname}, "
        f"PID={pid} at time {datetime.now()}: {failure.report()}\n"
    )
    # use stderr, not a logger because loggers are sometimes set
    # not print anything (e.g. in pytest)
    sys.stderr.write(message)
    sys.stderr.flush()
    # In addition to writing to stderr, log the event to telemetry.
    instant_event(message)
    sys.exit(1)
