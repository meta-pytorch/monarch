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


class UnhandledFaultHookException(Exception):
    """Wraps exceptions raised by the unhandled fault hook.

    When `unhandled_fault_hook` raises (e.g. `sys.exit`), the
    `RootClientActor` catches the exception, logs it, and re-raises
    it wrapped in this type. The Rust root-client message loop
    recognises this wrapper and skips re-dispatching the supervision
    event, which would otherwise call `__supervise__` a second time.
    """


def unhandled_fault_hook(failure: MeshFailure) -> None:
    """When a supervision event is unhandled and is propagated back to the client,
    this hook is called.
    The default implementation is to exit the process with error code 1
    after logging the event.
    If this function raises any exception (including BaseException classes such
    as SystemExit from sys.exit), this fault is considered unhandled.
    Any normal return value will cause the fault to be dropped. Logs will be
    written containing the failure message in either case.

    To customize this behavior, overwrite this function in your client code like so:
    ```
    import monarch.actor

    def my_unhandled_fault_hook(failure: MeshFailure) -> None:
        # log it, add metrics, etc.
        print(f"Mesh failure was not handled: {failure}")
        # To ignore this error, return any value (including None) without an exception.


    monarch.actor.unhandled_fault_hook = my_unhandled_fault_hook
    ```

    If the fault is unhandled, it exits the main thread by delivering a KeyboardInterrupt.
    This is done because the Python Interpreter can only be finalized to run
    destructors and atexit hooks from the main thread. So if you see a
    "KeyboardInterrupt" happening that you didn't send, it's because there was
    an unhandled fault.
    """
    sys.exit(1)
