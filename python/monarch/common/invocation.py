# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import traceback
from typing import Any, List, Optional, Tuple

from monarch._rust_bindings.monarch_hyperactor.proc import (  # @manual=//monarch/monarch_extension:monarch_extension
    ActorAddr,
)


Seq = int


class DeviceException(Exception):
    """
    Non-deterministic failure in the underlying worker, controller or its infrastructure.
    For example, a worker may enter a crash loop, or its GPU may be lost
    """

    def __init__(
        self,
        exception: Exception,
        frames: List[traceback.FrameSummary],
        source_actor_id: ActorAddr,
        message: str,
    ):
        self.exception = exception
        self.frames = frames
        self.source_actor_id = source_actor_id
        self.message = message

    def __str__(self):
        try:
            exe = str(self.exception)
            worker_tb = "".join(traceback.format_list(self.frames))
            return (
                f"{self.message}\n"
                f"Traceback of the failure on worker (most recent call last):\n{worker_tb}{type(self.exception).__name__}: {exe}"
            )
        except Exception as e:
            print(e)
            return "oops"


class RemoteException(Exception):
    """
    Deterministic problem with the user's code.
    For example, an OOM resulting in trying to allocate too much GPU memory, or violating
    some invariant enforced by the various APIs.
    """

    def __init__(
        self,
        seq: Seq,
        exception: Exception,
        controller_frame_index: Optional[int],
        controller_frames: Optional[List[traceback.FrameSummary]],
        worker_frames: List[traceback.FrameSummary],
        source_actor_id: ActorAddr,
        message="A remote function has failed asynchronously.",
    ):
        self.exception = exception
        self.worker_frames = worker_frames
        self.message = message
        self.seq = seq
        self.controller_frame_index = controller_frame_index
        self.source_actor_id = source_actor_id
        self.controller_frames = controller_frames

    def __str__(self):
        try:
            exe = str(self.exception)
            worker_tb = "".join(traceback.format_list(self.worker_frames))
            controller_tb = (
                "".join(traceback.format_list(self.controller_frames))
                if self.controller_frames is not None
                else "  <not related to a specific invocation>\n"
            )
            return (
                f"{self.message}\n"
                f"Traceback of where the remote function was issued on controller (most recent call last):\n{controller_tb}"
                f"Traceback of where the remote function failed on worker (most recent call last):\n{worker_tb}{type(self.exception).__name__}: {exe}"
            )
        except Exception as e:
            print(e)
            return "oops"


class Invocation:
    def __init__(self, seq: Seq):
        self.seq = seq
        self.users: Optional[set["Invocation"]] = set()
        self.failure: Optional[RemoteException] = None
        self.fut_value: Any = None

    def __repr__(self):
        return f"<Invocation {self.seq}>"

    def fail(self, remote_exception: RemoteException):
        if self.failure is None or self.failure.seq > remote_exception.seq:
            self.failure = remote_exception
            return True
        return False

    def add_user(self, r: "Invocation"):
        if self.users is not None:
            self.users.add(r)
        if self.failure is not None:
            r.fail(self.failure)

    def complete(self) -> Tuple[Any, Optional[RemoteException]]:
        """
        Complete the current invocation.
        Return the result and exception tuple.
        """
        # after completion we no longer need to inform users of failures
        # since they will just immediately get the value during add_user
        self.users = None

        return (self.fut_value if self.failure is None else None, self.failure)
