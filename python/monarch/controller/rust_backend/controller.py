# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import traceback
from collections import deque
from logging import Logger
from typing import List, NamedTuple, Optional, Sequence, Union

from monarch._rust_bindings.monarch_extension import (
    client,
    controller,
    debugger,
    tensor_worker,
)
from monarch._rust_bindings.monarch_extension.client import (  # @manual=//monarch/monarch_extension:monarch_extension
    ClientActor,
)
from monarch._rust_bindings.monarch_hyperactor.proc import (  # @manual=//monarch/monarch_extension:monarch_extension
    ActorId,
    Proc,
)
from monarch._rust_bindings.monarch_messages.debugger import DebuggerAction
from monarch._src.actor.shape import NDSlice
from monarch.common.controller_api import LogMessage, MessageResult
from monarch.common.device_mesh import no_mesh
from monarch.common.invocation import DeviceException, RemoteException
from monarch.common.tensor import Tensor
from monarch.controller.debugger import read as debugger_read, write as debugger_write
from pyre_extensions import none_throws

logger: Logger = logging.getLogger(__name__)


class RustController:
    def __init__(
        self,
        proc: Proc,
        client_actor: ClientActor,
        controller_id: ActorId,
        worker_world_name: str,
    ) -> None:
        self._controller_actor = controller_id
        self._proc = proc
        self._actor = client_actor
        # Attach the client to the controller
        # Errors will be raised if someone else has attached it already.
        self._actor.attach(self._controller_actor)
        self._worker_world_name = worker_world_name

        # Buffer for messages unrelated to debugging that are received while a
        # debugger session is active.
        self._non_debugger_pending_messages: deque[
            Optional[client.LogMessage | client.WorkerResponse]
        ] = deque()
        self._pending_debugger_sessions: deque[ActorId] = deque()

    def send(
        self,
        ranks: Union[NDSlice, List[NDSlice]],
        msg: NamedTuple,
    ) -> None:
        self._actor.send_obj(self._controller_actor, ranks, msg)

    def drop_refs(self, refs: Sequence[tensor_worker.Ref]) -> None:
        self._actor.drop_refs(self._controller_actor, list(refs))

    def node(
        self,
        seq: int,
        defs: Sequence["Tensor"],
        uses: Sequence["Tensor"],
    ) -> None:
        node = controller.Node(
            seq=seq,
            defs=[tensor_worker.Ref(id=t.ref) for t in defs if t.ref is not None],
            uses=[tensor_worker.Ref(id=t.ref) for t in uses if t.ref is not None],
        )

        self._actor.send(self._controller_actor, node.serialize())

    def next_message(
        self, timeout: Optional[float]
    ) -> Optional[LogMessage | MessageResult]:
        if self._non_debugger_pending_messages:
            msg = self._non_debugger_pending_messages.popleft()
        else:
            msg = self._actor.get_next_message(
                timeout_msec=int((timeout or 0.0) * 1000.0)
            )
        if msg is None:
            return None

        if isinstance(msg, client.WorkerResponse):
            return _worker_response_to_result(msg)
        elif isinstance(msg, client.LogMessage):
            return LogMessage(msg.level, msg.message)
        elif isinstance(msg, client.DebuggerMessage):
            self._run_debugger_loop(msg)

    def stop_mesh(self) -> None:
        logger.info("rust controller stopping the system")
        self._actor.stop_worlds(
            [self._controller_actor.world_name, self._worker_world_name]
        )

    def drain_and_stop(
        self,
    ) -> List[LogMessage | MessageResult | client.DebuggerMessage]:
        logger.info("rust controller shutting down")
        results = []
        for msg in self._actor.drain_and_stop():
            if isinstance(msg, client.WorkerResponse):
                results.append(_worker_response_to_result(msg))
            elif isinstance(msg, client.LogMessage):
                results.append(LogMessage(msg.level, msg.message))
            elif isinstance(msg, client.DebuggerMessage):
                results.append(msg)
            else:
                raise RuntimeError(f"Unexpected message type {type(msg)}")
        return results

    def _run_debugger_loop(self, message: client.DebuggerMessage) -> None:
        if not isinstance(message.action, DebuggerAction.Paused):
            raise RuntimeError(
                f"Unexpected debugger message {message} when no debugger session is running"
            )

        self._pending_debugger_sessions.append(message.debugger_actor_id)
        while self._pending_debugger_sessions:
            debugger_actor_id = self._pending_debugger_sessions.popleft()
            rank = debugger_actor_id.rank
            proc_id = debugger_actor_id.proc_id
            debugger_write(
                f"pdb attached to proc {proc_id} with rank {rank}, debugger actor {debugger_actor_id} \n"
            )

            self._actor.send(
                debugger_actor_id,
                debugger.DebuggerMessage(action=DebuggerAction.Attach()).serialize(),
            )

            while True:
                # TODO: Add appropriate timeout.
                msg = self._actor.get_next_message(timeout_msec=None)

                if not isinstance(msg, client.DebuggerMessage):
                    self._non_debugger_pending_messages.append(msg)
                    continue

                if msg.debugger_actor_id != debugger_actor_id:
                    if isinstance(msg.action, DebuggerAction.Paused):
                        self._pending_debugger_sessions.append(msg.debugger_actor_id)
                        continue
                    else:
                        raise RuntimeError(
                            f"unexpected debugger message {msg} from rank {msg.debugger_actor_id.rank} "
                            f"when debugging rank {debugger_actor_id.rank}"
                        )

                action = msg.action
                if isinstance(action, DebuggerAction.Detach):
                    break
                elif isinstance(action, DebuggerAction.Read):
                    self._actor.send(
                        debugger_actor_id,
                        debugger.DebuggerMessage(
                            action=DebuggerAction.Write(
                                bytes=debugger_read(action.requested_size)
                            )
                        ).serialize(),
                    )
                elif isinstance(action, DebuggerAction.Write):
                    debugger_write(
                        debugger.get_bytes_from_write_action(action).decode()
                    )
                else:
                    raise RuntimeError(
                        f"unexpected debugger message {msg} when debugging rank {debugger_actor_id.rank}"
                    )


# TODO: Handling conversion of the response can move to a separate module over time
# especially as we have structured error messages.
def _worker_response_to_result(result: client.WorkerResponse) -> MessageResult:
    if not result.is_exception():
        # The result of the message needs to be unwrapped on a real device.
        # Staying as a fake tensor will fail the tensor deserialization.
        with no_mesh.activate():
            return MessageResult(result.seq, result.result(), None)
    exc = none_throws(result.exception())
    if isinstance(exc, client.Error):
        worker_frames = [
            traceback.FrameSummary("<unknown>", None, frame)
            for frame in exc.backtrace.split("\\n")
        ]
        logger.error(f"Worker {exc.actor_id} failed")
        return MessageResult(
            seq=result.seq,
            result=None,
            error=RemoteException(
                seq=exc.caused_by_seq,
                exception=RuntimeError(exc.backtrace),
                controller_frame_index=0,  # TODO: fix this once we have recording support in rust
                controller_frames=None,
                worker_frames=worker_frames,
                source_actor_id=exc.actor_id,
                message=f"Worker {exc.actor_id} failed",
            ),
        )
    elif isinstance(exc, client.Failure):
        frames = [
            traceback.FrameSummary("<unknown>", None, frame)
            for frame in exc.backtrace.split("\n")
        ]
        reason = f"Actor {exc.actor_id} crashed on {exc.address}, check the host log for details"
        logger.error(reason)
        return MessageResult(
            seq=0,  # seq is not consumed for DeviceException; it will be directly thrown by the client
            result=None,
            error=DeviceException(
                exception=RuntimeError(reason),
                frames=frames,
                source_actor_id=exc.actor_id,
                message=reason,
            ),
        )
    else:
        raise RuntimeError(f"Unknown exception type: {type(exc)}")
