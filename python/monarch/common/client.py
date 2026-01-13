# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import atexit
import difflib
import itertools
import logging
import math
import time
import traceback
import weakref
from collections import defaultdict
from typing import (
    Callable,
    cast,
    Dict,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    TYPE_CHECKING,
    Union,
)
from weakref import WeakKeyDictionary

import torch
import torch.distributed
from monarch._rust_bindings.monarch_extension import tensor_worker
from monarch._rust_bindings.monarch_extension.client import (  # @manual=//monarch/monarch_extension:monarch_extension
    LogLevel,
)
from monarch._src.actor.shape import NDSlice
from monarch.common import messages
from monarch.common.borrows import Borrow, StorageAliases
from monarch.common.controller_api import LogMessage, MessageResult, TController
from monarch.common.device_mesh import DeviceMesh
from monarch.common.future import Future
from monarch.common.invocation import DeviceException, RemoteException, Seq
from monarch.common.recording import flatten_messages, Recording
from monarch.common.reference import Ref, Referenceable
from monarch.common.stream import StreamRef
from monarch.common.tensor import Tensor
from monarch.common.tree import tree_map

from . import _coalescing


logger = logging.getLogger(__name__)

_CONTROLLER_STATUS_INTERVAL = 2


def TTL(timeout: Optional[float]) -> Callable[[], float]:
    if timeout is None:
        return lambda: math.inf
    expiry = time.time() + timeout
    return lambda: max(expiry - time.time(), 0)


class Client:
    def __init__(
        self,
        controller: TController,
        world_size: int,
        gpu_per_host: int,
    ):
        self.inner = controller
        self._world_size = world_size
        self._gpu_per_host = gpu_per_host
        self.next_ref = itertools.count()
        self.failures: Dict[int, Dict[int, RemoteException]] = defaultdict(dict)
        self._pending_del: Dict[DeviceMesh, List[int]] = defaultdict(list)
        self._shutdown = False
        self.controller_status_ttl = TTL(_CONTROLLER_STATUS_INTERVAL)
        self._aliases: WeakKeyDictionary[torch.UntypedStorage, StorageAliases] = (
            WeakKeyDictionary()
        )

        # stream._active = Stream("main2", _default=True)

        self._backend_network_init = False
        self._backend_network_init_point_to_point: Set[
            Tuple["StreamRef", "StreamRef"]
        ] = set()

        self.seq_gen = itertools.count()
        # seq of the most recent message that was sent to controller
        self.last_assigned_seq = -1
        # seq of the last acked message from controller, ack message is initiated
        # by the _request_status() call. By comparing last_processed_seq and
        # last_assigned_seq, we can tell if all messages are processed by all
        # workers.
        self.last_processed_seq = -1

        # an error that we have received but know for certain has not
        # been propagated to a future. This will be reported on shutdown
        # to avoid hiding the error. This is best effort: we only keep
        # the error until the point the a future is dependent on
        # _any_ error, not particularly the tracked one.
        self._pending_shutdown_error = None

        self.recorder = Recorder()

        self.pending_results: Dict[
            Seq,  # seq of an invocation
            Tuple[
                Optional["Future"],  # future to set
                List[List[traceback.FrameSummary]],  # local call stacks
            ],
        ] = {}
        atexit.register(self._atexit)
        self.created_communicators = set()

    def send(
        self,
        ranks: Union[NDSlice, List[NDSlice]],
        msg: NamedTuple,
    ) -> None:
        if not _coalescing.is_active(self):
            return self.send_nocoalesce(ranks, msg)
        if _coalescing.is_recording(self):
            match msg:
                case messages.BorrowFirstUse() if (
                    msg.borrow not in self.recorder.borrow_entries_created
                ):
                    return self.send_nocoalesce(ranks, msg)
                case messages.BorrowLastUse() if (
                    msg.borrow not in self.recorder.borrow_entries_created
                ):
                    raise ValueError(
                        "cannot explicitly drop a tensor inside a compiled block that was borrowed outside of it."
                    )
        self.recorder.add_message(ranks, msg)

    def send_nocoalesce(
        self,
        ranks: Union[NDSlice, List[NDSlice]],
        msg: NamedTuple,
    ) -> None:
        self.inner.send(ranks, msg)

    def reset_recorder(self) -> "Recorder":
        old, self.recorder = self.recorder, Recorder()
        return old

    def drop_borrow(self, borrow: "Borrow") -> None:
        if not _coalescing.is_active(self):
            return
        if borrow._id not in self.recorder.borrow_entries_created:
            tb = borrow.traceback_string
            raise RuntimeError(
                f"Borrow Traceback:\n{tb}Cannot drop a borrow while repeating a coalesced block because it would cause the borrow to drop multiple times. "
            )
        del self.recorder.borrow_entries_created[borrow._id]

    def new_borrow(self, borrow_entry: "Borrow") -> None:
        if not _coalescing.is_active(self):
            return
        self.recorder.borrow_entries_created[borrow_entry._id] = borrow_entry

    @property
    def all_ranks(self) -> NDSlice:
        return NDSlice(offset=0, sizes=[self._world_size], strides=[1])

    @property
    def gpu_per_host(self) -> int:
        return self._gpu_per_host

    # shut down everything, including client/system/controller/workers.
    # the shutdown procedure will wait for all messages to be processed
    # by the worker, then stop the system.
    def shutdown(
        self,
        destroy_pg: bool = True,
        error_reason: Optional[RemoteException | DeviceException | Exception] = None,
    ) -> None:
        if self.has_shutdown:
            return
        logger.info("shutting down the client gracefully")

        atexit.unregister(self._atexit)
        self._shutdown = True

        # request status for the last sent seq, and wait for the result to make sure all
        # seqs are processed.
        if self.last_assigned_seq > self.last_processed_seq:
            self._request_status()

        # send Exit message to stop the workers, wait for a bit for the workers to Exit
        # with the correct exit code before we stop the system.
        self.send(self.all_ranks, messages.Exit(destroy_pg, error_reason))
        time.sleep(2)

        # put a overall timeout on the shutdown waiting for now, better shutdown for
        # multi-mesh setup will be implemented later.
        timeout = 60
        start_time = time.time()

        try:
            while (
                time.time() - start_time < timeout
                and self.last_assigned_seq > self.last_processed_seq
            ):
                # TODO(T216336422): retire client::drain_and_stop() as it doesn't
                # really drain all messages
                output = self.inner.next_message(1.0)
                if output is not None:
                    if isinstance(output, MessageResult):
                        # restart the timer as we got new result back
                        start_time = time.time()
                        self._handle_pending_result(output)
                    elif isinstance(output, LogMessage):
                        self._log_message(output)

            # Drain any remaining message in client queue (if any)
            for output in self.inner.drain_and_stop():
                if isinstance(output, MessageResult):
                    self._handle_pending_result(output)
                elif isinstance(output, LogMessage):
                    self._log_message(output)
        except DeviceException:
            # exception in message draining should be ignored during shutdown, as
            # we are shutting down the system anyway
            logger.warning(
                "exception in message draining during shutdown, "
                "ignoring and continue to stop the system"
            )
            pass

        # all messages are processed, we can now stop the system
        if time.time() - start_time >= timeout:
            logger.warning(
                "timeout waiting for all messages to be processed, stop the mesh anyway"
            )
        else:
            logger.info("all messages are processed, stop the mesh")
        self.inner.stop_mesh()

    @property
    def has_shutdown(self) -> bool:
        return self._shutdown

    def new_ref(self) -> int:
        r = next(self.next_ref)
        if _coalescing.is_active(self):
            self.recorder.first_ref = min(self.recorder.first_ref, r)
        return r

    def handle_deletes(
        self,
        ranks: Union[NDSlice, List[NDSlice]],
        refs: List[int],
        coalesce: bool = True,
    ):
        if coalesce:
            self.send(ranks, messages.DeleteRefs(refs))
        else:
            self.send_nocoalesce(ranks, messages.DeleteRefs(refs))
        self.inner.drop_refs([tensor_worker.Ref(id=ref) for ref in refs])

    def flush_deletes(self, coalesce: bool = True):
        for mesh, refs in self._pending_del.items():
            self.handle_deletes(mesh.processes, refs, coalesce)
        self._pending_del.clear()

    def delete_ref(self, device_mesh: DeviceMesh, ref: int) -> None:
        self._pending_del[device_mesh].append(ref)

    @property
    def aliases(self) -> WeakKeyDictionary[torch.UntypedStorage, StorageAliases]:
        return self._aliases

    def _request_status(self):
        self.send(
            self.all_ranks,
            messages.RequestStatus(self.last_assigned_seq, False),
        )

    def handle_next_message(self, timeout: Optional[float]) -> bool:
        output = self.inner.next_message(timeout)
        if output is not None:
            if isinstance(output, MessageResult):
                self._handle_pending_result(output)
            elif isinstance(output, LogMessage):
                self._log_message(output)
            return True
        return False

    def _log_message(self, msg: LogMessage) -> None:
        match msg.level:
            case LogLevel.INFO:
                logger.info(msg.message)
            case LogLevel.WARNING:
                logger.warning(msg.message)
            case LogLevel.ERROR:
                logger.error(msg.message)

    def _handle_pending_result(self, output: MessageResult) -> None:
        result = output.result
        seq = output.seq
        error = output.error

        self.last_processed_seq = max(self.last_processed_seq, seq)

        if error is not None:
            logging.info("Received error for seq %s: %s", seq, error)
            self._pending_shutdown_error = error
            # We should not have set result if we have an error.
            assert result is None
            if not isinstance(error, RemoteException):
                raise error

            # Populate controller tracebacks for the remote failure
            original_frame_seq = error.seq
            index = error.controller_frame_index
            assert index is not None
            # TODO: Populate tracebacks for dependent invocations
            if original_frame_seq == seq:
                # The current invocation is the one causing the remote failure.
                # We should have not populated the tracebacks yet.
                assert error.controller_frames is None
                _, tracebacks = self.pending_results[original_frame_seq]
                assert tracebacks is not None
                assert len(tracebacks) > index, (
                    f"tracebacks contains {len(tracebacks)} frames, but index is {index}"
                )
                error.controller_frames = tracebacks[index]

        fut, _ = self.pending_results[seq]
        if fut is not None:
            if error is None:
                fut._set_result(result)
            else:
                fut._set_result(error)
                self._pending_shutdown_error = None
        elif result is not None:
            logger.debug(f"{seq}: unused result {result}")
        elif error is not None:
            # errors get reported as results even if they
            # do not have futures attached.
            pass

        # We can safely delete the seq as tracebacks have been saved to the remote failure itself.
        del self.pending_results[seq]

    def split_comm(self, dims, device_mesh, stream_ref) -> None:
        """Create a split communicator group with the specified ranks, and
        associate it with a specific device mesh and stream.
        """
        # For simplicity, just send this message to all ranks and split from the
        # global communicator. As an optimization, the client could remember
        # which comms have already been created and issue a message to a smaller
        # set of ranks.
        if not self._backend_network_init:
            raise AssertionError(
                "split_comm called before backend network initialization"
            )

        msg = messages.SplitComm(tuple(sorted(dims)), device_mesh, stream_ref)
        if msg in self.created_communicators:
            return

        self.send_nocoalesce(self.all_ranks, msg)
        self.created_communicators.add(msg)

    def backend_network_init(self) -> None:
        if self._backend_network_init:
            return
        self._backend_network_init = True
        logger.info("Initializing backend network")
        self.send_nocoalesce(self.all_ranks, messages.BackendNetworkInit())

    def backend_network_point_to_point_init(
        self, from_stream_ref: "StreamRef", to_stream_ref: "StreamRef"
    ) -> None:
        key = (from_stream_ref, to_stream_ref)

        if key in self._backend_network_init_point_to_point:
            return
        self._backend_network_init_point_to_point.add(key)
        self.send_nocoalesce(
            self.all_ranks,
            messages.BackendNetworkPointToPointInit(from_stream_ref, to_stream_ref),
        )

    def new_node(
        self,
        defs: Sequence["Tensor"],
        uses: Sequence["Tensor"],
        future: Optional["Future"] = None,
        tracebacks: Optional[List[List[traceback.FrameSummary]]] = None,
    ) -> Seq:
        for t in uses:
            t._use()

        if tracebacks is None:
            tracebacks = [traceback.extract_stack()[:-2]]
        if _coalescing.is_recording(self):
            assert future is None, "this should have been checked in fetch shard"
            return self.recorder.add(defs, uses, tracebacks[0])
        else:
            return self.new_node_nocoalesce(defs, uses, future, tracebacks)

    def new_node_nocoalesce(
        self,
        defs: Sequence["Tensor"],
        uses: Sequence["Tensor"],
        future: Optional["Future"],
        tracebacks: List[List[traceback.FrameSummary]],
    ) -> Seq:
        seq = self._next_seq()
        self.pending_results[seq] = (future, tracebacks)
        for d in defs:
            d._seq = seq
        self.inner.node(seq, defs, uses)
        return seq

    def _next_seq(self) -> Seq:
        self.last_assigned_seq = next(self.seq_gen)
        return self.last_assigned_seq

    def _atexit(self) -> None:
        logger.warning(
            "Client is not shutting down properly before atexit. "
            "This may be due to an exception or because device_mesh.exit() "
            "was not called."
        )
        # Calling self.shutdown may cause a deadlock if something is wrong with
        # the networking. Or should we make shutdown() not wait indefinitely?
        self._shutdown = True

        # send shutdown message to stop other processes.
        self.inner.stop_mesh()

    def no_coalescing(self, reason):
        if _coalescing.is_active(self):
            raise NotImplementedError(f"NYI: {reason} during a coalescing block")

    def fetch(
        self,
        mesh: "DeviceMesh",
        stream: "StreamRef",
        shard,
        preprocess_message,
        args,
        kwargs,
        defs: Tuple["Tensor", ...],
        uses: Tuple["Tensor", ...],
    ) -> "Future":
        fut = Future(self)
        ident = self.new_node(defs, uses, fut)
        process = mesh._process(shard)
        self.send(
            process,
            messages.SendValue(
                ident,
                None,
                defs,
                preprocess_message,
                args,
                kwargs,
                stream,
            ),
        )
        # we have to ask for status updates
        # from workers to be sure they have finished
        # enough work to count this future as finished,
        # and all potential errors have been reported
        self._request_status()
        return fut


def tree_map_refs(first_ref: int, tree):
    def translate_id(ref: int) -> int:
        diff = ref - first_ref
        if diff >= 0:
            return -1 - diff
        return ref

    def translate_ref(obj):
        match obj:
            case Ref():
                return translate_id(obj.id)
            case Referenceable():
                return None if obj.ref is None else translate_id(obj.ref)
            case messages.DeleteRefs():
                # Python destructors may not run in a deterministic order across
                # traces of a recorded function, so we need to sort the refs to ensure
                # a fair comparison during validation.
                return messages.DeleteRefs(sorted([translate_id(r) for r in obj.refs]))
            case messages.BorrowCreate():
                result, borrow, *rest = [translate_ref(x) for x in obj]
                return messages.BorrowCreate(result, translate_id(borrow), *rest)
            case messages.BorrowDrop():
                return messages.BorrowDrop(translate_id(obj.borrow))
            case messages.BorrowFirstUse():
                return messages.BorrowFirstUse(translate_id(obj.borrow))
            case messages.BorrowLastUse():
                return messages.BorrowLastUse(translate_id(obj.borrow))
            case _:
                return obj

    return tree_map(
        translate_ref,
        tree,
        is_leaf=lambda x: isinstance(
            x,
            (
                Ref,
                Referenceable,
                messages.DeleteRefs,
                messages.BorrowCreate,
                messages.BorrowDrop,
                messages.BorrowFirstUse,
                messages.BorrowLastUse,
            ),
        ),
    )


class Recorder:
    def __init__(self):
        self.borrow_entries_created: Dict[int, Borrow] = {}
        self.messages: List[Union[NDSlice, List[NDSlice]], NamedTuple] = []
        # these tables track the externally captured tensors that we
        # use and mutate whenever this recording is run.
        self.uses = {}  # ordered set
        self.mutates = {}  # ordered set
        self.creates: List[weakref.ref] = []
        self.tracebacks = []
        self.first_ref: int = math.inf
        self.reference_recording: Optional["Recording"] = None
        # Map from formal tensor storage to its corresponding argument indices
        # in the recording input (there may be multiple aliases of the same
        # tensor in the recording input).
        self.formal_storages_to_indices: defaultdict[
            torch.UntypedStorage, List[int]
        ] = defaultdict(list)
        # Set of tensor storages for formals that are mutated during the recording.
        self.mutated_formal_storages: Set[torch.UntypedStorage] = set()

    def add_formal(self, formal: Tensor, argument_index: int) -> None:
        self.formal_storages_to_indices[formal._fake.untyped_storage()].append(
            argument_index
        )

    def add(
        self,
        defs: Sequence["Tensor"],
        uses: Sequence["Tensor"],
        traceback: List[traceback.FrameSummary],
    ):
        for u in uses:
            if u._seq is None:
                # a lack of sequence num on a tensor means it was created within
                # the recording and does not have to be tracked as a use
                continue
            self.uses[u] = None
        for d in defs:
            # a lack of sequence num means the tensor doesn't need to be tracked
            # as a mutates, unless that tensor is an alias of a formal tensor
            if d._seq is None:
                self.creates.append(weakref.ref(d))
                storage = d._fake.untyped_storage()
                if storage in self.formal_storages_to_indices:
                    self.mutated_formal_storages.add(storage)
            else:
                self.mutates[d] = None
        self.tracebacks.append(traceback)
        return len(self.tracebacks) - 1

    def _check(self):
        if self.borrow_entries_created:
            tbs = "------------\n".join(
                b.traceback_string for b in self.borrow_entries_created.values()
            )
            raise RuntimeError(
                f"Borrows created during recorded coalesced block need to be dropped before the block ends. Tracebacks of where the blocks were created: {tbs}"
            )

    @property
    def flat_messages(self):
        return flatten_messages(self.messages)

    def run_once(self, client: "Client"):
        self._check()
        for rank, msgs in self.flat_messages.items():
            client.send_nocoalesce(
                NDSlice(offset=rank, sizes=[], strides=[]), messages.CommandGroup(msgs)
            )

    def abandon(self):
        # an error happened and we will not use this recording. Every tensor created
        # as part of this recording has never been defined, so we blank out the
        # .ref to disarm the deletions.
        for w in self.creates:
            v = w()
            if v is not None:
                v.ref = None

    def add_message(self, ranks: Union[NDSlice, List[NDSlice]], msg: NamedTuple):
        if isinstance(msg, messages.RecordingFormal):
            self.add_formal(cast(Tensor, msg.result), msg.argument_index)

        # this is pretty expensive, but we can't hold tensor references without
        # extending their lifetime unnecessarily, so they must be converted to
        # references here. It also prevents a bug when a tensor is dropped,
        # after a message is recorded and will no longer have a ref field.
        msg = tree_map(
            lambda x: (
                Ref(x.ref) if isinstance(x, Tensor) and x.ref is not None else x
            ),
            msg,
        )
        self.messages.append((ranks, msg))
        reference_recording = self.reference_recording
        if reference_recording is not None:
            last_index = len(self.messages) - 1
            reference_messages = reference_recording.buffered_messages
            mine = self.messages[last_index]
            theirs = (
                reference_messages[last_index]
                if len(reference_messages) > last_index
                else None
            )
            mine = tree_map_refs(self.first_ref, mine)
            theirs = tree_map_refs(reference_recording.first_ref, theirs)
            if mine != theirs:
                traceback_index = len(self.tracebacks) - 1

                tb_mine = traceback.format_list(self.tracebacks[traceback_index])
                while tb_mine and "in _record_and_define" not in tb_mine[0]:
                    tb_mine.pop(0)

                tb_theirs = traceback.format_list(
                    reference_recording.tracebacks[traceback_index]
                )
                while tb_theirs and "in _record_and_define" not in tb_theirs[0]:
                    tb_theirs.pop(0)

                the_diff = "\n".join(difflib.ndiff([str(theirs)], [str(mine)]))
                raise RuntimeError(
                    f"monarch.compiled failed to verify recording. Recording diverges at operation {last_index}.\n{the_diff}\n\nTraceback of original recording\n{''.join(tb_theirs)}\n\nTraceback of second recording\n{''.join(tb_mine)}\n"
                )

    def verify_against(self, reference: Recording):
        self.reference_recording = reference

    def define_recording(
        self,
        client: "Client",
        nresults: int,
        nformals: int,
    ) -> Recording:
        self._check()
        # any remaining references to tensors we defined in the recording are
        # not valid for future use outside the recording, so drop them
        # such that we report an error if they are used.
        for w in self.creates:
            v = w()
            if v is not None:
                v._drop_ref()
        # It should be safe to use a list instead of a set here, since
        # no entry in formal_storages_to_indices should have any overlap
        # with any other entry. So mutated_formal_indices should automatically
        # have unique elements.
        mutated_formal_indices = []
        for storage in self.mutated_formal_storages:
            mutated_formal_indices.extend(self.formal_storages_to_indices[storage])
        return Recording(
            client,
            list(self.uses.keys()),
            list(self.mutates.keys()),
            sorted(mutated_formal_indices),
            self.tracebacks,
            self.messages,
            nresults,
            nformals,
            self.first_ref,
        )
